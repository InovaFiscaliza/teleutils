#!/usr/bin/env bash

# =============================================================================
# Descrição:
#   Envia para um armazenamento MinIO (via cliente "mc") os arquivos de CDR de
#   uma semana específica, organizados em duas categorias de origem: "text" e
#   "teleparser". Os diretórios de origem, os padrões de arquivo e os destinos
#   são definidos em um arquivo de configuração JSON.
#
# Uso:
#   upload_cdr_to_minio.sh --config ARQUIVO --week-dir SemanaNUMERO [OPÇÕES]
#
# Argumentos obrigatórios:
#   --config ARQUIVO       Arquivo JSON de configuração (um único objeto).
#   --week-dir NOME        Nome da semana, no formato "Semana" + dígitos.
#
# Opções:
#   --teleparser-folder CAMINHO  Substitui o diretório BASE da categoria teleparser.
#   --text-folder CAMINHO        Substitui o diretório BASE da categoria text.
#   --destination CAMINHO        Substitui bucket/prefixo de destino (sem alias do mc).
#   --dry-run                    Planeja e consulta o MinIO, sem copiar arquivos.
#   --help, -h                   Exibe a ajuda e encerra com código 0.
#   Cada opção com valor pode ser informada uma única vez e exige valor
#   não vazio que não comece com "--".
#
# Seleção de categorias:
#   Sem --teleparser-folder e sem --text-folder: processa text e teleparser.
#   Somente --teleparser-folder: processa apenas teleparser.
#   Somente --text-folder: processa apenas text.
#   Ambas: processa as duas, usando os dois caminhos informados.
#   Os campos de configuração de uma categoria não selecionada não são lidos.
#
# Configuração JSON (campos consumidos):
#   teleparser_folder, text_folder   Diretórios BASE de origem; o script acrescenta
#                                    --week-dir a cada base. Ignorados quando a
#                                    opção de linha de comando correspondente
#                                    é informada.
#   minio_config.alias               Alias do mc (letra inicial; letras, dígitos,
#                                    "_" e "-").
#   minio_config.batchsize           Inteiro de 1 a 2147483647: máximo de arquivos
#                                    por chamada "mc cp" em cada destino.
#   minio_config.destination         Bucket/prefixo; ignorado com --destination.
#   folder_mapping.<categoria>[]     Lista não vazia de objetos com
#                                    source_folder, destination_folder e pattern.
#
# Variáveis de ambiente:
#   MC_CONFIG_DIR  Diretório de configuração do mc. Se ausente, usa $HOME/.mc.
#   LC_ALL         Definida como C pelo próprio script (export).
#
# Dependências:
#   Bash 4 ou superior, jq, find, sort, realpath, stat e mc (MinIO Client).
#   O script usa opções como "realpath -e -z", "stat -L -c" e "sort -z"; a
#   compatibilidade depende de as ferramentas instaladas aceitá-las.
#   O mc precisa estar previamente inicializado (config.json com version "10",
#   diretórios share, certs e certs/CAs, e o alias configurado com URL http/https).
#   O script não cria nem migra o estado do mc.
#
# Fluxo de execução:
#   parse_arguments -> require_dependencies -> load_configuration
#     -> discover_files -> validate_mc -> process_destinations -> status final
#   1. Valida argumentos e define as categorias selecionadas.
#   2. Lê e valida o JSON, o destino, as bases de origem e os mapeamentos.
#   3. Expande cada mapeamento (source_folder aceita curingas por componente) e
#      seleciona arquivos pelo pattern, sem recursão além dos componentes
#      informados. Colisões de destino abortam a execução antes de qualquer cópia.
#   4. Valida o estado do mc e o acesso ao bucket (também no --dry-run).
#   5. Para cada diretório de destino, lista os objetos remotos, ignora os que já
#      existem com o mesmo tamanho, copia o restante em lotes e confirma cada
#      arquivo pela listagem remota.
#
# Regras principais:
#   - Alvo remoto: alias/bucket/prefixo/SEMANA/destination_folder/arquivo.
#   - Todo caminho selecionado deve permanecer, após resolução de links
#     simbólicos, dentro do diretório semanal da categoria.
#   - Objetos remotos existentes nunca são sobrescritos; tamanho diferente é erro.
#   - Na categoria teleparser, todo diretório de segundo nível abaixo da semana
#     deve corresponder a algum source_folder mapeado; caso contrário a categoria
#     é bloqueada e seus mapeamentos ficam pendentes.
#   - Mapeamento ausente ou sem arquivos correspondentes conta como pendente.
#   - Os arquivos de origem não são modificados.
#
# Entradas e saídas:
#   stdout  Linhas "PLAN origem -> destino" (--dry-run), ajuda (--help) e o
#           relatório final ("FINAL REPORT").
#   stderr  Mensagens de log no formato "[NÍVEL] mensagem".
#   Efeito colateral: objetos gravados no MinIO (exceto em --dry-run).
#
# Status final (campo "status" do relatório):
#   SUCCESS, PARTIAL_SUCCESS, FAILED e DRY_RUN.
#
# Códigos de saída:
#   0    Nenhuma falha e nenhum mapeamento pendente (inclui simulação sem falhas),
#        ou --help.
#   1    Falhas operacionais, colisões de destino ou mapeamentos pendentes.
#   2    Uso, configuração ou ambiente inválidos.
#   130  Interrompido por SIGINT.
#   143  Interrompido por SIGTERM.
#
# Exemplo:
#   upload_cdr_to_minio.sh --config config.json --week-dir Semana110 \
#     --teleparser-folder /data/teleparser --dry-run
# =============================================================================

# A semana precede o mapeamento para conservar as chaves dos scripts legados.
# Não usamos errexit: falhas recuperáveis devem permitir os próximos lotes.
if (( BASH_VERSINFO[0] < 4 )); then
    printf 'ERROR: Bash 4 or newer is required.\n' >&2
    exit 2
fi
set -o pipefail
shopt -u nocasematch
export LC_ALL=C

# Estado global compartilhado pelas funções.
# - sources, identities, sizes, targets e target_directories são arrays
#   paralelos: o mesmo índice descreve um único arquivo selecionado.
# - mapping_categories, mapping_sources, mapping_patterns e mapping_destinations
#   são arrays paralelos: um índice por mapeamento válido e único.
# - records, expanded, canonical, normalized e mc_output funcionam como
#   "valores de retorno" de collect_zero, expand_source, resolve_path,
#   normalize_path e capture_mc, e são sobrescritos a cada chamada.
config_file="" week_dir="" destination_override=""
teleparser_override="" text_override=""
teleparser_explicit=0 text_explicit=0 destination_explicit=0 dry_run=0
minio_alias="" destination="" destination_base="" mc_config_dir=""
batchsize=0 started_at=$SECONDS report_enabled=0 final_status=""
files_found=0 files_selected=0 files_existing=0 files_planned=0
files_transferred=0 failures=0 pending_mappings=0 collisions=0
batches_planned=0 batches_processed=0
declare -a categories=() records=() expanded=()
declare -a mapping_categories=() mapping_sources=() mapping_patterns=() mapping_destinations=()
declare -a sources=() identities=() sizes=() targets=() target_directories=() destinations=()
declare -A roots=() blocked=() option_seen=() mapping_seen=()
declare -A found_seen=() target_index=() destination_seen=() remote_objects=()
canonical="" normalized="" mc_output=""

# -----------------------------------------------------------------------------
# Funções de log e encerramento
#   log NÍVEL MENSAGEM     Escreve "[NÍVEL] MENSAGEM" em stderr.
#   quote VALOR            Escreve VALOR escapado por printf %q em stdout; usado
#                          para exibir caminhos e nomes sem ambiguidade.
#   error MENSAGEM         Incrementa o contador failures e registra ERROR.
#                          Não encerra o script.
#   fatal CÓDIGO MENSAGEM  Registra o erro com error, define final_status=FAILED
#                          e encerra com CÓDIGO (o trap EXIT emite o relatório).
# -----------------------------------------------------------------------------
log() { printf '[%s] %s\n' "$1" "$2" >&2; }
quote() { printf '%q' "$1"; }
error() { failures=$((failures + 1)); log ERROR "$1"; }
fatal() { error "$2"; final_status=FAILED; exit "$1"; }

# -----------------------------------------------------------------------------
# Função: usage
#
# Descrição:
#   Escreve o texto de ajuda em stdout (argument_error o redireciona para
#   stderr). Não altera estado.
# -----------------------------------------------------------------------------
usage() {
    printf '%s\n' 'Usage: upload_cdr_to_minio.sh --config FILE --week-dir SemanaNUMBER [OPTIONS]

Required:
  --config FILE               JSON configuration file.
  --week-dir NAME             Week name: Semana followed by digits.

Options:
  --teleparser-folder PATH    Override the Teleparser BASE directory.
  --text-folder PATH          Override the text BASE directory.
  --destination PATH          Override the bucket/prefix, WITHOUT the mc alias.
  --dry-run                   Plan and query MinIO, without copying or writing.
  --help, -h                  Show this help and exit successfully.

Source selection is based on explicitly supplied options:
  Neither source option: process both categories from JSON.
  Only --teleparser-folder: process ONLY Teleparser.
  Only --text-folder: process ONLY text.
  Both source options: process both, using the two CLI paths.

Source paths are BASE directories, not complete weekly directories.
The script appends --week-dir to each selected base directory.
Targets: alias/bucket/prefix/week/mapped-destination/filename.
--dry-run requires an existing mc configuration and read access to MinIO.

Example:
  upload_cdr_to_minio.sh --config config.json --week-dir Semana110 \
    --teleparser-folder /data/teleparser --dry-run

Exit codes: 0 success/simulation; 1 operational errors or pending mappings;
            2 invalid usage/configuration/environment; 130/143 interrupted.'
}

# -----------------------------------------------------------------------------
# Função: argument_error
#
# Descrição:
#   Trata erro de uso: desabilita o relatório final, registra o erro, escreve a
#   ajuda em stderr e encerra o script com código 2.
#
# Argumentos:
#   $1 - Mensagem de erro.
# -----------------------------------------------------------------------------
argument_error() {
    report_enabled=0
    error "$1"
    usage >&2
    exit 2
}

# -----------------------------------------------------------------------------
# Função: report
#
# Descrição:
#   Escreve em stdout o "FINAL REPORT": semana, categorias, origem de cada
#   categoria, base de destino, contadores de arquivos, falhas, mapeamentos
#   pendentes, lotes, duração em segundos e status final. Valores não
#   resolvidos aparecem como UNRESOLVED.
#
# Variáveis globais lidas:
#   week_dir, categories, roots, destination_base, contadores e final_status.
# -----------------------------------------------------------------------------
# O relatório é chamado indiretamente pelo trap EXIT.
# shellcheck disable=SC2317
report() {
    local category
    printf '\nFINAL REPORT\n'
    printf 'week: %s\n' "$(quote "${week_dir:-UNRESOLVED}")"
    printf 'categories:'
    for category in "${categories[@]}"; do printf ' %s' "$category"; done
    printf '\n'
    for category in "${categories[@]}"; do
        printf 'source_%s: %s\n' "$category" "$(quote "${roots[$category]:-UNRESOLVED}")"
    done
    printf 'destination_base: %s\n' "$(quote "${destination_base:-UNRESOLVED}")"
    printf 'files_found: %s\nfiles_selected: %s\n' "$files_found" "$files_selected"
    printf 'files_existing: %s\nfiles_planned: %s\n' "$files_existing" "$files_planned"
    printf 'files_transferred: %s\nfailures: %s\n' "$files_transferred" "$failures"
    printf 'pending_mappings: %s\n' "$pending_mappings"
    printf 'batches_planned: %s\nbatches_processed: %s\n' "$batches_planned" "$batches_processed"
    printf 'duration_seconds: %s\nstatus: %s\n' "$((SECONDS - started_at))" "$final_status"
}

# -----------------------------------------------------------------------------
# Função: on_exit
#
# Descrição:
#   Handler do trap EXIT. Quando report_enabled é 1, garante um status final
#   (se nenhum foi definido, assume FAILED e registra erro de encerramento
#   inesperado) e emite o relatório. Não altera o código de saída do script.
# -----------------------------------------------------------------------------
# shellcheck disable=SC2317
on_exit() {
    local code=$?
    if (( report_enabled )); then
        if [[ -z $final_status ]]; then
            final_status=FAILED
            log ERROR "Execution ended unexpectedly (exit $code)."
        fi
        report
    fi
}
# EXIT emite o relatório final. INT e TERM marcam o status como FAILED, contam
# o erro e encerram com 130 e 143, respectivamente; o trap EXIT ainda é executado.
trap on_exit EXIT
trap 'final_status=FAILED; error "Interrupted by SIGINT."; exit 130' INT
trap 'final_status=FAILED; error "Interrupted by SIGTERM."; exit 143' TERM

# -----------------------------------------------------------------------------
# Função: parse_arguments
#
# Descrição:
#   Interpreta a linha de comando e preenche config_file, week_dir, os
#   overrides (e suas flags *_explicit), dry_run e a lista categories.
#
# Argumentos:
#   "$@" - Opções de linha de comando (ver cabeçalho do script).
#
# Regras:
#   - Sem argumentos, ou sem --config/--week-dir, é erro de uso.
#   - --week-dir deve casar com ^Semana[0-9]+$.
#   - Opções com valor exigem valor não vazio que não comece com "--" e não
#     podem se repetir.
#   - --help/-h imprime a ajuda em stdout e encerra com 0, sem relatório.
#   - Categorias: text entra se --text-folder foi informado ou se nenhuma
#     opção de origem foi; teleparser, de forma análoga (ordem: text, teleparser).
#   - Só no final, com argumentos válidos, o relatório é habilitado
#     (report_enabled=1); erros de uso não geram relatório.
#
# Retorno:
#   Erros de uso encerram o script com 2 (via argument_error).
# -----------------------------------------------------------------------------
parse_arguments() {
    local option
    if (( $# == 0 )); then
        argument_error "Both --config and --week-dir are required."
    fi
    while (( $# )); do
        option=$1
        case "$option" in
            --help|-h) report_enabled=0; usage; exit 0 ;;
            --dry-run) dry_run=1; shift; continue ;;
            --config|--week-dir|--teleparser-folder|--text-folder|--destination)
                if (( $# < 2 )) || [[ -z $2 || $2 == --* ]]; then
                    argument_error "Missing value for $option."
                fi
                [[ -z ${option_seen[$option]+present} ]] ||
                    argument_error "Repeated option: $option."
                option_seen["$option"]=1
                case "$option" in
                    --config) config_file=$2 ;;
                    --week-dir) week_dir=$2 ;;
                    --teleparser-folder) teleparser_override=$2; teleparser_explicit=1 ;;
                    --text-folder) text_override=$2; text_explicit=1 ;;
                    --destination) destination_override=$2; destination_explicit=1 ;;
                esac
                shift 2
                ;;
            *) argument_error "Unknown argument: $(quote "$option")." ;;
        esac
    done
    [[ -n $config_file && -n $week_dir ]] ||
        argument_error "Both --config and --week-dir are required."
    [[ $week_dir =~ ^Semana[0-9]+$ ]] || argument_error "Invalid week name: $(quote "$week_dir")."
    if (( text_explicit || ! teleparser_explicit )); then categories+=(text); fi
    if (( teleparser_explicit || ! text_explicit )); then categories+=(teleparser); fi
    # Erros de uso não representam uma execução de transferência.
    report_enabled=1
}

# O PID é aguardado explicitamente: EOF não significa que find/jq terminou bem.
# NUL mantém nomes com quebras de linha intactos, sem arquivos temporários.
# -----------------------------------------------------------------------------
# Função: collect_zero
#
# Descrição:
#   Executa um comando e coleta sua saída, separada por NUL, no array global
#   records (sobrescrito a cada chamada).
#
# Argumentos:
#   "$@" - Comando e argumentos a executar.
#
# Retorno:
#   Status de saída do comando produtor (obtido com wait).
# -----------------------------------------------------------------------------
collect_zero() {
    local descriptor producer item
    records=()
    exec {descriptor}< <("$@")
    producer=$!
    while IFS= read -r -d '' item <&"$descriptor"; do records+=("$item"); done
    exec {descriptor}<&-
    wait "$producer"
}

# -----------------------------------------------------------------------------
# Função: normalize_path
#
# Descrição:
#   Valida e normaliza um caminho lógico: colapsa barras repetidas e remove a
#   barra final (exceto para "/"). O resultado é gravado em normalized.
#
# Argumentos:
#   $1 - Caminho a normalizar.
#   $2 - 1 para caminho relativo (rejeita caminhos absolutos e componentes "."),
#        0 para permitir caminhos absolutos.
#
# Retorno:
#   0 - Caminho aceito.
#   1 - Caminho vazio, com componente ".." ou incompatível com o modo relativo.
# -----------------------------------------------------------------------------
normalize_path() {
    local path=$1 relative=$2 component remainder
    [[ -n $path ]] || return 1
    (( ! relative )) || [[ $path != /* ]] || return 1
    remainder=$path
    while :; do
        component=${remainder%%/*}
        [[ $component != .. ]] || return 1
        if (( relative )) && [[ $component == . ]]; then return 1; fi
        [[ $remainder == */* ]] || break
        remainder=${remainder#*/}
    done
    while [[ $path == *//* ]]; do path=${path//\/\//\/}; done
    [[ $path == / ]] || path=${path%/}
    [[ -n $path ]] || return 1
    normalized=$path
}

# -----------------------------------------------------------------------------
# Função: has_week_component
#
# Descrição:
#   Verifica se algum componente do caminho (separado por "/") é igual a
#   week_dir. Usada para impedir que a semana apareça duplicada nos destinos,
#   já que o script a acrescenta ao alvo.
#
# Argumentos:
#   $1 - Caminho a verificar.
#
# Retorno:
#   0 - Existe um componente igual a week_dir.
#   1 - Não existe.
# -----------------------------------------------------------------------------
has_week_component() {
    local remaining=$1
    while :; do
        [[ ${remaining%%/*} != "$week_dir" ]] || return 0
        [[ $remaining == */* ]] || return 1
        remaining=${remaining#*/}
    done
}

# -----------------------------------------------------------------------------
# Função: resolve_path
#
# Descrição:
#   Resolve o caminho canônico (links simbólicos resolvidos) com "realpath -e -z"
#   e o grava em canonical.
#
# Argumentos:
#   $1 - Caminho a resolver.
#
# Retorno:
#   0 - Caminho existente e resolvido para exatamente um valor.
#   1 - Caso contrário (inexistente, link quebrado ou falha do realpath).
# -----------------------------------------------------------------------------
resolve_path() {
    collect_zero realpath -e -z -- "$1" || return 1
    (( ${#records[@]} == 1 )) || return 1
    canonical=${records[0]}
}

# -----------------------------------------------------------------------------
# Função: check_containment
#
# Descrição:
#   Garante que o caminho, após resolução de links simbólicos, é igual ou está
#   abaixo do diretório semanal canônico da categoria. Links internos são
#   aceitos; links que apontam para fora ou quebrados geram erro.
#   Define canonical como efeito colateral de resolve_path.
#
# Argumentos:
#   $1 - Caminho a verificar.
#   $2 - Diretório semanal canônico (valor de roots[categoria]).
#
# Retorno:
#   0 - Caminho contido na raiz.
#   1 - Não resolvido ou fora da raiz (error é chamado).
# -----------------------------------------------------------------------------
check_containment() {
    local path=$1 root=$2
    if ! resolve_path "$path"; then
        error "Cannot resolve path (possibly a broken link): $(quote "$path")."
        return 1
    fi
    if [[ $canonical != "$root" && $canonical != "$root/"* ]]; then
        error "Path escapes the weekly root: $(quote "$path")."
        return 1
    fi
}

# -----------------------------------------------------------------------------
# Função: require_dependencies
#
# Descrição:
#   Verifica a presença dos comandos jq, find, sort, realpath, stat e mc no
#   PATH. A ausência de qualquer um encerra o script com código 2.
# -----------------------------------------------------------------------------
require_dependencies() {
    local command_name
    for command_name in jq find sort realpath stat mc; do
        command -v "$command_name" >/dev/null 2>&1 ||
            fatal 2 "Required command not found: $command_name."
    done
}

# -----------------------------------------------------------------------------
# Função: load_configuration
#
# Descrição:
#   Lê e valida o JSON de configuração e preenche o estado global:
#   minio_alias, batchsize, destination, destination_base, roots e os arrays
#   mapping_*.
#
# Etapas:
#   1. O arquivo deve ser legível e conter um único objeto JSON.
#   2. minio_config.alias e minio_config.batchsize são validados.
#   3. O destino vem de --destination, se informado, ou de
#      minio_config.destination. É normalizado como caminho relativo; o primeiro
#      componente deve ser um nome de bucket S3, sem alias, URL, "?" ou "#", e
#      sem o componente da semana.
#   4. Para cada categoria selecionada, a base vem da opção de linha de comando
#      ou de <categoria>_folder. Ela não pode terminar no próprio week_dir; o
#      diretório base/week_dir precisa existir e ser acessível, e seu caminho
#      canônico é gravado em roots[categoria].
#   5. folder_mapping[categoria] deve ser uma lista não vazia de objetos com
#      source_folder, pattern e destination_folder (strings não vazias, sem NUL).
#      source_folder e destination_folder são normalizados como caminhos
#      relativos; pattern não pode conter "/" nem ser "." ou "..";
#      destination_folder não pode conter "?", "#" ou "://" nem o componente da
#      semana. Mapeamentos repetidos (categoria, origem, pattern, destino) são
#      ignorados com log INFO.
#
# Retorno:
#   Qualquer violação encerra o script com código 2 (via fatal).
# -----------------------------------------------------------------------------
load_configuration() {
    local category base source pattern mapped key config_json root
    [[ -f $config_file && -r $config_file ]] ||
        fatal 2 "Configuration is not a readable file: $(quote "$config_file")."
    config_json=$(jq -cse 'if length == 1 and (.[0] | type == "object")
        then .[0] else error("Expected one configuration object") end' -- "$config_file") ||
        fatal 2 "Invalid JSON configuration."
    # Campos não selecionados não são extraídos nem validados.
    jq -e '.minio_config | type == "object"' <<< "$config_json" >/dev/null ||
        fatal 2 "minio_config must be an object."
    # index detecta NUL corretamente; contains produz falsos positivos no jq 1.6.
    collect_zero jq -j '
        [.minio_config.alias, .minio_config.batchsize] |
        if (.[0] | type == "string" and length > 0 and index("\u0000") == null)
          and (.[1] | type == "number" and . > 0 and . == floor and . <= 2147483647)
        then .[] | tostring, "\u0000"
        else error("Invalid alias or batchsize (integer 1..2147483647 required)") end
        ' <<< "$config_json" || fatal 2 "Invalid MinIO settings."
    minio_alias=${records[0]} batchsize=${records[1]}
    [[ $minio_alias =~ ^[a-zA-Z][a-zA-Z0-9_-]*$ ]] || fatal 2 "Invalid MinIO alias."
    if (( destination_explicit )); then
        destination=$destination_override
    else
        collect_zero jq -j '.minio_config.destination |
            if type == "string" and length > 0 and index("\u0000") == null
            then ., "\u0000" else error("Invalid destination") end' <<< "$config_json" ||
            fatal 2 "Invalid MinIO destination."
        destination=${records[0]}
    fi
    normalize_path "$destination" 1 || fatal 2 "Invalid destination path."
    destination=$normalized
    [[ ${destination%%/*} =~ ^[a-z0-9][a-z0-9.-]*[a-z0-9]$ ]] ||
        fatal 2 "Destination must start with an S3 bucket name, without an alias."
    [[ $destination != *'://'* && $destination != *'?'* && $destination != *'#'* ]] ||
        fatal 2 "Destination must be a bucket/prefix, not a URL."
    has_week_component "$destination" && fatal 2 "Destination already contains the week."
    destination_base="$minio_alias/$destination"
    jq -e '.folder_mapping | type == "object"' <<< "$config_json" >/dev/null ||
        fatal 2 "folder_mapping must be an object grouped by category."
    for category in "${categories[@]}"; do
        if [[ $category == text ]] && (( text_explicit )); then
            base=$text_override
        elif [[ $category == teleparser ]] && (( teleparser_explicit )); then
            base=$teleparser_override
        else
            # $field é uma variável jq, não uma expansão do shell.
            # shellcheck disable=SC2016
            collect_zero jq -j --arg field "${category}_folder" '.[$field] |
                if type == "string" and length > 0 and index("\u0000") == null
                then ., "\u0000" else error("Invalid source base") end' <<< "$config_json" ||
                fatal 2 "Invalid source base for $category."
            base=${records[0]}
        fi
        normalize_path "$base" 0 || fatal 2 "Invalid $category base path."
        base=$normalized
        [[ ${base##*/} != "$week_dir" ]] ||
            fatal 2 "$category source must be a base directory, not the week directory."
        root="${base%/}/$week_dir"
        if [[ ! -d $root || ! -r $root || ! -x $root ]] || ! resolve_path "$root"; then
            fatal 2 "Weekly directory is missing or inaccessible: $(quote "$root")."
        fi
        roots["$category"]=$canonical
        log INFO "Selected $category source: $(quote "$canonical")."
        # shellcheck disable=SC2016
        collect_zero jq -j --arg category "$category" '
            .folder_mapping[$category] |
            if type != "array" or length == 0 then error("Expected nonempty mappings")
            else .[] |
              if type != "object" then error("Invalid mapping")
              else [.source_folder, .pattern, .destination_folder] |
                if all(.[]; type == "string" and length > 0 and index("\u0000") == null)
                then .[] | ., "\u0000" else error("Invalid mapping fields") end
              end
            end' <<< "$config_json" || fatal 2 "Invalid mappings for $category."
        local -a fields=("${records[@]}")
        local i
        for ((i=0; i<${#fields[@]}; i+=3)); do
            source=${fields[i]} pattern=${fields[i+1]} mapped=${fields[i+2]}
            normalize_path "$source" 1 || fatal 2 "Invalid source mapping: $(quote "$source")."
            source=$normalized
            normalize_path "$mapped" 1 || fatal 2 "Invalid destination mapping: $(quote "$mapped")."
            mapped=$normalized
            [[ $pattern != */* && $pattern != . && $pattern != .. ]] ||
                fatal 2 "Invalid filename pattern: $(quote "$pattern")."
            [[ $mapped != *'?'* && $mapped != *'#'* && $mapped != *'://'* ]] ||
                fatal 2 "Destination mappings must be literal object prefixes."
            has_week_component "$mapped" && fatal 2 "Mapping destination already contains the week."
            key=$(jq -cn --args '$ARGS.positional' "$category" "$source" "$pattern" "$mapped") ||
                fatal 2 "Cannot encode mapping identity."
            if [[ -n ${mapping_seen[$key]+present} ]]; then
                log INFO "Ignoring duplicate mapping: $(quote "$source")."
                continue
            fi
            mapping_seen["$key"]=1
            mapping_categories+=("$category")
            mapping_sources+=("$source")
            mapping_patterns+=("$pattern")
            mapping_destinations+=("$mapped")
        done
    done
}

# -----------------------------------------------------------------------------
# Função: children_stream
#
# Descrição:
#   Produz em stdout, separados por NUL e ordenados, os filhos imediatos
#   (arquivos, diretórios e links simbólicos) de um diretório. Consumida por
#   collect_zero.
#
# Argumentos:
#   $1 - Diretório a listar.
# -----------------------------------------------------------------------------
# O produtor é executado por collect_zero através de substituição de processo.
# shellcheck disable=SC2317
children_stream() {
    find -H "$1" -mindepth 1 -maxdepth 1 \( -type f -o -type d -o -type l \) -print0 | sort -z
}

# -----------------------------------------------------------------------------
# Função: list_children
#
# Descrição:
#   Lista os filhos imediatos de um diretório em records (via children_stream).
#
# Argumentos:
#   $1 - Diretório a listar.
#
# Retorno:
#   0 - Listagem concluída.
#   1 - Diretório sem permissão de leitura/travessia ou falha na descoberta
#       (error é chamado).
# -----------------------------------------------------------------------------
list_children() {
    [[ -r $1 && -x $1 ]] || { error "Cannot read directory: $(quote "$1")."; return 1; }
    if ! collect_zero children_stream "$1"; then
        error "Directory discovery failed: $(quote "$1")."
        return 1
    fi
}

# -----------------------------------------------------------------------------
# Função: matches_name
#
# Descrição:
#   Verifica se um nome (sem diretório) casa com um padrão glob. Nomes que
#   começam com "." só casam se o padrão também começar com ".".
#
# Argumentos:
#   $1 - Nome a testar.
#   $2 - Padrão glob (sem aspas na comparação, para que os curingas atuem).
#
# Retorno:
#   0 - Casa.
#   1 - Não casa.
# -----------------------------------------------------------------------------
matches_name() {
    # [[ == ]] não aplica a regra de dotfiles do glob de Bash por conta própria.
    # shellcheck disable=SC2053
    [[ $1 != .* || $2 == .* ]] && [[ $1 == $2 ]]
}

# -----------------------------------------------------------------------------
# Função: expand_source
#
# Descrição:
#   Expande o source_folder de um mapeamento (caminho relativo cujos componentes
#   podem conter *, ? ou [) a partir da raiz semanal, um componente por vez. Só
#   componentes com curinga listam diretórios; os demais são anexados
#   literalmente. A raiz em si nunca é expandida (seus caracteres são literais).
#   Cada correspondência precisa estar contida na raiz; as que violam essa regra
#   geram erro e são descartadas. Componentes inexistentes são ignorados sem erro.
#
# Argumentos:
#   $1 - Diretório semanal canônico da categoria.
#   $2 - source_folder normalizado.
#
# Saída:
#   Array global expanded com os caminhos correspondentes (possivelmente vazio).
# -----------------------------------------------------------------------------
expand_source() {
    local root=$1 remaining=$2 component parent path wildcard
    local -a current=("$root") next=() children=()
    while :; do
        component=${remaining%%/*}
        next=()
        wildcard=0
        [[ $component == *'*'* || $component == *'?'* || $component == *'['* ]] && wildcard=1
        for parent in "${current[@]}"; do
            [[ -d $parent ]] || continue
            if (( wildcard )); then
                list_children "$parent" || continue
                children=("${records[@]}")
            else
                children=("$parent/$component")
            fi
            for path in "${children[@]}"; do
                [[ -e $path || -L $path ]] || continue
                matches_name "${path##*/}" "$component" || continue
                check_containment "$path" "$root" || continue
                next+=("$path")
            done
        done
        current=("${next[@]}")
        [[ $remaining == */* ]] || break
        remaining=${remaining#*/}
    done
    expanded=("${current[@]}")
}

# -----------------------------------------------------------------------------
# Função: audit_teleparser
#
# Descrição:
#   Regra da categoria teleparser: todo diretório de segundo nível abaixo da
#   raiz semanal (caminho relativo "primeiro/segundo") deve casar, como glob, com
#   o source_folder de ao menos um mapeamento teleparser. Diretórios sem
#   mapeamento geram erro "Unmapped Teleparser directory".
#   Se a raiz não puder ser listada ou se qualquer erro novo for registrado
#   (inclusive de contenção de caminhos), blocked[teleparser] é definido: os
#   mapeamentos teleparser passam a contar como pendentes e nada dessa
#   categoria é copiado, mas as demais categorias continuam.
#   Não faz nada se teleparser não foi selecionada.
# -----------------------------------------------------------------------------
audit_teleparser() {
    [[ -n ${roots[teleparser]+present} ]] || return 0
    local root=${roots[teleparser]} parent directory relative index mapped before=$failures
    local -a parents=() children=()
    if ! list_children "$root"; then blocked[teleparser]=1; return; fi
    parents=("${records[@]}")
    for parent in "${parents[@]}"; do
        check_containment "$parent" "$root" || continue
        [[ -d $parent ]] || continue
        list_children "$parent" || continue
        children=("${records[@]}")
        for directory in "${children[@]}"; do
            check_containment "$directory" "$root" || continue
            [[ -d $directory ]] || continue
            relative=${directory#"$root/"}
            mapped=0
            for index in "${!mapping_sources[@]}"; do
                [[ ${mapping_categories[index]} == teleparser ]] || continue
                # shellcheck disable=SC2053
                if [[ $relative == ${mapping_sources[index]} ]]; then mapped=1; break; fi
            done
            if (( ! mapped )); then
                error "Unmapped Teleparser directory: $(quote "$relative")."
            fi
        done
    done
    if (( failures > before )); then
        blocked[teleparser]=1
        log ERROR "Teleparser is blocked; other selected categories may continue."
    fi
}

# -----------------------------------------------------------------------------
# Função: add_candidate
#
# Descrição:
#   Registra um arquivo como candidato à transferência.
#
# Argumentos:
#   $1 - Caminho do arquivo.
#   $2 - Diretório de destino remoto.
#   $3 - Diretório semanal canônico da categoria.
#
# Regras:
#   - O arquivo deve estar contido na raiz; sua identidade canônica é contada
#     uma única vez em files_found.
#   - Deve ser legível e ter tamanho obtido por "stat -L"; caso contrário, erro.
#   - O alvo é "destino/nome-do-arquivo". Se o alvo já foi registrado com a
#     mesma identidade, o duplicado é ignorado (INFO); com identidade diferente,
#     conta como colisão e erro.
#   - Arquivos aceitos entram nos arrays paralelos (sources, identities, sizes,
#     targets, target_directories), files_selected é incrementado e o diretório é
#     adicionado a destinations, preservando a ordem da primeira ocorrência.
# -----------------------------------------------------------------------------
add_candidate() {
    local file=$1 directory=$2 root=$3 identity target index size
    check_containment "$file" "$root" || return
    identity=$canonical
    if [[ -z ${found_seen[$identity]+present} ]]; then
        found_seen["$identity"]=1
        files_found=$((files_found + 1))
    fi
    [[ -f $file && -r $file ]] || { error "Unreadable file: $(quote "$file")."; return; }
    size=$(stat -L -c %s -- "$file") || { error "Cannot stat file: $(quote "$file")."; return; }
    target="$directory/${file##*/}"
    if [[ -n ${target_index[$target]+present} ]]; then
        index=${target_index[$target]}
        if [[ ${identities[index]} != "$identity" ]]; then
            collisions=$((collisions + 1))
            error "Destination collision: $(quote "$target") from $(quote "${sources[index]}") and $(quote "$file")."
        else
            log INFO "Ignoring duplicate source/target: $(quote "$target")."
        fi
        return
    fi
    target_index["$target"]=${#sources[@]}
    sources+=("$file") identities+=("$identity") sizes+=("$size")
    targets+=("$target") target_directories+=("$directory")
    files_selected=$((files_selected + 1))
    if [[ -z ${destination_seen[$directory]+present} ]]; then
        destination_seen["$directory"]=1
        destinations+=("$directory")
    fi
}

# -----------------------------------------------------------------------------
# Função: discover_files
#
# Descrição:
#   Para cada mapeamento, na ordem em que foi carregado, identifica os arquivos
#   a transferir.
#
# Regras:
#   - Executa antes audit_teleparser; mapeamentos de categoria bloqueada contam
#     como pendentes e não são processados.
#   - O destino remoto do mapeamento é destination_base/week_dir/destination_folder.
#   - Cada correspondência de source_folder (expand_source) que for diretório
#     contribui com seus filhos imediatos (sem recursão); se for arquivo ou link
#     para arquivo, ela própria é o candidato. O pattern é aplicado ao nome de
#     cada candidato; apenas arquivos regulares contidos na raiz são aceitos.
#   - Mapeamento sem arquivos correspondentes conta como pendente (WARNING).
#   - Ao final, qualquer colisão de destino encerra com código 1, antes de
#     qualquer cópia.
# -----------------------------------------------------------------------------
discover_files() {
    local index category root source pattern directory match file matched
    local -a matches=() children=()
    audit_teleparser
    for index in "${!mapping_sources[@]}"; do
        category=${mapping_categories[index]}
        if [[ -n ${blocked[$category]+present} ]]; then
            pending_mappings=$((pending_mappings + 1))
            continue
        fi
        root=${roots[$category]} source=${mapping_sources[index]} pattern=${mapping_patterns[index]}
        directory="$destination_base/$week_dir/${mapping_destinations[index]}"
        log INFO "Mapping $category: $(quote "$source") [$(quote "$pattern")] -> $(quote "$directory")."
        expand_source "$root" "$source"
        matches=("${expanded[@]}")
        matched=0
        for match in "${matches[@]}"; do
            if [[ -d $match ]]; then
                list_children "$match" || continue
                children=("${records[@]}")
            else
                children=("$match")
            fi
            for file in "${children[@]}"; do
                matches_name "${file##*/}" "$pattern" || continue
                check_containment "$file" "$root" || continue
                [[ -f $file ]] || continue
                matched=$((matched + 1))
                add_candidate "$file" "$directory" "$root"
            done
        done
        if (( matched == 0 )); then
            pending_mappings=$((pending_mappings + 1))
            log WARNING "Mapping is absent or has no matching files: $(quote "$root/$source")."
        fi
    done
    (( collisions == 0 )) || fatal 1 "Local destination collisions detected; no files were copied."
}

# -----------------------------------------------------------------------------
# Função: capture_mc
#
# Descrição:
#   Executa "mc --config-dir $mc_config_dir ARGS..." capturando stdout e stderr
#   juntos em mc_output.
#
# Argumentos:
#   "$@" - Argumentos do mc.
#
# Retorno:
#   0 - mc terminou com sucesso.
#   1 - mc falhou; apenas "mc <primeiro argumento> failed (exit N)" é registrado
#       via error, sem reproduzir a saída capturada.
# -----------------------------------------------------------------------------
capture_mc() {
    local code
    mc_output=$(mc --config-dir "$mc_config_dir" "$@" 2>&1)
    code=$?
    if (( code != 0 )); then
        # Não imprimir a configuração nem respostas que possam conter segredos.
        error "mc $1 failed (exit $code)."
        return 1
    fi
}

# -----------------------------------------------------------------------------
# Função: validate_mc
#
# Descrição:
#   Valida o ambiente do MinIO Client antes de qualquer consulta ou cópia
#   (inclusive em --dry-run). Define mc_config_dir como MC_CONFIG_DIR ou
#   $HOME/.mc.
#
# Verificações (qualquer falha encerra com código 2):
#   - config.json legível, com version "10".
#   - Diretórios share, certs e certs/CAs existentes e acessíveis.
#   - Ausência de share/urls.json (estado legado que exigiria migração).
#   - Alias configurado com URL http:// ou https://.
#   - "mc --version" iniciando com "mc version ".
#   - "mc stat --json" no bucket de destino com resposta de sucesso.
# -----------------------------------------------------------------------------
validate_mc() {
    local subdirectory
    mc_config_dir=${MC_CONFIG_DIR:-${HOME:-}/.mc}
    [[ -f $mc_config_dir/config.json && -r $mc_config_dir/config.json ]] ||
        fatal 2 "An existing readable mc config.json is required (MC_CONFIG_DIR or HOME/.mc)."
    # Até comandos de leitura do mc inicializam/migram seu estado local.
    # Exigir estado pronto evita escritas implícitas, especialmente no dry-run.
    jq -e '.version == "10"' -- "$mc_config_dir/config.json" >/dev/null ||
        fatal 2 "mc configuration version 10 is required; initialize or migrate mc separately."
    for subdirectory in share certs certs/CAs; do
        [[ -d $mc_config_dir/$subdirectory && -r $mc_config_dir/$subdirectory &&
            -x $mc_config_dir/$subdirectory ]] ||
            fatal 2 "mc is not fully initialized: missing or inaccessible $subdirectory directory."
    done
    [[ ! -e $mc_config_dir/share/urls.json && ! -L $mc_config_dir/share/urls.json ]] ||
        fatal 2 "Legacy mc share state requires migration outside this script."
    jq -e --arg alias "$minio_alias" '.aliases[$alias] |
        type == "object" and (.url | type == "string" and test("^https?://"))' \
        -- "$mc_config_dir/config.json" >/dev/null ||
        fatal 2 "The configured MinIO alias is missing or invalid."
    capture_mc --version || fatal 2 "Cannot identify the MinIO Client."
    [[ $mc_output == "mc version "* ]] || fatal 2 "The mc executable is not the MinIO Client."
    capture_mc stat --json -- "$minio_alias/${destination%%/*}" ||
        fatal 2 "Cannot access the destination bucket."
    jq -e -s 'length > 0 and all(.[]; type == "object" and .status == "success")' \
        <<< "$mc_output" >/dev/null || fatal 2 "Invalid bucket access response."
}

# -----------------------------------------------------------------------------
# Função: load_remote
#
# Descrição:
#   Lista com "mc ls --json" os objetos de um diretório remoto e preenche o
#   array associativo remote_objects (nome -> tamanho em bytes). Somente
#   entradas do tipo "file" são registradas; entradas "folder" são ignoradas.
#
# Argumentos:
#   $1 - Diretório remoto (alias/bucket/prefixo/...).
#
# Retorno:
#   0 - Listagem válida.
#   1 - Falha do mc, JSON inválido, chave de arquivo com "/" ou NUL, tamanho
#       inválido ou objeto duplicado. Uma falha não é tratada como diretório vazio.
# -----------------------------------------------------------------------------
load_remote() {
    local directory=$1 key size
    remote_objects=()
    if ! capture_mc ls --json -- "$directory/"; then
        log ERROR "Remote listing unavailable: $(quote "$directory")."
        return 1
    fi
    if ! collect_zero jq -s -j '
        if all(.[]; type == "object" and .status == "success"
            and (.type == "file" or .type == "folder")
            and (.key | type == "string" and length > 0 and index("\u0000") == null)
            and (if .type == "file" then
                (.key | contains("/") | not)
                and (.size | type == "number" and . >= 0 and . == floor)
                else true end))
        then .[] | select(.type == "file") | .key, "\u0000", (.size | tostring), "\u0000"
        else error("Invalid object listing") end' <<< "$mc_output"; then
        error "Invalid remote listing: $(quote "$directory")."
        return 1
    fi
    local index
    for ((index=0; index<${#records[@]}; index+=2)); do
        key=${records[index]} size=${records[index+1]}
        if [[ -n ${remote_objects[$key]+present} ]]; then
            error "Duplicate object in remote listing: $(quote "$directory/$key")."
            return 1
        fi
        remote_objects["$key"]=$size
    done
}

# -----------------------------------------------------------------------------
# Função: source_unchanged
#
# Descrição:
#   Confirma que o arquivo de origem registrado no índice informado continua
#   legível, resolve para a mesma identidade canônica e mantém o tamanho
#   registrado na descoberta.
#
# Argumentos:
#   $1 - Índice nos arrays paralelos de arquivos selecionados.
#
# Retorno:
#   0 - Origem inalterada.
#   1 - Origem inacessível, redirecionada ou com tamanho diferente (error é chamado).
# -----------------------------------------------------------------------------
source_unchanged() {
    local index=$1 size
    if [[ ! -r ${sources[index]} ]] || ! resolve_path "${sources[index]}" ||
        [[ $canonical != "${identities[index]}" ]]; then
        error "Source became inaccessible or changed target: $(quote "${sources[index]}")."
        return 1
    fi
    size=$(stat -L -c %s -- "${sources[index]}") || {
        error "Cannot stat source: $(quote "${sources[index]}")."; return 1;
    }
    if [[ $size != "${sizes[index]}" ]]; then
        error "Source size changed: $(quote "${sources[index]}")."
        return 1
    fi
}

# -----------------------------------------------------------------------------
# Função: transfer_batch
#
# Descrição:
#   Copia um lote de arquivos para um diretório remoto com uma única chamada
#   "mc cp --json" e confirma cada arquivo individualmente.
#
# Argumentos:
#   $1    - Diretório remoto de destino.
#   $2..  - Índices dos arquivos do lote.
#
# Regras:
#   - Antes de copiar, descarta (com erro) os arquivos que não passam em
#     source_unchanged; lote sem arquivos válidos retorna 0 sem copiar.
#   - Uma falha do mc cp é apenas registrada; a confirmação vem da nova
#     listagem remota (load_remote). Um arquivo só é contado em
#     files_transferred se o nome existir no destino com o tamanho registrado;
#     caso contrário, erro "Transfer not confirmed".
#   - Se a listagem após a cópia falhar, nenhum arquivo do lote é contado.
#
# Efeitos colaterais:
#   Grava objetos no MinIO; incrementa batches_processed e files_transferred.
# -----------------------------------------------------------------------------
transfer_batch() {
    local directory=$1 index name
    shift
    local -a batch=() indices=()
    for index in "$@"; do
        source_unchanged "$index" || continue
        batch+=("${sources[index]}") indices+=("$index")
    done
    (( ${#batch[@]} )) || return 0
    batches_processed=$((batches_processed + 1))
    # O retorno do lote não identifica quais arquivos chegaram. Verificamos todos.
    capture_mc cp --json -- "${batch[@]}" "$directory/" ||
        log ERROR "Copy batch failed for $(quote "$directory"); checking individual results."
    load_remote "$directory" || return
    for index in "${indices[@]}"; do
        name=${sources[index]##*/}
        source_unchanged "$index" || continue
        if [[ -n ${remote_objects[$name]+present} && ${remote_objects[$name]} == "${sizes[index]}" ]]; then
            files_transferred=$((files_transferred + 1))
            log INFO "Transferred: $(quote "${sources[index]}") -> $(quote "${targets[index]}")."
        else
            error "Transfer not confirmed: $(quote "${targets[index]}")."
        fi
    done
}

# -----------------------------------------------------------------------------
# Função: process_destinations
#
# Descrição:
#   Processa cada diretório de destino na ordem de descoberta.
#
# Regras:
#   - Lista os objetos remotos do destino; se a listagem falhar, o destino é
#     ignorado (nada é copiado nele).
#   - Arquivo cuja origem mudou é descartado (erro). Se o nome já existe no
#     destino com o mesmo tamanho, conta em files_existing e não é recopiado;
#     com tamanho diferente, registra erro e nunca sobrescreve.
#   - Os pendentes (files_planned) são divididos em lotes de até batchsize
#     arquivos (batches_planned).
#   - Com --dry-run, cada arquivo do lote gera uma linha "PLAN origem -> destino"
#     em stdout e nada é copiado; sem --dry-run, cada lote vai para transfer_batch.
# -----------------------------------------------------------------------------
process_destinations() {
    local directory index name offset
    local -a pending=() batch=()
    for directory in "${destinations[@]}"; do
        load_remote "$directory" || continue
        pending=()
        for index in "${!sources[@]}"; do
            [[ ${target_directories[index]} == "$directory" ]] || continue
            source_unchanged "$index" || continue
            name=${sources[index]##*/}
            if [[ -n ${remote_objects[$name]+present} ]]; then
                if [[ ${remote_objects[$name]} == "${sizes[index]}" ]]; then
                    files_existing=$((files_existing + 1))
                    log INFO "Already exists with matching size: $(quote "${targets[index]}")."
                else
                    error "Remote size conflict; not overwritten: $(quote "${targets[index]}")."
                fi
                continue
            fi
            pending+=("$index")
        done
        files_planned=$((files_planned + ${#pending[@]}))
        for ((offset=0; offset<${#pending[@]}; offset+=batchsize)); do
            batch=("${pending[@]:offset:batchsize}")
            batches_planned=$((batches_planned + 1))
            log INFO "Batch $batches_planned: ${#batch[@]} files -> $(quote "$directory")."
            if (( dry_run )); then
                for index in "${batch[@]}"; do
                    printf 'PLAN %s -> %s\n' "$(quote "${sources[index]}")" "$(quote "${targets[index]}")"
                done
            else
                transfer_batch "$directory" "${batch[@]}"
            fi
        done
    done
}

# -----------------------------------------------------------------------------
# Função: main
#
# Descrição:
#   Orquestra a execução: parse_arguments, require_dependencies,
#   load_configuration, discover_files, validate_mc e process_destinations.
#
# Status final (final_status):
#   DRY_RUN          - --dry-run informado (independe de falhas).
#   SUCCESS          - Sem falhas e sem mapeamentos pendentes.
#   PARTIAL_SUCCESS  - Há falhas ou pendências, mas houve ao menos um arquivo
#                      transferido ou já existente.
#   FAILED           - Demais casos.
#
# Retorno:
#   0 se failures e pending_mappings são ambos 0; 1 caso contrário. O código é
#   o status da última expressão aritmética, repassado por exit "$?" no fim do
#   arquivo. Falhas fatais anteriores encerram com o código próprio de fatal.
# -----------------------------------------------------------------------------
main() {
    parse_arguments "$@"
    log INFO "Starting CDR upload for $week_dir (dry_run=$dry_run)."
    require_dependencies
    load_configuration
    log INFO "Destination base: $(quote "$destination_base")."
    discover_files
    validate_mc
    process_destinations
    if (( dry_run )); then
        final_status=DRY_RUN
    elif (( failures == 0 && pending_mappings == 0 )); then
        final_status=SUCCESS
    elif (( files_transferred + files_existing > 0 )); then
        final_status=PARTIAL_SUCCESS
    else
        final_status=FAILED
    fi
    (( failures == 0 && pending_mappings == 0 ))
}

main "$@"
exit "$?"
