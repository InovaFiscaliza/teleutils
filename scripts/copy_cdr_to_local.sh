#!/usr/bin/env bash

# =============================================================================
# Descrição:
#   Copia para uma pasta local do servidor Linux os arquivos de CDR de uma
#   semana específica, nas categorias "text" e "teleparser". É o equivalente
#   local de upload_cdr_to_minio.sh: reaproveita o mesmo arquivo de
#   configuração JSON, os mesmos argumentos e as mesmas regras de seleção
#   (mapeamentos, curingas, padrões, auditoria do teleparser), mas grava em um
#   diretório local em vez do MinIO.
#
# Uso:
#   copy_cdr_to_local.sh --config ARQUIVO --week-dir SemanaNUMERO [OPÇÕES]
#
# Argumentos obrigatórios:
#   --config ARQUIVO       Arquivo JSON de configuração (um único objeto).
#   --week-dir NOME        Nome da semana, no formato "Semana" + dígitos.
#
# Opções:
#   --teleparser-folder CAMINHO  Substitui o diretório BASE da categoria teleparser.
#   --text-folder CAMINHO        Substitui o diretório BASE da categoria text.
#   --destination CAMINHO        Diretório local de destino (raiz; a semana é
#                                acrescentada). Substitui local_config.destination.
#   --jobs N                     Cópias simultâneas (1 a 256). Substitui
#                                local_config.jobs. Padrão: min(nproc, 8).
#   --dry-run                    Planeja e consulta o destino, sem copiar nem criar.
#   --help, -h                   Exibe a ajuda e encerra com código 0.
#   Cada opção com valor pode ser informada uma única vez e exige valor
#   não vazio que não comece com "--".
#
# Seleção de categorias (idêntica ao script de upload):
#   Sem --teleparser-folder e sem --text-folder: processa text e teleparser.
#   Somente --teleparser-folder: processa apenas teleparser.
#   Somente --text-folder: processa apenas text.
#   Ambas: processa as duas, usando os dois caminhos informados.
#
# Configuração JSON (campos consumidos; o mesmo arquivo do upload serve):
#   teleparser_folder, text_folder   Diretórios BASE de origem; o script acrescenta
#                                    --week-dir a cada base.
#   folder_mapping.<categoria>[]     Lista não vazia de objetos com
#                                    source_folder, destination_folder e pattern.
#   local_config.destination         Opcional. Diretório local de destino quando
#                                    --destination não é informado.
#   local_config.jobs                Opcional. Cópias simultâneas (1 a 256).
#   local_config.batchsize           Opcional. Máximo de arquivos por chamada
#                                    "cp"; se ausente, usa minio_config.batchsize
#                                    e, na falta dele, 100.
#   minio_config.*                   Não usado, exceto batchsize como fallback.
#   Os campos de uma categoria não selecionada não são lidos.
#
# Dependências:
#   Bash 4.4 ou superior, jq e as ferramentas GNU find, cp, mv, rm, mkdir,
#   mktemp, realpath, df, tail e nproc.
#
# Estratégia de desempenho:
#   - Nenhum processo externo é criado por arquivo. A descoberta usa glob do
#     Bash (por mapeamento) e uma única chamada "find -printf" por mapeamento,
#     que já devolve o tamanho; um único "realpath" por mapeamento valida a
#     contenção dos diretórios; um único "find" lista o destino existente. As
#     listagens passam por arquivo temporário + "mapfile" (leitura em bloco; o
#     Bash lê pipes byte a byte).
#   - Os arquivos são agrupados em lotes (por quantidade, bytes e tamanho da
#     linha de comando) e copiados por até --jobs processos "cp -t" em paralelo,
#     com "--reflink=auto" (cópia instantânea em btrfs/XFS) e o mtime preservado.
#   - Cada lote é copiado para uma pasta temporária ".partial.PID.N" no próprio
#     diretório de destino e movido (rename atômico, mesmo sistema de arquivos)
#     para o nome final. Nunca existe arquivo parcial com o nome final.
#   - A confirmação é feita por uma listagem única do destino ao final.
#   - O espaço livre do destino é verificado antes de copiar.
#
# Regras principais:
#   - Alvo local: DESTINO/SEMANA/destination_folder/arquivo.
#   - Todo diretório selecionado deve permanecer, após resolução de links
#     simbólicos, dentro do diretório semanal da categoria.
#   - Apenas arquivos regulares são copiados. Links simbólicos que casam com o
#     padrão não são seguidos: são reportados como erro.
#   - Arquivos de destino existentes nunca são sobrescritos; mesmo tamanho conta
#     como "existente" (execução idempotente) e tamanho diferente é erro.
#   - Na categoria teleparser, todo diretório de segundo nível abaixo da semana
#     deve corresponder a algum source_folder mapeado; caso contrário a categoria
#     é bloqueada e seus mapeamentos ficam pendentes.
#   - Mapeamento ausente ou sem arquivos correspondentes conta como pendente.
#   - O destino não pode coincidir nem se sobrepor ao diretório semanal de origem.
#   - Os arquivos de origem não são modificados.
#
# Entradas e saídas:
#   stdout  Linhas "PLAN origem -> destino" (--dry-run), ajuda (--help) e o
#           relatório final ("FINAL REPORT").
#   stderr  Mensagens de log no formato "[NÍVEL] mensagem".
#   Efeito colateral: arquivos gravados no destino (exceto em --dry-run).
#
# Status final (campo "status" do relatório):
#   SUCCESS, PARTIAL_SUCCESS, FAILED e DRY_RUN.
#
# Códigos de saída:
#   0    Nenhuma falha e nenhum mapeamento pendente (inclui simulação sem falhas),
#        ou --help.
#   1    Falhas operacionais, colisões de destino, falta de espaço ou
#        mapeamentos pendentes.
#   2    Uso, configuração ou ambiente inválidos.
#   130  Interrompido por SIGINT.
#   143  Interrompido por SIGTERM.
#
# Exemplo:
#   copy_cdr_to_local.sh --config config.json --week-dir Semana110 \
#     --destination /data/cdr_local --jobs 8 --dry-run
# =============================================================================

if (( BASH_VERSINFO[0] < 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] < 4) )); then
    printf 'ERROR: Bash 4.4 or newer is required.\n' >&2
    exit 2
fi
set -o pipefail
shopt -u nocasematch
export LC_ALL=C

# Limites de um lote: o que ocorrer primeiro fecha o lote. O limite de bytes
# equilibra a carga entre os processos; o de caracteres protege o ARG_MAX.
# 1 GiB
readonly BATCH_MAX_BYTES=1073741824
readonly BATCH_MAX_CHARS=65536
readonly DEFAULT_BATCHSIZE=100
readonly DEFAULT_MAX_JOBS=8

# Estado global.
# - src_list, size_list, name_list e tdir_list são arrays paralelos: o mesmo
#   índice descreve um único arquivo selecionado.
# - mapping_* são arrays paralelos: um índice por mapeamento válido e único.
# - batch_dirs e batch_items são paralelos: um índice por lote planejado;
#   batch_items guarda índices de arquivos separados por espaço.
# - records funciona como "valor de retorno" de collect_zero.
config_file="" week_dir="" destination_override="" jobs_override=""
teleparser_override="" text_override=""
teleparser_explicit=0 text_explicit=0 destination_explicit=0 jobs_explicit=0 dry_run=0
destination="" dest_root="" dest_week="" batchsize=$DEFAULT_BATCHSIZE jobs=1
started_at=$SECONDS report_enabled=0 final_status="" copy_started=0 running=0
files_found=0 files_selected=0 files_existing=0 files_planned=0
files_transferred=0 failures=0 pending_mappings=0 collisions=0
batches_planned=0 batches_launched=0 bytes_planned=0 bytes_transferred=0
matched_count=0 current_dest_dir="" current_pattern="" scratch_dir="" scratch_file=""
declare -a categories=() records=()
declare -a mapping_categories=() mapping_sources=() mapping_patterns=() mapping_destinations=()
declare -a src_list=() size_list=() name_list=() tdir_list=() destinations=()
declare -a batch_dirs=() batch_items=() planned_items=()
declare -A roots=() blocked=() option_seen=() mapping_seen=()
declare -A found_seen=() target_index=() destination_seen=()
declare -A existing_meta=()

# -----------------------------------------------------------------------------
# Funções de log e encerramento
#   log NÍVEL FORMATO [ARGS]  Escreve "[NÍVEL] mensagem" em stderr; FORMATO é
#                             um formato printf (%q exibe caminhos sem ambiguidade).
#   error FORMATO [ARGS]      Incrementa failures e registra ERROR; não encerra.
#   fatal CÓDIGO FORMATO ...  Registra o erro, define final_status=FAILED e
#                             encerra com CÓDIGO (o trap EXIT emite o relatório).
# Todas são builtins puras, sem subshell, para serem baratas em laços grandes.
# -----------------------------------------------------------------------------
log() {
    local level=$1 format=$2
    shift 2
    # shellcheck disable=SC2059
    printf "[%s] $format\n" "$level" "$@" >&2
}
error() { failures=$((failures + 1)); log ERROR "$@"; }
fatal() { local code=$1; shift; error "$@"; final_status=FAILED; exit "$code"; }

# -----------------------------------------------------------------------------
# Função: usage
#
# Descrição:
#   Escreve o texto de ajuda em stdout (argument_error o redireciona para stderr).
# -----------------------------------------------------------------------------
usage() {
    printf '%s\n' 'Usage: copy_cdr_to_local.sh --config FILE --week-dir SemanaNUMBER [OPTIONS]

Required:
  --config FILE               JSON configuration file (same as upload_cdr_to_minio.sh).
  --week-dir NAME             Week name: Semana followed by digits.

Options:
  --teleparser-folder PATH    Override the Teleparser BASE directory.
  --text-folder PATH          Override the text BASE directory.
  --destination PATH          Local destination root (the week is appended).
                              Overrides local_config.destination.
  --jobs N                    Concurrent copy processes (1-256).
                              Overrides local_config.jobs. Default: min(nproc, 8).
  --dry-run                   Plan and inspect the destination, without writing.
  --help, -h                  Show this help and exit successfully.

Source selection is based on explicitly supplied options:
  Neither source option: process both categories from JSON.
  Only --teleparser-folder: process ONLY Teleparser.
  Only --text-folder: process ONLY text.
  Both source options: process both, using the two CLI paths.

Source paths are BASE directories, not complete weekly directories.
Targets: DESTINATION/week/mapped-destination/filename.
Existing files are never overwritten; same-size files are skipped.

Example:
  copy_cdr_to_local.sh --config config.json --week-dir Semana110 \
    --destination /data/cdr_local --jobs 8 --dry-run

Exit codes: 0 success/simulation; 1 operational errors or pending mappings;
            2 invalid usage/configuration/environment; 130/143 interrupted.'
}

# -----------------------------------------------------------------------------
# Função: argument_error
#
# Descrição:
#   Trata erro de uso: desabilita o relatório final, registra o erro, escreve a
#   ajuda em stderr e encerra com código 2.
#
# Argumentos:
#   "$@" - Formato e argumentos da mensagem (ver log).
# -----------------------------------------------------------------------------
argument_error() {
    report_enabled=0
    error "$@"
    usage >&2
    exit 2
}

# -----------------------------------------------------------------------------
# Função: report
#
# Descrição:
#   Escreve em stdout o "FINAL REPORT": semana, categorias, origens, destino,
#   contadores de arquivos e bytes, falhas, pendências, lotes, cópias
#   simultâneas, duração e status final.
# -----------------------------------------------------------------------------
# O relatório é chamado indiretamente pelo trap EXIT.
# shellcheck disable=SC2317
report() {
    local category
    printf '\nFINAL REPORT\n'
    printf 'week: %q\n' "${week_dir:-UNRESOLVED}"
    printf 'categories:'
    for category in "${categories[@]}"; do printf ' %s' "$category"; done
    printf '\n'
    for category in "${categories[@]}"; do
        printf 'source_%s: %q\n' "$category" "${roots[$category]:-UNRESOLVED}"
    done
    printf 'destination_root: %q\n' "${dest_week:-UNRESOLVED}"
    printf 'files_found: %s\nfiles_selected: %s\n' "$files_found" "$files_selected"
    printf 'files_existing: %s\nfiles_planned: %s\n' "$files_existing" "$files_planned"
    printf 'files_transferred: %s\nfailures: %s\n' "$files_transferred" "$failures"
    printf 'bytes_planned: %s\nbytes_transferred: %s\n' "$bytes_planned" "$bytes_transferred"
    printf 'pending_mappings: %s\n' "$pending_mappings"
    printf 'batches_planned: %s\nbatches_launched: %s\njobs: %s\n' \
        "$batches_planned" "$batches_launched" "$jobs"
    printf 'duration_seconds: %s\nstatus: %s\n' "$((SECONDS - started_at))" "$final_status"
}

# -----------------------------------------------------------------------------
# Função: stop_workers
#
# Descrição:
#   Envia SIGTERM aos processos de cópia em segundo plano e aguarda seu
#   encerramento. Cada worker repassa o sinal ao "cp" que executa.
# -----------------------------------------------------------------------------
# shellcheck disable=SC2317
stop_workers() {
    local pid
    for pid in $(jobs -p); do kill "$pid" 2>/dev/null; done
    wait 2>/dev/null
}

# -----------------------------------------------------------------------------
# Função: on_exit
#
# Descrição:
#   Handler do trap EXIT. Remove pastas temporárias ".partial.PID.*" deixadas
#   por esta execução (somente se alguma cópia foi iniciada) e emite o
#   relatório quando habilitado. Não altera o código de saída.
# -----------------------------------------------------------------------------
# shellcheck disable=SC2317
on_exit() {
    local code=$?
    [[ -z $scratch_dir ]] || rm -rf -- "$scratch_dir"
    if (( copy_started )) && [[ -n $dest_week ]]; then
        find "$dest_week" -type d -name ".partial.$$.*" -prune \
            -exec rm -rf -- {} + 2>/dev/null
    fi
    if (( report_enabled )); then
        if [[ -z $final_status ]]; then
            final_status=FAILED
            log ERROR "Execution ended unexpectedly (exit %s)." "$code"
        fi
        report
    fi
}
trap on_exit EXIT
trap 'stop_workers; final_status=FAILED; error "Interrupted by SIGINT."; exit 130' INT
trap 'stop_workers; final_status=FAILED; error "Interrupted by SIGTERM."; exit 143' TERM

# -----------------------------------------------------------------------------
# Função: parse_arguments
#
# Descrição:
#   Interpreta a linha de comando e preenche config_file, week_dir, os
#   overrides (e suas flags *_explicit), jobs_override, dry_run e categories.
#
# Regras:
#   - Sem --config/--week-dir é erro de uso; --week-dir casa com ^Semana[0-9]+$.
#   - Opções com valor exigem valor não vazio que não comece com "--" e não
#     podem se repetir.
#   - --help/-h imprime a ajuda em stdout e encerra com 0, sem relatório.
#   - Só no final, com argumentos válidos, o relatório é habilitado.
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
            --config|--week-dir|--teleparser-folder|--text-folder|--destination|--jobs)
                if (( $# < 2 )) || [[ -z $2 || $2 == --* ]]; then
                    argument_error "Missing value for %s." "$option"
                fi
                [[ -z ${option_seen[$option]+present} ]] ||
                    argument_error "Repeated option: %s." "$option"
                option_seen["$option"]=1
                case "$option" in
                    --config) config_file=$2 ;;
                    --week-dir) week_dir=$2 ;;
                    --teleparser-folder) teleparser_override=$2; teleparser_explicit=1 ;;
                    --text-folder) text_override=$2; text_explicit=1 ;;
                    --destination) destination_override=$2; destination_explicit=1 ;;
                    --jobs) jobs_override=$2; jobs_explicit=1 ;;
                esac
                shift 2
                ;;
            *) argument_error "Unknown argument: %q." "$option" ;;
        esac
    done
    [[ -n $config_file && -n $week_dir ]] ||
        argument_error "Both --config and --week-dir are required."
    [[ $week_dir =~ ^Semana[0-9]+$ ]] || argument_error "Invalid week name: %q." "$week_dir"
    if (( jobs_explicit )); then
        if [[ ! $jobs_override =~ ^[0-9]{1,3}$ ]] || (( 10#$jobs_override < 1 || 10#$jobs_override > 256 )); then
            argument_error "Invalid --jobs value (integer 1..256 required): %q." "$jobs_override"
        fi
    fi
    if (( text_explicit || ! teleparser_explicit )); then categories+=(text); fi
    if (( teleparser_explicit || ! text_explicit )); then categories+=(teleparser); fi
    report_enabled=1
}

# -----------------------------------------------------------------------------
# Função: collect_zero
#
# Descrição:
#   Executa um comando e coleta sua saída, separada por NUL, no array records.
#   O PID é aguardado: EOF não significa que o produtor terminou bem.
#
# Retorno:
#   Status de saída do comando produtor.
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
# Função: collect_zero_buffered
#
# Descrição:
#   Como collect_zero, mas para saídas grandes (listagens de arquivos). A saída
#   do comando vai para um arquivo temporário e é lida com "mapfile". O Bash lê
#   pipes byte a byte (um syscall por byte); em arquivo comum a leitura é em
#   bloco, o que reduz de forma drástica o tempo de sistema com dezenas de
#   milhares de arquivos. Preenche records e retorna o status do comando.
# -----------------------------------------------------------------------------
collect_zero_buffered() {
    local status
    "$@" > "$scratch_file"
    status=$?
    mapfile -d '' -t records < "$scratch_file"
    return "$status"
}

# -----------------------------------------------------------------------------
# Função: normalize_path
#
# Descrição:
#   Valida e normaliza um caminho lógico: colapsa barras repetidas e remove a
#   barra final (exceto "/"). O resultado é gravado em normalized.
#
# Argumentos:
#   $1 - Caminho a normalizar.
#   $2 - 1 para caminho relativo (rejeita absolutos e componentes "."),
#        0 para permitir caminhos absolutos.
#
# Retorno:
#   0 - Aceito. 1 - Vazio, com componente ".." ou incompatível com o modo.
# -----------------------------------------------------------------------------
normalized=""
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
#   Verifica se algum componente do caminho é igual a week_dir. Impede a semana
#   duplicada nos destinos, já que o script a acrescenta ao alvo.
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
# Função: require_dependencies
#
# Descrição:
#   Verifica a presença dos comandos usados. A ausência de qualquer um encerra
#   o script com código 2.
# -----------------------------------------------------------------------------
require_dependencies() {
    local command_name
    for command_name in jq find cp mv rm mkdir mktemp realpath df tail nproc; do
        command -v "$command_name" >/dev/null 2>&1 ||
            fatal 2 "Required command not found: %s." "$command_name"
    done
    scratch_dir=$(mktemp -d) || fatal 2 "Cannot create a temporary directory."
    scratch_file="$scratch_dir/listing"
}

# -----------------------------------------------------------------------------
# Função: load_configuration
#
# Descrição:
#   Lê e valida o JSON e preenche: destination/dest_root/dest_week, batchsize,
#   jobs, roots e os arrays mapping_*. Usa apenas duas chamadas jq (configurações
#   gerais e mapeamentos de cada categoria), mais uma por categoria.
#
# Etapas:
#   1. O arquivo deve conter um único objeto JSON.
#   2. destination vem de --destination ou local_config.destination; relativo
#      é resolvido a partir do diretório atual; o caminho canônico (realpath -m,
#      sem exigir existência) é gravado em dest_root.
#   3. batchsize (local_config.batchsize, senão minio_config.batchsize, senão
#      100) e jobs (--jobs, local_config.jobs ou min(nproc, 8)).
#   4. Para cada categoria selecionada, a base vem da opção de linha de comando
#      ou de <categoria>_folder; base/week_dir deve existir e seu caminho
#      canônico vai para roots[categoria].
#   5. O destino não pode coincidir nem se sobrepor a uma raiz semanal.
#   6. folder_mapping[categoria] deve ser uma lista não vazia de objetos válidos;
#      mapeamentos repetidos são ignorados com log INFO.
#
# Retorno:
#   Qualquer violação encerra o script com código 2 (via fatal).
# -----------------------------------------------------------------------------
load_configuration() {
    local category base source pattern mapped key config_json root i
    local need_text=false need_teleparser=false cfg_base_text="" cfg_base_teleparser=""
    local source_overlap
    [[ -f $config_file && -r $config_file ]] ||
        fatal 2 "Configuration is not a readable file: %q." "$config_file"
    config_json=$(jq -cse 'if length == 1 and (.[0] | type == "object")
        then .[0] else error("Expected one configuration object") end' -- "$config_file") ||
        fatal 2 "Invalid JSON configuration."
    for category in "${categories[@]}"; do
        if [[ $category == text ]] && (( ! text_explicit )); then need_text=true; fi
        if [[ $category == teleparser ]] && (( ! teleparser_explicit )); then need_teleparser=true; fi
    done
    # Campos de categorias não selecionadas ou sobrescritas não são lidos.
    # $text, $l etc. são variáveis jq, não expansões do shell.
    # shellcheck disable=SC2016
    collect_zero jq -j --argjson text "$need_text" --argjson teleparser "$need_teleparser" '
        def optstr($v; $n):
            if $v == null then ""
            elif ($v | type == "string" and length > 0 and index("\u0000") == null) then $v
            else error("Invalid " + $n) end;
        def optint($v; $n; $max):
            if $v == null then ""
            elif ($v | type == "number" and . > 0 and . == floor and . <= $max) then ($v | tostring)
            else error("Invalid " + $n) end;
        (.local_config // {}) as $l | (.minio_config // {}) as $m |
        [ optstr($l.destination; "local_config.destination"),
          optint($l.batchsize // $m.batchsize; "batchsize"; 2147483647),
          optint($l.jobs; "local_config.jobs"; 256),
          (if $text then optstr(.text_folder; "text_folder") else "" end),
          (if $teleparser then optstr(.teleparser_folder; "teleparser_folder") else "" end)
        ] | .[] | ., "\u0000"' <<< "$config_json" || fatal 2 "Invalid general settings."
    destination=${records[0]}
    [[ -z ${records[1]} ]] || batchsize=${records[1]}
    cfg_base_text=${records[3]} cfg_base_teleparser=${records[4]}
    if (( jobs_explicit )); then
        jobs=$((10#$jobs_override))
    elif [[ -n ${records[2]} ]]; then
        jobs=${records[2]}
    else
        jobs=$(nproc 2>/dev/null) || jobs=1
        [[ $jobs =~ ^[0-9]+$ ]] && (( jobs >= 1 )) || jobs=1
        (( jobs <= DEFAULT_MAX_JOBS )) || jobs=$DEFAULT_MAX_JOBS
    fi

    (( ! destination_explicit )) || destination=$destination_override
    [[ -n $destination ]] ||
        fatal 2 "Local destination is required (--destination or local_config.destination)."
    [[ $destination == /* ]] || destination="$PWD/$destination"
    normalize_path "$destination" 0 || fatal 2 "Invalid destination path."
    destination=$normalized
    [[ $destination != / ]] || fatal 2 "Destination must not be the filesystem root."
    has_week_component "$destination" && fatal 2 "Destination already contains the week."
    if ! collect_zero realpath -m -z -- "$destination" || (( ${#records[@]} != 1 )); then
        fatal 2 "Cannot resolve destination: %q." "$destination"
    fi
    dest_root=${records[0]}
    dest_week="$dest_root/$week_dir"

    jq -e '.folder_mapping | type == "object"' <<< "$config_json" >/dev/null ||
        fatal 2 "folder_mapping must be an object grouped by category."
    for category in "${categories[@]}"; do
        if [[ $category == text ]]; then
            if (( text_explicit )); then base=$text_override; else base=$cfg_base_text; fi
        else
            if (( teleparser_explicit )); then base=$teleparser_override; else base=$cfg_base_teleparser; fi
        fi
        [[ -n $base ]] || fatal 2 "Missing source base for %s." "$category"
        normalize_path "$base" 0 || fatal 2 "Invalid %s base path." "$category"
        base=$normalized
        [[ ${base##*/} != "$week_dir" ]] ||
            fatal 2 "%s source must be a base directory, not the week directory." "$category"
        root="${base%/}/$week_dir"
        if [[ ! -d $root || ! -r $root || ! -x $root ]] ||
            ! collect_zero realpath -e -z -- "$root" || (( ${#records[@]} != 1 )); then
            fatal 2 "Weekly directory is missing or inaccessible: %q." "$root"
        fi
        roots["$category"]=${records[0]}
        log INFO "Selected %s source: %q." "$category" "${roots[$category]}"
        source_overlap=${roots[$category]}
        if [[ $dest_week == "$source_overlap" || $dest_week == "$source_overlap/"* ||
            $source_overlap == "$dest_week/"* ]]; then
            fatal 2 "Destination overlaps the %s source directory." "$category"
        fi
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
            end' <<< "$config_json" || fatal 2 "Invalid mappings for %s." "$category"
        local -a fields=("${records[@]}")
        for ((i=0; i<${#fields[@]}; i+=3)); do
            source=${fields[i]} pattern=${fields[i+1]} mapped=${fields[i+2]}
            normalize_path "$source" 1 || fatal 2 "Invalid source mapping: %q." "$source"
            source=$normalized
            normalize_path "$mapped" 1 || fatal 2 "Invalid destination mapping: %q." "$mapped"
            mapped=$normalized
            [[ $pattern != */* && $pattern != . && $pattern != .. ]] ||
                fatal 2 "Invalid filename pattern: %q." "$pattern"
            has_week_component "$mapped" && fatal 2 "Mapping destination already contains the week."
            key="$category"$'\x01'"$source"$'\x01'"$pattern"$'\x01'"$mapped"
            if [[ -n ${mapping_seen[$key]+present} ]]; then
                log INFO "Ignoring duplicate mapping: %q." "$source"
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
# Função: expand_glob
#
# Descrição:
#   Expande o source_folder (componentes podem ter *, ? ou [) a partir da raiz
#   semanal usando apenas o glob do Bash, sem processos externos. Executada em
#   subshell (via substituição de processo), portanto o "cd" não afeta o
#   restante do script. Imprime, separados por NUL, os caminhos "./relativo"
#   existentes. Nomes iniciados por "." só casam se o componente também iniciar.
#
# Argumentos:
#   $1 - Diretório semanal canônico.  $2 - source_folder normalizado.
# -----------------------------------------------------------------------------
# shellcheck disable=SC2317
expand_glob() {
    local match
    cd -- "$1" 2>/dev/null || exit 1
    shopt -s nullglob
    IFS=
    # IFS vazio impede divisão em palavras, mas mantém a expansão de curingas.
    # shellcheck disable=SC2231
    for match in ./$2; do
        [[ -e $match ]] && printf '%s\0' "$match"
    done
}

# -----------------------------------------------------------------------------
# Função: audit_teleparser
#
# Descrição:
#   Regra da categoria teleparser: todo diretório de segundo nível abaixo da
#   raiz semanal ("primeiro/segundo") deve casar, como glob, com o source_folder
#   de ao menos um mapeamento teleparser. Usa uma única chamada "find -L".
#   Diretórios sem mapeamento geram erro e bloqueiam a categoria: seus
#   mapeamentos passam a contar como pendentes e nada dela é copiado, mas as
#   demais categorias continuam. Não faz nada se teleparser não foi selecionada.
# -----------------------------------------------------------------------------
audit_teleparser() {
    [[ -n ${roots[teleparser]+present} ]] || return 0
    local root=${roots[teleparser]} relative index mapped before=$failures
    if ! collect_zero find -L "$root" -mindepth 2 -maxdepth 2 -type d -printf '%P\0'; then
        error "Directory discovery failed: %q." "$root"
        blocked[teleparser]=1
        return
    fi
    for relative in "${records[@]}"; do
        mapped=0
        for index in "${!mapping_sources[@]}"; do
            [[ ${mapping_categories[index]} == teleparser ]] || continue
            # shellcheck disable=SC2053
            if [[ $relative == ${mapping_sources[index]} ]]; then mapped=1; break; fi
        done
        (( mapped )) || error "Unmapped Teleparser directory: %q." "$relative"
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
#   Registra um arquivo como candidato à cópia.
#
# Argumentos:
#   $1 - Caminho canônico completo da origem.  $2 - Tamanho em bytes.
#   $3 - Nome do arquivo.                      $4 - Diretório de destino.
#
# Regras:
#   - A origem conta uma única vez em files_found.
#   - Alvo duplicado com a mesma origem é ignorado (INFO); com origem diferente
#     é colisão e erro (a execução aborta antes de copiar).
#   - Arquivos aceitos entram nos arrays paralelos e o diretório entra em
#     destinations, preservando a ordem da primeira ocorrência.
# -----------------------------------------------------------------------------
add_candidate() {
    local file=$1 size=$2 name=$3 directory=$4 target index
    target="$directory/$name"
    if [[ -z ${found_seen[$file]+present} ]]; then
        found_seen["$file"]=1
        files_found=$((files_found + 1))
    fi
    if [[ -n ${target_index[$target]+present} ]]; then
        index=${target_index[$target]}
        if [[ ${src_list[index]} != "$file" ]]; then
            collisions=$((collisions + 1))
            error "Destination collision: %q from %q and %q." "$target" "${src_list[index]}" "$file"
        else
            log INFO "Ignoring duplicate source/target: %q." "$target"
        fi
        return
    fi
    target_index["$target"]=${#src_list[@]}
    src_list+=("$file") size_list+=("$size") name_list+=("$name") tdir_list+=("$directory")
    files_selected=$((files_selected + 1))
    if [[ -z ${destination_seen[$directory]+present} ]]; then
        destination_seen["$directory"]=1
        destinations+=("$directory")
    fi
}

# -----------------------------------------------------------------------------
# Função: scan_entries
#
# Descrição:
#   Lista, com UMA chamada "find -printf" para todos os caminhos informados, os
#   arquivos que casam com current_pattern e os registra com add_candidate. O
#   find já informa tipo e tamanho, evitando "stat" por arquivo. Entradas
#   iniciadas por "." só casam se o padrão também iniciar por ".". Links
#   simbólicos não são seguidos: geram erro.
#
# Argumentos:
#   $1 - Profundidade (1 = filhos de diretórios; 0 = os próprios arquivos).
#   $2.. - Caminhos canônicos.
#
# Efeitos:
#   Incrementa matched_count; usa current_dest_dir e current_pattern.
# -----------------------------------------------------------------------------
scan_entries() {
    local depth=$1 first=$2 index parent meta name
    shift
    local -a expression=(-name "$current_pattern")
    [[ $current_pattern == .* ]] || expression+=(! -name '.*')
    collect_zero_buffered find -H "$@" -mindepth "$depth" -maxdepth "$depth" \
        \( -type f -o -type l \) "${expression[@]}" -printf '%h\0%y %s\0%f\0' ||
        error "File discovery failed under %q." "$first"
    for ((index=0; index+2<${#records[@]}; index+=3)); do
        parent=${records[index]} meta=${records[index+1]} name=${records[index+2]}
        if [[ ${meta%% *} == l ]]; then
            error "Symbolic link not followed: %q." "$parent/$name"
            continue
        fi
        matched_count=$((matched_count + 1))
        add_candidate "$parent/$name" "${meta#* }" "$name" "$current_dest_dir"
    done
}

# -----------------------------------------------------------------------------
# Função: discover_mapping
#
# Descrição:
#   Descobre os arquivos de um mapeamento: expande o source_folder (glob do
#   Bash), resolve todos os caminhos com um único "realpath", exige contenção na
#   raiz semanal e lista os arquivos com scan_entries. Se source_folder aponta
#   para um arquivo, ele próprio é o candidato.
#
# Argumentos:
#   $1 - Índice nos arrays mapping_*.
# -----------------------------------------------------------------------------
discover_mapping() {
    local index=$1 category root source entry candidate
    category=${mapping_categories[index]}
    root=${roots[$category]} source=${mapping_sources[index]}
    current_pattern=${mapping_patterns[index]}
    current_dest_dir="$dest_week/${mapping_destinations[index]}"
    matched_count=0
    local -a relative=() absolute=() directories=() single_files=()
    local -A seen=()
    log INFO "Mapping %s: %q [%q] -> %q." \
        "$category" "$source" "$current_pattern" "$current_dest_dir"
    mapfile -d '' -t relative < <(expand_glob "$root" "$source")
    for entry in "${relative[@]}"; do absolute+=("$root/${entry#./}"); done
    if (( ${#absolute[@]} )); then
        if ! collect_zero realpath -e -z -- "${absolute[@]}" ||
            (( ${#records[@]} != ${#absolute[@]} )); then
            error "Cannot resolve mapped paths (possibly a broken link): %q." "$root/$source"
        else
            for candidate in "${records[@]}"; do
                if [[ $candidate != "$root" && $candidate != "$root/"* ]]; then
                    error "Path escapes the weekly root: %q." "$candidate"
                    continue
                fi
                [[ -z ${seen[$candidate]+present} ]] || continue
                seen["$candidate"]=1
                if [[ -d $candidate ]]; then
                    directories+=("$candidate")
                elif [[ -f $candidate ]]; then
                    single_files+=("$candidate")
                fi
            done
        fi
    fi
    (( ${#directories[@]} == 0 )) || scan_entries 1 "${directories[@]}"
    (( ${#single_files[@]} == 0 )) || scan_entries 0 "${single_files[@]}"
    if (( matched_count == 0 )); then
        pending_mappings=$((pending_mappings + 1))
        log WARNING "Mapping is absent or has no matching files: %q." "$root/$source"
    fi
}

# -----------------------------------------------------------------------------
# Função: discover_files
#
# Descrição:
#   Executa audit_teleparser e, para cada mapeamento, discover_mapping.
#   Mapeamentos de categoria bloqueada contam como pendentes e não são
#   processados. Ao final, qualquer colisão de destino encerra com código 1,
#   antes de qualquer escrita.
# -----------------------------------------------------------------------------
discover_files() {
    local index
    audit_teleparser
    for index in "${!mapping_sources[@]}"; do
        if [[ -n ${blocked[${mapping_categories[index]}]+present} ]]; then
            pending_mappings=$((pending_mappings + 1))
            continue
        fi
        discover_mapping "$index"
    done
    (( collisions == 0 )) || fatal 1 "Local destination collisions detected; no files were copied."
}

# -----------------------------------------------------------------------------
# Função: load_existing
#
# Descrição:
#   Lista, com uma única chamada "find", as entradas imediatas de todos os
#   diretórios de destino já existentes e preenche existing_meta
#   (caminho completo -> "tipo tamanho"). Substitui o conteúdo anterior.
#
# Retorno:
#   0 - Listagem válida (inclusive sem diretórios existentes). 1 - Falha do find.
# -----------------------------------------------------------------------------
load_existing() {
    local index directory status=0
    local -a present=()
    existing_meta=()
    for directory in "${destinations[@]}"; do
        [[ ! -d $directory ]] || present+=("$directory")
    done
    (( ${#present[@]} )) || return 0
    collect_zero_buffered find -H "${present[@]}" -mindepth 1 -maxdepth 1 \
        -printf '%h\0%y %s\0%f\0' || status=$?
    for ((index=0; index+2<${#records[@]}; index+=3)); do
        existing_meta["${records[index]}/${records[index+2]}"]=${records[index+1]}
    done
    return "$status"
}

# -----------------------------------------------------------------------------
# Função: plan_transfers
#
# Descrição:
#   Compara cada arquivo selecionado com o destino existente. Mesmo tipo
#   regular e tamanho: conta em files_existing e não é recopiado. Qualquer
#   outra entrada com o mesmo nome: erro, sem sobrescrever. Os pendentes são
#   agrupados em lotes (batch_dirs/batch_items) que fecham ao atingir
#   batchsize arquivos, BATCH_MAX_BYTES ou BATCH_MAX_CHARS. Preenche
#   planned_items, files_planned, bytes_planned e batches_planned.
# -----------------------------------------------------------------------------
plan_transfers() {
    local directory index key meta
    local -A cur_items=() cur_count=() cur_bytes=() cur_chars=()
    load_existing || fatal 1 "Cannot list the destination directories."
    for index in "${!src_list[@]}"; do
        directory=${tdir_list[index]}
        key="$directory/${name_list[index]}"
        meta=${existing_meta[$key]-}
        if [[ -n $meta ]]; then
            if [[ $meta == "f ${size_list[index]}" ]]; then
                files_existing=$((files_existing + 1))
            else
                error "Destination conflict; not overwritten: %q." "$key"
            fi
            continue
        fi
        planned_items+=("$index")
        bytes_planned=$((bytes_planned + size_list[index]))
        # Os lotes são montados por diretório; cada string tem no máximo um lote,
        # evitando a concatenação quadrática de uma lista com todos os arquivos.
        cur_items["$directory"]+=" $index"
        cur_count["$directory"]=$((${cur_count[$directory]-0} + 1))
        cur_bytes["$directory"]=$((${cur_bytes[$directory]-0} + size_list[index]))
        cur_chars["$directory"]=$((${cur_chars[$directory]-0} + ${#src_list[index]} + 1))
        if (( cur_count[$directory] >= batchsize || cur_bytes[$directory] >= BATCH_MAX_BYTES ||
            cur_chars[$directory] >= BATCH_MAX_CHARS )); then
            batch_dirs+=("$directory") batch_items+=("${cur_items[$directory]}")
            cur_items["$directory"]="" cur_count["$directory"]=0
            cur_bytes["$directory"]=0 cur_chars["$directory"]=0
        fi
    done
    for directory in "${destinations[@]}"; do
        if (( ${cur_count[$directory]-0} > 0 )); then
            batch_dirs+=("$directory") batch_items+=("${cur_items[$directory]}")
        fi
    done
    files_planned=${#planned_items[@]}
    batches_planned=${#batch_dirs[@]}
}

# -----------------------------------------------------------------------------
# Função: check_free_space
#
# Descrição:
#   Compara bytes_planned com o espaço livre do sistema de arquivos do destino
#   (do ancestral existente mais próximo). Espaço insuficiente encerra com
#   código 1 antes de copiar; em --dry-run vira apenas WARNING. Se o df falhar,
#   a verificação é ignorada com WARNING.
# -----------------------------------------------------------------------------
check_free_space() {
    local ancestor=$dest_root available
    (( bytes_planned > 0 )) || return 0
    while [[ ! -d $ancestor && $ancestor != / ]]; do ancestor=${ancestor%/*}; [[ -n $ancestor ]] || ancestor=/; done
    available=$(df --output=avail -B1 -- "$ancestor" 2>/dev/null | tail -n 1)
    available=${available//[[:space:]]/}
    if [[ ! $available =~ ^[0-9]+$ ]]; then
        log WARNING "Cannot determine free space for %q; check skipped." "$ancestor"
        return 0
    fi
    if (( bytes_planned > available )); then
        if (( dry_run )); then
            log WARNING "Insufficient free space: need %s bytes, %s available." "$bytes_planned" "$available"
        else
            fatal 1 "Insufficient free space: need %s bytes, %s available." "$bytes_planned" "$available"
        fi
    fi
}

# -----------------------------------------------------------------------------
# Função: copy_batch
#
# Descrição:
#   Worker executado em segundo plano (subshell) para um lote: copia os
#   arquivos com um único "cp -t" para uma pasta temporária do próprio destino
#   e move o resultado para o diretório final com "mv -n" (rename atômico, sem
#   sobrescrever). O sucesso real é decidido depois, pela verificação de nomes e
#   tamanhos em verify_transfers; aqui apenas se retorna o status do cp/mv.
#   Recebe SIGTERM de stop_workers e o repassa ao cp.
#
# Argumentos:
#   $1 - Diretório de destino.  $2 - Número do lote.  $3.. - Índices dos arquivos.
# -----------------------------------------------------------------------------
copy_batch() {
    local directory=$1 stage="$1/.partial.$$.$2" index status=0 cp_pid
    shift 2
    local -a files=()
    for index in "$@"; do files+=("${src_list[index]}"); done
    mkdir -p -- "$stage" || exit 1
    cp --reflink=auto --preserve=timestamps -t "$stage" -- "${files[@]}" &
    cp_pid=$!
    trap 'kill "$cp_pid" 2>/dev/null; exit 143' TERM
    wait "$cp_pid" || status=$?
    find "$stage" -mindepth 1 -maxdepth 1 -exec mv -n -t "$directory" -- {} + || status=$?
    rm -rf -- "$stage"
    exit "$status"
}

# -----------------------------------------------------------------------------
# Função: run_transfers
#
# Descrição:
#   Cria os diretórios de destino (uma chamada "mkdir -p") e executa os lotes
#   com no máximo "jobs" workers simultâneos, usando "wait -n" para repor a
#   vaga assim que um termina. Ao final aguarda todos os workers.
# -----------------------------------------------------------------------------
run_transfers() {
    local number directory
    local -a needed=()
    local -A created=()
    for directory in "${batch_dirs[@]}"; do
        if [[ -z ${created[$directory]+present} ]]; then
            created["$directory"]=1
            needed+=("$directory")
        fi
    done
    (( ${#needed[@]} )) || return 0
    mkdir -p -- "${needed[@]}" || fatal 1 "Cannot create destination directories."
    copy_started=1
    for number in "${!batch_dirs[@]}"; do
        if (( running >= jobs )); then
            wait -n
            running=$((running - 1))
        fi
        batches_launched=$((batches_launched + 1))
        log INFO "Batch %s/%s -> %q." "$batches_launched" "$batches_planned" "${batch_dirs[number]}"
        # Índices sem aspas: lista de inteiros separados por espaço.
        # shellcheck disable=SC2086
        copy_batch "${batch_dirs[number]}" "$batches_launched" ${batch_items[number]} &
        running=$((running + 1))
    done
    wait
    running=0
}

# -----------------------------------------------------------------------------
# Função: verify_transfers
#
# Descrição:
#   Confirma o resultado com uma nova listagem única do destino. Cada arquivo
#   planejado só conta em files_transferred se existir como arquivo regular com
#   o tamanho registrado na descoberta. Ausente: erro "Transfer not confirmed".
#   Com tamanho diferente (cópia parcial ou origem alterada durante a cópia):
#   erro e remoção do arquivo, que foi criado por esta execução.
# -----------------------------------------------------------------------------
verify_transfers() {
    local index key meta
    local -a invalid=()
    load_existing || { error "Cannot list the destination directories for verification."; return; }
    for index in "${planned_items[@]}"; do
        key="${tdir_list[index]}/${name_list[index]}"
        meta=${existing_meta[$key]-}
        if [[ $meta == "f ${size_list[index]}" ]]; then
            files_transferred=$((files_transferred + 1))
            bytes_transferred=$((bytes_transferred + size_list[index]))
        elif [[ -z $meta ]]; then
            error "Transfer not confirmed: %q." "$key"
        else
            error "Size mismatch after copy (source changed or partial copy); removed: %q." "$key"
            [[ $meta != f\ * ]] || invalid+=("$key")
        fi
    done
    (( ${#invalid[@]} == 0 )) || rm -f -- "${invalid[@]}"
}

# -----------------------------------------------------------------------------
# Função: main
#
# Descrição:
#   Orquestra: parse_arguments, require_dependencies, load_configuration,
#   discover_files, plan_transfers, check_free_space e, conforme --dry-run,
#   impressão do plano ou run_transfers + verify_transfers.
#
# Status final (final_status):
#   DRY_RUN          - --dry-run informado (independe de falhas).
#   SUCCESS          - Sem falhas e sem mapeamentos pendentes.
#   PARTIAL_SUCCESS  - Há falhas ou pendências, mas ao menos um arquivo foi
#                      copiado ou já existia.
#   FAILED           - Demais casos.
#
# Retorno:
#   0 se failures e pending_mappings são ambos 0; 1 caso contrário.
# -----------------------------------------------------------------------------
main() {
    local index
    parse_arguments "$@"
    log INFO "Starting local CDR copy for %s (dry_run=%s)." "$week_dir" "$dry_run"
    require_dependencies
    load_configuration
    log INFO "Destination: %q (jobs=%s, batchsize=%s)." "$dest_week" "$jobs" "$batchsize"
    discover_files
    plan_transfers
    check_free_space
    if (( dry_run )); then
        for index in "${planned_items[@]}"; do
            printf 'PLAN %q -> %q\n' "${src_list[index]}" "${tdir_list[index]}/${name_list[index]}"
        done
        final_status=DRY_RUN
    else
        run_transfers
        (( ${#planned_items[@]} == 0 )) || verify_transfers
        if (( failures == 0 && pending_mappings == 0 )); then
            final_status=SUCCESS
        elif (( files_transferred + files_existing > 0 )); then
            final_status=PARTIAL_SUCCESS
        else
            final_status=FAILED
        fi
    fi
    (( failures == 0 && pending_mappings == 0 ))
}

main "$@"
exit "$?"
