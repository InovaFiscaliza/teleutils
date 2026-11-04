#!/usr/bin/env bash

# =============================================================================
# Descrição:
#   Versão de alto desempenho de upload_cdr_to_minio.sh. Envia para o MinIO
#   (via cliente "mc") os arquivos de CDR de uma semana, nas categorias "text"
#   e "teleparser", direto das origens e sem etapa de staging local. Usa os
#   mesmos argumentos, o mesmo arquivo de configuração JSON e as mesmas regras
#   de seleção, destino e segurança do script original (que não é alterado).
#
# Uso:
#   upload_cdr_to_minio_parallel.sh --config ARQUIVO --week-dir SemanaNUMERO [OPÇÕES]
#
# Argumentos obrigatórios:
#   --config ARQUIVO       Arquivo JSON de configuração (um único objeto).
#   --week-dir NOME        Nome da semana, no formato "Semana" + dígitos.
#
# Opções:
#   --teleparser-folder CAMINHO  Substitui o diretório BASE da categoria teleparser.
#   --text-folder CAMINHO        Substitui o diretório BASE da categoria text.
#   --destination CAMINHO        Substitui bucket/prefixo de destino (sem alias do mc).
#   --jobs N                     Processos "mc cp" simultâneos (1 a 256).
#                                Substitui minio_config.jobs. Padrão: 3.
#   --multipart-threads N        Threads de multipart por "mc cp" (1 a 256);
#                                exportadas como MC_UPLOAD_MULTIPART_THREADS.
#                                Substitui minio_config.multipart_threads.
#                                Padrão: o valor já presente no ambiente ou 16.
#   --dry-run                    Planeja e consulta o MinIO, sem copiar arquivos.
#   --help, -h                   Exibe a ajuda e encerra com código 0.
#   Cada opção com valor pode ser informada uma única vez e exige valor
#   não vazio que não comece com "--".
#
# Seleção de categorias (idêntica ao script original):
#   Sem --teleparser-folder e sem --text-folder: processa text e teleparser.
#   Somente --teleparser-folder: processa apenas teleparser.
#   Somente --text-folder: processa apenas text.
#   Ambas: processa as duas, usando os dois caminhos informados.
#   Os campos de configuração de uma categoria não selecionada não são lidos.
#
# Configuração JSON (campos consumidos):
#   teleparser_folder, text_folder   Diretórios BASE de origem; o script acrescenta
#                                    --week-dir a cada base.
#   minio_config.alias               Alias do mc (letra inicial; letras, dígitos,
#                                    "_" e "-").
#   minio_config.destination         Bucket/prefixo; ignorado com --destination.
#   minio_config.batchsize           Opcional (1 a 2147483647): máximo de arquivos
#                                    por chamada "mc cp". Padrão: 500.
#   minio_config.jobs                Opcional (1 a 256): "mc cp" simultâneos.
#   minio_config.multipart_threads   Opcional (1 a 256): threads de multipart.
#   folder_mapping.<categoria>[]     Lista não vazia de objetos com
#                                    source_folder, destination_folder e pattern.
#   local_config.*                   Ignorado (usado por copy_cdr_to_local.sh).
#
# Variáveis de ambiente:
#   MC_CONFIG_DIR                   Diretório de configuração do mc (padrão $HOME/.mc).
#   MC_UPLOAD_MULTIPART_THREADS     Usada como padrão quando --multipart-threads e
#                                   minio_config.multipart_threads estão ausentes.
#   LC_ALL                          Definida como C pelo próprio script (export).
#
# Dependências:
#   Bash 4.4 ou superior, jq, mc (MinIO Client) e as ferramentas GNU find,
#   realpath, mktemp e rm. O mc precisa estar previamente inicializado
#   (config.json com version "10", diretórios share, certs e certs/CAs e o alias
#   configurado com URL http/https); o script não cria nem migra o estado do mc.
#
# Por que é mais rápido que upload_cdr_to_minio.sh:
#   Medições no ambiente alvo mostraram que o limite é por conexão (um arquivo
#   grande com 4 threads: ~60 MiB/s; com 16 threads: ~120 MiB/s) e que o "mc cp"
#   já paraleliza internamente vários arquivos. O script original perdia tempo
#   com o link ocioso entre lotes sequenciais, com um "mc ls" por lote e com
#   "realpath"/"stat" por arquivo na origem (NFS). Aqui:
#   - até --jobs "mc cp" rodam ao mesmo tempo (o link não fica ocioso nas caudas
#     dos lotes) e cada um usa --multipart-threads threads;
#   - os lotes fecham por quantidade (batchsize), por bytes (BATCH_MAX_BYTES) ou
#     por tamanho da linha de comando; lotes grandes reduzem o custo de iniciar
#     o "mc";
#   - o remoto é listado UMA vez ("mc ls --recursive") antes e UMA vez depois do
#     envio para confirmar nomes e tamanhos, em vez de uma listagem por lote;
#   - a descoberta não cria processos por arquivo: glob do Bash, um
#     "find -printf" (com tipo e tamanho) e um "realpath" por mapeamento, com
#     leitura em bloco via arquivo temporário e "mapfile".
#
# Regras principais (iguais às do script original):
#   - Alvo remoto: alias/bucket/prefixo/SEMANA/destination_folder/arquivo.
#   - Todo diretório selecionado deve permanecer, após resolução de links
#     simbólicos, dentro do diretório semanal da categoria.
#   - Apenas arquivos regulares são enviados. Links simbólicos que casam com o
#     padrão não são seguidos: são reportados como erro.
#   - Objetos remotos existentes nunca são sobrescritos: mesmo tamanho conta como
#     existente (execução idempotente) e tamanho diferente é erro.
#   - Na categoria teleparser, todo diretório de segundo nível abaixo da semana
#     deve corresponder a algum source_folder mapeado; caso contrário a categoria
#     é bloqueada e seus mapeamentos ficam pendentes.
#   - Mapeamento ausente ou sem arquivos correspondentes conta como pendente.
#   - Os arquivos de origem não são modificados.
#   - Uma falha de "mc cp" não é tratada como definitiva: a confirmação vem da
#     listagem final. Para reenviar o que faltou, basta executar de novo.
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
#   upload_cdr_to_minio_parallel.sh --config config.json --week-dir Semana111 \
#     --jobs 3 --multipart-threads 16 --dry-run
# =============================================================================

if (( BASH_VERSINFO[0] < 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] < 4) )); then
    printf 'ERROR: Bash 4.4 or newer is required.\n' >&2
    exit 2
fi
set -o pipefail
shopt -u nocasematch
export LC_ALL=C

# Limites de um lote: o que ocorrer primeiro fecha o lote. O limite de bytes
# equilibra a carga entre os "mc cp"; o de caracteres protege o ARG_MAX.
readonly BATCH_MAX_BYTES=4294967296
readonly BATCH_MAX_CHARS=65536
readonly DEFAULT_BATCHSIZE=500
readonly DEFAULT_JOBS=3
readonly DEFAULT_MULTIPART_THREADS=16

# Estado global.
# - src_list, size_list, name_list, tdir_list e rkey_list são arrays paralelos:
#   o mesmo índice descreve um único arquivo selecionado. tdir_list guarda o
#   diretório remoto completo; rkey_list, a chave relativa à semana.
# - mapping_* são arrays paralelos: um índice por mapeamento válido e único.
# - batch_dirs e batch_items são paralelos: um índice por lote planejado;
#   batch_items guarda índices de arquivos separados por espaço.
# - records funciona como "valor de retorno" de collect_zero.
config_file="" week_dir="" destination_override="" jobs_override="" threads_override=""
teleparser_override="" text_override=""
teleparser_explicit=0 text_explicit=0 destination_explicit=0
jobs_explicit=0 threads_explicit=0 dry_run=0
minio_alias="" destination="" destination_base="" mc_config_dir=""
batchsize=$DEFAULT_BATCHSIZE jobs=$DEFAULT_JOBS multipart_threads=$DEFAULT_MULTIPART_THREADS
started_at=$SECONDS report_enabled=0 final_status="" running=0
files_found=0 files_selected=0 files_existing=0 files_planned=0
files_transferred=0 failures=0 pending_mappings=0 collisions=0
batches_planned=0 batches_launched=0 bytes_planned=0 bytes_transferred=0
matched_count=0 current_dest_dir="" current_rel_dir="" current_pattern=""
scratch_dir="" normalized=""
declare -a categories=() records=()
declare -a mapping_categories=() mapping_sources=() mapping_patterns=() mapping_destinations=()
declare -a src_list=() size_list=() name_list=() tdir_list=() rkey_list=()
declare -a batch_dirs=() batch_items=() planned_items=()
declare -A roots=() blocked=() option_seen=() mapping_seen=()
declare -A found_seen=() target_index=() remote_meta=()

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
    printf '%s\n' 'Usage: upload_cdr_to_minio_parallel.sh --config FILE --week-dir SemanaNUMBER [OPTIONS]

Required:
  --config FILE               JSON configuration file (same as upload_cdr_to_minio.sh).
  --week-dir NAME             Week name: Semana followed by digits.

Options:
  --teleparser-folder PATH    Override the Teleparser BASE directory.
  --text-folder PATH          Override the text BASE directory.
  --destination PATH          Override the bucket/prefix, WITHOUT the mc alias.
  --jobs N                    Concurrent "mc cp" processes (1-256).
                              Overrides minio_config.jobs. Default: 3.
  --multipart-threads N       Multipart threads per "mc cp" (1-256), exported as
                              MC_UPLOAD_MULTIPART_THREADS. Overrides
                              minio_config.multipart_threads. Default: the value
                              already in the environment, or 16.
  --dry-run                   Plan and query MinIO, without copying or writing.
  --help, -h                  Show this help and exit successfully.

Source selection is based on explicitly supplied options:
  Neither source option: process both categories from JSON.
  Only --teleparser-folder: process ONLY Teleparser.
  Only --text-folder: process ONLY text.
  Both source options: process both, using the two CLI paths.

Source paths are BASE directories, not complete weekly directories.
Targets: alias/bucket/prefix/week/mapped-destination/filename.
Existing objects are never overwritten; same-size objects are skipped.

Example:
  upload_cdr_to_minio_parallel.sh --config config.json --week-dir Semana111 \
    --jobs 3 --multipart-threads 16 --dry-run

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
#   contadores de arquivos e bytes, falhas, pendências, lotes, paralelismo,
#   duração e status final.
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
    printf 'destination_base: %q\n' "${destination_base:-UNRESOLVED}"
    printf 'files_found: %s\nfiles_selected: %s\n' "$files_found" "$files_selected"
    printf 'files_existing: %s\nfiles_planned: %s\n' "$files_existing" "$files_planned"
    printf 'files_transferred: %s\nfailures: %s\n' "$files_transferred" "$failures"
    printf 'bytes_planned: %s\nbytes_transferred: %s\n' "$bytes_planned" "$bytes_transferred"
    printf 'pending_mappings: %s\n' "$pending_mappings"
    printf 'batches_planned: %s\nbatches_launched: %s\n' "$batches_planned" "$batches_launched"
    printf 'jobs: %s\nmultipart_threads: %s\n' "$jobs" "$multipart_threads"
    printf 'duration_seconds: %s\nstatus: %s\n' "$((SECONDS - started_at))" "$final_status"
}

# -----------------------------------------------------------------------------
# Função: stop_workers
#
# Descrição:
#   Envia SIGTERM aos processos de envio em segundo plano e aguarda seu
#   encerramento. Cada worker repassa o sinal ao "mc cp" que executa.
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
#   Handler do trap EXIT. Remove o diretório temporário e emite o relatório
#   quando habilitado. Não altera o código de saída.
# -----------------------------------------------------------------------------
# shellcheck disable=SC2317
on_exit() {
    local code=$?
    [[ -z $scratch_dir ]] || rm -rf -- "$scratch_dir"
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
#   overrides (e suas flags *_explicit), dry_run e a lista categories.
#
# Regras:
#   - Sem --config/--week-dir é erro de uso; --week-dir casa com ^Semana[0-9]+$.
#   - Opções com valor exigem valor não vazio que não comece com "--" e não
#     podem se repetir; --jobs e --multipart-threads são inteiros de 1 a 256.
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
            --config|--week-dir|--teleparser-folder|--text-folder|--destination|--jobs|--multipart-threads)
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
                    --multipart-threads) threads_override=$2; threads_explicit=1 ;;
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
    if (( threads_explicit )); then
        if [[ ! $threads_override =~ ^[0-9]{1,3}$ ]] || (( 10#$threads_override < 1 || 10#$threads_override > 256 )); then
            argument_error "Invalid --multipart-threads value (integer 1..256 required): %q." "$threads_override"
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
#   O PID é aguardado: EOF não significa que o produtor terminou bem. Para
#   saídas pequenas (configuração, realpath).
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
#   bloco, o que reduz de forma drástica o tempo com dezenas de milhares de
#   arquivos. Preenche records e retorna o status do comando.
# -----------------------------------------------------------------------------
collect_zero_buffered() {
    local status
    "$@" > "$scratch_dir/listing"
    status=$?
    mapfile -d '' -t records < "$scratch_dir/listing"
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
#   Verifica a presença dos comandos usados e cria o diretório temporário
#   (listagens e logs). A ausência de qualquer comando encerra com código 2.
# -----------------------------------------------------------------------------
require_dependencies() {
    local command_name
    for command_name in jq find realpath mktemp rm mc; do
        command -v "$command_name" >/dev/null 2>&1 ||
            fatal 2 "Required command not found: %s." "$command_name"
    done
    scratch_dir=$(mktemp -d) || fatal 2 "Cannot create a temporary directory."
}

# -----------------------------------------------------------------------------
# Função: load_configuration
#
# Descrição:
#   Lê e valida o JSON e preenche: minio_alias, batchsize, jobs,
#   multipart_threads, destination, destination_base, roots e os arrays
#   mapping_*. Usa uma chamada jq para as configurações gerais e uma por
#   categoria para os mapeamentos.
#
# Etapas:
#   1. O arquivo deve conter um único objeto JSON com minio_config objeto.
#   2. alias é obrigatório; batchsize, jobs e multipart_threads são opcionais.
#      Precedência de jobs e threads: opção de linha de comando, JSON, padrão
#      (threads: ambiente MC_UPLOAD_MULTIPART_THREADS e depois 16).
#   3. O destino vem de --destination ou de minio_config.destination; o primeiro
#      componente deve ser um nome de bucket S3, sem alias, URL, "?" ou "#", e
#      sem o componente da semana.
#   4. Para cada categoria selecionada, a base vem da opção de linha de comando
#      ou de <categoria>_folder; base/week_dir precisa existir e seu caminho
#      canônico vai para roots[categoria].
#   5. folder_mapping[categoria] deve ser uma lista não vazia de objetos válidos;
#      mapeamentos repetidos são ignorados com log INFO.
#
# Retorno:
#   Qualquer violação encerra o script com código 2 (via fatal).
# -----------------------------------------------------------------------------
load_configuration() {
    local category base source pattern mapped key config_json root i
    local need_text=false need_teleparser=false need_destination=true
    local cfg_base_text="" cfg_base_teleparser=""
    [[ -f $config_file && -r $config_file ]] ||
        fatal 2 "Configuration is not a readable file: %q." "$config_file"
    config_json=$(jq -cse 'if length == 1 and (.[0] | type == "object")
        then .[0] else error("Expected one configuration object") end' -- "$config_file") ||
        fatal 2 "Invalid JSON configuration."
    jq -e '.minio_config | type == "object"' <<< "$config_json" >/dev/null ||
        fatal 2 "minio_config must be an object."
    for category in "${categories[@]}"; do
        if [[ $category == text ]] && (( ! text_explicit )); then need_text=true; fi
        if [[ $category == teleparser ]] && (( ! teleparser_explicit )); then need_teleparser=true; fi
    done
    (( ! destination_explicit )) || need_destination=false
    # Campos de categorias não selecionadas ou sobrescritas não são lidos.
    # $m, $text etc. são variáveis jq, não expansões do shell.
    # shellcheck disable=SC2016
    collect_zero jq -j --argjson text "$need_text" --argjson teleparser "$need_teleparser" \
        --argjson dest "$need_destination" '
        def optstr($v; $n):
            if $v == null then ""
            elif ($v | type == "string" and length > 0 and index("\u0000") == null) then $v
            else error("Invalid " + $n) end;
        def optint($v; $n; $max):
            if $v == null then ""
            elif ($v | type == "number" and . > 0 and . == floor and . <= $max) then ($v | tostring)
            else error("Invalid " + $n) end;
        .minio_config as $m |
        [ optstr($m.alias; "minio_config.alias"),
          optint($m.batchsize; "minio_config.batchsize"; 2147483647),
          optint($m.jobs; "minio_config.jobs"; 256),
          optint($m.multipart_threads; "minio_config.multipart_threads"; 256),
          (if $dest then optstr($m.destination; "minio_config.destination") else "" end),
          (if $text then optstr(.text_folder; "text_folder") else "" end),
          (if $teleparser then optstr(.teleparser_folder; "teleparser_folder") else "" end)
        ] | .[] | ., "\u0000"' <<< "$config_json" || fatal 2 "Invalid general settings."
    minio_alias=${records[0]}
    [[ $minio_alias =~ ^[a-zA-Z][a-zA-Z0-9_-]*$ ]] || fatal 2 "Invalid MinIO alias."
    [[ -z ${records[1]} ]] || batchsize=${records[1]}
    if (( jobs_explicit )); then
        jobs=$((10#$jobs_override))
    elif [[ -n ${records[2]} ]]; then
        jobs=${records[2]}
    fi
    if (( threads_explicit )); then
        multipart_threads=$((10#$threads_override))
    elif [[ -n ${records[3]} ]]; then
        multipart_threads=${records[3]}
    elif [[ ${MC_UPLOAD_MULTIPART_THREADS:-} =~ ^[0-9]{1,3}$ ]] &&
        (( 10#$MC_UPLOAD_MULTIPART_THREADS >= 1 && 10#$MC_UPLOAD_MULTIPART_THREADS <= 256 )); then
        multipart_threads=$((10#$MC_UPLOAD_MULTIPART_THREADS))
    fi
    if (( destination_explicit )); then destination=$destination_override; else destination=${records[4]}; fi
    cfg_base_text=${records[5]} cfg_base_teleparser=${records[6]}
    [[ -n $destination ]] || fatal 2 "Invalid MinIO destination."
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
            [[ $mapped != *'?'* && $mapped != *'#'* && $mapped != *'://'* ]] ||
                fatal 2 "Destination mappings must be literal object prefixes."
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
#   mapeamentos passam a contar como pendentes e nada dela é enviado, mas as
#   demais categorias continuam. Não faz nada se teleparser não foi selecionada.
# -----------------------------------------------------------------------------
audit_teleparser() {
    [[ -n ${roots[teleparser]+present} ]] || return 0
    local root=${roots[teleparser]} relative index mapped before=$failures
    if ! collect_zero_buffered find -L "$root" -mindepth 2 -maxdepth 2 -type d -printf '%P\0'; then
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
#   Registra um arquivo como candidato ao envio.
#
# Argumentos:
#   $1 - Caminho canônico completo da origem.  $2 - Tamanho em bytes.
#   $3 - Nome do arquivo.
#
# Regras:
#   - A origem conta uma única vez em files_found.
#   - Alvo duplicado com a mesma origem é ignorado (INFO); com origem diferente
#     é colisão e erro (a execução aborta antes de enviar).
#   - Arquivos aceitos entram nos arrays paralelos. O diretório remoto é
#     current_dest_dir e a chave relativa à semana é current_rel_dir/nome.
# -----------------------------------------------------------------------------
add_candidate() {
    local file=$1 size=$2 name=$3 target index
    target="$current_dest_dir/$name"
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
    src_list+=("$file") size_list+=("$size") name_list+=("$name")
    tdir_list+=("$current_dest_dir") rkey_list+=("$current_rel_dir/$name")
    files_selected=$((files_selected + 1))
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
#   Incrementa matched_count; usa current_* como contexto do mapeamento.
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
        add_candidate "$parent/$name" "${meta#* }" "$name"
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
    current_rel_dir=${mapping_destinations[index]}
    current_dest_dir="$destination_base/$week_dir/$current_rel_dir"
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
#   antes de qualquer envio.
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
    (( collisions == 0 )) || fatal 1 "Destination collisions detected; no files were copied."
}

# -----------------------------------------------------------------------------
# Função: capture_mc
#
# Descrição:
#   Executa "mc --config-dir $mc_config_dir ARGS..." capturando stdout e stderr
#   juntos em mc_output. Para respostas pequenas (versão, stat).
#
# Retorno:
#   0 - mc terminou com sucesso.
#   1 - mc falhou; apenas "mc <primeiro argumento> failed (exit N)" é registrado
#       via error, sem reproduzir a saída capturada (pode conter segredos).
# -----------------------------------------------------------------------------
mc_output=""
capture_mc() {
    local code
    mc_output=$(mc --config-dir "$mc_config_dir" "$@" 2>&1)
    code=$?
    if (( code != 0 )); then
        error "mc %s failed (exit %s)." "$1" "$code"
        return 1
    fi
}

# -----------------------------------------------------------------------------
# Função: validate_mc
#
# Descrição:
#   Valida o ambiente do MinIO Client antes de qualquer consulta ou envio
#   (inclusive em --dry-run). Define mc_config_dir como MC_CONFIG_DIR ou
#   $HOME/.mc.
#
# Validações:
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
    # Exigir estado pronto evita escritas implícitas, especialmente no dry-run
    # e com vários "mc" simultâneos.
    jq -e '.version == "10"' -- "$mc_config_dir/config.json" >/dev/null ||
        fatal 2 "mc configuration version 10 is required; initialize or migrate mc separately."
    for subdirectory in share certs certs/CAs; do
        [[ -d $mc_config_dir/$subdirectory && -r $mc_config_dir/$subdirectory &&
            -x $mc_config_dir/$subdirectory ]] ||
            fatal 2 "mc is not fully initialized: missing or inaccessible %s directory." "$subdirectory"
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
#   Lista UMA vez, com "mc ls --recursive --json", todos os objetos do prefixo
#   da semana e preenche remote_meta (chave relativa à semana -> tamanho em
#   bytes). Substitui as listagens por lote do script original. Somente
#   entradas do tipo "file" são registradas. Se o mc devolver a chave com o
#   prefixo completo, ele é removido. Uma falha não é tratada como prefixo vazio.
#
# Retorno:
#   0 - Listagem válida.
#   1 - Falha do mc, JSON inválido, chave com NUL, tamanho inválido ou objeto
#       duplicado (error é chamado).
# -----------------------------------------------------------------------------
load_remote() {
    local prefix="$destination_base/$week_dir" index key size
    remote_meta=()
    if ! mc --config-dir "$mc_config_dir" ls --recursive --json -- "$prefix/" \
        > "$scratch_dir/ls.json" 2>/dev/null; then
        error "Remote listing unavailable: %q." "$prefix"
        return 1
    fi
    if ! jq -s -j '
        if all(.[]; type == "object" and .status == "success"
            and (.type == "file" or .type == "folder")
            and (.key | type == "string" and length > 0 and index("\u0000") == null)
            and (if .type == "file" then
                (.size | type == "number" and . >= 0 and . == floor)
                else true end))
        then .[] | select(.type == "file") | .key, "\u0000", (.size | tostring), "\u0000"
        else error("Invalid object listing") end' \
        < "$scratch_dir/ls.json" > "$scratch_dir/ls.bin"; then
        error "Invalid remote listing: %q." "$prefix"
        return 1
    fi
    mapfile -d '' -t records < "$scratch_dir/ls.bin"
    for ((index=0; index+1<${#records[@]}; index+=2)); do
        key=${records[index]} size=${records[index+1]}
        key=${key#"$prefix/"}
        if [[ -n ${remote_meta[$key]+present} ]]; then
            error "Duplicate object in remote listing: %q." "$prefix/$key"
            return 1
        fi
        remote_meta["$key"]=$size
    done
}

# -----------------------------------------------------------------------------
# Função: plan_transfers
#
# Descrição:
#   Compara cada arquivo selecionado com o remoto listado. Mesmo tamanho: conta
#   em files_existing e não é reenviado. Tamanho diferente: erro, sem
#   sobrescrever. Os pendentes são agrupados por diretório remoto em lotes
#   (batch_dirs/batch_items) que fecham ao atingir batchsize arquivos,
#   BATCH_MAX_BYTES ou BATCH_MAX_CHARS. Preenche planned_items, files_planned,
#   bytes_planned e batches_planned.
# -----------------------------------------------------------------------------
plan_transfers() {
    local directory index key meta
    local -a order=()
    local -A cur_items=() cur_count=() cur_bytes=() cur_chars=()
    for index in "${!src_list[@]}"; do
        directory=${tdir_list[index]}
        key=${rkey_list[index]}
        meta=${remote_meta[$key]-}
        if [[ -n $meta ]]; then
            if [[ $meta == "${size_list[index]}" ]]; then
                files_existing=$((files_existing + 1))
            else
                error "Remote size conflict; not overwritten: %q." "$directory/${name_list[index]}"
            fi
            continue
        fi
        planned_items+=("$index")
        bytes_planned=$((bytes_planned + size_list[index]))
        if [[ -z ${cur_count[$directory]+present} ]]; then order+=("$directory"); fi
        # Cada string guarda no máximo um lote, evitando a concatenação
        # quadrática de uma lista com todos os arquivos.
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
    for directory in "${order[@]}"; do
        if (( ${cur_count[$directory]-0} > 0 )); then
            batch_dirs+=("$directory") batch_items+=("${cur_items[$directory]}")
        fi
    done
    files_planned=${#planned_items[@]}
    batches_planned=${#batch_dirs[@]}
}

# -----------------------------------------------------------------------------
# Função: upload_batch
#
# Descrição:
#   Worker executado em segundo plano (subshell) para um lote: uma única
#   chamada "mc cp --json" com todos os arquivos do lote para o diretório
#   remoto. A saída do mc vai para um log temporário, descartado ao final; só o
#   código de saída é registrado (a saída pode conter dados sensíveis). O
#   sucesso real é decidido depois, pela listagem final em verify_transfers.
#   Recebe SIGTERM de stop_workers e o repassa ao mc.
#
# Argumentos:
#   $1 - Diretório remoto.  $2 - Número do lote.  $3.. - Índices dos arquivos.
# -----------------------------------------------------------------------------
upload_batch() {
    local directory=$1 number=$2 index status=0 mc_pid
    local output="$scratch_dir/batch.$2.log"
    shift 2
    local -a files=()
    for index in "$@"; do files+=("${src_list[index]}"); done
    mc --config-dir "$mc_config_dir" cp --json -- "${files[@]}" "$directory/" \
        > "$output" 2>&1 &
    mc_pid=$!
    trap 'kill "$mc_pid" 2>/dev/null; exit 143' TERM
    wait "$mc_pid" || status=$?
    rm -f -- "$output"
    if (( status == 0 )); then
        log INFO "Batch %s finished: %s files -> %q." "$number" "$#" "$directory"
    else
        log ERROR "mc cp failed for batch %s (exit %s); checking individual results." \
            "$number" "$status"
    fi
    exit "$status"
}

# -----------------------------------------------------------------------------
# Função: run_transfers
#
# Descrição:
#   Exporta MC_UPLOAD_MULTIPART_THREADS e executa os lotes com no máximo "jobs"
#   workers simultâneos, usando "wait -n" para repor a vaga assim que um
#   termina. Ao final aguarda todos os workers.
# -----------------------------------------------------------------------------
run_transfers() {
    local number
    export MC_UPLOAD_MULTIPART_THREADS=$multipart_threads
    for number in "${!batch_dirs[@]}"; do
        if (( running >= jobs )); then
            wait -n
            running=$((running - 1))
        fi
        batches_launched=$((batches_launched + 1))
        log INFO "Batch %s/%s started -> %q." "$batches_launched" "$batches_planned" "${batch_dirs[number]}"
        # Índices sem aspas: lista de inteiros separados por espaço.
        # shellcheck disable=SC2086
        upload_batch "${batch_dirs[number]}" "$batches_launched" ${batch_items[number]} &
        running=$((running + 1))
    done
    wait
    running=0
}

# -----------------------------------------------------------------------------
# Função: verify_transfers
#
# Descrição:
#   Confirma o resultado com uma nova listagem recursiva única. Cada arquivo
#   planejado só conta em files_transferred se existir no remoto com o tamanho
#   registrado na descoberta. Ausente: erro "Transfer not confirmed". Tamanho
#   diferente (envio parcial ou origem alterada durante o envio): erro; o objeto
#   remoto não é removido.
# -----------------------------------------------------------------------------
verify_transfers() {
    local index key meta
    load_remote || return
    for index in "${planned_items[@]}"; do
        key=${rkey_list[index]}
        meta=${remote_meta[$key]-}
        if [[ -z $meta ]]; then
            error "Transfer not confirmed: %q." "${tdir_list[index]}/${name_list[index]}"
        elif [[ $meta == "${size_list[index]}" ]]; then
            files_transferred=$((files_transferred + 1))
            bytes_transferred=$((bytes_transferred + size_list[index]))
        else
            error "Size mismatch after upload (source changed or partial upload): %q." \
                "${tdir_list[index]}/${name_list[index]}"
        fi
    done
}

# -----------------------------------------------------------------------------
# Função: main
#
# Descrição:
#   Orquestra: parse_arguments, require_dependencies, load_configuration,
#   discover_files, validate_mc, load_remote, plan_transfers e, conforme
#   --dry-run, impressão do plano ou run_transfers + verify_transfers.
#
# Status final (final_status):
#   DRY_RUN          - --dry-run informado (independe de falhas).
#   SUCCESS          - Sem falhas e sem mapeamentos pendentes.
#   PARTIAL_SUCCESS  - Há falhas ou pendências, mas ao menos um arquivo foi
#                      enviado ou já existia.
#   FAILED           - Demais casos.
#
# Retorno:
#   0 se failures e pending_mappings são ambos 0; 1 caso contrário.
# -----------------------------------------------------------------------------
main() {
    local index
    parse_arguments "$@"
    log INFO "Starting CDR upload for %s (dry_run=%s)." "$week_dir" "$dry_run"
    require_dependencies
    load_configuration
    log INFO "Destination base: %q (jobs=%s, multipart_threads=%s, batchsize=%s)." \
        "$destination_base" "$jobs" "$multipart_threads" "$batchsize"
    discover_files
    validate_mc
    load_remote || fatal 1 "Cannot list the remote objects; nothing was uploaded."
    plan_transfers
    if (( dry_run )); then
        for index in "${planned_items[@]}"; do
            printf 'PLAN %q -> %q\n' "${src_list[index]}" "${tdir_list[index]}/${name_list[index]}"
        done
        final_status=DRY_RUN
    else
        (( ${#batch_dirs[@]} == 0 )) || run_transfers
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
