#!/usr/bin/env bash
#==============================================================================
# upload_all_cdr_to_minio.sh
#
# CONTEXTO
#   As prestadoras enviam arquivos que são armazenados em pastas por semana
#   (Semana100, Semana101, ...). Entre eles há CDR brutos binários, que são
#   convertidos pela ferramenta externa "teleparser" (cada .gz vira um .parquet),
#   e CDR em formato texto, que NÃO passam pelo teleparser. Cada grupo é enviado
#   ao MinIO por um script próprio:
#     - upload_text_cdr_to_minio.sh        (CDR em formato texto)
#     - upload_teleparser_cdr_to_minio.sh  (arquivos convertidos pelo teleparser)
#
# O QUE ESTE SCRIPT FAZ
#   Orquestra o envio de uma semana completa: executa, em sequência, os dois
#   scripts acima (primeiro o de CDR texto, depois o do teleparser), repassando
#   a cada um o caminho de origem correspondente e os demais argumentos
#   (destino no MinIO e modo simulação). No final, imprime um relatório com o
#   resultado de cada etapa.
#
#   As etapas são independentes: se a primeira terminar com pendências ou erros,
#   a segunda é executada mesmo assim. Os detalhes de cada cópia (arquivos
#   enviados, conflitos, pendências) são os relatórios de cada script, exibidos
#   durante a execução.
#
# PRÉ-REQUISITOS
#   - bash 4 ou superior.
#   - Os dois scripts de cópia na mesma pasta deste script (ou na pasta indicada
#     por SCRIPT_DIR).
#   - Cliente MinIO ("mc") instalado e com o alias do destino configurado
#     (o padrão usa o alias "bganatel"; confira com: mc alias list).
#   - Permissão de leitura nas pastas de origem e de escrita no destino do MinIO.
#
# USO
#   ./upload_all_cdr_to_minio.sh -t <caminho_cdr_texto> -p <caminho_cdr_teleparser> [-d <destino_minio>] [-n]
#
#   Parâmetros:
#     -t | --text <caminho>         (obrigatório) Pasta da semana com os CDR em
#                                   formato texto.
#                                   Ex.: /assets/monitoring/cdr/cdr_bruto/Semana100
#     -p | --teleparser <caminho>   (obrigatório) Pasta da semana com os arquivos
#                                   convertidos pelo teleparser.
#                                   Ex.: /assets/data/cdr/teleparser/Semana100
#     -d | --destination <destino>  (opcional) Pasta de destino no MinIO, no
#                                   formato alias/bucket/caminho. É repassada aos
#                                   dois scripts. Se omitida, cada script usa o
#                                   seu valor padrão.
#     -n | --dry-run                Simulação: repassada aos dois scripts; nada é
#                                   copiado (o MinIO é apenas consultado).
#     -h | --help                   Mostra a ajuda.
#
#   Os caminhos podem ser relativos (ex.: ./Semana100); nos relatórios eles são
#   sempre exibidos como caminho absoluto. As opções também aceitam a forma
#   --text=<caminho>, --teleparser=<caminho> e --destination=<destino>.
#
#   Exemplos:
#     ./upload_all_cdr_to_minio.sh \
#         -t /assets/monitoring/cdr/cdr_bruto/Semana100 \
#         -p /assets/data/cdr/teleparser/Semana100 -n
#     ./upload_all_cdr_to_minio.sh \
#         -t /assets/monitoring/cdr/cdr_bruto/Semana100 \
#         -p /assets/data/cdr/teleparser/Semana100
#     ./upload_all_cdr_to_minio.sh \
#         -t /assets/monitoring/cdr/cdr_bruto/Semana100 \
#         -p /assets/data/cdr/teleparser/Semana100 \
#         -d bganatel/sandbox-sfi/cdr/outra_pasta
#
#   Variáveis de ambiente (exportadas) como MINIO_DESTINATION e BATCH_SIZE são
#   herdadas pelos dois scripts de cópia.
#
# REGRAS
#   - O nome da última pasta de cada caminho (ex.: Semana100) é usado pelos
#     scripts de cópia no destino do MinIO. Os dois caminhos devem terminar na
#     mesma pasta (ex.: Semana100 e Semana100); se os nomes forem diferentes
#     (ex.: Semana100 e Semana101), o script exibe uma mensagem de erro e
#     termina com código 2, sem executar nenhuma etapa.
#   - Todos os argumentos são validados antes da primeira etapa: se algo estiver
#     errado, nada é executado.
#
# CÓDIGOS DE SAÍDA
#   0 = as duas etapas terminaram sem pendências
#   1 = ao menos uma etapa terminou com pendências ou erros
#   2 = uso incorreto ou ambiente inválido (argumento, pasta, script ou "mc"
#       ausente; caminhos terminando em pastas de nomes diferentes)
#==============================================================================

# Falhas pontuais não devem interromper a execução; cada etapa é executada e
# contabilizada (por isso não usamos "set -e").

#------------------------------------------------------------------------------
# CONFIGURAÇÃO (edite aqui)
#------------------------------------------------------------------------------

# Pasta onde estão os scripts de cópia. Padrão: a mesma pasta deste script. Pode
# ser sobrescrita pela variável de ambiente de mesmo nome.
SCRIPT_DIR="${SCRIPT_DIR:-$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)}"

# Nomes dos scripts de cópia executados em cada etapa.
TEXT_SCRIPT_NAME="upload_text_cdr_to_minio.sh"
TELEPARSER_SCRIPT_NAME="upload_teleparser_cdr_to_minio.sh"

#------------------------------------------------------------------------------
# A PARTIR DAQUI NÃO É NECESSÁRIO ALTERAR
#------------------------------------------------------------------------------

if (( BASH_VERSINFO[0] < 4 )); then
    echo "ERRO: este script requer bash 4 ou superior." >&2
    exit 2
fi

# Exibe como usar o script e encerra com código 2.
usage() {
    echo "Uso: $0 -t <caminho_cdr_texto> -p <caminho_cdr_teleparser> [-d <destino_minio>] [-n]" >&2
    echo "  ex.: $0 -t /assets/monitoring/cdr/cdr_bruto/Semana100 -p /assets/data/cdr/teleparser/Semana100" >&2
    echo "  -t   pasta da semana com os CDR em formato texto" >&2
    echo "  -p   pasta da semana com os arquivos convertidos pelo teleparser" >&2
    echo "  -d   pasta de destino no MinIO (padrão: o de cada script de cópia)" >&2
    echo "  -n   simulação (não copia nada)" >&2
    exit 2
}

# Garante que a opção $1 recebeu um valor ($2) não vazio.
require_value() {
    if [[ -z "$2" ]]; then
        echo "ERRO: a opção $1 exige um valor." >&2
        usage
    fi
}

# --- leitura dos argumentos ---------------------------------------------------
text_path=""
teleparser_path=""
dry_run=0
# Destino no MinIO: vazio = não repassar (cada script usa o seu padrão)
minio_destination=""
while (( $# > 0 )); do
    case "$1" in
        -n|--dry-run) dry_run=1 ;;
        -h|--help)    usage ;;
        -t|--text)
            require_value "$1" "${2-}"
            text_path="$2"
            shift
            ;;
        --text=*)
            text_path="${1#--text=}"
            ;;
        -p|--teleparser)
            require_value "$1" "${2-}"
            teleparser_path="$2"
            shift
            ;;
        --teleparser=*)
            teleparser_path="${1#--teleparser=}"
            ;;
        -d|--destination)
            require_value "$1" "${2-}"
            minio_destination="$2"
            shift
            ;;
        --destination=*)
            minio_destination="${1#--destination=}"
            # --destination= sem valor é erro (diferente de omitir a opção)
            if [[ -z "$minio_destination" ]]; then
                echo "ERRO: o destino no MinIO não pode ser vazio." >&2
                exit 2
            fi
            ;;
        *)
            echo "ERRO: argumento desconhecido: '$1'" >&2
            usage
            ;;
    esac
    shift
done

if [[ -z "$text_path" || -z "$teleparser_path" ]]; then
    echo "ERRO: informe os dois caminhos de origem (-t e -p)." >&2
    usage
fi

# Converte um caminho em caminho absoluto (sem resolver links simbólicos) e o
# imprime. Em caso de erro, escreve a mensagem em stderr e retorna 1.
# Parâmetros: $1 = descrição do caminho (para as mensagens), $2 = caminho informado
resolve_absolute_path() {
    local label="$1" argument="$2" path="${2%/}"
    if [[ -z "$path" ]]; then
        echo "ERRO: caminho inválido ($label): '$argument'" >&2
        return 1
    fi
    if [[ ! -d "$path" ]]; then
        echo "ERRO: diretório não encontrado ($label): $path" >&2
        return 1
    fi
    # "CDPATH=" evita que o cd use essa variável e altere o resultado
    CDPATH= cd -- "$path" 2>/dev/null && pwd || {
        echo "ERRO: não foi possível acessar o diretório ($label): $argument" >&2
        return 1
    }
}

text_path="$(resolve_absolute_path "CDR texto" "$text_path")" || exit 2
teleparser_path="$(resolve_absolute_path "CDR teleparser" "$teleparser_path")" || exit 2

# Os dois caminhos devem terminar na mesma pasta (ex.: Semana100); caso contrário
# os arquivos seriam enviados para semanas diferentes no MinIO.
text_week_name="${text_path##*/}"
teleparser_week_name="${teleparser_path##*/}"
if [[ "$text_week_name" != "$teleparser_week_name" ]]; then
    echo "ERRO: os caminhos de origem devem terminar com o mesmo nome de pasta (ex.: Semana100)." >&2
    echo "      -t (CDR texto)     : $text_path  -> '${text_week_name}'" >&2
    echo "      -p (CDR teleparser): $teleparser_path  -> '${teleparser_week_name}'" >&2
    echo "      Informe pastas da mesma semana, por exemplo:" >&2
    echo "        -t /assets/monitoring/cdr/cdr_bruto/Semana100 -p /assets/data/cdr/teleparser/Semana100" >&2
    exit 2
fi

# Confere se os scripts de cópia existem
text_script_path="${SCRIPT_DIR%/}/${TEXT_SCRIPT_NAME}"
teleparser_script_path="${SCRIPT_DIR%/}/${TELEPARSER_SCRIPT_NAME}"
for script_path in "$text_script_path" "$teleparser_script_path"; do
    if [[ ! -f "$script_path" ]]; then
        echo "ERRO: script de cópia não encontrado: $script_path" >&2
        echo "      Coloque-o na mesma pasta deste script ou ajuste SCRIPT_DIR." >&2
        exit 2
    fi
done

# Os dois scripts de cópia consultam o MinIO, então o "mc" é sempre necessário
if ! command -v mc >/dev/null 2>&1; then
    echo "ERRO: comando 'mc' não encontrado no PATH." >&2
    exit 2
fi

# Retorna a hora atual (HH:MM:SS), usada nos logs.
timestamp() { date '+%H:%M:%S'; }

# Converte uma quantidade de segundos em HH:MM:SS.
# Parâmetro: $1 = segundos
format_duration() {
    printf '%02d:%02d:%02d' $(( $1 / 3600 )) $(( $1 % 3600 / 60 )) $(( $1 % 60 ))
}

# Resultados por etapa (usados no relatório final).
# Todos os arrays são indexados pela posição da etapa.
declare -a step_name step_source step_script step_exit_code step_status step_duration
total_steps=2

# Executa um script de cópia, exibe a saída dele em tempo real e guarda o
# resultado da etapa para o relatório final.
# Parâmetros: $1 = número da etapa, $2 = descrição, $3 = caminho do script,
#             $4 = caminho de origem da semana
run_step() {
    local number="$1" name="$2" script_path="$3" source_path="$4"
    local index=$(( number - 1 ))
    local -a script_args=( "$source_path" )
    [[ -n "$minio_destination" ]] && script_args+=( -d "$minio_destination" )
    (( dry_run )) && script_args+=( -n )

    echo
    echo "=============================================================="
    echo " ETAPA ${number}/${total_steps} - ${name}  ($(timestamp))"
    echo " Script: ${script_path##*/}"
    echo "=============================================================="
    echo

    local started_at=$SECONDS
    bash "$script_path" "${script_args[@]}"
    local exit_code=$?

    local status
    case "$exit_code" in
        0) status="OK" ;;
        1) status="COM PENDÊNCIAS" ;;
        2) status="FALHA DE USO OU AMBIENTE" ;;
        *) status="ERRO INESPERADO" ;;
    esac

    step_name[$index]="$name"
    step_source[$index]="$source_path"
    step_script[$index]="${script_path##*/}"
    step_exit_code[$index]="$exit_code"
    step_status[$index]="$status"
    step_duration[$index]="$(format_duration $(( SECONDS - started_at )))"
}

# Interrupção pelo usuário (Ctrl+C): encerra com mensagem clara
trap 'echo; echo "Interrompido pelo usuário." >&2; exit 130' INT

#------------------------------------------------------------------------------
# EXECUÇÃO
#------------------------------------------------------------------------------

echo "=============================================================="
echo " Cópia de CDR (texto e teleparser) para o MinIO  ($(date '+%d/%m/%Y %H:%M:%S'))"
echo " Origem CDR texto     : $text_path"
echo " Origem CDR teleparser: $teleparser_path"
if [[ -n "$minio_destination" ]]; then
    echo " Destino no MinIO     : ${minio_destination%/}/"
else
    echo " Destino no MinIO     : padrão de cada script"
fi
(( dry_run )) && echo " MODO SIMULAÇÃO: nada será copiado."
echo "=============================================================="

run_step 1 "CDR TEXTO"      "$text_script_path"       "$text_path"
run_step 2 "CDR TELEPARSER" "$teleparser_script_path" "$teleparser_path"

#------------------------------------------------------------------------------
# RELATÓRIO FINAL
#------------------------------------------------------------------------------
ok_steps=0
problem_steps=0
for i in "${!step_name[@]}"; do
    if [[ "${step_exit_code[i]}" == "0" ]]; then
        (( ok_steps++ ))
    else
        (( problem_steps++ ))
    fi
done

echo
echo "=============================================================="
echo " RELATÓRIO FINAL - CÓPIA DE CDR PARA O MINIO"
echo "=============================================================="
for i in "${!step_name[@]}"; do
    printf '%2d) %s\n' $((i+1)) "${step_name[i]}"
    printf '      Origem: %s\n' "${step_source[i]}"
    printf '      Script: %s | Código de saída: %s | Duração: %s | %s\n' \
        "${step_script[i]}" "${step_exit_code[i]}" "${step_duration[i]}" "${step_status[i]}"
done
echo "--------------------------------------------------------------"
printf ' %-20s: %s\n' "Etapas executadas" "${#step_name[@]}"
printf ' %-20s: %s\n' "Etapas concluídas OK" "$ok_steps"
printf ' %-20s: %s\n' "Etapas com pendência" "$problem_steps"

if (( problem_steps > 0 )); then
    echo " Etapas com pendência ou erro:"
    for i in "${!step_name[@]}"; do
        [[ "${step_exit_code[i]}" != "0" ]] && echo "  - ${step_name[i]}  ->  ${step_source[i]}  : ${step_status[i]}"
    done
fi
echo "=============================================================="

if (( problem_steps > 0 )); then
    echo " Concluído COM PENDÊNCIAS."
    exit 1
fi
if (( dry_run )); then
    echo " Simulação concluída."
else
    echo " Concluído com sucesso."
fi
exit 0
