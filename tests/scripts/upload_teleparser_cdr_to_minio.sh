#!/usr/bin/env bash
#==============================================================================
# upload_teleparser_cdr_to_minio.sh
#
# CONTEXTO
#   As prestadoras enviam arquivos que são armazenados em pastas por semana
#   (Semana100, Semana101, ...). Entre eles há CDR brutos binários (.gz), que
#   são convertidos pela ferramenta externa "teleparser": cada arquivo binário
#   .gz vira um arquivo .parquet. As pastas resultantes dessa conversão ficam
#   em /assets/data/cdr/teleparser/SemanaXX.
#
# O QUE ESTE SCRIPT FAZ
#   Envia para o MinIO os arquivos .parquet gerados pelo teleparser em uma
#   semana, conforme a tabela FOLDER_MAPPING, mais abaixo neste arquivo. Cada
#   item da tabela indica: onde procurar os arquivos (origem), quais arquivos
#   pegar (máscara, sempre *.parquet) e para onde enviá-los (destino).
#
#   Antes de copiar, o script consulta o MinIO e só envia o que ainda não existe
#   lá. Depois da cópia, confere se cada arquivo chegou com o mesmo tamanho e
#   imprime um relatório final.
#
#   (Para os CDR em formato texto, que não passam pelo teleparser, use o script
#   upload_text_cdr_to_minio.sh.)
#
# PRÉ-REQUISITOS
#   - bash 4 ou superior.
#   - Cliente MinIO ("mc") instalado e com o alias do destino configurado
#     (o padrão usa o alias "bganatel"; confira com: mc alias list).
#     O "mc" é exigido sempre, inclusive no modo simulação, pois o script
#     consulta o MinIO para saber o que já foi enviado.
#   - Permissão de leitura na pasta da semana e de escrita no destino do MinIO.
#
# USO
#   ./upload_teleparser_cdr_to_minio.sh <caminho_completo_da_semana> [-d <destino_minio>] [-n]
#
#   Parâmetros:
#     <caminho_completo_da_semana>  Pasta da semana a ser enviada. O nome da
#                                   última pasta (ex.: Semana100) também é usado
#                                   no destino do MinIO. Aceita caminho relativo
#                                   (ex.: ./Semana100 ou .), que é convertido em
#                                   caminho absoluto nos relatórios.
#     -d | --destination <destino>  (opcional) Pasta de destino no MinIO, no
#                                   formato alias/bucket/caminho. Se omitido, usa
#                                   o valor padrão definido em MINIO_DESTINATION
#                                   neste script. Também aceita --destination=<destino>.
#     -n | --dry-run                Simulação: informa o que seria copiado, sem
#                                   copiar nada (o MinIO é apenas consultado).
#     -h | --help                   Mostra a ajuda.
#
#   Exemplos:
#     ./upload_teleparser_cdr_to_minio.sh /assets/data/cdr/teleparser/Semana100 -n
#     ./upload_teleparser_cdr_to_minio.sh /assets/data/cdr/teleparser/Semana100
#     ./upload_teleparser_cdr_to_minio.sh /assets/data/cdr/teleparser/Semana100 \
#         -d bganatel/sandbox-sfi/cdr/outra_pasta
#
#   Destino no MinIO - ordem de prioridade:
#     1) argumento -d / --destination
#     2) variável de ambiente MINIO_DESTINATION
#     3) valor padrão definido em MINIO_DESTINATION neste script
#        (bganatel/sandbox-sfi/cdr/ingestao)
#
#   Outra variável de ambiente opcional:
#     BATCH_SIZE          Quantidade de arquivos enviados por chamada do
#                         "mc cp". Padrão: 100
#
# COMO A ESTRUTURA É MAPEADA
#   Origem : <caminho_da_semana>/<origem do mapeamento>   (aceita curingas)
#   Destino: <destino_minio>/<nome_da_semana>/<destino do mapeamento>/
#
#   Exemplo: .../teleparser/Semana100/algar/voz/*.parquet
#         -> bganatel/sandbox-sfi/cdr/ingestao/Semana100/algar/smp/nokia/
#
# REGRAS
#   - Somente os arquivos que atendem à máscara (*.parquet) diretamente dentro
#     da pasta de origem são copiados. Outros arquivos e subpastas são ignorados.
#   - Validação do mapeamento: o script varre o nível <prestadora>/<subpasta>
#     da semana. Se existir alguma pasta nesse nível que NÃO esteja no
#     mapeamento, o script informa quais são, sugere a linha a incluir em
#     FOLDER_MAPPING e NÃO copia nada.
#   - Pasta mapeada que não existe na semana, ou que não tem arquivos que
#     atendam à máscara, é registrada como pendência no relatório final.
#   - Arquivo que já existe no MinIO com o mesmo tamanho é pulado.
#   - Arquivo que já existe no MinIO com tamanho DIFERENTE não é sobrescrito e
#     é registrado como conflito.
#   - A verificação final compara nome e tamanho (não o conteúdo).
#   - Nos relatórios, o caminho de origem é sempre exibido como caminho
#     absoluto, mesmo que o caminho informado seja relativo.
#
# COMO INCLUIR UM NOVO MAPEAMENTO
#   Adicione uma linha em FOLDER_MAPPING no formato "origem|máscara|destino"
#   (veja a explicação detalhada junto da tabela abaixo).
#
# CÓDIGOS DE SAÍDA
#   0 = tudo certo
#   1 = pasta sem mapeamento, ou pendências/erros na cópia
#   2 = uso incorreto ou ambiente inválido (parâmetro, pasta ou "mc" ausente)
#==============================================================================

# Falhas pontuais não devem interromper a execução; cada erro é tratado,
# informado e contabilizado (por isso não usamos "set -e").

#------------------------------------------------------------------------------
# CONFIGURAÇÃO (edite aqui)
#------------------------------------------------------------------------------

# Prefixo de destino PADRÃO no MinIO (alias/bucket/caminho). Pode ser sobrescrito
# pela variável de ambiente de mesmo nome ou pelo argumento -d / --destination.
MINIO_DESTINATION="${MINIO_DESTINATION:-bganatel/sandbox-sfi/cdr/ingestao}"

# Quantidade de arquivos enviados por chamada do "mc cp". Pode ser sobrescrita
# pela variável de ambiente de mesmo nome.
BATCH_SIZE="${BATCH_SIZE:-100}"

# Tabela de mapeamento. UMA LINHA POR ITEM, no formato:
#
#     "origem|máscara|destino"
#
#   - origem   : caminho relativo à pasta da semana, no nível
#                <prestadora>/<subpasta>. Pode conter curingas (*, ?, [..]).
#   - máscara  : filtra os nomes dos arquivos. Para os arquivos do teleparser é
#                sempre *.parquet.
#   - destino  : caminho relativo a <destino_minio>/<nome_da_semana>/.
#
#   Outras regras:
#   - Linhas começando com # são ignoradas; linhas repetidas são executadas
#     apenas uma vez.
#   - Para acrescentar um caminho, basta incluir uma nova linha;
#     para desativar, comente com #.
FOLDER_MAPPING=(
  # origem                |máscara       |destino
  "algar/voz              |*.parquet     |algar/smp/nokia"
  "claro/ericsson         |*.parquet     |claro/smp/ericsson"
  "claro/fixa_bds         |*.parquet     |claro/stfc/bds"
  "claro/fixa_sigma       |*.parquet     |claro/stfc/sigma"
  "claro/nokia            |*.parquet     |claro/smp/nokia"
  "claro/volte            |*.parquet     |claro/smp/volte"
  "tim/ericsson           |*.parquet     |tim/smp/ericsson"
  "tim/ngn_italtel        |*.parquet     |tim/stfc/ngn_italtel"
  "tim/volte              |*.parquet     |tim/smp/volte"
  "vivo/ericsson          |*.parquet     |vivo/smp/ericsson"
  "vivo/volte             |*.parquet     |vivo/smp/volte"
)

#------------------------------------------------------------------------------
# A PARTIR DAQUI NÃO É NECESSÁRIO ALTERAR
#------------------------------------------------------------------------------

if (( BASH_VERSINFO[0] < 4 )); then
    echo "ERRO: este script requer bash 4 ou superior." >&2
    exit 2
fi

# Exibe como usar o script e encerra com código 2.
usage() {
    echo "Uso: $0 <caminho_completo_da_semana> [-d <destino_minio>] [-n]" >&2
    echo "  ex.: $0 /assets/monitoring/cdr/cdr_bruto/Semana100" >&2
    echo "  -d   pasta de destino no MinIO (padrão: ${MINIO_DESTINATION})" >&2
    echo "  -n   simulação (não copia nada)" >&2
    exit 2
}

# --- leitura dos argumentos ---------------------------------------------------
week_path=""
dry_run=0
# Destino efetivo no MinIO: começa no valor padrão e pode ser trocado por -d
minio_destination="$MINIO_DESTINATION"
while (( $# > 0 )); do
    case "$1" in
        -n|--dry-run) dry_run=1 ;;
        -h|--help)    usage ;;
        -d|--destination)
            # exige um valor não vazio logo após a opção
            if [[ $# -lt 2 || -z "$2" ]]; then
                echo "ERRO: a opção $1 exige o caminho de destino no MinIO." >&2
                usage
            fi
            minio_destination="$2"
            shift
            ;;
        --destination=*)
            minio_destination="${1#--destination=}"
            ;;
        *)
            # o primeiro argumento posicional é o caminho; um segundo é erro de uso
            [[ -z "$week_path" ]] && week_path="$1" || usage
            ;;
    esac
    shift
done
[[ -z "$week_path" ]] && usage

if [[ -z "$minio_destination" ]]; then
    echo "ERRO: o destino no MinIO não pode ser vazio." >&2
    exit 2
fi

# Remove a barra final (guarda o valor informado para as mensagens de erro)
week_argument="$week_path"
week_path="${week_path%/}"

if [[ -z "$week_path" ]]; then
    echo "ERRO: caminho inválido: '$week_argument'" >&2
    exit 2
fi
if [[ ! -d "$week_path" ]]; then
    echo "ERRO: diretório não encontrado: $week_path" >&2
    exit 2
fi

# Converte para caminho absoluto, para que os relatórios sempre exibam o caminho
# completo (ex.: ./Semana111 -> /assets/monitoring/cdr/cdr_bruto/Semana111).
# "pwd" sem -P não resolve links simbólicos; "CDPATH=" evita que o cd use essa
# variável (o que poderia imprimir o caminho e alterar o resultado).
week_path="$(CDPATH= cd -- "$week_path" 2>/dev/null && pwd)" || {
    echo "ERRO: não foi possível acessar o diretório: $week_argument" >&2
    exit 2
}

# Extrai o nome da semana (ex.: Semana100) do caminho já absoluto
week_name="${week_path##*/}"
if [[ -z "$week_name" ]]; then
    echo "ERRO: caminho inválido: '$week_argument'" >&2
    exit 2
fi

if ! command -v mc >/dev/null 2>&1; then
    echo "ERRO: comando 'mc' não encontrado no PATH." >&2
    exit 2
fi
if ! [[ "$BATCH_SIZE" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERRO: BATCH_SIZE deve ser um número inteiro positivo (recebido: '$BATCH_SIZE')." >&2
    exit 2
fi

# Remove espaços no início e no fim de um texto.
trim() {
    local text="$1"
    text="${text#"${text%%[![:space:]]*}"}"
    text="${text%"${text##*[![:space:]]}"}"
    printf '%s' "$text"
}

# Retorna a hora atual (HH:MM:SS), usada nos logs.
timestamp() { date '+%H:%M:%S'; }

# Lista os arquivos que já existem em uma pasta do MinIO e preenche o array
# associativo global remote_objects (nome do arquivo -> tamanho em bytes).
# Parâmetro: $1 = caminho no MinIO (alias/bucket/pasta)
declare -A remote_objects=()
list_remote_objects() {
    local destination_path="$1" line key size
    local regex_size='"size":([0-9]+)' regex_key='"key":"([^"]+)"'
    remote_objects=()
    while IFS= read -r line; do
        [[ $line =~ $regex_size ]] || continue
        size="${BASH_REMATCH[1]}"
        [[ $line =~ $regex_key ]] || continue
        key="${BASH_REMATCH[1]}"
        [[ $key == */ ]] && continue   # ignora subpastas
        remote_objects["$key"]="$size"
    done < <(mc ls --json "${destination_path}/" 2>/dev/null)
}

# Resultados por item do mapeamento (usados no relatório final).
# Todos os arrays são indexados pela posição do item.
declare -a result_source result_destination result_mask result_found
declare -a result_copied result_existing result_errors result_verified result_status
declare -a error_log        # detalhe de cada erro, exibido no fim
declare -a pending_list     # caminhos com pendência, exibidos no fim
declare -A already_processed  # evita executar linhas repetidas do mapeamento
total_items=0; total_found=0; total_copied=0
total_existing=0; total_errors=0; total_pending=0

# Guarda o resultado de um item para o relatório final.
# Parâmetros: índice, origem, destino, máscara, encontrados, copiados,
#             já existentes, erros, verificação, status
record_result() {
    result_source[$1]="$2"; result_destination[$1]="$3"; result_mask[$1]="$4"
    result_found[$1]=$5; result_copied[$1]=$6; result_existing[$1]=$7
    result_errors[$1]=$8; result_verified[$1]="$9"; result_status[$1]="${10}"
}

# Registra uma pendência (texto exibido no relatório final) e a contabiliza.
# Parâmetro: $1 = descrição da pendência
register_pending() {
    pending_list+=( "$1" )
    (( total_pending++ ))
}

# Processa um item do mapeamento: localiza os arquivos de origem, compara com o
# que já existe no MinIO, copia o que falta e confere o resultado.
# Parâmetros: $1 = origem (relativa), $2 = máscara, $3 = destino (relativo)
process_mapping_item() {
    local source_rel="$1" mask="$2" destination_rel="$3"
    local source="${week_path}/${source_rel#/}"
    destination_rel="${destination_rel#/}"; destination_rel="${destination_rel%/}"
    local destination="${minio_destination%/}/${week_name}/${destination_rel}"

    local index=${#result_source[@]}
    local found=0 copied=0 existing=0 errors=0 verified=0 status="OK"

    echo
    echo "[$(timestamp)] ($((index+1))) Origem : $source"
    echo "           Máscara: $mask"
    echo "           Destino: $destination/"

    # --- expande os curingas da origem -----------------------------------------
    local -a matches=() files=()
    local -A seen_files=()
    local old_ifs="$IFS"; IFS=
    shopt -s nullglob
    matches=( $source )
    shopt -u nullglob
    IFS="$old_ifs"

    if (( ${#matches[@]} == 0 )); then
        echo "           [AVISO] Caminho de origem NÃO ENCONTRADO. Nada a fazer para este item."
        record_result "$index" "$source" "$destination" "$mask" 0 0 0 0 "-" "ORIGEM NÃO ENCONTRADA"
        register_pending "$source  [$mask]  ->  $destination/  : ORIGEM NÃO ENCONTRADA"
        return
    fi

    # --- coleta os arquivos que atendem à máscara -------------------------------
    local match file file_name
    for match in "${matches[@]}"; do
        if [[ -d $match ]]; then
            # a origem é uma pasta: pega os arquivos que casam com a máscara
            old_ifs="$IFS"; IFS=
            shopt -s nullglob
            for file in "${match%/}"/$mask; do
                [[ -f $file && -z ${seen_files[$file]+x} ]] && { seen_files[$file]=1; files+=( "$file" ); }
            done
            shopt -u nullglob
            IFS="$old_ifs"
        elif [[ -f $match ]]; then
            # a origem já aponta direto para um arquivo: confere a máscara
            file_name="${match##*/}"
            # shellcheck disable=SC2053
            if [[ $file_name == $mask && -z ${seen_files[$match]+x} ]]; then
                seen_files[$match]=1; files+=( "$match" )
            fi
        fi
    done
    found=${#files[@]}
    echo "           Pastas/itens de origem localizados: ${#matches[@]} | Arquivos na origem que atendem à máscara: $found"

    if (( found == 0 )); then
        echo "           [AVISO] Nenhum arquivo corresponde à máscara. Nada copiado."
        record_result "$index" "$source" "$destination" "$mask" 0 0 0 0 "-" "SEM ARQUIVOS"
        register_pending "$source  [$mask]  ->  $destination/  : SEM ARQUIVOS"
        return
    fi

    # --- compara com o que já existe no MinIO -----------------------------------
    list_remote_objects "$destination"

    local -a pending_files=()                 # arquivos que precisam ser enviados
    local -A size_by_name=() seen_name_sizes=()
    local local_size
    for file in "${files[@]}"; do
        file_name="${file##*/}"
        local_size=$(stat -c %s -- "$file" 2>/dev/null)
        if [[ -z $local_size ]]; then
            (( errors++ ))
            error_log+=( "Não foi possível ler o arquivo de origem: $file" )
            continue
        fi
        # mesmo nome vindo de outra pasta de origem para o mesmo destino
        if [[ -n ${seen_name_sizes[$file_name]+x} ]]; then
            if [[ ${seen_name_sizes[$file_name]} == "$local_size" ]]; then
                (( existing++ ))
            else
                (( errors++ ))
                error_log+=( "Conflito (mesmo nome, tamanho diferente na origem; não enviado): $file" )
            fi
            continue
        fi
        seen_name_sizes[$file_name]="$local_size"
        # já existe no MinIO
        if [[ -n ${remote_objects[$file_name]+x} ]]; then
            if [[ ${remote_objects[$file_name]} == "$local_size" ]]; then
                (( existing++ ))
            else
                (( errors++ ))
                error_log+=( "Conflito (já existe no MinIO com tamanho diferente, não sobrescrito): ${destination}/${file_name}  <-  $file" )
            fi
            continue
        fi
        pending_files+=( "$file" )
        size_by_name[$file_name]="$local_size"
    done

    # --- simulação: informa o que seria feito e encerra o item -------------------
    if (( dry_run )); then
        copied=${#pending_files[@]}
        echo "           [SIMULAÇÃO] A copiar: $copied | Já existentes idênticos: $existing | Conflitos/erros: $errors"
        record_result "$index" "$source" "$destination" "$mask" "$found" "$copied" "$existing" "$errors" "-" "SIMULAÇÃO"
        (( total_found += found, total_copied += copied, total_existing += existing, total_errors += errors ))
        (( errors > 0 )) && register_pending "$source  [$mask]  ->  $destination/  : CONFLITOS/ERROS ($errors)"
        return
    fi

    # --- copia em lotes -----------------------------------------------------------
    local i output
    local -a batch
    for (( i=0; i<${#pending_files[@]}; i+=BATCH_SIZE )); do
        batch=( "${pending_files[@]:i:BATCH_SIZE}" )
        if ! output=$(mc cp "${batch[@]}" "${destination}/" 2>&1); then
            error_log+=( "mc cp retornou erro (lote iniciado em ${batch[0]}): $(tail -n 3 <<< "$output" | tr '\n' ' ')" )
        fi
    done

    # --- verificação: cada arquivo enviado precisa estar no MinIO com o mesmo tamanho
    if (( ${#pending_files[@]} > 0 )); then
        list_remote_objects "$destination"
        for file in "${pending_files[@]}"; do
            file_name="${file##*/}"
            if [[ -n ${remote_objects[$file_name]+x} && ${remote_objects[$file_name]} == "${size_by_name[$file_name]}" ]]; then
                (( copied++ ))
            else
                (( errors++ ))
                error_log+=( "Não confirmado no MinIO após a cópia: ${destination}/${file_name}  <-  $file" )
            fi
        done
    fi

    verified=$(( existing + copied ))
    if (( verified != found || errors > 0 )); then
        status="DIVERGÊNCIA"
        register_pending "$source  [$mask]  ->  $destination/  : DIVERGÊNCIA (verificação $verified/$found, erros: $errors)"
    fi
    echo "           Copiados: $copied | Já existentes idênticos: $existing | Erros: $errors | Verificação: $verified/$found  => $status"

    record_result "$index" "$source" "$destination" "$mask" "$found" "$copied" "$existing" "$errors" "$verified/$found" "$status"
    (( total_found += found, total_copied += copied, total_existing += existing, total_errors += errors ))
}

#------------------------------------------------------------------------------
# EXECUÇÃO
#------------------------------------------------------------------------------
echo "=============================================================="
echo " Cópia de CDR TELEPARSER para o MinIO - $week_name  ($(date '+%d/%m/%Y %H:%M:%S'))"
echo " Caminho de origem: $week_path"
echo " Destino no MinIO : ${minio_destination%/}/${week_name}/"
(( dry_run )) && echo " MODO SIMULAÇÃO: nada será copiado."
echo "=============================================================="

#------------------------------------------------------------------------------
# VALIDAÇÃO DO MAPEAMENTO
#------------------------------------------------------------------------------
# Varre o nível <prestadora>/<subpasta> (2 níveis abaixo da pasta da semana) e
# confere se cada pasta encontrada está coberta por alguma origem do
# FOLDER_MAPPING. Se alguma não estiver, nada é copiado e o usuário é orientado
# a incluir o mapeamento no script.
unmapped_folders=()
while IFS= read -r -d '' directory; do
    relative_path="${directory#"$week_path"/}"
    is_mapped=0
    for mapping_entry in "${FOLDER_MAPPING[@]}"; do
        IFS='|' read -r entry_source _ <<< "$mapping_entry"
        entry_source=$(trim "$entry_source")
        entry_source="${entry_source%/}"
        # a origem do mapeamento pode conter curingas, por isso a comparação
        # com o padrão (sem aspas) é intencional
        # shellcheck disable=SC2053
        if [[ -n $entry_source && $relative_path == $entry_source ]]; then
            is_mapped=1
            break
        fi
    done
    (( is_mapped )) || unmapped_folders+=("$relative_path")
done < <(find "$week_path" -mindepth 2 -maxdepth 2 -type d -print0 | sort -z)

if (( ${#unmapped_folders[@]} > 0 )); then
    echo >&2
    echo "ERRO: foram encontradas pastas sem mapeamento em ${week_path}:" >&2
    echo >&2
    for relative_path in "${unmapped_folders[@]}"; do
        echo "   - /${relative_path}" >&2
    done
    echo >&2
    echo "Nada foi copiado." >&2
    echo "Inclua a(s) linha(s) abaixo na tabela FOLDER_MAPPING no início do script," >&2
    echo "substituindo DESTINO pelo caminho de destino correto:" >&2
    echo >&2
    for relative_path in "${unmapped_folders[@]}"; do
        echo "    \"${relative_path}|*.parquet|DESTINO\"" >&2
    done
    exit 1
fi

for mapping_entry in "${FOLDER_MAPPING[@]}"; do
    # descarta linhas vazias
    [[ -z $(trim "$mapping_entry") ]] && continue
    # separa os três campos: origem | máscara | destino (um 4º campo é erro)
    IFS='|' read -r entry_source entry_mask entry_destination entry_extra <<< "$mapping_entry"
    entry_source=$(trim "$entry_source")
    entry_mask=$(trim "$entry_mask")
    entry_destination=$(trim "$entry_destination")
    if [[ -z $entry_source || -z $entry_mask || -z $entry_destination || -n $entry_extra ]]; then
        echo
        echo "[AVISO] Linha mal formatada ignorada: '$mapping_entry'"
        error_log+=( "Linha mal formatada ignorada: $mapping_entry" )
        register_pending "(linha mal formatada na lista) $mapping_entry"
        continue
    fi
    entry_key="$entry_source|$entry_mask|$entry_destination"
    if [[ -n ${already_processed[$entry_key]+x} ]]; then
        echo
        echo "[INFO] Item duplicado ignorado: $entry_source | $entry_mask"
        continue
    fi
    already_processed[$entry_key]=1
    (( total_items++ ))
    process_mapping_item "$entry_source" "$entry_mask" "$entry_destination"
done

#------------------------------------------------------------------------------
# RELATÓRIO FINAL
#------------------------------------------------------------------------------
echo
echo "=============================================================="
echo " RELATÓRIO FINAL - $week_name"
echo "=============================================================="
for i in "${!result_source[@]}"; do
    printf '%2d) %s\n' $((i+1)) "${result_source[i]}  [${result_mask[i]}]"
    printf '      -> %s/\n' "${result_destination[i]}"
    printf '      Encontrados: %s | Copiados: %s | Já existentes: %s | Erros: %s | Verificação origem x MinIO: %s | %s\n' \
        "${result_found[i]}" "${result_copied[i]}" "${result_existing[i]}" "${result_errors[i]}" "${result_verified[i]}" "${result_status[i]}"
done
echo "--------------------------------------------------------------"
printf ' Itens processados : %s\n' "$total_items"
printf ' Arquivos na origem: %s\n' "$total_found"
if (( dry_run )); then
    printf ' A copiar          : %s\n' "$total_copied"
else
    printf ' Copiados com êxito: %s\n' "$total_copied"
fi
printf ' Já existentes     : %s\n' "$total_existing"
printf ' Erros de cópia    : %s\n' "$total_errors"
printf ' Itens com pendência (origem ausente, sem arquivos, divergência ou erro): %s\n' "$total_pending"

if (( ${#pending_list[@]} > 0 )); then
    echo " Caminhos com pendência:"
    for pending_item in "${pending_list[@]}"; do echo "  - $pending_item"; done
fi

if (( ${#error_log[@]} > 0 )); then
    echo "--------------------------------------------------------------"
    echo " Detalhe dos erros:"
    for error_message in "${error_log[@]}"; do echo "  - $error_message"; done
fi
echo "=============================================================="

if (( total_pending > 0 || total_errors > 0 )); then
    echo " Concluído COM PENDÊNCIAS."
    exit 1
fi
if (( dry_run )); then
    echo " Simulação concluída."
else
    echo " Concluído com sucesso."
fi
exit 0
