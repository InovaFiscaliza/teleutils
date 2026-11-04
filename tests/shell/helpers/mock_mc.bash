#!/usr/bin/env bash

# =============================================================================
# Descrição:
#   Dublê (mock) do cliente de linha de comando `mc` (MinIO Client), usado
#   pelos testes Bats de scripts/upload_cdr_to_minio.sh. Não acessa rede: o
#   "armazenamento remoto" é um diretório local definido por MOCK_MC_REMOTE.
#   Nos testes, é copiado para um diretório do PATH com o nome `mc`.
#
# Uso (sintaxe aceita):
#   mc --config-dir DIR --version
#   mc --config-dir DIR stat [ARGUMENTOS...]
#   mc --config-dir DIR ls --json -- [ORIGENS...] DESTINO
#   mc --config-dir DIR cp --json -- ORIGEM [ORIGEM...] DESTINO
#
# Argumentos:
#   --config-dir DIR  Obrigatório e deve ser o primeiro argumento. O valor
#                     precisa ser não vazio, mas não é usado além disso.
#   COMANDO           --version, stat, ls ou cp. Qualquer outro encerra com 93.
#   --json --         Obrigatório (nessa ordem) apenas para ls e cp.
#   DESTINO           Último argumento de ls/cp. É o caminho relativo, dentro de
#                     MOCK_MC_REMOTE, que é listado (ls) ou recebe os arquivos
#                     (cp).
#
# Variáveis de ambiente:
#   MOCK_MC_REMOTE  Obrigatória. Diretório local que representa o remoto.
#   MOCK_MC_CALLS   Obrigatória. Arquivo que registra cada chamada recebida em
#                   JSON (uma linha por chamada). O arquivo
#                   "$MOCK_MC_CALLS.copied" é usado como marcador de que um cp
#                   já ocorreu.
#
#   Variáveis opcionais de injeção de falhas (vazias/ausentes = sem falha):
#   MOCK_MC_STAT_FAIL    Diferente de 0: `stat` retorna 1.
#   MOCK_MC_FAIL_LIST    Caminho de destino exato para o qual `ls` retorna 1.
#   MOCK_MC_VERIFY_FAIL  1: `ls` retorna 1 depois que algum cp já ocorreu.
#   MOCK_MC_BAD_JSON     1: `ls` imprime texto que não é JSON.
#   MOCK_MC_ERROR_JSON   1: `ls` imprime JSON com "status":"error".
#   MOCK_MC_NUL_KEY      1: `ls` imprime um item cuja chave contém caractere NUL.
#   MOCK_MC_FAIL_NAME    Nome de arquivo que o `cp` não copia; o cp termina com 1.
#   MOCK_MC_LIE_NAME     Nome de arquivo que o `cp` não copia, mas sem sinalizar
#                        falha (simula cópia que não ocorreu).
#   MOCK_MC_WRONG_SIZE   Nome de arquivo cuja cópia remota recebe 1 byte extra.
#   MOCK_MC_MUTATE_SOURCE  Nome de arquivo cuja origem recebe 1 byte extra após
#                          ser copiada.
#
# Dependências externas:
#   jq, find, sort, stat (GNU, opção -c), cp, mkdir, touch.
#
# Saídas:
#   stdout: JSON (um objeto por linha) para ls/cp/stat, ou a linha de versão.
#   stderr: mensagem apenas para comando inesperado.
#
# Efeitos colaterais:
#   Acrescenta uma linha a MOCK_MC_CALLS em toda chamada. No cp, cria o diretório
#   de destino em MOCK_MC_REMOTE, copia arquivos e cria "$MOCK_MC_CALLS.copied".
#   Os arquivos copiados sobrescrevem silenciosamente os já existentes.
#
# Códigos de saída:
#   0   Sucesso.
#   1   Falha simulada (variáveis MOCK_MC_*) ou falha de cp definida por
#       MOCK_MC_FAIL_NAME.
#   90  MOCK_MC_REMOTE ou MOCK_MC_CALLS ausente/vazia.
#   91  --config-dir ausente, fora da primeira posição ou sem valor.
#   92  Falha ao registrar a chamada em MOCK_MC_CALLS.
#   93  Comando não suportado.
#   94  ls/cp sem `--json --` logo após o comando.
#   95  DESTINO vazio, absoluto ou contendo segmento `..`.
#   96  Falha ao obter o tamanho de um item listado.
#   97  Falha ao criar o diretório de destino do cp.
#   98  Falha ao copiar um arquivo no cp.
#
# Observações:
#   Apenas `set -o pipefail` é ativado; não há `set -e` nem `set -u`. Os
#   códigos de saída são determinados pelas verificações explícitas abaixo.
# =============================================================================
set -o pipefail

# Este executável nunca acessa rede: o estado remoto vive apenas no fixture.
[[ -n ${MOCK_MC_REMOTE:-} && -n ${MOCK_MC_CALLS:-} ]] || exit 90
[[ ${1:-} == --config-dir && -n ${2:-} ]] || exit 91
shift 2
command_name=${1:-}
shift
# A chamada é registrada antes de qualquer validação do comando; assim, até
# comandos não suportados (que terminam com 93) ficam em MOCK_MC_CALLS.
jq -cn --arg command "$command_name" --args \
    '{command: $command, args: $ARGS.positional}' -- "$@" >> "$MOCK_MC_CALLS" || exit 92

case "$command_name" in
    --version)
        printf 'mc version RELEASE.mock\n'
        exit 0
        ;;
    # `stat` não valida argumentos nem consulta MOCK_MC_REMOTE: a resposta
    # (pasta existente) é fixa, salvo MOCK_MC_STAT_FAIL.
    stat)
        [[ ${MOCK_MC_STAT_FAIL:-0} == 0 ]] || exit 1
        printf '{"status":"success","type":"folder","size":0}\n'
        exit 0
        ;;
    ls|cp) ;;
    *) printf 'Unexpected mock command: %s\n' "$command_name" >&2; exit 93 ;;
esac
[[ ${1:-} == --json && ${2:-} == -- ]] || exit 94
shift 2
# O último argumento é o destino; barra final removida. Caminhos absolutos ou com
# segmento `..` são rejeitados para que o mock não saia de MOCK_MC_REMOTE.
target=${!#}
target=${target%/}
[[ -n $target && $target != /* && /$target/ != */../* ]] || exit 95
remote_path="$MOCK_MC_REMOTE/$target"

# Ramo `ls`: lista apenas os filhos diretos do destino, ordenados por nome. Pastas
# têm chave terminada em "/". Destino inexistente produz saída vazia com código 0.
# As falhas injetadas são avaliadas nesta ordem: FAIL_LIST, VERIFY_FAIL, BAD_JSON,
# ERROR_JSON, NUL_KEY.
if [[ $command_name == ls ]]; then
    [[ ${MOCK_MC_FAIL_LIST:-} != "$target" ]] || exit 1
    if [[ ${MOCK_MC_VERIFY_FAIL:-0} == 1 && -e $MOCK_MC_CALLS.copied ]]; then exit 1; fi
    if [[ ${MOCK_MC_BAD_JSON:-0} == 1 ]]; then printf 'invalid JSON\n'; exit 0; fi
    if [[ ${MOCK_MC_ERROR_JSON:-0} == 1 ]]; then
        printf '{"status":"error","error":{"message":"Access denied"}}\n'
        exit 0
    fi
    if [[ ${MOCK_MC_NUL_KEY:-0} == 1 ]]; then
        jq -cn '{status:"success",type:"file",key:"bad\u0000name",size:1}'
        exit 0
    fi
    [[ -d $remote_path ]] || exit 0
    # O laço lê de uma substituição de processo para executar no shell principal;
    # assim, `exit 96` encerra o script. Os caminhos são separados por NUL.
    while IFS= read -r -d '' path; do
        key=${path##*/}
        if [[ -d $path ]]; then
            jq -cn --arg key "$key/" '{status:"success",type:"folder",key:$key,size:0}'
        else
            size=$(stat -c %s -- "$path") || exit 96
            jq -cn --arg key "$key" --argjson size "$size" \
                '{status:"success",type:"file",key:$key,size:$size}'
        fi
    done < <(find "$remote_path" -mindepth 1 -maxdepth 1 -print0 | sort -z)
    exit 0
fi

# Ramo `cp`: o destino é criado e o marcador ".copied" é gravado antes de copiar,
# mesmo que depois haja falha injetada. Todos os argumentos, exceto o último,
# são tratados como origens e copiados para o destino com o seu nome-base.
# Sempre imprime {"status":"success"}; falhas aparecem apenas no código de saída
# (MOCK_MC_FAIL_NAME define 1 ao final do laço, sem interrompê-lo).
mkdir -p -- "$remote_path" || exit 97
touch -- "$MOCK_MC_CALLS.copied"
result=0
while (( $# > 1 )); do
    source_path=$1
    name=${source_path##*/}
    if [[ ${MOCK_MC_FAIL_NAME:-} == "$name" ]]; then
        result=1
    elif [[ ${MOCK_MC_LIE_NAME:-} != "$name" ]]; then
        cp -- "$source_path" "$remote_path/$name" || exit 98
        if [[ ${MOCK_MC_WRONG_SIZE:-} == "$name" ]]; then printf x >> "$remote_path/$name"; fi
        if [[ ${MOCK_MC_MUTATE_SOURCE:-} == "$name" ]]; then printf x >> "$source_path"; fi
    fi
    shift
done
printf '{"status":"success"}\n'
exit "$result"
