#!/usr/bin/env bash

# =============================================================================
# Descrição:
#   Dublê (mock) do cliente `mc` usado pelos testes Bats de
#   scripts/upload_cdr_to_minio_parallel.sh. Não acessa rede: o "armazenamento
#   remoto" é o diretório local MOCK_MC_REMOTE. Diferente de mock_mc.bash, o
#   `ls` aqui é recursivo (as chaves são relativas ao prefixo listado) e o
#   `cp` pode ser chamado em paralelo.
#
# Sintaxe aceita:
#   mc --config-dir DIR --version
#   mc --config-dir DIR stat [ARGUMENTOS...]
#   mc --config-dir DIR ls --recursive --json -- DESTINO
#   mc --config-dir DIR cp --json -- ORIGEM [ORIGEM...] DESTINO
#
# Variáveis de ambiente:
#   MOCK_MC_REMOTE     Obrigatória. Diretório local que representa o remoto.
#   MOCK_MC_CALLS      Obrigatória. Arquivo que registra cada chamada (JSON por
#                      linha), incluindo MC_UPLOAD_MULTIPART_THREADS visto.
#   MOCK_MC_LS_FAIL    1: `ls` retorna 1.
#   MOCK_MC_FAIL_NAME  Nome de arquivo que o `cp` não copia; o cp termina com 1.
#   MOCK_MC_WRONG_SIZE Nome de arquivo cuja cópia remota recebe 1 byte extra.
#   MOCK_MC_SLEEP      Segundos de espera dentro do `cp` (para testar paralelismo).
#
# Códigos de saída: 0 sucesso; 1 falha simulada; 90/91 ambiente inválido;
#   93 comando não suportado; 94 sintaxe inesperada; 95 destino inválido.
# =============================================================================
set -o pipefail

[[ -n ${MOCK_MC_REMOTE:-} && -n ${MOCK_MC_CALLS:-} ]] || exit 90
[[ ${1:-} == --config-dir && -n ${2:-} ]] || exit 91
shift 2
command_name=${1:-}
shift
jq -cn --arg command "$command_name" --arg threads "${MC_UPLOAD_MULTIPART_THREADS:-}" \
    --args '{command: $command, threads: $threads, args: $ARGS.positional}' -- "$@" \
    >> "$MOCK_MC_CALLS"

case "$command_name" in
    --version) printf 'mc version RELEASE.mock\n'; exit 0 ;;
    stat) printf '{"status":"success","type":"folder","size":0}\n'; exit 0 ;;
    ls)
        [[ ${1:-} == --recursive && ${2:-} == --json && ${3:-} == -- ]] || exit 94
        shift 3
        ;;
    cp)
        [[ ${1:-} == --json && ${2:-} == -- ]] || exit 94
        shift 2
        ;;
    *) exit 93 ;;
esac

target=${!#}
target=${target%/}
[[ -n $target && $target != /* && /$target/ != */../* ]] || exit 95
remote_path="$MOCK_MC_REMOTE/$target"

if [[ $command_name == ls ]]; then
    [[ ${MOCK_MC_LS_FAIL:-0} != 1 ]] || exit 1
    [[ -d $remote_path ]] || exit 0
    find "$remote_path" -type f -printf '%P\t%s\n' |
        jq -Rc 'split("\t") | {status: "success", type: "file", key: .[0], size: (.[1] | tonumber)}'
    exit 0
fi

mkdir -p -- "$remote_path" || exit 97
[[ -z ${MOCK_MC_SLEEP:-} ]] || sleep "$MOCK_MC_SLEEP"
result=0
while (( $# > 1 )); do
    name=${1##*/}
    if [[ ${MOCK_MC_FAIL_NAME:-} == "$name" ]]; then
        result=1
    else
        cp -- "$1" "$remote_path/$name" || exit 98
        if [[ ${MOCK_MC_WRONG_SIZE:-} == "$name" ]]; then printf x >> "$remote_path/$name"; fi
    fi
    shift
done
printf '{"status":"success"}\n'
exit "$result"
