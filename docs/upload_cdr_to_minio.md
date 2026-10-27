# Transferência semanal de CDRs para MinIO

O [script consolidado](../scripts/upload_cdr_to_minio.sh) seleciona e envia CDRs
texto e/ou Parquet Teleparser de uma semana. A implementação é Bash, interpreta
JSON com `jq` e utiliza o MinIO Client `mc`. Não transforma o conteúdo dos CDRs,
não descomprime arquivos e não modifica, move ou exclui as origens.

## 1. Análise dos scripts originais

Os três scripts foram lidos integralmente e comparados com os anexos da tarefa:
as cópias eram idênticas. Nenhum original foi alterado.

| Referência | Responsabilidades |
|---|---|
| [upload_all_cdr_to_minio.sh](../tests/scripts/upload_all_cdr_to_minio.sh) | Valida dois caminhos semanais, exige nomes de semana iguais e executa texto seguido de Teleparser. A segunda etapa continua se a primeira falhar. Consolida status e duração; não possui mapeamentos próprios nem copia diretamente. |
| [upload_text_cdr_to_minio.sh](../tests/scripts/upload_text_cdr_to_minio.sh) | Aplica 12 mapeamentos de CDRs texto STFC, predominantemente `*.gz`. Ignora diretórios não mapeados. Descobre arquivos, consulta o destino, copia em lotes e verifica tamanhos. |
| [upload_teleparser_cdr_to_minio.sh](../tests/scripts/upload_teleparser_cdr_to_minio.sh) | Aplica 11 mapeamentos `*.parquet`. Antes da cópia, procura pastas de profundidade exatamente 2, correspondentes a prestadora/subpasta; diretórios não mapeados bloqueiam essa categoria. |

### Mapeamento e descoberta legados

`FOLDER_MAPPING` é um array Bash de strings `origem|máscara|destino`, com
espaços de alinhamento removidos antes do uso. A origem é relativa ao diretório
semanal, pode conter `*`, `?` e classes `[...]` e pode indicar uma pasta ou
diretamente um arquivo.

Os scripts usam globs, `nullglob`, `IFS` vazio e arrays. Em cada diretório
selecionado, somente os arquivos diretamente contidos que atendem à máscara são
copiados. Não há recursividade. A auditoria Teleparser usa `find` com
`-mindepth 2 -maxdepth 2 -type d -print0`, seguido de `sort -z`.

Mapeamentos idênticos são deduplicados. Pastas mapeadas ausentes ou sem arquivos
geram pendências. Um caminho literal inexistente pode ser registrado pelo
legado como “sem arquivos”, pois `nullglob` não remove uma palavra sem curingas.
Comentários Bash fora das strings são ignorados pelo shell; não existe
tratamento específico de strings do array iniciadas por `#`.

### Organização das origens confirmada

O registro de uma inspeção anterior de diretórios e contagens em `Semana110`,
sem leitura do conteúdo dos CDRs, descreve a organização abaixo. Nesta validação,
as origens reais não foram acessadas; os testes utilizam diretórios temporários.

- Teleparser utiliza estruturas como `algar/voz`, `claro/fixa_sigma` e
  `tim/ngn_italtel`, com nomes em minúsculas.
- Texto utiliza `Oi/CDR`, `Vivo/FIXA`, `Algar/FIXA/NGN` e
  `Tim/FIXA/NGN_Huawei`, com nomes sensíveis a maiúsculas/minúsculas.
- Claro texto possui
  `Claro/home/*/*/coleta/coletados/<equipamento>_<tipo>`.
- Existem pastas de texto que não devem ser enviadas pelos mapeamentos atuais.
- `claro/volte` não existia no Teleparser dessa semana. Seu mapeamento foi
  preservado; ausência em uma semana não justifica removê-lo da configuração.

Essas observações não certificam a estrutura de todas as semanas.

### Transferência, lotes e erros legados

Os dois scripts individuais constroem:

```text
<alias>/<bucket>/<prefixo>/<semana>/<destino_do_mapeamento>/<basename>
```

Arquivos de várias pastas correspondentes a um glob são achatados no mesmo
destino. A árvore local não é reproduzida.

O padrão antigo é `bganatel/sandbox-sfi/cdr/ingestao`. O destino era resolvido
pela CLI, depois pela variável `MINIO_DESTINATION`, depois pelo valor interno.
`BATCH_SIZE` também podia vir do ambiente, com padrão 100.

`list_remote_objects` executa `mc ls --json` por destino e constrói uma relação
nome/tamanho. Objetos de mesmo nome e tamanho são pulados. Tamanho diferente
gera conflito sem sobrescrita. Cada `mc cp` recebe um lote de fontes para uma
única pasta remota; as chamadas são sequenciais. Após a cópia, outra listagem
confirma os arquivos pelo tamanho, inclusive quando o lote retornou erro.

Os relatórios individuais usam arrays por mapeamento, `record_result` e
`register_pending`. O orquestrador agrega códigos e duração. Não usam `set -e`,
permitindo continuar após falhas recuperáveis. Os códigos individuais são
0 sem pendências, 1 com pendências/erros e 2 para uso/ambiente inválido.

Problemas tratados na consolidação:

- Regex sobre JSON não decodifica corretamente nomes escapados.
- Falhas de `mc ls` eram ocultadas, podendo parecer um destino vazio.
- EOF em substituição de processo não confirmava sucesso de `find`.
- A deduplicação local por basename/tamanho podia descartar arquivos distintos
  e não detectava colisões entre todos os mapeamentos/categorias.
- O dry-run reutilizava contadores internos de cópias para operações simuladas.
- Faltavam validações de caminhos, traversal e acessibilidade.

## 2. Decisões de compatibilidade

1. **A semana permanece antes do mapeamento**, diferentemente do exemplo
   conceitual inicial da tarefa. Mover a semana mudaria todas as chaves já
   utilizadas pelos consumidores.
2. Preservam-se os **23 mapeamentos**, padrões, capitalização e achatamento.
   Não são acrescentadas subpastas nem o nome da categoria às chaves.
3. O JSON agrupa as listas em `folder_mapping.text` e
   `folder_mapping.teleparser`. Isso elimina inferência pela extensão e
   permite ignorar a categoria não selecionada.
4. O dry-run mantém consultas remotas de leitura para identificar existentes
   e conflitos, mas conta zero transferências e zero lotes executados.
5. Texto não mapeado é ignorado. Teleparser não mapeado bloqueia somente
   Teleparser. Mapeamentos ausentes/vazios permanecem pendências.
6. Arquivos locais distintos para a mesma chave geram colisão fatal antes das
   cópias, **mesmo com tamanhos iguais**. Não há renomeação automática.
7. Links internos à raiz semanal física resolvida são permitidos; links
   externos/quebrados são erros e não são percorridos/enviados.

## 3. Pré-requisitos

- Linux, Bash 4 ou superior.
- GNU `find`, `sort` com `-z`, `realpath` com `-e -z` e `stat` com `-L -c`.
- `jq` 1.6 ou superior.
- MinIO Client `mc`, não o programa homônimo Midnight Commander.
- Leitura e travessia dos diretórios semanais selecionados e leitura dos arquivos.
- Alias já configurado, acesso de leitura ao bucket/prefixos e permissão de
  escrita para a execução real. Leitura do bucket não comprova permissão de
  escrita: uma negação na cópia será registrada como falha.

A validação sintática e os smoke checks foram executados em Bash 5.2.15.
A execução integral em Bash 4 ainda não foi certificada.

### Estado local do `mc` e dry-run sem escrita

O cliente inicializa e migra estado local mesmo antes de alguns comandos de
leitura. Por isso, **antes de qualquer invocação de `mc`**, o script exige:

- `MC_CONFIG_DIR/config.json`, ou `$HOME/.mc/config.json`, legível;
- configuração na versão `"10"` e alias solicitado com URL HTTP(S);
- diretórios já existentes e acessíveis `share`, `certs` e `certs/CAs`;
- ausência do arquivo legado `share/urls.json`, que seria removido na migração.

Inicialize/migre o cliente separadamente, fora desta execução, com o
procedimento administrativo do seu ambiente. O script não cria esses diretórios,
não configura aliases, não cria buckets e não imprime credenciais.
Mudanças futuras no formato/inicialização do cliente devem ser reavaliadas
antes de utilizar outra versão em produção.

A versão `"10"` identifica o schema da configuração, não uma versão de release
do executável. Os testes automatizados usam um mock e não certificam releases
reais do cliente ou do servidor MinIO.

Referências consultadas:
[mc ls](https://docs.min.io/aistor/reference/cli/mc-ls/),
[mc cp](https://docs.min.io/aistor/reference/cli/mc-cp/),
[inicialização do cliente](https://github.com/minio/mc/blob/master/cmd/main.go) e
[migração do estado share](https://github.com/minio/mc/blob/master/cmd/share-main.go).

## 4. Configuração JSON

Use o [exemplo completo](../scripts/config.example.json), que contém as raízes
padrão e todos os 23 mapeamentos. Exemplo reduzido:

```json
{
  "teleparser_folder": "/assets/data/cdr/teleparser/",
  "text_folder": "/assets/monitoring/cdr/cdr_bruto/",
  "minio_config": {
    "alias": "bganatel",
    "destination": "sandbox-sfi/cdr/ingestao",
    "batchsize": 100
  },
  "folder_mapping": {
    "text": [
      {
        "source_folder": "Oi/CDR",
        "pattern": "*TN*.gz",
        "destination_folder": "oi/stfc/tropico"
      }
    ],
    "teleparser": [
      {
        "source_folder": "algar/voz",
        "pattern": "*.parquet",
        "destination_folder": "algar/smp/nokia"
      }
    ]
  }
}
```

O exemplo reduzido ilustra o formato; não cobre uma semana real com todos os
diretórios Teleparser.

| Campo | Contrato |
|---|---|
| `teleparser_folder`, `text_folder` | Diretórios-base, não diretórios semanais. Exigidos somente para categorias selecionadas sem sobrescrita pela CLI. |
| `minio_config.alias` | Alias existente no `mc`; inicia com letra, seguido de letras, dígitos, `_` ou `-`. |
| `minio_config.destination` | Bucket e prefixo opcional, sem alias e sem semana. Pode ser substituído pela CLI. |
| `minio_config.batchsize` | Número JSON inteiro entre 1 e 2147483647. O limite superior evita overflow em aritmética de 32 bits; não é um valor recomendado de lote. O exemplo utiliza 100. |
| `folder_mapping.<categoria>` | Lista não vazia para cada categoria selecionada. A lista não selecionada não é consultada nem validada. |
| `source_folder` | Caminho relativo à semana, com globs Bash por componente. Diretório ou arquivo. |
| `pattern` | Padrão de basename, sem `/`. `*.gz` não inclui `.oculto.gz`; use máscara iniciada por `.` para incluir ocultos. |
| `destination_folder` | Prefixo relativo literal, sem semana. Não é expandido como glob. |

Valores obrigatórios não têm fallback silencioso. O arquivo de exemplo fornece
os padrões; a implementação não inventa valores ausentes.
`MINIO_DESTINATION`, `BATCH_SIZE` e `SCRIPT_DIR` do legado não influenciam esta
interface. `MC_CONFIG_DIR` continua controlando a configuração externa do cliente.

O documento JSON inteiro deve ser sintaticamente válido. Tipos e valores dos
campos da categoria excluída não são validados. Valores efetivamente substituídos
pela CLI não precisam ser válidos no JSON.

## 5. CLI e precedência

```text
--config FILE                 obrigatório
--week-dir SemanaNUMBER       obrigatório, regex ^Semana[0-9]+$
--teleparser-folder BASE      opcional
--text-folder BASE            opcional
--destination BUCKET/PREFIX   opcional, sem alias
--dry-run                     opcional
--help / -h                   ajuda, saída 0 sem dependências
```

As opções com valor usam argumentos separados, não `--opção=valor`.
Opções escalares repetidas, desconhecidas e valores ausentes/vazios são erros.
Erros na validação da CLI exibem a mensagem de erro seguida da ajuda,
retornam código 2 e não exibem relatório final. Isso também se aplica à
execução sem argumentos.
O relatório é habilitado somente após a validação completa dos argumentos.

| Opções explícitas de origem | Seleção |
|---|---|
| Nenhuma | Texto e Teleparser, com as bases do JSON |
| Somente `--teleparser-folder` | **Exclusivamente Teleparser** |
| Somente `--text-folder` | **Exclusivamente texto** |
| Ambas | Ambas, usando exclusivamente as bases fornecidas pela CLI |

A CLI tem precedência sobre o JSON. A semana sempre vem da CLI e é concatenada
às bases selecionadas. Uma base já terminada no nome da semana é rejeitada.

### Exemplos

As duas categorias:

```bash
./scripts/upload_cdr_to_minio.sh \
  --config scripts/config.example.json --week-dir Semana110
```

Somente Teleparser:

```bash
./scripts/upload_cdr_to_minio.sh \
  --config scripts/config.example.json --week-dir Semana110 \
  --teleparser-folder /data/teleparser/
```

Somente texto:

```bash
./scripts/upload_cdr_to_minio.sh \
  --config scripts/config.example.json --week-dir Semana110 \
  --text-folder /data/cdr_bruto/
```

Duas bases personalizadas:

```bash
./scripts/upload_cdr_to_minio.sh \
  --config scripts/config.example.json --week-dir Semana110 \
  --teleparser-folder /data/parquet/ --text-folder /data/texto/
```

Outro bucket/prefixo:

```bash
./scripts/upload_cdr_to_minio.sh \
  --config scripts/config.example.json --week-dir Semana110 \
  --destination cdr-backup
```

Simulação com consultas remotas, sem escrita:

```bash
./scripts/upload_cdr_to_minio.sh \
  --config scripts/config.example.json --week-dir Semana110 --dry-run
```

Exemplo concreto de resolução:

```text
base:    /assets/data/cdr/teleparser/
week:    Semana110
source:  algar/voz
pattern: *.parquet
local:   /assets/data/cdr/teleparser/Semana110/algar/voz/a.parquet
target:  bganatel/sandbox-sfi/cdr/ingestao/Semana110/algar/smp/nokia/a.parquet
```

Barras redundantes/finais são normalizadas. Componentes `..` são rejeitados
antes da normalização; mapeamentos também rejeitam componentes `.` e caminhos
absolutos. O destino-base e o destino do mapeamento não podem conter a semana
como componente. O nome de um arquivo contendo a palavra `Semana110` não é
modificado.

## 6. Fluxo, conflitos e recuperação

1. Validar argumentos, dependências e configuração efetiva.
2. Resolver todas as raízes semanais selecionadas; uma raiz semanal inexistente
   ou inacessível impede iniciar as cópias.
3. Auditar Teleparser, se selecionado.
4. Expandir mapeamentos e descobrir arquivos com delimitação NUL, verificando
   explicitamente o retorno de cada produtor.
5. Construir a associação arquivo/chave e detectar colisões globais.
6. Validar o estado do cliente, sua identidade e o acesso ao bucket.
7. Para cada destino, listar objetos e classificar candidatos.
8. Dividir pendentes em lotes de no máximo `batchsize`, sem paralelismo.
9. Simular ou executar `mc cp` com arrays e um único destino por lote.
10. Confirmar cada cópia por nome/tamanho e apresentar o relatório.

Uma falha de consulta nunca equivale a destino vazio. Falhas de listagem impedem
envios para o destino afetado, mas não os demais destinos. Falhas de cópia ou
verificação permitem continuar lotes posteriores.

O mesmo caminho físico resolvido para a mesma chave é deduplicado. Caminhos
distintos para a mesma chave são conflitos, inclusive hardlinks com nomes
distintos. O mesmo arquivo para chaves diferentes representa operações distintas.

Tamanhos remotos divergentes não são sobrescritos. Mesmo tamanho remoto é
considerado existente, **não comprova identidade do conteúdo**.
Mudanças observáveis do tamanho ou do alvo simbólico local durante o envio
impedem a certificação daquele arquivo.

### Limitações operacionais

- O script não calcula checksum.
- Arquivos de origem devem permanecer estáveis durante a execução.
- Listar/copiar/verificar não é uma operação atômica contra outros escritores.
  Coordene externamente execuções para o mesmo prefixo; não há lock distribuído.
- Planejamento e listagens ficam em memória para detectar colisões antes de
  copiar. Lotes muito grandes também estão sujeitos ao limite de argumentos
  do sistema operacional; mantenha um valor operacional razoável, como 100.
- Não há recursividade, retries automáticos, criação de bucket ou alteração
  de mapeamentos para acomodar pastas desconhecidas.
- Logs operacionais vão para stderr e o relatório/planejamento para stdout.
  Não há arquivo de log criado pelo script.

## 7. Relatório e automação

Mensagens, identificadores, ajuda e campos de relatório são em inglês.
Caminhos são escapados com `printf '%q'`, preservando a distinção entre nomes
com espaços, caracteres de controle e quebras de linha.

| Campo | Significado |
|---|---|
| `week`, `categories`, `source_text`, `source_teleparser`, `destination_base` | Configuração resolvida; raízes não selecionadas não aparecem. |
| `files_found` | Caminhos físicos resolvidos únicos elegíveis pelas máscaras. Não conta todos os arquivos arbitrários da árvore. |
| `files_selected` | Associações arquivo/chave únicas após a validação/deduplicação local, antes de comparar o remoto. |
| `files_existing` | Associações dispensadas por mesmo nome/tamanho remoto. |
| `files_planned` | Operações pendentes organizadas para transferência/simulação. |
| `files_transferred` | Operações confirmadas; sempre zero no dry-run. |
| `failures` | Eventos de erro, não quantidade de arquivos; uma falha de lote/listagem pode afetar vários arquivos. |
| `pending_mappings` | Mapeamentos ausentes, vazios ou bloqueados pela auditoria. |
| `batches_planned` | Lotes planejados, inclusive no dry-run. |
| `batches_processed` | Lotes realmente submetidos ao `mc cp`; zero no dry-run. |
| `duration_seconds`, `status` | Duração total e resultado. |

| Situação | Status | Código |
|---|---|---|
| Execução real sem erros/pendências, inclusive apenas objetos existentes | `SUCCESS` | 0 |
| Sucessos/existentes confirmados e também erros/pendências | `PARTIAL_SUCCESS` | 1 |
| Erros/pendências sem resultados confirmados | `FAILED` | 1 |
| Simulação sem erros/pendências | `DRY_RUN` | 0 |
| Simulação concluída com erros recuperáveis/pendências | `DRY_RUN` | 1 |
| Argumentos da CLI inválidos ou ausentes | Sem relatório | 2 |
| Configuração, dependências, raízes semanais ou acesso inicial inválidos | `FAILED` | 2 |
| Colisão local global, antes das cópias | `FAILED` | 1 |
| SIGINT / SIGTERM | `FAILED` | 130 / 143 |

Use o código de saída, não somente o texto `DRY_RUN`, para automação.

## 8. Testes e validação

A [suíte Bats](../tests/shell/upload_cdr_to_minio.bats) utiliza um
[mock Bash de mc](../tests/shell/helpers/mock_mc.bash), configuração isolada e
diretórios temporários administrados pelo Bats. O mock nunca acessa a rede.
Seu registro de chamadas é instrumentação do teste, não escrita do uploader.

Cobertura implementada:

- quatro combinações de seleção, configuração ignorada e precedência;
- semana obrigatória, raízes semanais, normalização e traversal;
- JSON inválido, campos inválidos, batchsize e dependências;
- todos os 23 mapeamentos e comparação com os arrays dos originais;
- globs multinível, origem arquivo, não recursividade e ocultos;
- nomes especiais, raízes literais com metacaracteres e links;
- deduplicações e colisões dentro/entre categorias;
- objetos existentes, conflitos, lotes e falhas parciais;
- JSON remoto inválido, falhas de consulta/verificação e continuidade;
- rejeição de NUL em todos os campos de caminhos/mapeamentos e em chaves remotas;
- dry-run sem escrita, contadores, relatório e códigos;
- preservação dos arquivos locais.

Com `jq`, Bats e ShellCheck disponíveis **no container**, execute na raiz:

```bash
bash -n scripts/upload_cdr_to_minio.sh
bash -n tests/shell/helpers/mock_mc.bash
bash -n tests/shell/upload_cdr_to_minio.bats
jq -e . scripts/config.example.json
shellcheck scripts/upload_cdr_to_minio.sh tests/shell/helpers/mock_mc.bash
bats tests/shell/upload_cdr_to_minio.bats
```

Os testes não exigem o cliente real `mc`, credenciais ou acesso ao MinIO.
Para executar o uploader fora dos testes, `mc` é obrigatório.

### Resultado efetivamente obtido nesta entrega

Validação executada em 26/10/2026, em Linux com Bash 5.2.15, jq 1.6,
Bats 1.8.2 e ShellCheck 0.9.0:

- `bash -n` no uploader, mock e suíte Bats: aprovado.
- `jq -e . scripts/config.example.json`: aprovado.
- ShellCheck no uploader e mock: aprovado, sem avisos.
- Suíte Bats: **52 testes executados, 52 aprovados, zero falhas**.
- Os 23 mapeamentos foram exercitados e comparados aos arrays dos originais.
- `git diff --exit-code` nos três scripts originais: aprovado, sem alterações.
- Diagnósticos do editor nos arquivos Bash alterados: sem erros.
- Nenhuma dependência foi instalada durante esta validação.
- Não foram realizadas transferências nem consultas ao MinIO real.

O baseline inicial apresentou 36 falhas em 50 testes. Foram corrigidos dois
problemas: `contains("\u0000")` no jq 1.6 produzia falsos positivos para strings
normais, e o mock não separava opções de argumentos ao registrar chamadas no
jq. A implementação utiliza `index("\u0000") == null`; duas regressões adicionais
confirmam que NUL real continua sendo rejeitado. Os avisos do ShellCheck foram
resolvidos com condições explícitas e diretivas locais para chamadas indiretas
e variáveis próprias do jq.

Esses resultados validam o contrato de transferência com o mock, mas não
certificam integração com um serviço MinIO real nem a ausência de escritas
implícitas em releases do cliente que não tenham sido avaliados separadamente.
