# Transferência semanal de CDRs para MinIO

Três scripts Bash em [scripts/](../scripts) selecionam CDRs texto e/ou Parquet
Teleparser de uma semana e os enviam ao MinIO ou os copiam para uma pasta local.
Todos interpretam o mesmo arquivo de configuração JSON com `jq`, aceitam
praticamente os mesmos argumentos e aplicam as mesmas regras de seleção. Não
transformam o conteúdo dos CDRs, não descomprimem arquivos e não modificam,
movem ou excluem as origens.

| Script | Finalidade | Particularidades |
|---|---|---|
| [upload_cdr_to_minio.sh](../scripts/upload_cdr_to_minio.sh) | Envia ao MinIO com `mc`, em lotes sequenciais. | Uma listagem remota por destino; links simbólicos internos à raiz semanal são aceitos. |
| [upload_cdr_to_minio_parallel.sh](../scripts/upload_cdr_to_minio_parallel.sh) | Versão de alto desempenho do upload, sem etapa de cópia local intermediária. | Vários `mc cp` simultâneos (`--jobs`), threads de multipart (`--multipart-threads`), uma listagem remota recursiva antes e outra depois do envio. Requer Bash 4.4. |
| [copy_cdr_to_local.sh](../scripts/copy_cdr_to_local.sh) | Equivalente local: copia os mesmos arquivos para um diretório do servidor. | Cópias simultâneas com `cp`, grava em pasta temporária e move por rename; verifica espaço livre. Requer Bash 4.4. |

Salvo indicação contrária, as seções abaixo valem para os três scripts; as
diferenças são destacadas.

## 1. Decisões de compatibilidade

1. **A semana vem antes do mapeamento** no destino
   (`.../<semana>/<destination_folder>/<arquivo>`). Os consumidores já utilizam
   essas chaves; mover a semana as alteraria.
2. O JSON de exemplo, [uploader.config.example.json](../scripts/uploader.config.example.json),
   contém **23 mapeamentos** (12 de texto e 11 de Teleparser). Padrões,
   capitalização e achatamento são preservados: arquivos de várias pastas
   correspondentes ao mesmo mapeamento caem no mesmo destino, sem reproduzir a
   árvore de origem nem acrescentar o nome da categoria à chave.
3. O JSON agrupa os mapeamentos em `folder_mapping.text` e
   `folder_mapping.teleparser`. Isso elimina inferência pela extensão e permite
   ignorar a categoria não selecionada.
4. O `--dry-run` não grava nada, mas consulta o destino (MinIO ou pasta local)
   para identificar objetos existentes e conflitos. Conta zero transferências e
   zero lotes executados.
5. Texto não mapeado é ignorado. Teleparser não mapeado bloqueia somente a
   categoria Teleparser. Mapeamentos ausentes ou sem arquivos são pendências.
6. Arquivos locais distintos para a mesma chave de destino geram colisão fatal
   antes de qualquer cópia, **mesmo com tamanhos iguais**. Não há renomeação
   automática.
7. Objetos ou arquivos já existentes nunca são sobrescritos: mesmo tamanho conta
   como existente (execução idempotente) e tamanho diferente é erro.
8. Tratamento de links simbólicos:
   - `upload_cdr_to_minio.sh`: links internos à raiz semanal física resolvida
     são permitidos; links externos ou quebrados são erros e não são enviados.
   - `upload_cdr_to_minio_parallel.sh` e `copy_cdr_to_local.sh`: apenas arquivos
     regulares são processados; um link simbólico que casa com o padrão **não é
     seguido** e é reportado como erro. Diretórios mapeados são resolvidos e
     precisam permanecer dentro da raiz semanal.

## 2. Pré-requisitos

- Linux. Bash 4 ou superior para `upload_cdr_to_minio.sh`; Bash 4.4 ou superior
  para `upload_cdr_to_minio_parallel.sh` e `copy_cdr_to_local.sh`.
- `jq` 1.6 ou superior.
- `upload_cdr_to_minio.sh`: GNU `find`, `sort` com `-z`, `realpath` com
  `-e -z` e `stat` com `-L -c`, e `mc`.
- `upload_cdr_to_minio_parallel.sh`: GNU `find`, `realpath`, `mktemp`, `rm` e `mc`.
- `copy_cdr_to_local.sh`: GNU `find`, `cp`, `mv`, `rm`, `mkdir`, `mktemp`,
  `realpath`, `df`, `tail` e `nproc`. Não usa `mc`.
- MinIO Client `mc`, não o programa homônimo Midnight Commander.
- Leitura e travessia dos diretórios semanais selecionados e leitura dos arquivos.
- Nos uploads: alias já configurado, acesso de leitura ao bucket/prefixos e
  permissão de escrita para a execução real. Leitura do bucket não comprova
  permissão de escrita: uma negação na cópia será registrada como falha.
- Na cópia local: permissão de escrita no destino.

Os scripts verificam a presença das ferramentas no `PATH`; a ausência de uma
delas encerra com código 2. A compatibilidade real depende de as versões
instaladas aceitarem as opções citadas.

O registro de validação da seção 7 foi feito em Bash 5.2.15. A execução
integral em Bash 4 / 4.4 não foi certificada.

### Estado local do `mc` e dry-run sem escrita

Aplica-se somente aos dois scripts de upload.

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

## 3. Arquivo JSON de configuração

Os três scripts recebem o arquivo por `--config` e aceitam **o mesmo arquivo**.
Cada script lê apenas as chaves de que precisa e ignora as demais; por isso um
único JSON pode servir ao upload serial, ao upload paralelo e à cópia local.

### 3.1 Estrutura geral

O arquivo deve conter **um único objeto JSON** válido. Estrutura completa, com
todas as chaves reconhecidas (valores do
[exemplo completo](../scripts/uploader.config.example.json), mais as chaves
opcionais `jobs` e `multipart_threads` de `minio_config`, que o exemplo não traz):

```json
{
  "teleparser_folder": "/assets/data/cdr/teleparser/",
  "text_folder": "/assets/monitoring/cdr/cdr_bruto/",
  "minio_config": {
    "alias": "bganatel",
    "destination": "sandbox-sfi/cdr/uploaded",
    "batchsize": 100,
    "jobs": 3,
    "multipart_threads": 16
  },
  "local_config": {
    "destination": "/assets/data/cdr/to_minio",
    "jobs": 8
  },
  "folder_mapping": {
    "text": [ { "source_folder": "...", "pattern": "...", "destination_folder": "..." } ],
    "teleparser": [ { "source_folder": "...", "pattern": "...", "destination_folder": "..." } ]
  }
}
```

| Bloco | Finalidade |
|---|---|
| `teleparser_folder`, `text_folder` | Diretórios-base de origem de cada categoria. |
| `minio_config` | Destino e ajustes de envio ao MinIO. |
| `local_config` | Destino e ajustes da cópia local (`copy_cdr_to_local.sh`). |
| `folder_mapping` | Regras que ligam pastas de origem a pastas de destino, por categoria. |

### 3.2 Chaves reconhecidas por script

| Chave | `upload_cdr_to_minio.sh` | `upload_cdr_to_minio_parallel.sh` | `copy_cdr_to_local.sh` |
|---|---|---|---|
| `teleparser_folder`, `text_folder` | Obrigatória para cada categoria selecionada cuja base não vem da CLI | Idem | Idem |
| `minio_config` (objeto) | Obrigatório | Obrigatório | Opcional |
| `minio_config.alias` | Obrigatório | Obrigatório | Não usado |
| `minio_config.destination` | Obrigatório, exceto com `--destination` | Idem | Não usado |
| `minio_config.batchsize` | Obrigatório | Opcional (padrão 500) | Só como alternativa a `local_config.batchsize` |
| `minio_config.jobs` | Ignorado | Opcional (padrão 3) | Ignorado |
| `minio_config.multipart_threads` | Ignorado | Opcional | Ignorado |
| `local_config.destination` | Ignorado | Ignorado | Obrigatório, exceto com `--destination` |
| `local_config.jobs` | Ignorado | Ignorado | Opcional (padrão `min(nproc, 8)`) |
| `local_config.batchsize` | Ignorado | Ignorado | Opcional; se ausente, usa `minio_config.batchsize` e, na falta dele, 100 |
| `folder_mapping.text`, `folder_mapping.teleparser` | Lista não vazia por categoria selecionada | Idem | Idem |

Tipos e faixas:

| Chave | Tipo e regra |
|---|---|
| `teleparser_folder`, `text_folder` | Texto não vazio, caminho **base** (não a pasta da semana). A semana é acrescentada pelo script; uma base já terminada no nome da semana é rejeitada. A pasta `<base>/<semana>` precisa existir e ser acessível. |
| `minio_config.alias` | Texto; inicia com letra, seguida de letras, dígitos, `_` ou `-`. Deve existir no `mc`. |
| `minio_config.destination` | Texto `bucket[/prefixo]`, relativo, **sem alias, sem URL (`://`), sem `?` ou `#` e sem a semana como componente**. O primeiro componente deve ser um nome de bucket S3 (minúsculas, dígitos, `.` e `-`, iniciando e terminando com letra ou dígito). |
| `minio_config.batchsize` | Número inteiro de 1 a 2147483647: máximo de arquivos por chamada `mc cp` em cada destino. O limite superior evita overflow aritmético; não é um valor recomendado (o exemplo usa 100). |
| `minio_config.jobs`, `local_config.jobs` | Inteiro de 1 a 256: lotes simultâneos. |
| `minio_config.multipart_threads` | Inteiro de 1 a 256, exportado como `MC_UPLOAD_MULTIPART_THREADS`. Se ausente no JSON e na CLI, o script paralelo usa o valor numérico válido (1 a 256) já presente nessa variável de ambiente ou 16. |
| `local_config.destination` | Texto: diretório local de destino (raiz); a semana é acrescentada. Caminho relativo é resolvido a partir do diretório atual. Não pode ser `/`, conter a semana como componente, coincidir com a pasta semanal de origem nem se sobrepor a ela. |
| `local_config.batchsize` | Inteiro de 1 a 2147483647: máximo de arquivos por chamada `cp`. |
| `folder_mapping` | Objeto cujas chaves são as categorias `text` e `teleparser`. |

Valores obrigatórios não têm fallback silencioso. Chaves opcionais ausentes
recebem os padrões indicados. Chaves de categorias não selecionadas, ou cujo
valor foi substituído por uma opção da CLI, não são lidas nem validadas; o
documento JSON como um todo precisa, porém, ser sintaticamente válido.
`MC_CONFIG_DIR` não faz parte do JSON: controla a configuração externa do `mc`.

### 3.3 Mapeamentos (`folder_mapping`)

Cada categoria é uma **lista não vazia** de objetos com exatamente os três
campos abaixo, todos texto não vazio e sem caractere NUL:

| Campo | Contrato |
|---|---|
| `source_folder` | Caminho relativo à pasta da semana (`<base>/<semana>/`). Sem `..`, sem componente `.` e sem `/` inicial. Cada componente aceita os curingas `*`, `?` e `[...]`. Pode indicar uma pasta (seus filhos imediatos são examinados) ou diretamente um arquivo. Cada correspondência deve permanecer dentro da pasta semanal após a resolução de links simbólicos. |
| `pattern` | Glob aplicado **apenas ao nome** do arquivo; não pode conter `/` nem ser `.` ou `..`. Não há recursão. `*.gz` não inclui arquivos ocultos (`.oculto.gz`); use um padrão iniciado por `.` para incluí-los. |
| `destination_folder` | Prefixo relativo **literal** (não é expandido como glob), sem `..`, sem `.` como componente, sem `/` inicial e sem a semana como componente. Nos uploads, também sem `?`, `#` ou `://`. |

Como o destino é montado:

```text
MinIO:  <alias>/<bucket>/<prefixo>/<semana>/<destination_folder>/<arquivo>
Local:  <destino>/<semana>/<destination_folder>/<arquivo>
```

Regras de interpretação:

- Barras repetidas e finais são normalizadas.
- Arquivos de várias pastas casadas pelo mesmo `source_folder` com curingas são
  **achatados** no mesmo destino; a árvore de origem não é reproduzida.
- Mapeamentos idênticos (categoria, origem, padrão e destino) são ignorados com
  uma mensagem `INFO`.
- Dois arquivos de origem distintos que resultem na mesma chave de destino são
  uma colisão fatal, detectada antes de qualquer cópia.
- Mapeamento cuja pasta não existe, ou que não casa nenhum arquivo, é
  **pendência** e faz o código de saída ser 1.
- Categoria `teleparser`: todo diretório de segundo nível abaixo da semana
  (`primeiro/segundo`) deve casar com o `source_folder` de ao menos um
  mapeamento; caso contrário a categoria é bloqueada e seus mapeamentos ficam
  pendentes. A categoria `text` ignora diretórios não mapeados.
- Os nomes de pastas de texto são sensíveis a maiúsculas/minúsculas
  (`Oi/CDR`, `Vivo/FIXA`); os de Teleparser do exemplo estão em minúsculas.

### 3.4 Como construir o arquivo

1. Copie [uploader.config.example.json](../scripts/uploader.config.example.json)
   para um arquivo seu e ajuste `teleparser_folder` e `text_folder` para os
   diretórios que **contêm** as pastas `SemanaNNN`.
2. Em `minio_config`, informe o `alias` já configurado no `mc` e o `destination`
   como `bucket` ou `bucket/prefixo`, sem alias e sem semana. Ajuste
   `batchsize` e, se usar o script paralelo, `jobs` e `multipart_threads`.
3. Para a cópia local, preencha `local_config.destination` (e, se desejar,
   `jobs`). Se o arquivo só servir ao upload, o bloco `local_config` pode ser omitido.
4. Em `folder_mapping`, adicione um objeto por combinação
   origem/padrão/destino. Os destinos do exemplo seguem a forma
   `<prestadora>/<serviço>/<fabricante>` (`claro/stfc/axe`, `tim/smp/volte`);
   é uma convenção observada nos exemplos, não validada pelos scripts.
5. Se uma pasta de origem tiver tipos de arquivo diferentes, crie um
   mapeamento por padrão, repetindo o `source_folder` (como `Oi/CDR`, com
   `*7N*.gz` e `*TN*.gz` em destinos distintos).
6. Valide a sintaxe: `jq -e . meu_config.json`. Isso não valida as regras
   acima; quem as valida é o script.
7. Execute com `--dry-run` e confira as linhas `PLAN origem -> destino` e os
   mapeamentos pendentes antes da execução real.

Exemplo reduzido (não cobre uma semana real com todos os diretórios Teleparser;
com o `source_folder` de Teleparser abaixo, qualquer outro diretório de segundo
nível da semana bloquearia a categoria):

```json
{
  "teleparser_folder": "/assets/data/cdr/teleparser/",
  "text_folder": "/assets/monitoring/cdr/cdr_bruto/",
  "minio_config": {
    "alias": "bganatel",
    "destination": "sandbox-sfi/cdr/uploaded",
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

## 4. CLI e precedência

Opções dos três scripts:

```text
--config FILE                 obrigatório
--week-dir SemanaNUMBER       obrigatório, regex ^Semana[0-9]+$
--teleparser-folder BASE      opcional
--text-folder BASE            opcional
--destination CAMINHO         opcional (ver abaixo)
--dry-run                     opcional
--help / -h                   ajuda, saída 0
```

Opções adicionais e significado de `--destination` por script:

| Script | `--destination` | Opções adicionais |
|---|---|---|
| `upload_cdr_to_minio.sh` | `bucket[/prefixo]`, sem alias | nenhuma |
| `upload_cdr_to_minio_parallel.sh` | `bucket[/prefixo]`, sem alias | `--jobs N` (1 a 256; padrão: `minio_config.jobs` ou 3) e `--multipart-threads N` (1 a 256; padrão: `minio_config.multipart_threads`, o ambiente ou 16) |
| `copy_cdr_to_local.sh` | Diretório local raiz (a semana é acrescentada) | `--jobs N` (1 a 256; padrão: `local_config.jobs` ou `min(nproc, 8)`) |

As opções com valor usam argumentos separados, não `--opção=valor`.
Cada opção com valor só pode aparecer uma vez e exige valor não vazio que não
comece com `--`; opções desconhecidas e valores ausentes são erros.
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
  --config scripts/uploader.config.example.json --week-dir Semana110
```

Somente Teleparser:

```bash
./scripts/upload_cdr_to_minio.sh \
  --config scripts/uploader.config.example.json --week-dir Semana110 \
  --teleparser-folder /data/teleparser/
```

Somente texto:

```bash
./scripts/upload_cdr_to_minio.sh \
  --config scripts/uploader.config.example.json --week-dir Semana110 \
  --text-folder /data/cdr_bruto/
```

Duas bases personalizadas:

```bash
./scripts/upload_cdr_to_minio.sh \
  --config scripts/uploader.config.example.json --week-dir Semana110 \
  --teleparser-folder /data/parquet/ --text-folder /data/texto/
```

Outro bucket/prefixo:

```bash
./scripts/upload_cdr_to_minio.sh \
  --config scripts/uploader.config.example.json --week-dir Semana110 \
  --destination cdr-backup
```

Simulação com consultas remotas, sem escrita:

```bash
./scripts/upload_cdr_to_minio.sh \
  --config scripts/uploader.config.example.json --week-dir Semana110 --dry-run
```

Upload paralelo (três `mc cp` simultâneos, 16 threads de multipart cada),
simulando primeiro:

```bash
./scripts/upload_cdr_to_minio_parallel.sh \
  --config scripts/uploader.config.example.json --week-dir Semana111 \
  --jobs 3 --multipart-threads 16 --dry-run
```

Cópia local, simulando primeiro:

```bash
./scripts/copy_cdr_to_local.sh \
  --config scripts/uploader.config.example.json --week-dir Semana110 \
  --destination /data/cdr_local --jobs 8 --dry-run
```

As opções de seleção de categoria, `--teleparser-folder`, `--text-folder` e
`--destination` combinam-se da mesma forma nos três scripts. Para executar de
verdade, remova `--dry-run`.

Exemplo concreto de resolução (upload):

```text
base:    /assets/data/cdr/teleparser/
week:    Semana110
source:  algar/voz
pattern: *.parquet
local:   /assets/data/cdr/teleparser/Semana110/algar/voz/a.parquet
target:  bganatel/sandbox-sfi/cdr/uploaded/Semana110/algar/smp/nokia/a.parquet
```

Na cópia local, o alvo seria
`/assets/data/cdr/to_minio/Semana110/algar/smp/nokia/a.parquet`
(`local_config.destination` do exemplo).

Barras redundantes/finais são normalizadas. Componentes `..` são rejeitados
antes da normalização; mapeamentos também rejeitam componentes `.` e caminhos
absolutos. O destino-base e o destino do mapeamento não podem conter a semana
como componente. O nome de um arquivo contendo a palavra `Semana110` não é
modificado.

## 5. Fluxo, conflitos e recuperação

1. Validar argumentos, dependências e configuração efetiva.
2. Resolver todas as raízes semanais selecionadas; uma raiz semanal inexistente
   ou inacessível impede iniciar as cópias.
3. Auditar Teleparser, se selecionado.
4. Expandir mapeamentos e descobrir arquivos, verificando explicitamente o
   retorno de cada produtor.
5. Construir a associação arquivo/chave e detectar colisões globais; qualquer
   colisão encerra com código 1 antes de copiar.
6. Nos uploads, validar o estado do cliente `mc`, sua identidade e o acesso ao
   bucket (também no `--dry-run`). Na cópia local, não há essa etapa.
7. Listar o destino e classificar candidatos (existente, conflito ou pendente).
8. Dividir pendentes em lotes e simular ou executar a transferência.
9. Confirmar cada arquivo por nome/tamanho e apresentar o relatório.

Uma falha de consulta nunca equivale a destino vazio. Falhas de cópia ou
verificação permitem continuar lotes posteriores.

### Diferenças entre os scripts

| Aspecto | `upload_cdr_to_minio.sh` | `upload_cdr_to_minio_parallel.sh` | `copy_cdr_to_local.sh` |
|---|---|---|---|
| Execução dos lotes | Sequencial; um `mc cp` por lote e um único destino por lote | Até `jobs` lotes simultâneos (`wait -n` repõe a vaga) | Até `jobs` lotes simultâneos |
| Fechamento do lote | `batchsize` arquivos | O que ocorrer primeiro: `batchsize` (padrão 500), 4 GiB ou 65536 caracteres de linha de comando | O que ocorrer primeiro: `batchsize` (padrão 100), 1 GiB ou 65536 caracteres de linha de comando |
| Listagem do destino | `mc ls --json` por destino, antes e depois da cópia | Uma `mc ls --recursive --json` do prefixo da semana antes e outra depois | Um `find` do destino antes e outro depois |
| Falha de listagem | Impede o envio apenas ao destino afetado | Falha fatal (código 1) na listagem inicial: nada é enviado | Falha fatal (código 1) na listagem inicial |
| Descoberta de arquivos | `find`, `sort`, `realpath` e `stat` por arquivo | Glob do Bash, um `find -printf` e um `realpath` por mapeamento | Idem ao paralelo |
| Tamanho divergente após o envio | Erro; objeto remoto não removido | Erro; objeto remoto não removido | Erro; o arquivo criado por esta execução é removido |
| Escrita | `mc cp` direto ao destino remoto | `mc cp` direto ao destino remoto | `cp --reflink=auto --preserve=timestamps` para `.partial.PID.N` no destino, seguido de `mv -n` (rename atômico, sem sobrescrever) |
| Verificações extras | — | — | Espaço livre do destino antes de copiar: insuficiente é fatal (código 1) e, no `--dry-run`, apenas aviso |
| Interrupção | `FAILED` (130/143) | Encerra os workers e repassa o sinal ao `mc cp`; `FAILED` (130/143) | Encerra os workers e repassa o sinal ao `cp`; `FAILED` (130/143) |

O mesmo caminho físico resolvido para a mesma chave é deduplicado. Caminhos
distintos para a mesma chave são conflitos, inclusive hardlinks com nomes
distintos. O mesmo arquivo para chaves diferentes representa operações distintas.

Tamanhos divergentes no destino não são sobrescritos. Mesmo tamanho no destino é
considerado existente, **não comprova identidade do conteúdo**.
Mudanças observáveis do tamanho ou, no `upload_cdr_to_minio.sh`, do alvo
simbólico local durante a transferência impedem a certificação daquele arquivo.

Para reenviar o que faltou após uma falha, basta executar novamente o mesmo
comando: os arquivos já presentes com o mesmo tamanho são contados como
existentes.

### Limitações operacionais

- Os scripts não calculam checksum.
- Arquivos de origem devem permanecer estáveis durante a execução.
- Listar/copiar/verificar não é uma operação atômica contra outros escritores.
  Coordene externamente execuções para o mesmo prefixo ou diretório; não há
  lock distribuído.
- Planejamento e listagens ficam em memória para detectar colisões antes de
  copiar. Lotes muito grandes também estão sujeitos ao limite de argumentos
  do sistema operacional; mantenha um valor operacional razoável. Os scripts
  paralelo e local também limitam o lote por bytes e por tamanho da linha.
- No script paralelo, o limite por conexão e `--jobs` × `--multipart-threads`
  devem ser calibrados no ambiente alvo; os valores padrão (3 e 16) não foram
  certificados aqui como ótimos.
- Não há recursividade, retries automáticos, criação de bucket ou alteração
  de mapeamentos para acomodar pastas desconhecidas.
- Logs operacionais vão para stderr e o relatório/planejamento para stdout.
  Os scripts não criam arquivo de log.

## 6. Relatório e automação

Mensagens, identificadores, ajuda e campos de relatório são em inglês.
Caminhos são escapados com `printf '%q'`, preservando a distinção entre nomes
com espaços, caracteres de controle e quebras de linha.

| Campo | Significado |
|---|---|
| `week`, `categories`, `source_text`, `source_teleparser` | Configuração resolvida; raízes não selecionadas não aparecem. |
| `destination_base` (uploads) / `destination_root` (cópia local) | Destino resolvido: `alias/bucket/prefixo` nos uploads; `<destino>/<semana>` na cópia local. |
| `files_found` | Caminhos físicos resolvidos únicos elegíveis pelas máscaras. Não conta todos os arquivos arbitrários da árvore. |
| `files_selected` | Associações arquivo/chave únicas após a validação/deduplicação local, antes de comparar o destino. |
| `files_existing` | Associações dispensadas por mesmo nome/tamanho no destino. |
| `files_planned` | Operações pendentes organizadas para transferência/simulação. |
| `files_transferred` | Operações confirmadas; sempre zero no dry-run. |
| `failures` | Eventos de erro, não quantidade de arquivos; uma falha de lote/listagem pode afetar vários arquivos. |
| `pending_mappings` | Mapeamentos ausentes, vazios ou bloqueados pela auditoria. |
| `batches_planned` | Lotes planejados, inclusive no dry-run. |
| `batches_processed` (`upload_cdr_to_minio.sh`) / `batches_launched` (paralelo e local) | Lotes realmente submetidos; zero no dry-run. |
| `bytes_planned`, `bytes_transferred` (paralelo e local) | Bytes planejados e confirmados. |
| `jobs` (paralelo e local), `multipart_threads` (paralelo) | Paralelismo efetivamente utilizado. |
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
| Espaço livre insuficiente (cópia local, execução real) | `FAILED` | 1 |
| SIGINT / SIGTERM | `FAILED` | 130 / 143 |

Use o código de saída, não somente o texto `DRY_RUN`, para automação.

## 7. Testes e validação

Há uma suíte Bats por script, em [tests/shell/](../tests/shell):

| Suíte | Script testado |
|---|---|
| [upload_cdr_to_minio.bats](../tests/shell/upload_cdr_to_minio.bats) | `upload_cdr_to_minio.sh` |
| [upload_cdr_to_minio_parallel.bats](../tests/shell/upload_cdr_to_minio_parallel.bats) | `upload_cdr_to_minio_parallel.sh` |
| [copy_cdr_to_local.bats](../tests/shell/copy_cdr_to_local.bats) | `copy_cdr_to_local.sh` |

As suítes de upload utilizam mocks Bash de `mc`
([mock_mc.bash](../tests/shell/helpers/mock_mc.bash) e
[mock_mc_recursive.bash](../tests/shell/helpers/mock_mc_recursive.bash)),
configuração isolada e diretórios temporários administrados pelo Bats. Os mocks
nunca acessam a rede. Seu registro de chamadas é instrumentação do teste, não
escrita do uploader. A suíte da cópia local usa apenas diretórios temporários.

Cobertura da suíte de `upload_cdr_to_minio.sh`:

- quatro combinações de seleção, configuração ignorada e precedência;
- semana obrigatória, raízes semanais, normalização e traversal;
- JSON inválido, campos inválidos, batchsize e dependências;
- todos os 23 mapeamentos do exemplo;
- globs multinível, origem arquivo, não recursividade e ocultos;
- nomes especiais, raízes literais com metacaracteres e links;
- deduplicações e colisões dentro/entre categorias;
- objetos existentes, conflitos, lotes e falhas parciais;
- JSON remoto inválido, falhas de consulta/verificação e continuidade;
- rejeição de NUL em todos os campos de caminhos/mapeamentos e em chaves remotas;
- dry-run sem escrita, contadores, relatório e códigos;
- preservação dos arquivos locais.

Cobertura das suítes dos scripts paralelo (17 testes) e de cópia local (11
testes), conforme os nomes dos testes: seleção de categorias com curingas,
dry-run sem escrita, idempotência, tamanho divergente sem sobrescrita, link
simbólico reportado e não seguido, colisão antes da transferência, diretório
Teleparser não mapeado, argumentos/configuração inválidos; no paralelo, também
listagens remotas únicas, precedência de `--multipart-threads`, execução
paralela e com `--jobs 1`, falha de `mc cp` detectada pela listagem final,
divergência de tamanho após o envio, falha fatal de listagem e agrupamento de
lotes; na cópia local, `local_config.destination` e rejeição de destino
sobreposto à origem.

Com `jq`, Bats e ShellCheck disponíveis **no container**, execute na raiz:

```bash
bash -n scripts/upload_cdr_to_minio.sh scripts/upload_cdr_to_minio_parallel.sh scripts/copy_cdr_to_local.sh
jq -e . scripts/uploader.config.example.json
shellcheck scripts/upload_cdr_to_minio.sh scripts/upload_cdr_to_minio_parallel.sh scripts/copy_cdr_to_local.sh
bats tests/shell/upload_cdr_to_minio.bats
bats tests/shell/upload_cdr_to_minio_parallel.bats
bats tests/shell/copy_cdr_to_local.bats
```

Os testes não exigem o cliente real `mc`, credenciais ou acesso ao MinIO.
Para executar os uploaders fora dos testes, `mc` é obrigatório.

### Estado atual das suítes (execução de 04/11/2026)

Execução das três suítes no container (Bash 5.2.15, jq 1.6, Bats 1.8.2), sem
acesso à rede, após corrigir a suíte de `upload_cdr_to_minio.sh`:

| Suíte | Resultado |
|---|---|
| `copy_cdr_to_local.bats` | 11 aprovados, 0 reprovados |
| `upload_cdr_to_minio_parallel.bats` | 17 aprovados, 0 reprovados |
| `upload_cdr_to_minio.bats` | 53 aprovados, 0 reprovados |

Antes da correção, dois testes (`all_23_original_mappings_are_exercised` e
`example_mapping_values_match_the_original_scripts`) falhavam porque abriam
`scripts/config.example.json`, hoje chamado `scripts/uploader.config.example.json`.
Os dois passaram a usar o nome atual. Além disso,
`example_mapping_values_match_the_original_scripts` comparava os mapeamentos em
ordem, mas o exemplo atual lista os de texto em outra ordem; a comparação agora
ignora a ordem das entradas (`sort`). Esse teste ainda lê os scripts em
`tests/scripts/` como referência. Nesta execução não foram executados `bash -n`
nem ShellCheck, e não houve transferência real nem consulta a MinIO real.

### Resultado efetivamente obtido nesta entrega

> **Nota de atualização:** o registro abaixo é histórico e menciona
> `scripts/config.example.json`. No estado atual do repositório, o exemplo está em
> `scripts/uploader.config.example.json`. Ao revisar esta documentação, a suíte
> Bats ainda referenciava o nome antigo (`tests/shell/upload_cdr_to_minio.bats`,
> linhas 580 e 620). Uma execução de `bats tests/shell/upload_cdr_to_minio.bats`
> nesse estado coletou 53 testes e reprovou dois
> (`all_23_original_mappings_are_exercised` e
> `example_mapping_values_match_the_original_scripts`), por arquivo inexistente.
> A documentação não altera testes; a pendência foi apenas registrada naquela
> revisão e foi corrigida posteriormente (ver "Estado atual das suítes").

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
