# Referência da API Pública

## Visão geral

O pacote `teleutils` oferece utilitários de pré-processamento e um pipeline
Spark para extrair CDRs, gravar um Parquet intermediário e transformá-lo em um
contrato analítico padronizado.

A API pública é exposta pelos subpacotes:

- `teleutils.core.extractors`;
- `teleutils.core.extractors.schemas`;
- `teleutils.core.transformers`;
- `teleutils.core.orchestrators`;
- `teleutils.core.loaders`; e
- `teleutils.preprocessing`.

Esta referência apresenta os componentes principais na sequência conceitual
do fluxo (extração, transformação, orquestração e carga) e, depois deles, os
módulos auxiliares. A ordem de apresentação não é uma ordem obrigatória de
execução: extratores, transformadores e loader podem ser utilizados de forma
independente, e o orquestrador não invoca o loader.

O pacote raiz não reexporta essas interfaces. Importe-as dos subpacotes
indicados.

Não há CLI Python nem ponto de entrada registrado: `pyproject.toml` não define
`[project.scripts]` e `teleutils` não expõe a função `main`.

Os utilitários de linha de comando existentes são os scripts Bash descritos em
[Transferência semanal de CDRs para MinIO](teleutils_upload_cdr_to_minio.md).

O pacote `teleutils.robocalls` está depreciado e não faz parte desta API.

## Requisitos de uso

- Python 3.9 ou superior;
- `pandas` e `pyarrow`, instalados como dependências do pacote;
- Apache Spark por meio de `pyspark` para UDFs, extração e transformação.

As funções Python `extract_number` e `is_valid_cnpj` podem ser chamadas sem
criar uma sessão Spark. Extratores, transformadores e a pandas UDF usada pelo
transformador exigem um ambiente Spark funcional. Importar
`teleutils.core.transformers` não requer um `SparkContext` ativo, mas instanciar
`CDRTransformer` exige uma `SparkSession` válida.

Os exemplos de uso desta referência são ilustrativos: ajuste caminhos, buckets
e nomes ao seu ambiente. Os que usam `spark` pressupõem uma `SparkSession` já
criada. Credenciais do MinIO/S3A nunca aparecem nos exemplos; elas vêm do
ambiente (`MINIO_ENDPOINT`, `MINIO_USER` e `MINIO_SECRET`) ou da configuração do
Spark/Hadoop. Os valores de saída só aparecem em comentários quando são
determinísticos.

## `teleutils.core.extractors`

Esta camada reúne os contratos declarativos de `teleutils.core.extractors.schemas`,
apresentados primeiro, e os extratores que os utilizam.

### `teleutils.core.extractors.schemas`

Contratos declarativos usados pelos extratores.

Exporta `CDRParquetSchema`,
`CDRTextSchema`, `PARQUET_DEFAULT_SCHEMAS`, `TEXT_DEFAULT_SCHEMAS` e
`resolve_cdr_schema`.

Os catálogos padrão podem ser fornecidos aos construtores
dos extratores para substituir ou estender os layouts suportados.

#### `CDRParquetSchema`

Dataclass imutável (`frozen=True`) com os atributos:

- `name: str`: nome do layout;
- `column_mapping: tuple[tuple[str, str], ...]`: pares `(origem, destino)`; a
  ordem define a ordem das colunas extraídas;
- `job_description: str`: descrição do contrato, não utilizada pela extração;
- `fill_char: str = ""`: quando não vazio, o extrator remove ocorrências
  repetidas desse conteúdo no fim do valor de cada coluna mapeada, usando
  `regexp_replace` com o padrão `fill_char + "+$"`, sem escape de metacaracteres.

`column_mapping` não pode estar vazio e cada item deve ser uma tupla de duas
strings. Violações levantam `ValueError` na instanciação. A validação é
estrutural: não verifica se as colunas existem na origem.

##### Exemplo de uso

Contrato customizado que mapeia três colunas de um Parquet de origem. A
instância só passa a valer para um extrator quando é inserida em um catálogo
(veja [`PARQUET_DEFAULT_SCHEMAS` e `TEXT_DEFAULT_SCHEMAS`](#parquet_default_schemas-e-text_default_schemas)).

```python
from teleutils.core.extractors.schemas import CDRParquetSchema

layout_parquet = CDRParquetSchema(
    name="Layout Parquet de exemplo",
    column_mapping=(
        ("chamador", "numero_origem"),
        ("chamado", "numero_destino"),
        ("segundos", "duracao"),
    ),
    job_description="Mapeia três colunas do Parquet de origem.",
    fill_char=" ",  # a origem preenche os valores com espaços à direita
)
```

#### `CDRTextSchema`

Dataclass imutável (`frozen=True`) para arquivos CSV, texto delimitado ou de
largura fixa, com os atributos, na ordem do construtor:

| Atributo | Contrato |
| :-- | :-- |
| `name: str` | Nome do layout. |
| `delimiter: str \| None` | Delimitador da leitura CSV. |
| `schema: StructType \| str \| None` | Schema repassado ao leitor CSV; `None` mantém a leitura sem schema e sem inferência de tipos. |
| `has_header: bool` | Indica se a primeira linha é cabeçalho. |
| `lines_to_keep: tuple[str, str] \| None` | Filtro `(coluna, valor)` aplicado após a seleção e a adição de metadados. Igualdade, ou nulidade quando o valor é `"is null"` ou `"is not null"`; `None` desativa o filtro. |
| `column_indices: tuple[int, ...]` | Índices zero-based das colunas lidas; com `column_sizes`, posições iniciais de caracteres repassadas a `substring` (primeira posição = 1). |
| `column_sizes: tuple[int, ...]` | Comprimentos dos campos de largura fixa; vazio ativa a seleção por coluna. |
| `column_names: tuple[str, ...]` | Nomes de saída, pareados com `column_indices`. |
| `job_description: str` | Descrição do contrato, não utilizada pela extração. |
| `read_rdd: bool = False` | Se verdadeiro, lê as linhas via RDD e produz a coluna `value`, sem aplicar `delimiter`, `has_header` ou `schema`. |
| `fill_char: str = ""` | Remove preenchimento final de campos de largura fixa, sem escape, antes de `trim`. |

O recorte de largura fixa exige uma coluna `value`, produzida por
`read_rdd=True` ou definida por um schema CSV como `"value string"`.

Levantam `ValueError` na instanciação: `schema` de tipo inválido; `lines_to_keep`
que não seja `None` nem tupla de duas strings, ou cuja coluna não esteja em
`column_names`; índices e nomes de tamanhos diferentes; `column_sizes` não vazio
com tamanho diferente de `column_indices`; ausência de índices; índice negativo;
e maior índice incompatível com a quantidade de campos de um `StructType`.
Comprimentos individuais, a sintaxe de schemas em string e a existência da
coluna `value` não são validados.

##### Exemplo de uso

Layout delimitado, sem cabeçalho, que seleciona colunas por índice e mantém
apenas os registros cujo `tipo_registro` é `"1"`:

```python
from teleutils.core.extractors.schemas import CDRTextSchema

layout_delimitado = CDRTextSchema(
    name="Layout CSV de exemplo",
    delimiter=";",
    schema=None,
    has_header=False,
    lines_to_keep=("tipo_registro", "1"),
    column_indices=(0, 2, 3),
    column_sizes=(),
    column_names=("tipo_registro", "numero_origem", "numero_destino"),
    job_description="Seleciona três colunas de um CSV delimitado.",
)
```

Layout de largura fixa lido via RDD. Com `column_sizes`, `column_indices` são as
posições iniciais (a primeira posição é 1) e os comprimentos de cada campo:

```python
from teleutils.core.extractors.schemas import CDRTextSchema

layout_largura_fixa = CDRTextSchema(
    name="Layout de largura fixa de exemplo",
    delimiter=None,
    schema=None,
    has_header=False,
    lines_to_keep=None,
    column_indices=(1, 12),
    column_sizes=(11, 11),
    column_names=("numero_origem", "numero_destino"),
    job_description="Recorta dois campos de 11 caracteres de cada linha.",
    read_rdd=True,  # produz a coluna `value`, exigida pelo recorte
)
```

#### `resolve_cdr_schema(schemas, cdr_schema)`

Retorna o contrato associado à chave exata `cdr_schema` no catálogo `schemas`,
sem normalização e sem modificar o catálogo. Levanta `TypeError` se a chave não
for string e `ValueError` se não existir no catálogo, listando as chaves
disponíveis. É a função usada internamente pelos métodos `extract` e
`extract_dataframe`.

##### Exemplo de uso

```python
from teleutils.core.extractors.schemas import (
    PARQUET_DEFAULT_SCHEMAS,
    resolve_cdr_schema,
)

contrato = resolve_cdr_schema(PARQUET_DEFAULT_SCHEMAS, "smp_ericsson_gsm")
assert contrato is PARQUET_DEFAULT_SCHEMAS["smp_ericsson_gsm"]

try:
    resolve_cdr_schema(PARQUET_DEFAULT_SCHEMAS, "layout_inexistente")
except ValueError as erro:
    print(erro)  # a mensagem lista as chaves disponíveis no catálogo
```

#### `PARQUET_DEFAULT_SCHEMAS` e `TEXT_DEFAULT_SCHEMAS`

Dicionários mutáveis de contratos padrão, indexados pela chave `cdr_schema`
usada nos extratores, no transformador e na configuração do orquestrador. As
chaves ativas são:

| Catálogo | Chaves |
| :-- | :-- |
| `PARQUET_DEFAULT_SCHEMAS` | `smp_ericsson_gsm`, `smp_nokia_gsm`, `smp_huawei_volte_tim`, `smp_ericsson_volte_vivo`, `stfc_italtel_ngn_tim`, `stfc_sigma_claro` |
| `TEXT_DEFAULT_SCHEMAS` | `stfc_huawei_ngn`, `stfc_huawei_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_axe_claro`, `stfc_pcl_claro`, `stfc_tropico_claro_lega`, `stfc_tropico_claro_legb`, `stfc_pit_claro`, `stfc_ss8bf_claro` |

Os layouts `stfc_7n_oi` e `stfc_tropico_oi` usam leitura via RDD e largura
fixa; `stfc_axe_claro` e `stfc_pcl_claro` usam largura fixa pela leitura CSV.
As demais chaves de texto usam seleção por coluna delimitada.

##### Exemplo: catálogo customizado

Os catálogos padrão são dicionários globais e mutáveis: alterá-los afeta todos
os extratores criados depois com o catálogo padrão. Para acrescentar um layout,
copie o catálogo e passe a cópia ao construtor do extrator. O exemplo reutiliza
`layout_parquet` e `layout_delimitado` definidos acima.

```python
from teleutils.core.extractors import CDRParquetExtractor, CDRTextExtractor
from teleutils.core.extractors.schemas import (
    PARQUET_DEFAULT_SCHEMAS,
    TEXT_DEFAULT_SCHEMAS,
)

catalogo_parquet = {**PARQUET_DEFAULT_SCHEMAS, "meu_layout_parquet": layout_parquet}
catalogo_texto = {**TEXT_DEFAULT_SCHEMAS, "meu_layout_texto": layout_delimitado}

parquet_extractor = CDRParquetExtractor(spark, catalogo_parquet)
text_extractor = CDRTextExtractor(spark, catalogo_texto)
# A chave `cdr_schema` passa a aceitar "meu_layout_parquet" e "meu_layout_texto".
```

O catálogo customizado vale apenas para extratores instanciados diretamente. O
orquestrador usa os catálogos do pacote (veja [`run_etl`](#run_etlconfigs--spark-base_foldernone-week_dirnone-materialize_intermediatetrue---etlreport)).

### Comportamento comum dos extratores

Os extratores recebem uma `SparkSession`. `extract` grava com `overwrite` e
retorna o caminho; `extract_dataframe` retorna o DataFrame sem essa escrita.
Ambos retornam `None` e registram um aviso quando uma entrada não é encontrada
na leitura inicial; nesse caso não há escrita. Chamadores devem verificar o
retorno antes de usá-lo. Outras exceções e erros tardios do Spark mantêm o
tratamento normal.
Os argumentos `operator`, `service` e `cdr_type` prevalecem sobre a inferência
da hierarquia `prestadora/servico/tipo_cdr/arquivo`. A coluna `_tipo_cdr`, quando
presente, prevalece inclusive quando nula.

### `CDRParquetExtractor(spark, schemas=PARQUET_DEFAULT_SCHEMAS)`

Extrai CDRs de Parquet.

- `spark: SparkSession`: sessão usada para leitura, projeção e escrita;
- `schemas: dict[str, CDRParquetSchema]`: catálogo que substitui os contratos
  padrão quando fornecido; `cdr_schema` seleciona uma chave desse catálogo.

| Método | Entrada | Comportamento e retorno |
| :-- | :-- | :-- |
| `extract(source_file, target_file, cdr_schema, unique=False, *, operator="", service="", cdr_type="") -> str \| None` | Caminho Parquet ou lista não vazia de caminhos | Delega a `extract_dataframe`, grava com overwrite e retorna `target_file`, ou `None` para entrada ausente. |
| `extract_dataframe(source_file, cdr_schema, unique=False, *, operator="", service="", cdr_type="") -> DataFrame \| None` | Mesma origem e chave do catálogo | Lê com `mergeSchema=true` e `ignoreCorruptFiles=true`, projeta campos e metadados, aplica deduplicação opcional e retorna o DataFrame sem escrita, ou `None` para entrada ausente. |

Quando uma coluna declarada no schema não existe no Parquet, o extrator a
representa como literal nulo tipado como string. Os metadados `prestadora`,
`servico`, `tipo_cdr`, `esquema` e `arquivo_origem` seguem o contrato de
metadados compartilhado. `unique=True` considera também os metadados e ocorre
antes da remoção de `_tipo_cdr`.

#### Exemplo de uso

`extract` grava o Parquet intermediário e retorna o caminho; verifique o retorno
antes de usá-lo. A hierarquia `prestadora/servico/tipo_cdr/arquivo` do caminho
de origem fornece os metadados.

```python
from teleutils.core.extractors import CDRParquetExtractor

extractor = CDRParquetExtractor(spark)

intermediario = extractor.extract(
    source_file="/dados/cdr/tim/stfc/ngn_italtel/entrada.parquet",
    target_file="/dados/extraido/tim_stfc_ngn_italtel",
    cdr_schema="stfc_italtel_ngn_tim",
)
if intermediario is None:
    # Entrada não encontrada: um aviso foi registrado e nada foi gravado.
    raise FileNotFoundError("Nenhum Parquet de origem foi encontrado.")
```

`extract_dataframe` aceita uma lista de origens e devolve o DataFrame sem
escrita. Os argumentos `operator`, `service` e `cdr_type` prevalecem sobre a
inferência pelo caminho, e `unique=True` remove registros duplicados.

```python
df = extractor.extract_dataframe(
    source_file=[
        "/dados/cdr/tim/stfc/ngn_italtel/parte_1.parquet",
        "/dados/cdr/tim/stfc/ngn_italtel/parte_2.parquet",
    ],
    cdr_schema="stfc_italtel_ngn_tim",
    unique=True,
    operator="tim",
    service="stfc",
    cdr_type="ngn_italtel",
)
if df is not None:  # None: entrada não encontrada
    df.printSchema()
```

### `CDRTextExtractor(spark, schemas=TEXT_DEFAULT_SCHEMAS)`

Extrai CDRs de arquivos CSV/texto.

- `spark: SparkSession`: sessão usada para leitura, seleção e escrita;
- `schemas: dict[str, CDRTextSchema]`: catálogo de contratos, selecionados
  pela chave `cdr_schema`.

| Método | Entrada | Comportamento e retorno |
| :-- | :-- | :-- |
| `extract(source_file, target_file, cdr_schema, *, operator="", service="", cdr_type="") -> str \| None` | Caminho CSV/texto ou lista não vazia de caminhos | Delega a `extract_dataframe`, grava com overwrite e retorna `target_file`, ou `None` para entrada ausente. |
| `extract_dataframe(source_file, cdr_schema, *, operator="", service="", cdr_type="") -> DataFrame \| None` | Mesma origem e chave do catálogo | Lê CSV ou RDD, seleciona campos ou recorta largura fixa, adiciona metadados e aplica `lines_to_keep`, sem escrita intermediária, ou retorna `None` para entrada ausente. |

`arquivo_origem` é decodificado preservando sinais de mais literais; uma
coluna `arquivo_origem` já mapeada pelo contrato é mantida. O filtro
`lines_to_keep` mantém os registros que atendem à condição do contrato.

#### Exemplo de uso

```python
from teleutils.core.extractors import CDRTextExtractor

extractor = CDRTextExtractor(spark)

intermediario = extractor.extract(
    source_file=[
        "/dados/cdr/tim/stfc/huawei_ngn/entrada_1.csv",
        "/dados/cdr/tim/stfc/huawei_ngn/entrada_2.csv",
    ],
    target_file="/dados/extraido/tim_stfc_huawei_ngn",
    cdr_schema="stfc_huawei_ngn_tim",
)
if intermediario is None:
    # Entrada não encontrada: um aviso foi registrado e nada foi gravado.
    raise FileNotFoundError("Nenhum arquivo de origem foi encontrado.")

# Sem escrita intermediária, informando os metadados explicitamente.
df = extractor.extract_dataframe(
    source_file="/dados/cdr/tim/stfc/huawei_ngn/entrada_1.csv",
    cdr_schema="stfc_huawei_ngn_tim",
    operator="tim",
    service="stfc",
    cdr_type="huawei_ngn",
)
```

Em ambos os extratores, `extract_dataframe` mantém as validações existentes,
inclusive a ação para verificar metadados inferidos. Seu retorno comprova
construção do plano e validações antecipadas, não execução de todos os dados.
A origem permanece necessária até a ação final. Não há cache automático.
O método Parquet `extract` preserva a supressão temporária de loggers Spark
durante sua execução; `extract_dataframe` não suprime avisos da leitura futura.

## `teleutils.core.transformers`

### `CDRTransformer(spark)`

Transforma um Parquet intermediário ou DataFrame extraído no contrato final.
Cada método específico aceita `source_file: str | DataFrame`, aplica regras do
layout, escreve `target_file` em Parquet com
`overwrite` e particionamento por `no_tipo_chamada`, e retorna `target_file`.
O construtor recebe uma `SparkSession`, cria uma nova sessão com o mesmo
`SparkContext` por meio de `newSession()` e configura
`spark.sql.timestampType = TIMESTAMP_NTZ` nessa nova sessão.

Para entrada DataFrame, é exigida a identidade com `transformer.spark`.
Compartilhar o SparkContext ou ter NTZ configurado em outra sessão não basta.
Use `CDRTextExtractor(transformer.spark)` ou
`CDRParquetExtractor(transformer.spark)` e depois `extract_dataframe`.
Não há migração via RDD, coleta, view ou arquivo temporário.

O tipo configurado é validado na entrada e antes da escrita. Configuração
incompatível gera `ValueError`, sem correção silenciosa. A sessão do chamador
não é modificada e seus overrides SQL de runtime não são copiados para a filha.
Não altere a sessão compartilhada durante o pipeline; a biblioteca não a encerra.
Essa garantia cobre o plano construído nessa sessão, não corrige datas que
já tenham sido interpretadas incorretamente antes da extração.

| Método | Layout e comportamento específico |
| :-- | :-- |
| `transform_smp_ericsson_gsm` | Ericsson GSM; também recebe `output_format="default"`. |
| `transform_smp_nokia_gsm` | Nokia GSM. |
| `transform_smp_huawei_volte_tim` | Huawei VoLTE TIM. |
| `transform_smp_ericsson_volte_vivo` | Ericsson VoLTE Vivo. |
| `transform_stfc_huawei_ngn`, `transform_stfc_huawei_ngn_tim` | Huawei NGN e variante TIM. |
| `transform_stfc_italtel_ngn_tim` | Italtel NGN TIM. |
| `transform_stfc_fcdr_vivo` | FCDR Vivo. |
| `transform_stfc_tropico_oi`, `transform_stfc_7n_oi` | Trópico e 7N Oi. |
| `transform_stfc_axe_claro`, `transform_stfc_pcl_claro` | AXE e PCL Claro. |
| `transform_stfc_tropico_claro`, `transform_stfc_tropico_claro_lega`, `transform_stfc_tropico_claro_legb` | Trópico Claro; os dois últimos nomes são aliases do primeiro. |
| `transform_stfc_pit_claro`, `transform_stfc_ss8bf_claro` | PIT e SS8BF Claro. |
| `transform_stfc_sigma_claro` | Sigma Claro. |

Esses métodos mantêm `target_file: str` e retorno `str`. O despachante genérico
`transform` ainda procura funções globais; utilize os métodos de layout ou o
orquestrador, que resolve os métodos da instância.

Todos os métodos aplicam o pipeline comum interno antes da escrita: completam
colunas ausentes do contrato, normalizam duração/datas, normalizam números,
classificam autenticação quando disponível e preenchem nulos da chave primária.

O contrato final possui 38 colunas, definido em `TARGET_SCHEMA`. A referência
de origem e transformação por coluna está em
[Linhagem e Transformações dos Dados](teleutils_linhagem_transformacoes_dados.md).

#### Exemplo de uso

A partir de um Parquet intermediário gravado por um extrator. O método de layout
grava o Parquet final particionado por `no_tipo_chamada` e retorna `target_file`.

```python
from teleutils.core.transformers import CDRTransformer

transformer = CDRTransformer(spark)

resultado = transformer.transform_stfc_italtel_ngn_tim(
    source_file="/dados/extraido/tim_stfc_ngn_italtel",  # retorno de `extract`
    target_file="/dados/transformado/tim_stfc_ngn_italtel",
)
assert resultado == "/dados/transformado/tim_stfc_ngn_italtel"
```

A partir de um DataFrame extraído, sem gravar o intermediário. O extrator deve
usar `transformer.spark`: DataFrames de outra sessão são rejeitados.

```python
from teleutils.core.extractors import CDRParquetExtractor
from teleutils.core.transformers import CDRTransformer

transformer = CDRTransformer(spark)
extractor = CDRParquetExtractor(transformer.spark)

df = extractor.extract_dataframe(
    source_file="/dados/cdr/tim/stfc/ngn_italtel/entrada.parquet",
    cdr_schema="stfc_italtel_ngn_tim",
)
if df is not None:  # None: entrada não encontrada
    transformer.transform_stfc_italtel_ngn_tim(
        source_file=df,
        target_file="/dados/transformado/tim_stfc_ngn_italtel",
    )
```

## `teleutils.core.orchestrators`

Coordena extração e transformação sem implementar regras de CDR. A importação
da API não carrega Spark nem os transformadores. A validação reutiliza a função
de metadados dos extratores (e suas dependências Python), sem iniciar uma sessão
ou executar ações Spark. Os componentes de execução são instanciados somente
quando há itens preparados para executar.

### `load_etl_configs(path, *, endpoint=None, access_key=None, secret_key=None, secure=False)`

Lê um arquivo JSON em caminho absoluto Linux ou URI `s3a://bucket/chave` e
retorna uma lista de dicionários. Em S3A, usa o SDK MinIO; opções omitidas são
obtidas de `MINIO_ENDPOINT`, `MINIO_USER` e `MINIO_SECRET` no ambiente.
O endpoint é o hostname com porta opcional, sem protocolo. `secure=True` usa
HTTPS; o padrão `False` usa HTTP sem criptografia. Para arquivos locais, essas
opções não são usadas.

O arquivo deve conter uma lista de objetos; prefixos `output_file_prefix` não
vazios devem ser únicos, com comparação exata e sensível a maiúsculas.
Duplicatas levantam `ValueError` com os índices; erros de leitura, JSON e SDK
são propagados. Os demais campos são validados por `run_etl`. Essa verificação
abrange apenas o arquivo lido, não outros arquivos ou execuções.

#### Exemplo de uso

O arquivo é uma lista de objetos com os campos descritos em
[`build_config_paths`](#build_config_pathsconfig--materialize_intermediatetrue---configpaths).
Itens sem `storage_type` usam S3A.

```json
[
  {
    "source_folder": "sandbox-sfi/cdr/bruto/tim/stfc/huawei_ngn",
    "pattern": "*.gz",
    "schema": "stfc_huawei_ngn_tim",
    "extracted_folder": "sandbox-sfi/cdr/extraido",
    "transformed_folder": "sandbox-sfi/cdr/transformado",
    "output_file_prefix": "tim_stfc_huawei_ngn"
  },
  {
    "source_folder": "sandbox-sfi/cdr/bruto/tim/stfc/ngn_italtel",
    "pattern": "*.parquet",
    "schema": "stfc_italtel_ngn_tim",
    "extracted_folder": "sandbox-sfi/cdr/extraido",
    "transformed_folder": "sandbox-sfi/cdr/transformado",
    "output_file_prefix": "tim_stfc_ngn_italtel"
  }
]
```

```python
from teleutils.core.orchestrators import load_etl_configs

# Arquivo local: caminho absoluto Linux.
configs = load_etl_configs("/dados/config/etl.json")

# Arquivo no MinIO: o endpoint e as credenciais vêm de MINIO_ENDPOINT,
# MINIO_USER e MINIO_SECRET; `secure=True` usa HTTPS.
configs = load_etl_configs("s3a://sandbox-sfi/config/etl.json", secure=True)
```

### `list_files(base_folder, pattern="", recursive=False, *, endpoint=None, access_key=None, secret_key=None, secure=False)`

Lista arquivos lógicos no MinIO sem iniciar Spark ou ler o conteúdo dos objetos.
Aceita uma pasta contêiner em `bucket[/prefixo]` ou
`s3a://bucket[/prefixo]`, inclusive a raiz do bucket e barras finais.
Retorna `list[str]` ordenada, sem duplicatas, sempre com `s3a://` e sem barra
final. Reutiliza a validação de caminhos S3A da configuração.

Um diretório com sufixo literal `.parquet` representa um único dataset:
suas partições, arquivos `part-*` e auxiliares internos nunca são listados.
A função não verifica o formato nem a integridade dos dados. Diretórios sem
esse sufixo são pastas comuns; arquivos físicos `.parquet` fora dos datasets
são arquivos comuns. A base não pode ser um dataset nem um caminho dentro dele.

- `recursive=False`: somente arquivos e datasets imediatamente abaixo da base.
- `recursive=True`: percorre subpastas comuns, mas nunca entra em datasets,
  mesmo quando eles não correspondem à máscara.
- `pattern=""`: inclui todos os tipos e arquivos auxiliares fora dos datasets.
- A máscara usa `fnmatchcase` sobre o **nome**, não o caminho: aceita `*`, `?`
  e classes `[]`, com comparação sensível a maiúsculas. Não aceita caminhos
  ou protocolos e não expande alternativas `{a,b}`. Não confundir com o
  `pattern` da configuração ETL, que compõe um caminho glob para Spark/Hadoop.
- Marcadores de diretórios comuns não são retornados como arquivos.

As opções de conexão seguem `load_etl_configs`: argumentos explícitos prevalecem
sobre `MINIO_ENDPOINT`, `MINIO_USER` e `MINIO_SECRET`; endpoint sem protocolo.
O padrão `secure=False` usa HTTP sem criptografia; use `secure=True` para HTTPS.
Tipos inválidos geram `TypeError`; caminhos, máscaras ou configurações de conexão
inválidos geram `ValueError`. Prefixos sem objetos ou sem correspondências
retornam `[]`. Erros do SDK, inclusive durante a paginação, são propagados,
sem retornar listas vazias ou resultados parciais para esconder falhas.

#### Exemplo de uso

Listagem não recursiva com máscara. Endpoint e credenciais vêm de
`MINIO_ENDPOINT`, `MINIO_USER` e `MINIO_SECRET`; datasets `.parquet` são
retornados como uma única entrada, sem suas partições.

```python
from teleutils.core.orchestrators import list_files

files = list_files(
    "sandbox-sfi/cdr/processed/transformed",
    pattern="*.parquet",
    secure=True,
)
# Se esse for o único dataset correspondente:
# ["s3a://sandbox-sfi/cdr/processed/transformed/"
#  "semana110_algar_smp_nokia_transformed.parquet"]
```

Listagem recursiva de arquivos comuns. O resultado é uma lista ordenada de
URIs `s3a://`, sem duplicatas.

```python
from teleutils.core.orchestrators import list_files

arquivos = list_files(
    "s3a://sandbox-sfi/cdr/bruto/tim",
    pattern="*.gz",
    recursive=True,
)
for arquivo in arquivos:
    print(arquivo)
```

### `build_config_paths(config, *, materialize_intermediate=True) -> ConfigPaths`

Valida um mapping sem modificá-lo e constrói caminhos S3A ou locais.

| Campo | Contrato |
|---|---|
| `storage_type` | Opcional: `"s3a"` (padrão quando ausente) ou `"local"`, sem normalização. |
| `source_folder` | String obrigatória com a pasta de origem, conforme storage_type. |
| `pattern` | String obrigatória com padrão relativo, incluindo glob. Barras iniciais são removidas. |
| `schema` | String obrigatória; chave preservada sem normalização. |
| `extracted_folder` | Pasta intermediária; obrigatória no padrão, opcional no modo direto. Quando informada, é validada mesmo sem ser usada. |
| `transformed_folder` | String obrigatória com a pasta final, conforme storage_type. |
| `output_file_prefix` | String obrigatória; prefixo sem separadores ou curingas. |
| `operator`, `service`, `cdr_type` | Opcionais: strings ou `None`; vazio/`None` preservam a inferência dos extratores. |

Não há aliases de configuração: qualquer chave desconhecida é rejeitada.
`materialize_intermediate` é argumento da função, não chave do mapping.
Exige `bool`, sem coerção de strings ou inteiros.
Os metadados não são normalizados; strings compostas somente por espaços são
inválidas. `_tipo_cdr` presente no conteúdo mantém a precedência implementada
pelos extratores.

Em `"s3a"`, pastas aceitam `bucket/caminho` ou `s3a://bucket/caminho`.
O protocolo é acrescentado somente quando ausente; barras finais são removidas,
mas barras internas e espaços internos das chaves são preservados. Caminhos
iniciados por `/` agora são rejeitados nesse modo, em vez de serem interpretados
como S3A. Configurações antigas com bucket/caminho continuam válidas sem
`storage_type`.

Em `"local"`, as três pastas devem ser caminhos absolutos Linux, como `/dados/cdr`.
Não são aceitos caminhos relativos, expansão de `~`, caminhos Windows ou
protocolos na entrada, inclusive `file:///`. A construção normaliza barras
redundantes e finais lexicalmente, preservando a raiz, e gera caminhos
`file:///dados/cdr/...` compatíveis com Spark/Hadoop. Isso evita depender de
`fs.defaultFS`. Caracteres locais, como espaços, Unicode, `+`, `@`, `#` e `%`,
são encaminhados literalmente ao construtor de caminhos do Hadoop, sem
percent-encoding adicional pela biblioteca. Os curingas do padrão são preservados.

Não há conversão silenciosa de `storage_type`: `None`, booleanos e outros tipos
não textuais geram `TypeError`; valores como `"LOCAL"`, `" local "` ou `"s3"`
geram `ValueError`. Um item usa o tipo selecionado nos três diretórios.
Não há armazenamento separado para origem e destino ou cópia entre tipos.

Em ambos os modos, protocolos incompatíveis, bucket vazio no modo S3A, espaços nas bordas das pastas,
caracteres de controle, barras invertidas e segmentos `.`/`..` são rejeitados.
A checagem de segmentos considera também percent-encoding. Curingas são
permitidos no padrão, não nas pastas nem no prefixo.

`ConfigPaths` é uma `NamedTuple` com:

- `schema`;
- `source_file`: pasta de origem e padrão, com `s3a://` ou `file:///`;
- `extracted_file`: pasta intermediária e `<prefix>_extracted.parquet`, ou
  `None` quando `materialize_intermediate=False`;
- `transformed_file`: pasta final e `<prefix>_transformed.parquet`.

Os destinos representam diretórios/datasets Parquet. A validação não consulta
S3 nem filesystem local, verifica existência ou certifica integridade.
Suporte ao schema e conflitos
de caminhos são verificados por `run_etl`, não por `build_config_paths`.

Tipos inválidos levantam `TypeError`; campos ausentes, desconhecidos, vazios
ou com valores inválidos levantam `ValueError`.

#### Exemplo de uso

Planejamento de caminhos no armazenamento padrão (`s3a`) e no modo `local`. A
função apenas calcula os caminhos: não consulta S3 nem o filesystem.

```python
from teleutils.core.orchestrators import build_config_paths

config_s3a = {
    "source_folder": "sandbox-sfi/cdr/bruto/tim/stfc/huawei_ngn",
    "pattern": "*.csv",
    "schema": "stfc_huawei_ngn_tim",
    "extracted_folder": "sandbox-sfi/cdr/extraido",
    "transformed_folder": "sandbox-sfi/cdr/transformado",
    "output_file_prefix": "tim_huawei_ngn",
}
paths = build_config_paths(config_s3a)
print(paths.source_file)
# -> s3a://sandbox-sfi/cdr/bruto/tim/stfc/huawei_ngn/*.csv
print(paths.extracted_file)
# -> s3a://sandbox-sfi/cdr/extraido/tim_huawei_ngn_extracted.parquet
print(paths.transformed_file)
# -> s3a://sandbox-sfi/cdr/transformado/tim_huawei_ngn_transformed.parquet

config_local = {
    **config_s3a,
    "storage_type": "local",
    "source_folder": "/dados/cdr/bruto/tim/stfc/huawei_ngn",
    "extracted_folder": "/dados/cdr/extraido",
    "transformed_folder": "/dados/cdr/transformado",
}
paths = build_config_paths(config_local)
print(paths.source_file)
# -> file:///dados/cdr/bruto/tim/stfc/huawei_ngn/*.csv
```

Com `materialize_intermediate=False`, não há intermediário e `extracted_file` é
`None`:

```python
paths = build_config_paths(config_local, materialize_intermediate=False)
assert paths.extracted_file is None
print(paths.transformed_file)
# -> file:///dados/cdr/transformado/tim_huawei_ngn_transformed.parquet
```

### `run_etl(configs, *, spark, base_folder=None, week_dir=None, materialize_intermediate=True) -> ETLReport`

Recebe uma sequência finita de mappings e uma `SparkSession` existente.
Strings, mappings no lugar da sequência e iteradores são rejeitados com
`TypeError`, registrado em log. Uma sequência vazia retorna relatório vazio,
sem instanciar componentes.

`run_etl` não verifica a unicidade global de prefixos em listas fornecidas
diretamente. Para arquivos JSON, `load_etl_configs` faz essa verificação durante
a leitura. Conflitos entre caminhos de destino continuam sendo rejeitados.

O padrão mantém escrita e leitura do intermediário. Com
`materialize_intermediate=False`, o orquestrador passa o DataFrame retornado
por `extract_dataframe` ao transformador e administra a sessão compatível.
O parâmetro vale para todo o lote e exige `bool`.

No modo direto, os extratores são criados na sessão isolada NTZ do transformador.
No padrão, continuam usando a sessão recebida do chamador. Portanto, overrides
SQL de runtime presentes apenas no chamador não governam a extração direta.
Essa diferença é intencional; não se garante equivalência irrestrita para
schemas customizados que dependam desses overrides.

`base_folder` e `week_dir` são opcionais, mas devem ser informados juntos.
Quando ambos são fornecidos, cada `source_folder` configurado é composto como
`{base_folder}/{week_dir}/{source_folder}`, e `output_file_prefix` passa a ser
`{week_dir em minúsculas}_{output_file_prefix}`, o que altera os nomes dos
Parquets de saída. A configuração original não é alterada. Informar apenas um
deles levanta `ValueError`.

O lote:

1. Valida cada configuração e constrói todos os caminhos.
2. Rejeita itens envolvidos em destinos iguais/sobrepostos ou sobreposição
   entre destino e pasta de origem, inclusive entre itens.
3. Seleciona formatos pelos catálogos existentes; schemas ausentes ou
   ambíguos são rejeitados.
4. Instancia os extratores necessários e o transformador uma vez por lote.
5. Resolve um extrator e um método chamável `transform_<schema>` por item.
6. Executa extração e transformação sequencialmente para cada item válido.
7. Retorna os resultados na ordem original, continuando após falhas individuais.

A comparação distingue protocolo e autoridade/bucket antes dos segmentos:
armazenamentos ou buckets diferentes não conflitam por semelhança de nomes.
Em S3A, mantém-se a comparação conservadora de percent-encoding e barras
redundantes. Em local, os nomes são literais (`a%20b` e `a b` são distintos)
e as barras são normalizadas na preparação. A raiz local sobrepõe qualquer
descendente local. Não resolve globs, links simbólicos ou montagens.
Todos os participantes de um conflito são rejeitados; itens independentes
continuam. Não existe encadeamento de saída de um item como entrada de outro.
Destinos preexistentes fora do lote não são consultados: permanece `overwrite`.
No modo direto, somente o destino final participa da detecção de conflitos e
da escrita. Intermediários antigos não são lidos, sobrescritos nem removidos.
Cada item pode escolher seu armazenamento, mas nenhum item pode ser híbrido.
Não há alteração na seleção de extratores e transformadores por armazenamento.

A seleção usa os catálogos do pacote; não há registro público adicional nem
parâmetro de catálogo customizado nesta API. Os extratores recebem cópias
rasas dos catálogos. Não há tratamento específico de LEGA/LEGB: os aliases
`transform_stfc_tropico_claro_lega` e `transform_stfc_tropico_claro_legb` passam
pela mesma resolução dinâmica dos demais métodos.

O contexto Spark fornecido não é criado nem encerrado. A sessão SQL derivada
criada pelo transformador mantém seu comportamento original. O orquestrador
não configura credenciais, conectores, Iceberg, handlers de logging ou níveis
globais; utiliza um logger de módulo. Os decoradores dos componentes continuam
registrando suas operações e, quando aplicável, ajustando temporariamente
loggers Spark conforme o comportamento preexistente.

#### Exemplo de uso

Lote com um item de texto e um item Parquet, no armazenamento `local` (sem
MinIO). Omitir `storage_type` usa S3A, com as pastas no formato
`bucket/caminho`. Os itens nunca dependem um do outro, e a falha de um não
interrompe os demais.

```python
from teleutils.core.orchestrators import run_etl

configs = [
    {
        "storage_type": "local",
        "source_folder": "/dados/cdr/bruto/tim/stfc/huawei_ngn",
        "pattern": "*.csv",
        "schema": "stfc_huawei_ngn_tim",
        "extracted_folder": "/dados/cdr/extraido",
        "transformed_folder": "/dados/cdr/transformado",
        "output_file_prefix": "tim_stfc_huawei_ngn",
    },
    {
        "storage_type": "local",
        "source_folder": "/dados/cdr/bruto/tim/stfc/ngn_italtel",
        "pattern": "*.parquet",
        "schema": "stfc_italtel_ngn_tim",
        "extracted_folder": "/dados/cdr/extraido",
        "transformed_folder": "/dados/cdr/transformado",
        "output_file_prefix": "tim_stfc_ngn_italtel",
    },
]

report = run_etl(configs, spark=spark)
print(report.total, report.successes)
```

`base_folder` e `week_dir`, sempre informados juntos, prefixam cada
`source_folder` por `{base_folder}/{week_dir}/{source_folder}` e o
`output_file_prefix` por `{week_dir em minúsculas}_`. O modo direto
(`materialize_intermediate=False`) dispensa `extracted_folder` e o Parquet
intermediário:

```python
configs_semana = [
    {
        "storage_type": "local",
        "source_folder": "tim/stfc/huawei_ngn",
        "pattern": "*.csv",
        "schema": "stfc_huawei_ngn_tim",
        "transformed_folder": "/dados/cdr/transformado",
        "output_file_prefix": "tim_stfc_huawei_ngn",
    },
]

# Lê de /dados/cdr/bruto/Semana110/tim/stfc/huawei_ngn/*.csv
report = run_etl(
    configs_semana,
    spark=spark,
    base_folder="/dados/cdr/bruto",
    week_dir="Semana110",
    materialize_intermediate=False,
)
# Grava /dados/cdr/transformado/semana110_tim_stfc_huawei_ngn_transformed.parquet
```

O retorno é um `ETLReport`; consulte
[Resultados imutáveis](#resultados-imutáveis) para tratar cada item. A carga em
Iceberg não faz parte de `run_etl` (veja
[`TrinoLoader`](#trinoloaderspark)).

### `run_etl_item(config, *, spark, base_folder=None, week_dir=None, materialize_intermediate=True) -> ETLItemResult`

Executa um lote de um item e retorna seu resultado, com `index=0`. Possui as
mesmas validações, resultados e política de erros de `run_etl`.
Os argumentos opcionais `base_folder` e `week_dir` são repassados a `run_etl`
e devem ser informados juntos; eles prefixam a origem e o `output_file_prefix`
sem alterar `config`.
`materialize_intermediate` é repassado com o mesmo contrato de `run_etl`.

#### Exemplo de uso

```python
from teleutils.core.orchestrators import run_etl_item

resultado = run_etl_item(
    {
        "storage_type": "local",
        "source_folder": "/dados/cdr/bruto/tim/stfc/huawei_ngn",
        "pattern": "*.csv",
        "schema": "stfc_huawei_ngn_tim",
        "extracted_folder": "/dados/cdr/extraido",
        "transformed_folder": "/dados/cdr/transformado",
        "output_file_prefix": "tim_stfc_huawei_ngn",
    },
    spark=spark,
)
print(resultado.index, resultado.status, resultado.stage)
# Em caso de sucesso: 0 success completed
```

### Resultados imutáveis

`ETLItemResult` e `ETLReport` são dataclasses congeladas.

| Campo de `ETLItemResult` | Conteúdo |
|---|---|
| `index` | Posição do item na sequência, iniciando em zero. |
| `schema` | Chave informada, quando identificável; caso contrário `None`. |
| `paths` | `ConfigPaths` planejado ou `None` se a configuração não foi resolvida. |
| `status` | `success`, `validation_error` ou `execution_error`. |
| `stage` | `configuration`, `paths`, `resolution`, `initialization`, `extraction`, `transformation`, `pipeline` ou `completed`. |
| `completed_stages` | Tupla das etapas concluídas: vazia, `("extraction",)` ou `("extraction", "transformation")`. |
| `error_type` | Nome da classe da exceção, `InputNotFound` para entrada ausente detectada pelo extrator, ou `None` no sucesso. |
| `error_message` | Mensagem da falha, ou `None` no sucesso. |

`ETLReport.items` é uma tupla ordenada, com um resultado por item.
As propriedades `total`, `successes`, `validation_errors` e `execution_errors`
são calculadas a partir dessa tupla:

```text
total = successes + validation_errors + execution_errors
```

Erros de configuração, caminhos e resolução têm status `validation_error`.
Falhas de inicialização ou das etapas têm status `execution_error`, mesmo
quando a exceção original é `ValueError`. Falha de inicialização compartilhada
é reportada para todos os itens ainda válidos.

Uma extração malsucedida impede a transformação daquele item. No modo
de entrada não encontrada na leitura inicial, o item recebe `execution_error`
na etapa `extraction`, com `error_type="InputNotFound"` e etapas concluídas
vazias. O extrator registra um aviso sem traceback e o lote prossegue. No modo
materializado, falha na transformação preserva o registro de extração concluída,
mas não é sucesso. No modo direto, `completed_stages` permanece vazio até a
escrita final concluir; falhas após obter o DataFrame usam `stage="pipeline"`,
pois podem ter origem na extração lazy. No sucesso, ambos os modos registram
`("extraction", "transformation")`. A impressão informa explicitamente quando
o intermediário não foi materializado.
`KeyboardInterrupt` e `SystemExit` propagam; não há relatório final garantido
após interrupção do usuário.

#### Exemplo: inspeção do relatório

O término do lote não significa que todos os itens tiveram sucesso. Percorra
`report.items` e use `status`, `stage` e `error_message` para tratar as falhas.

```python
for item in report.items:
    if item.status == "success":
        print(item.index, item.schema, item.paths.transformed_file)
    else:
        # validation_error: configuração; execution_error: execução.
        print(item.index, item.status, item.stage, item.error_message)

if report.successes != report.total:
    raise RuntimeError(
        f"{report.validation_errors} erro(s) de validação e "
        f"{report.execution_errors} erro(s) de execução no lote."
    )
```

### `print_etl_report(report: ETLReport | ETLItemResult) -> None`

Imprime na saída padrão as contagens do lote e, na ordem original, o índice,
schema, status, etapa e etapas concluídas de cada item. Quando disponíveis,
também imprime os caminhos planejados e os detalhes de erro. Um relatório vazio
é identificado explicitamente. Um `ETLItemResult`, como o retornado por
`run_etl_item`, é apresentado como relatório de um item, com as mesmas
contagens e detalhes. Outros tipos levantam `TypeError`. A função não altera
os resultados nem certifica os dados processados. Caminhos e mensagens de erro
podem conter dados sensíveis.

#### Exemplo de uso

```python
from teleutils.core.orchestrators import print_etl_report

print_etl_report(report)     # `ETLReport` devolvido por `run_etl`
print_etl_report(resultado)  # `ETLItemResult` devolvido por `run_etl_item`
```

### Limites e validação local

O escopo é apenas extração e transformação. Não há carga, retry, paralelismo,
CLI, retomada, rollback ou exclusão automática de saídas parciais.
No modo direto, uma nova tentativa pode repetir a extração; não há checkpoint.
Os logs de `extract_dataframe` medem construção/validação, não todo o trabalho
adiado. O tempo relevante para comparar os modos termina na escrita final.
Para benchmark A/B, use os mesmos dados, recursos e armazenamento, repita as
medições controlando aquecimento/cache e observe bytes de I/O, tarefas,
shuffle/spill e tempo Python. Os testes sintéticos verificam equivalência e
ausência de I/O intermediário; não certificam ganho percentual de produção.
Não se usa existência de arquivo como prova de conclusão. Os componentes
mantêm suas políticas de tolerância a colunas ausentes/arquivos corrompidos;
portanto, sucesso técnico não certifica completude ou qualidade dos CDRs.

Os testes unitários usam fakes, inclusive para lotes mistos S3A/local. Os testes
de integração utilizam a API pública com `storage_type="local"`, Spark local,
diretórios temporários e dados sintéticos, sem substituição da construção de
caminhos. Exercitam globs e caracteres especiais, comparando conteúdo, schema
e particionamento com chamadas diretas. MinIO não é necessário para os testes.

O ambiente Spark/Hadoop é responsável pelos conectores, endpoint e credenciais
do MinIO/S3A. O modo local força o filesystem `file`, mas não distribui arquivos:
em um cluster, os executores precisam ter acesso aos caminhos locais necessários
em suas próprias máquinas. A existência apenas no driver não é suficiente.
Não se verifica existência, permissões, links simbólicos ou equivalência de
montagens; a detecção de conflitos é lexical.

Para uso no Jupyter, consulte o [exemplo do README](../README.md#processar-lotes-de-cdrs-orquestrador).

## `teleutils.core.loaders`

### `TrinoLoader(spark)`

Recebe uma sessão Spark configurada com extensões Iceberg e catálogo acessível
também pelo Trino. A escrita e as consultas são feitas pelo **Spark**, não por
conexão direta ao Trino. O loader não configura nem encerra a sessão.

```python
def upsert_iceberg(
    self,
    source_file: str,
    target_table: str,
) -> None:
    ...
```

- `source_file`: arquivo ou diretório Parquet com todas as colunas finais de
  `TARGET_SCHEMA` e todas as linhas de cada origem presente.
- `target_table`: nome com um a três componentes separados por pontos, sem
  delimitadores SQL. Componentes vazios são rejeitados; crases são escapadas.
- Retorno: `None`. Não retorna DataFrame ou contagens.

#### Exemplo de uso

Pré-requisitos: JARs do Iceberg no classpath do Spark e um catálogo Iceberg
(`catalogo`, neste exemplo) acessível também pelo Trino. O tipo, a URI e o
warehouse do catálogo dependem do ambiente e não são definidos pelo loader.

```python
from pyspark.sql import SparkSession

from teleutils.core.loaders import TrinoLoader

spark = (
    SparkSession.builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.catalogo", "org.apache.iceberg.spark.SparkCatalog")
    # Demais propriedades de `spark.sql.catalog.catalogo.*` e `spark.jars`,
    # conforme o catálogo do ambiente.
    .getOrCreate()
)

loader = TrinoLoader(spark)
loader.upsert_iceberg(
    source_file="/dados/cdr/transformado/tim_stfc_ngn_italtel_transformed.parquet",
    target_table="catalogo.cdr.chamadas",
)
```

Para recarregar uma origem já presente, chame `upsert_iceberg` novamente com o
Parquet completo reprocessado: as linhas cujo `no_arquivo_origem` consta na
entrada são excluídas e a entrada inteira é inserida.

```python
loader.upsert_iceberg(
    source_file="/dados/cdr/transformado/tim_stfc_ngn_italtel_transformed.parquet",
    target_table="catalogo.cdr.chamadas",
)
```

#### Substituição por origem

1. Valida as colunas finais e exige `no_arquivo_origem` do tipo string,
   sem valores nulos, vazios ou compostos apenas por espaços. Se o Parquet
   tiver schema válido e nenhuma linha, não acessa o destino.
2. Cria o destino ausente como tabela Iceberg com o schema da entrada. Para
   um destino existente, valida a coluna de origem e consulta se algum nome
   da entrada já está presente. Os nomes distintos são reutilizados no Spark;
   não são coletados no driver.
3. Havendo correspondência, verifica a compatibilidade das colunas antes de
   excluir somente as linhas atribuídas às origens da entrada. Insere todas
   as linhas do Parquet, incluindo duplicadas, sem aplicar casts ou filtros
   no loader. Sem correspondências, insere sem executar `DELETE`.

A comparação usa o valor de `no_arquivo_origem`, sem normalização, não o
caminho físico do Parquet. Cada nome precisa identificar uma única origem no
destino. Não envie um fragmento isolado se a mesma origem estiver distribuída
em outros arquivos ou partições. Não há tabela externa de controle.

#### Consistência e desempenho

- **`DELETE` e `INSERT` são commits separados.** Leitores do Iceberg podem
  observar o estado intermediário. Falhas são relançadas; se a inserção falhar
  após a exclusão, verifique o estado do destino e reprocesse a mesma entrada
  completa. Não há rollback ou retry automático.
- Serialize as escritas no destino e mantenha o Parquet imutável e disponível
  até validar a carga. A rotina não bloqueia outros escritores.
- A consulta de existência lê apenas a coluna de origem e pode parar no
  primeiro resultado. Em recargas, o destino é acessado novamente pelo
  `DELETE`. Sem poda eficaz de arquivos, ambas as operações podem ler grande
  parte da tabela. O `DELETE` por linha pode reescrever arquivos afetados;
  o custo depende do layout, dos metadados e da configuração Iceberg. O loader
  não altera o particionamento.
- O chamador configura catálogo, namespace, extensões Iceberg, permissões
  de armazenamento e logs. O Trino deve acessar o mesmo catálogo e os mesmos
  dados; o loader não abre conexão direta ao Trino.

#### Testes de integração do loader

Os testes unitários não precisam de um catálogo real:

```bash
uv run pytest tests/test_trino_loader.py
```

Os testes Spark/Parquet não precisam dos conectores Iceberg:

```bash
uv run pytest tests/test_trino_loader_parquet.py
```

Os testes em `tests/test_trino_loader_integration.py` usam Spark local e catálogo
JDBC Iceberg com H2 em memória, sem conexão Trino. São habilitados explicitamente
e exigem JARs compatíveis com Spark/Scala:

```bash
ICEBERG_TEST_JARS=/caminho/iceberg-spark-runtime-3.5_2.12-1.9.2.jar,/caminho/h2-2.3.232.jar \
  uv run pytest tests/test_trino_loader_integration.py
```

Se os conectores já estiverem no classpath, use `ICEBERG_TESTS=1`. Sem essas
opções, a suíte de integração é pulada explicitamente, não validada por mocks.
Uma validação no Trino requer ambiente compartilhado adicional. Validar S3A
real exige conector, endpoint e bucket de testes autorizados; mocks não
comprovam permissões ou comportamento real de S3A.

### Exemplo de fluxo combinado

As etapas são independentes: cada uma pode ser usada sozinha, e a ordem abaixo
é apenas a ordem conceitual de apresentação. A carga é opcional e exige a sessão
Iceberg descrita em [`TrinoLoader`](#trinoloaderspark).

**Etapas chamadas diretamente:**

```python
from teleutils.core.extractors import CDRParquetExtractor
from teleutils.core.loaders import TrinoLoader
from teleutils.core.transformers import CDRTransformer

# 1. Extração (independente): grava o Parquet intermediário.
intermediario = CDRParquetExtractor(spark).extract(
    source_file="/dados/cdr/tim/stfc/ngn_italtel/entrada.parquet",
    target_file="/dados/extraido/tim_stfc_ngn_italtel",
    cdr_schema="stfc_italtel_ngn_tim",
)
if intermediario is None:
    raise FileNotFoundError("Nenhum Parquet de origem foi encontrado.")

# 2. Transformação (independente): lê qualquer intermediário compatível.
final = CDRTransformer(spark).transform_stfc_italtel_ngn_tim(
    source_file=intermediario,
    target_file="/dados/transformado/tim_stfc_ngn_italtel",
)

# 3. Carga (opcional e independente): lê qualquer Parquet no contrato final.
TrinoLoader(spark).upsert_iceberg(final, "catalogo.cdr.chamadas")
```

**Mesmo fluxo via orquestrador, com a carga como etapa explícita e separada:**
`run_etl` só extrai e transforma; o loader não é chamado por ele.

```python
from teleutils.core.loaders import TrinoLoader
from teleutils.core.orchestrators import print_etl_report, run_etl

configs = [
    {
        "storage_type": "local",
        "source_folder": "/dados/cdr/bruto/tim/stfc/ngn_italtel",
        "pattern": "*.parquet",
        "schema": "stfc_italtel_ngn_tim",
        "extracted_folder": "/dados/cdr/extraido",
        "transformed_folder": "/dados/cdr/transformado",
        "output_file_prefix": "tim_stfc_ngn_italtel",
    },
]

# Extração e transformação.
report = run_etl(configs, spark=spark)
print_etl_report(report)

# Carga, somente dos itens bem-sucedidos.
loader = TrinoLoader(spark)
for item in report.items:
    if item.status == "success":
        loader.upsert_iceberg(item.paths.transformed_file, "catalogo.cdr.chamadas")
```

## Módulos auxiliares

Componentes complementares, que não fazem parte do fluxo principal de
extração, transformação, orquestração e carga.

### `teleutils.preprocessing`

Funções para extração de números telefônicos brasileiros e validação de
CNPJ, exportadas por `__all__`: `ExtractionResult`, `extract_number` e
`is_valid_cnpj`. A classe `ExtractionResult` é definida em
`teleutils.preprocessing.phone_number_extractor` e também pode ser importada
de `teleutils.preprocessing`.

#### `extract_number(subscriber_number, remove_non_numeric=False) -> ExtractionResult`

Extrai um número reconhecido pelos padrões implementados em
[`phone_number_extractor.py`](../src/teleutils/preprocessing/phone_number_extractor.py).

- `subscriber_number`: obrigatório e sem anotação de tipo. Valores falsy são
  tratados como vazios; os demais são convertidos com `str`.
- `remove_non_numeric`: opcional, padrão `False`. Quando `True`, remove
  caracteres não numéricos do texto usado na extração. Não há argumento para
  adicionar DDD.
- Retorno: `ExtractionResult(number: str, success: bool)`, uma tupla nomeada.
  Entrada vazia (falsy) retorna `(SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE, False)`;
  o sentinela atual, definido em `teleutils._config`, é `"5599999999999"`.
  Quando a entrada não é vazia e nenhuma extração é aceita, retorna
  `(subscriber_number, False)`, com o valor de entrada original, sem limpeza
  nem conversão para texto.
- A limpeza converte para minúsculas, remove um `f` final e, se
  `remove_non_numeric=True`, caracteres não numéricos. Também remove zeros
  iniciais e sufixos de oito ou mais dígitos iguais. Com o padrão `False`, não
  remove pontuação e espaços em geral.
- A busca inicial depende do comprimento. O fallback prioriza móveis, fixos
  e CNG, com buscas no fim e início; depois chamada a cobrar e buscas em
  qualquer posição, conforme `FALLBACK_PATTERNS`.
- `success` indica extração aceita pelas expressões regulares, não confirmação
  de existência ou atribuição do número. O padrão de rejeição de repetições
  também é aplicado a serviços: por exemplo, `"199"` retorna falha.

##### Exemplo de uso

```python
from teleutils.preprocessing import extract_number

extraction_result = extract_number(subscriber_number="5511987654321")
assert extraction_result.number == "11987654321"
assert extraction_result.success is True
```

Com `remove_non_numeric=True`, a pontuação é removida antes da extração. Quando
nada é extraído, o valor original é devolvido com `success=False`, e uma entrada
vazia devolve o sentinela:

```python
from teleutils.preprocessing import extract_number

# Padrão: a pontuação não é removida, e a extração falha.
resultado = extract_number("(11) 98765-4321")
assert resultado == ("(11) 98765-4321", False)

resultado = extract_number("(11) 98765-4321", remove_non_numeric=True)
assert resultado.number == "11987654321"
assert resultado.success is True

assert extract_number("199") == ("199", False)
assert extract_number("") == ("5599999999999", False)  # sentinela de entrada vazia
```

#### Integração com o transformador base

A pandas UDF privada `_extract_numbers(subscriber_number_series)` fica em
[`base_transformer.py`](../src/teleutils/core/transformers/base_transformer.py),
não no namespace público de pré-processamento. Ela chama
`extract_number(subscriber_number=...)` para cada elemento e converte
explicitamente os atributos do resultado para colunas em português:

| Campo | Tipo |
| :-- | :-- |
| `numero_extraido` | string anulável |
| `extracao_sucesso` | booleano anulável |

`CDRBaseTransformer._extract_phone_numbers` aplica a UDF à origem e ao
destino. Os structs temporários `_extracao_numero_origem` e
`_extracao_numero_destino` são expandidos e removidos. O mapeamento em
[`TARGET_SCHEMA`](../src/teleutils/_config.py) preserva o contrato final:

| Coluna intermediária | Coluna final | Conteúdo |
| :-- | :-- | :-- |
| `numero_origem_extraido` | `co_numero_origem` | Número extraído; valor de entrada se a extração falha; sentinela se vazio |
| `sucesso_extracao_origem` | `ic_numero_origem_valido` | Sucesso da extração da origem |
| `numero_destino_extraido` | `co_numero_destino` | Número extraído; valor de entrada se a extração falha; sentinela se vazio |
| `sucesso_extracao_destino` | `ic_numero_destino_valido` | Sucesso da extração do destino |

Os nomes finais `ic_*_valido` permanecem por compatibilidade, com valores
provenientes de `success`. Essa etapa final é apenas renomeação e conversão
para os tipos já existentes. A extração é a transformação do conteúdo.
`numero_origem` e `numero_destino` preservam os valores brutos; quando existem,
`_numero_origem_original` e `_numero_destino_original` são restaurados após a
extração. Não há complemento de DDD nem inferência entre origem e destino.

#### `is_valid_cnpj(cnpj: str | int) -> bool`

Valida os dígitos verificadores de um CNPJ numérico ou alfanumérico, conforme
[`utils.py`](../src/teleutils/preprocessing/utils.py).

- `cnpj`: texto, com ou sem máscara, ou inteiro.
- Retorno: `True` se a estrutura e os dois dígitos verificadores forem válidos;
  `False` caso contrário. A função não levanta exceção para entradas inválidas.
- Sanitização: `None` e booleanos retornam `False`; tudo que não for ASCII
  alfanumérico é removido e as letras são convertidas para maiúsculas. Entradas
  totalmente numéricas são completadas com zeros à esquerda até 14 dígitos.
  Entradas com letras precisam ter exatamente 14 caracteres após a sanitização.
- O formato aceito é de 12 caracteres alfanuméricos (`0-9` e `A-Z`) seguidos de
  dois dígitos verificadores numéricos, calculados por módulo 11. Sequências de
  14 caracteres idênticos são rejeitadas.
- A função não consulta cadastros externos: validar o formato e os dígitos não
  comprova a existência do CNPJ.

##### Exemplo de uso

```python
from teleutils.preprocessing import is_valid_cnpj

assert is_valid_cnpj("11.222.333/0001-81") is True
assert is_valid_cnpj("12ABC34501DE35") is True
```

Entradas inválidas retornam `False`, sem levantar exceção:

```python
from teleutils.preprocessing import is_valid_cnpj

assert is_valid_cnpj("11222333000180") is False  # dígito verificador incorreto
assert is_valid_cnpj("11111111111111") is False  # sequência de dígitos idênticos
assert is_valid_cnpj(None) is False
```

### Outros utilitários e scripts auxiliares

O script Bash de transferência de CDRs não faz parte da API Python; consulte
[Transferência semanal de CDRs para MinIO](teleutils_upload_cdr_to_minio.md).

#### Exemplo de uso

Os scripts de `scripts/` aceitam `--dry-run`, que planeja a operação sem copiar
(o envio ao MinIO exige uma configuração `mc` existente e acesso de leitura). As
opções estão em `--help` e no documento acima.

```bash
# Planeja o envio dos CDRs Teleparser da semana, sem copiar.
./scripts/upload_cdr_to_minio.sh \
  --config scripts/uploader.config.example.json --week-dir Semana110 \
  --teleparser-folder /data/teleparser/ --dry-run

# Planeja a cópia para um diretório local, sem escrever.
./scripts/copy_cdr_to_local.sh \
  --config scripts/uploader.config.example.json --week-dir Semana110 \
  --destination /dados/cdr --dry-run
```

## Erros e efeitos colaterais

- Operações Spark de leitura, transformação e escrita propagam as exceções
  geradas pelo Spark.
- Os extratores e transformadores substituem o destino existente por usarem
  `mode("overwrite")`.
- Cada instância de `CDRTransformer` usa sua própria sessão Spark, criada a
  partir da sessão recebida e configurada com
  `spark.sql.timestampType = TIMESTAMP_NTZ`.
