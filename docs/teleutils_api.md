# Referência da API Pública

## Visão geral

O pacote `teleutils` oferece utilitários de pré-processamento e um pipeline
Spark para extrair CDRs, gravar um Parquet intermediário e transformá-lo em um
contrato analítico padronizado. A API pública é exposta pelos subpacotes
`teleutils.preprocessing`, `teleutils.core.extractors`,
`teleutils.core.extractors.schemas`, `teleutils.core.transformers` e
`teleutils.core.orchestrators`.

O pacote raiz não reexporta essas interfaces. Importe-as dos subpacotes
indicados.

## Requisitos de uso

- Python 3.9 ou superior;
- `pandas` e `pyarrow`, instalados como dependências do pacote;
- Apache Spark por meio de `pyspark` para UDFs, extração e transformação.

As funções Python de normalização e validação podem ser chamadas sem criar uma
sessão Spark. Extratores, transformadores e pandas UDFs exigem um ambiente
Spark funcional. Na implementação atual, importe
`teleutils.core.transformers` somente depois de criar uma `SparkSession`: a
configuração carregada pelo módulo constrói uma expressão Spark durante o
import e requer um `SparkContext` ativo.

## `teleutils.preprocessing`

Funções para extração de números telefônicos brasileiros e validação de
CNPJ.

### `extract_number(subscriber_number) -> ExtractionResult`

Extrai um número reconhecido pelos padrões implementados em
[`phone_number_extractor.py`](../src/teleutils/preprocessing/phone_number_extractor.py).

- `subscriber_number`: único argumento, obrigatório e sem anotação de tipo.
  Valores falsy são tratados como vazios; os demais são convertidos com `str`.
  Não há argumento para adicionar DDD.
- Retorno: `ExtractionResult(number: str, success: bool)`, uma tupla nomeada.
  Entrada vazia ou extração malsucedida retorna
  `(SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE, False)`; o sentinela atual é
  `"99999999999"`, não o texto original inválido.
- A limpeza converte para minúsculas, remove um `f` final, zeros iniciais e
  sufixos de oito ou mais dígitos iguais. Não remove pontuação e espaços em geral.
- A busca inicial depende do comprimento. O fallback prioriza móveis, fixos
  e CNG, com buscas no fim e início; depois chamada a cobrar e buscas em
  qualquer posição, conforme `FALLBACK_PATTERNS`.
- `success` indica extração aceita pelas expressões regulares, não confirmação
  de existência ou atribuição do número. O padrão de rejeição de repetições
  também é aplicado a serviços: por exemplo, `"199"` retorna falha.

```python
from teleutils.preprocessing import extract_number

extraction_result = extract_number(subscriber_number="5511987654321")
assert extraction_result.number == "11987654321"
assert extraction_result.success is True
```

### Integração com o transformador base

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
| `numero_origem_extraido` | `nu_origem` | Número extraído ou sentinela |
| `sucesso_extracao_origem` | `ic_origem_valido` | Sucesso da extração da origem |
| `numero_destino_extraido` | `nu_destino` | Número extraído ou sentinela |
| `sucesso_extracao_destino` | `ic_destino_valido` | Sucesso da extração do destino |

Os nomes finais `ic_*_valido` permanecem por compatibilidade, com valores
provenientes de `success`. Essa etapa final é apenas renomeação e conversão
para os tipos já existentes. A extração é a transformação do conteúdo.
`numero_origem` e `numero_destino` preservam os valores brutos; quando existem,
`_numero_origem_original` e `_numero_destino_original` são restaurados após a
extração. Não há complemento de DDD nem inferência entre origem e destino.

### `validar_cnpj(cnpj)`

Valida os dígitos verificadores de um CNPJ. Aceita texto ou inteiro, remove
caracteres não numéricos, completa até 14 dígitos com zeros à esquerda e
retorna `True` ou `False`. Valores nulos, booleanos, vazios, com mais de 14
dígitos ou compostos por um único dígito repetido retornam `False`.

### `spark_validar_cnpj(cnpj_series)`

Pandas UDF que aplica `validar_cnpj` a uma série pandas e retorna um
`pandas.DataFrame` estruturado com o campo booleano anulável `cnpj_valido`.

## `teleutils.core.extractors.schemas`

Contratos declarativos usados pelos extratores. Os catálogos padrão podem ser
fornecidos aos construtores para substituir ou estender os layouts suportados.

### `CDRParquetSchema`

Dataclass imutável com os atributos:

- `name: str`;
- `column_mapping: tuple[tuple[str, str], ...]`;
- `job_description: str`.

`column_mapping` não pode estar vazio e cada item deve ser uma tupla de duas
strings. Violações levantam `ValueError` na instanciação.

### `CDRTextSchema`

Dataclass imutável para arquivos CSV/texto, com os atributos `name`,
`delimiter`, `schema`, `has_header`, `column_to_filter`, `column_indices`,
`column_names` e `job_description`.

Os índices são zero-based. A classe exige ao menos um índice e valida a
correspondência entre índices e nomes, a ausência de índices negativos, o tipo
e a extensão de um schema Spark explícito e, quando presente, o formato da
tupla de filtro e a existência da coluna filtrada em `column_names`.
Configurações inválidas levantam `ValueError`.

### `PARQUET_DEFAULT_SCHEMAS` e `TEXT_DEFAULT_SCHEMAS`

Catálogos de schemas padrão. As chaves ativas são:

| Catálogo | Chaves |
| :-- | :-- |
| `PARQUET_DEFAULT_SCHEMAS` | `ericsson`, `lte_huawei_tim`, `lte_ericsson_vivo`, `nokia` |
| `TEXT_DEFAULT_SCHEMAS` | `algar_huawei` |

## `teleutils.core.extractors`

Os extratores recebem uma `SparkSession`. `extract` grava com `overwrite` e
retorna o caminho; `extract_dataframe` retorna o DataFrame sem essa escrita.
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
| `extract(source_file, target_file, cdr_schema, unique=False, *, operator="", service="", cdr_type="") -> str` | Caminho Parquet ou lista não vazia de caminhos | Delega a `extract_dataframe`, grava com overwrite e retorna `target_file`. |
| `extract_dataframe(source_file, cdr_schema, unique=False, *, operator="", service="", cdr_type="") -> DataFrame` | Mesma origem e chave do catálogo | Lê com `mergeSchema=true` e `ignoreCorruptFiles=true`, projeta campos e metadados, aplica deduplicação opcional e retorna o DataFrame sem escrita. |

Quando uma coluna declarada no schema não existe no Parquet, o extrator a
representa como literal nulo tipado como string. Os metadados `prestadora`,
`servico`, `tipo_cdr`, `esquema` e `arquivo_origem` seguem o contrato de
metadados compartilhado. `unique=True` considera também os metadados e ocorre
antes da remoção de `_tipo_cdr`.

### `CDRTextExtractor(spark, schemas=TEXT_DEFAULT_SCHEMAS)`

Extrai CDRs de arquivos CSV/texto.

- `spark: SparkSession`: sessão usada para leitura, seleção e escrita;
- `schemas: dict[str, CDRTextSchema]`: catálogo de contratos, selecionados
  pela chave `cdr_schema`.

| Método | Entrada | Comportamento e retorno |
| :-- | :-- | :-- |
| `extract(source_file, target_file, cdr_schema, *, operator="", service="", cdr_type="") -> str` | Caminho CSV/texto ou lista não vazia de caminhos | Delega a `extract_dataframe`, grava com overwrite e retorna `target_file`. |
| `extract_dataframe(source_file, cdr_schema, *, operator="", service="", cdr_type="") -> DataFrame` | Mesma origem e chave do catálogo | Lê CSV ou RDD, seleciona campos ou recorta largura fixa, adiciona metadados e aplica `lines_to_keep`, sem escrita intermediária. |

`arquivo_origem` é decodificado preservando sinais de mais literais. O filtro
`lines_to_keep` mantém os registros que atendem à condição do contrato.

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
| `transform_stfc_tropico_claro` | Trópico Claro, incluindo os aliases do catálogo. |
| `transform_stfc_pit_claro`, `transform_stfc_ss8bf_claro` | PIT e SS8BF Claro. |
| `transform_stfc_sigma_claro` | Sigma Claro. |

Esses métodos mantêm `target_file: str` e retorno `str`. O despachante genérico
`transform` ainda procura funções globais; utilize os métodos de layout ou o
orquestrador, que resolve os métodos da instância.

Todos os métodos aplicam o pipeline comum interno antes da escrita: completam
colunas ausentes do contrato, normalizam duração/datas, normalizam números,
classificam autenticação quando disponível e preenchem nulos da chave primária.

O contrato final possui 35 colunas, definido em `TARGET_SCHEMA`. A referência
de origem e transformação por coluna está em
[Linhagem e Transformações dos Dados](teleutils_linhagem_transformacoes_dados.md).

## `teleutils.core.orchestrators`

Coordena extração e transformação sem implementar regras de CDR. A importação
da API não carrega Spark nem os transformadores. A validação reutiliza a função
de metadados dos extratores (e suas dependências Python), sem iniciar uma sessão
ou executar ações Spark. Os componentes de execução são instanciados somente
quando há itens preparados para executar.

### `load_config(path, *, endpoint=None, access_key=None, secret_key=None, secure=False)`

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

As opções de conexão seguem `load_config`: argumentos explícitos prevalecem
sobre `MINIO_ENDPOINT`, `MINIO_USER` e `MINIO_SECRET`; endpoint sem protocolo.
O padrão `secure=False` usa HTTP sem criptografia; use `secure=True` para HTTPS.
Tipos inválidos geram `TypeError`; caminhos, máscaras ou configurações de conexão
inválidos geram `ValueError`. Prefixos sem objetos ou sem correspondências
retornam `[]`. Erros do SDK, inclusive durante a paginação, são propagados,
sem retornar listas vazias ou resultados parciais para esconder falhas.

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

### `get_config(config, *, materialize_intermediate=True) -> ConfigPaths`

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
de caminhos são verificados por `run_etl`, não por `get_config`.

Tipos inválidos levantam `TypeError`; campos ausentes, desconhecidos, vazios
ou com valores inválidos levantam `ValueError`.

### `run_etl(configs, *, spark, base_folder=None, week_dir=None, materialize_intermediate=True) -> ETLReport`

Recebe uma sequência finita de mappings e uma `SparkSession` existente.
Strings, mappings no lugar da sequência e iteradores são rejeitados com
`TypeError`, registrado em log. Uma sequência vazia retorna relatório vazio,
sem instanciar componentes.

`run_etl` não verifica a unicidade global de prefixos em listas fornecidas
diretamente. Para arquivos JSON, `load_config` faz essa verificação durante
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
`{base_folder}/{week_dir}/{source_folder}`. A configuração original não é
alterada. Informar apenas um deles levanta `ValueError`.

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
rasas dos catálogos. Não há tratamento específico de LEGB: seu alias passa pela
mesma resolução dinâmica dos demais métodos.

O contexto Spark fornecido não é criado nem encerrado. A sessão SQL derivada
criada pelo transformador mantém seu comportamento original. O orquestrador
não configura credenciais, conectores, Iceberg, handlers de logging ou níveis
globais; utiliza um logger de módulo. Os decoradores dos componentes continuam
registrando suas operações e, quando aplicável, ajustando temporariamente
loggers Spark conforme o comportamento preexistente.

### `run_etl_item(config, *, spark, base_folder=None, week_dir=None, materialize_intermediate=True) -> ETLItemResult`

Executa um lote de um item e retorna seu resultado, com `index=0`. Possui as
mesmas validações, resultados e política de erros de `run_etl`.
Os argumentos opcionais `base_folder` e `week_dir` são repassados a `run_etl`
e devem ser informados juntos; eles prefixam a origem sem alterar `config`.
`materialize_intermediate` é repassado com o mesmo contrato de `run_etl`.

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
| `error_type` | Nome da classe da exceção, ou `None` no sucesso. |
| `error_message` | Mensagem da exceção, ou `None` no sucesso. |

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
materializado, falha na transformação preserva o registro de extração concluída,
mas não é sucesso. No modo direto, `completed_stages` permanece vazio até a
escrita final concluir; falhas após obter o DataFrame usam `stage="pipeline"`,
pois podem ter origem na extração lazy. No sucesso, ambos os modos registram
`("extraction", "transformation")`. A impressão informa explicitamente quando
o intermediário não foi materializado.
`KeyboardInterrupt` e `SystemExit` propagam; não há relatório final garantido
após interrupção do usuário.

### `print_etl_report(report: ETLReport | ETLItemResult) -> None`

Imprime na saída padrão as contagens do lote e, na ordem original, o índice,
schema, status, etapa e etapas concluídas de cada item. Quando disponíveis,
também imprime os caminhos planejados e os detalhes de erro. Um relatório vazio
é identificado explicitamente. Um `ETLItemResult`, como o retornado por
`run_etl_item`, é apresentado como relatório de um item, com as mesmas
contagens e detalhes. Outros tipos levantam `TypeError`. A função não altera
os resultados nem certifica os dados processados. Caminhos e mensagens de erro
podem conter dados sensíveis.

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

A correção de LEGB pertence ao transformador e não ao orquestrador. O teste
de regressão local sinaliza `xfail` somente após confirmar a falha conhecida
de resolução de `_tipo_chamada`; outros erros falham normalmente. Quando essa
correção independente estiver disponível, o teste poderá concluir com sucesso.

Para uso no Jupyter, consulte o [exemplo do README](../README.md#orquestração-de-lotes-no-jupyter).

## Erros e efeitos colaterais

- Operações Spark de leitura, transformação e escrita propagam as exceções
  geradas pelo Spark.
- Os extratores e transformadores substituem o destino existente por usarem
  `mode("overwrite")`.
- Cada instância de `CDRTransformer` usa sua própria sessão Spark, criada a
  partir da sessão recebida e configurada com
  `spark.sql.timestampType = TIMESTAMP_NTZ`.
