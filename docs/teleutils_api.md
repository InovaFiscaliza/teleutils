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

Os extratores recebem uma `SparkSession` e escrevem sempre em modo `overwrite`.
Todos retornam a string do caminho de destino, e não um DataFrame. Para derivar
corretamente os metadados, cada arquivo de entrada deve estar em uma hierarquia
cujo final seja `prestadora/tipo_cdr/arquivo`: os três últimos componentes de
`input_file_name()` alimentam, nessa ordem, `prestadora`, `tipo_cdr` e
`arquivo_origem`.

### `CDRParquetExtractor(spark, schemas=None)`

Extrai CDRs de Parquet.

- `spark: SparkSession`: sessão usada para leitura, projeção e escrita;
- `schemas: dict[str, CDRParquetSchema] | None`: catálogo opcional que substitui
  integralmente `PARQUET_DEFAULT_SCHEMAS` na instância. Para continuar usando
  os métodos específicos, o catálogo fornecido deve conter as respectivas
  chaves esperadas.

| Método | Entrada | Comportamento e retorno |
| :-- | :-- | :-- |
| `extract_cdr(source_file: str, target_file: str, schema: CDRParquetSchema, unique: bool = False) -> str` | Caminho Parquet; a implementação também aceita uma lista de caminhos; schema `CDRParquetSchema` | Lê com `mergeSchema=true`, seleciona/renomeia campos, inclui metadados do caminho e grava o intermediário. `unique=True` executa `dropDuplicates()`. Retorna `target_file`. |
| `extract_cdr_ericsson(source_file, target_file)` | Parquet Ericsson | Usa o schema `ericsson`; retorna `target_file`. |
| `extract_cdr_lte_huawei_tim(source_file, target_file)` | Parquet LTE Huawei TIM | Usa `lte_huawei_tim` e remove duplicatas; retorna `target_file`. |
| `extract_cdr_lte_ericsson_vivo(source_file, target_file)` | Parquet LTE Ericsson Vivo | Usa `lte_ericsson_vivo`; retorna `target_file`. |
| `extract_cdr_nokia(source_file, target_file)` | Parquet Nokia | Usa `nokia`; retorna `target_file`. |

Quando uma coluna declarada no schema não existe no Parquet, o extrator a
representa como literal nulo tipado como string. Os metadados `prestadora`,
`tipo_cdr` e `arquivo_origem` são derivados de `input_file_name()`.

### `CDRTextExtractor(spark, schemas=None)`

Extrai CDRs de arquivos CSV/texto.

- `spark: SparkSession`: sessão usada para leitura, seleção e escrita;
- `schemas: dict[str, CDRTextSchema] | None`: catálogo opcional que substitui
  integralmente `TEXT_DEFAULT_SCHEMAS` na instância. O método específico requer
  a chave `algar_huawei`.

| Método | Entrada | Comportamento e retorno |
| :-- | :-- | :-- |
| `extract_cdr(source_file: str, target_file: str, schema: CDRTextSchema) -> str` | CSV/texto e `CDRTextSchema` | Lê conforme o schema, seleciona por índice, adiciona metadados e grava o intermediário. Retorna `target_file`. Pode levantar `ValueError` se o arquivo não possuir o maior índice configurado. |
| `extract_cdr_algar_huawei(source_file, target_file)` | CSV Algar Huawei | Usa o schema `algar_huawei`; retorna `target_file`. |

No fluxo textual, `arquivo_origem` passa por `url_decode`. Se o schema definir
`column_to_filter`, registros com o valor configurado são removidos.

## `teleutils.core.transformers`

### `CDRTransformer(spark)`

Transforma um Parquet intermediário no contrato final do projeto. Cada método
lê `source_file`, aplica regras do layout, escreve `target_file` em Parquet com
`overwrite` e particionamento por `no_tipo_chamada`, e retorna `target_file`.
O construtor recebe uma `SparkSession`, cria uma nova sessão com o mesmo
`SparkContext` por meio de `newSession()` e configura
`spark.sql.timestampType = TIMESTAMP_NTZ` nessa nova sessão.

| Método | Layout e comportamento específico |
| :-- | :-- |
| `transform_cdr_ericsson(source_file: str, target_file: str) -> str` | Converte duração `HH:mm:ss` em segundos e compõe célula, IMSI e IMEI a partir de componentes. |
| `transform_cdr_lte_huawei_tim(source_file: str, target_file: str) -> str` | Trata registros ATS/IBCF, extrai dados JSON/SIP e atribui célula, IMEI e IMSI por papel de chamada. |
| `transform_cdr_lte_ericsson_vivo(source_file: str, target_file: str) -> str` | Separa número e autenticação, mapeia códigos, remove hífens de IMEI e decodifica células hexadecimais. |
| `transform_cdr_nokia(source_file: str, target_file: str) -> str` | Consolida duração e datas, aplica regras UCA/FORW, imputa MCC/MNC de células e agrupa status. |
| `transform_cdr_algar_huawei(source_file: str, target_file: str) -> str` | Combina campos temporais do CSV e mapeia códigos de tipo e status. |

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

### `get_config(config: Mapping[str, object]) -> ConfigPaths`

Valida um mapping sem modificá-lo e constrói caminhos S3A ou locais.

| Campo | Contrato |
|---|---|
| `storage_type` | Opcional: `"s3a"` (padrão quando ausente) ou `"local"`, sem normalização. |
| `source_folder` | String obrigatória com a pasta de origem, conforme storage_type. |
| `pattern` | String obrigatória com padrão relativo, incluindo glob. Barras iniciais são removidas. |
| `schema` | String obrigatória; chave preservada sem normalização. |
| `extracted_folder` | String obrigatória com a pasta intermediária, conforme storage_type. |
| `transformed_folder` | String obrigatória com a pasta final, conforme storage_type. |
| `output_file_prefix` | String obrigatória; prefixo sem separadores ou curingas. |
| `operator`, `service`, `cdr_type` | Opcionais: strings ou `None`; vazio/`None` preservam a inferência dos extratores. |

Não há aliases de configuração: qualquer chave desconhecida é rejeitada.
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
- `extracted_file`: pasta intermediária e `<prefix>_extracted.parquet`;
- `transformed_file`: pasta final e `<prefix>_transformed.parquet`.

Os destinos representam diretórios/datasets Parquet. A validação não consulta
S3 nem filesystem local, verifica existência ou certifica integridade.
Suporte ao schema e conflitos
de caminhos são verificados por `run_etl`, não por `get_config`.

Tipos inválidos levantam `TypeError`; campos ausentes, desconhecidos, vazios
ou com valores inválidos levantam `ValueError`.

### `run_etl(configs, *, spark, base_folder=None, week_dir=None) -> ETLReport`

Recebe uma sequência finita de mappings e uma `SparkSession` existente.
Strings, mappings no lugar da sequência e iteradores são rejeitados com
`TypeError`, registrado em log. Uma sequência vazia retorna relatório vazio,
sem instanciar componentes.

`run_etl` não verifica a unicidade global de prefixos em listas fornecidas
diretamente. Para arquivos JSON, `load_config` faz essa verificação durante
a leitura. Conflitos entre caminhos de destino continuam sendo rejeitados.

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

### `run_etl_item(config, *, spark, base_folder=None, week_dir=None) -> ETLItemResult`

Executa um lote de um item e retorna seu resultado, com `index=0`. Possui as
mesmas validações, resultados e política de erros de `run_etl`.
Os argumentos opcionais `base_folder` e `week_dir` são repassados a `run_etl`
e devem ser informados juntos; eles prefixam a origem sem alterar `config`.

### Resultados imutáveis

`ETLItemResult` e `ETLReport` são dataclasses congeladas.

| Campo de `ETLItemResult` | Conteúdo |
|---|---|
| `index` | Posição do item na sequência, iniciando em zero. |
| `schema` | Chave informada, quando identificável; caso contrário `None`. |
| `paths` | `ConfigPaths` planejado ou `None` se a configuração não foi resolvida. |
| `status` | `success`, `validation_error` ou `execution_error`. |
| `stage` | `configuration`, `paths`, `resolution`, `initialization`, `extraction`, `transformation` ou `completed`. |
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

Uma extração malsucedida impede a transformação daquele item. Falha na
transformação preserva o registro de extração concluída, mas não é sucesso.
`KeyboardInterrupt` e `SystemExit` propagam; não há relatório final garantido
após interrupção do usuário.

### `print_etl_report(report: ETLReport) -> None`

Imprime na saída padrão as contagens do lote e, na ordem original, o índice,
schema, status, etapa e etapas concluídas de cada item. Quando disponíveis,
também imprime os caminhos planejados e os detalhes de erro. Um relatório vazio
é identificado explicitamente. A função não altera o relatório nem certifica
os dados processados. Caminhos e mensagens de erro podem conter dados sensíveis.

### Limites e validação local

O escopo é apenas extração e transformação. Não há carga, retry, paralelismo,
CLI, retomada, rollback ou exclusão automática de saídas parciais.
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
