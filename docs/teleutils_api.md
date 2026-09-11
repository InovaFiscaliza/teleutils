# Referência da API Pública

## Visão geral

O pacote `teleutils` oferece utilitários de pré-processamento e um pipeline
Spark para extrair CDRs, gravar um Parquet intermediário e transformá-lo em um
contrato analítico padronizado. A API pública é exposta pelos subpacotes
`teleutils.preprocessing`, `teleutils.core.extractors`,
`teleutils.core.extractors.schemas` e `teleutils.core.transformers`.

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

Funções para normalização de números telefônicos brasileiros e validação de
CNPJ. O subpacote reexporta cinco interfaces.

### `normalize_number(subscriber_number, national_destination_code="")`

Normaliza e valida um número telefônico brasileiro.

- `subscriber_number`: valor a normalizar. Pode conter pontuação, letras,
  caracteres de preenchimento e, quando conversível, qualquer valor textual.
- `national_destination_code`: DDD opcional usado para completar números locais
  válidos de oito ou nove dígitos.
- Retorno: tupla `(numero, valido)`. Para entrada vazia, retorna
  `("5599999999999", False)`; para uma entrada inválida não vazia, retorna o
  valor processado e `False`.

```python
from teleutils.preprocessing import normalize_number

numero, valido = normalize_number("(11) 99999-9999")
```

### `normalize_number_pair(number_a, number_b, national_destination_code="")`

Normaliza dois números e, quando `number_a` é válido e tem dez ou onze
dígitos, usa seus dois primeiros dígitos como DDD de `number_b` caso não tenha
sido informado um DDD. Retorna
`(numero_a, numero_a_valido, numero_b, numero_b_valido)`.

### `spark_normalize_number(number_series)`

Pandas UDF para uso em expressões Spark. Recebe uma série pandas e retorna um
`pandas.DataFrame` com o schema estruturado:

| Campo | Tipo |
| :-- | :-- |
| `numero_formatado` | string anulável |
| `numero_valido` | booleano anulável |

```python
from pyspark.sql import functions as F
from teleutils.preprocessing import spark_normalize_number

df = df.withColumn("normalizado", spark_normalize_number("numero"))
df = df.select(F.col("normalizado.numero_formatado"))
```

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
| `TEXT_DEFAULT_SCHEMAS` | `algar_hauwei` |

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
  a chave `algar_hauwei`.

| Método | Entrada | Comportamento e retorno |
| :-- | :-- | :-- |
| `extract_cdr(source_file: str, target_file: str, schema: CDRTextSchema) -> str` | CSV/texto e `CDRTextSchema` | Lê conforme o schema, seleciona por índice, adiciona metadados e grava o intermediário. Retorna `target_file`. Pode levantar `ValueError` se o arquivo não possuir o maior índice configurado. |
| `extract_cdr_algar_hauwei(source_file, target_file)` | CSV Algar Hauwei | Usa o schema `algar_hauwei`; retorna `target_file`. |

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
| `transform_cdr_algar_hauwei(source_file: str, target_file: str) -> str` | Combina campos temporais do CSV e mapeia códigos de tipo e status. |

Todos os métodos aplicam o pipeline comum interno antes da escrita: completam
colunas ausentes do contrato, normalizam duração/datas, normalizam números,
classificam autenticação quando disponível e preenchem nulos da chave primária.

O contrato final possui 35 colunas, definido em `TARGET_SCHEMA`. A referência
de origem e transformação por coluna está em
[Linhagem e Transformações dos Dados](teleutils_linhagem_transformacoes_dados.md).

## Erros e efeitos colaterais

- Operações Spark de leitura, transformação e escrita propagam as exceções
  geradas pelo Spark.
- Os extratores e transformadores substituem o destino existente por usarem
  `mode("overwrite")`.
- Cada instância de `CDRTransformer` usa sua própria sessão Spark, criada a
  partir da sessão recebida e configurada com
  `spark.sql.timestampType = TIMESTAMP_NTZ`.