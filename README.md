[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/InovaFiscaliza/teleutils)

# TeleUtils

Ferramentas utilitárias para extração, transformação e análise de registros de chamadas telefônicas (CDR) de operadoras brasileiras com Apache Spark.

---

## Visão Geral

O TeleUtils implementa um pipeline de dados para CDRs dividido em três etapas:

1. Extração de arquivos CSV heterogêneos para um formato parquet intermediário.
2. Transformação e padronização dos registros, incluindo normalização de números, parsing de datas e enriquecimento com indicadores.
3. Análise agregada por originador e hora para identificação de padrões de chamadas abusivas.

O projeto combina PySpark para processamento distribuído com Pandas e PyArrow para operações vetorizadas e I/O eficiente.

## Estado Atual do Projeto

Desde o último Pull Request integrado na branch principal, o projeto passou a refletir os seguintes pontos relevantes:

- Suporte completo ao formato Claro Nokia nas camadas de extração e transformação.
- Remoção do suporte público ao formato TIM STIR.
- Exposição pública da UDF `spark_normalize_number` para normalização em lote no Spark.
- Evolução da extração para usar esquemas configuráveis com schema Spark opcional e filtro por coluna quando necessário.
- Ajustes nas heurísticas de caixa postal e autenticação nas transformações.
- Consolidação das métricas de análise para contabilizar chamadas autenticadas, chamadas curtas autenticadas e chamadas de caixa postal autenticadas.
- Inclusão de `pytest` nas dependências de desenvolvimento e ampliação da cobertura de testes para normalização numérica.

---

## Formatos Suportados

| Formato | Extração | Transformação | Observações |
|--------|----------|---------------|-------------|
| Ericsson | `extract_cdr_ericsson` | `transform_cdr_ericsson` | Filtra chamadas terminadas (`TER`) e inicializa autenticação e caixa postal como `0`. |
| TIM VoLTE | `extract_cdr_tim_volte` | `transform_cdr_tim_volte` | Leitura com schema Spark explícito para arquivos sem cabeçalho confiável e detecção de caixa postal por registros `FORv`. |
| Vivo VoLTE | `extract_cdr_vivo_volte` | `transform_cdr_vivo_volte` | Extrai autenticação a partir do campo concatenado e identifica caixa postal via relação entre registros tipo `3` e `4`. |
| Claro Nokia | `extract_cdr_claro_nokia` | `transform_cdr_claro_nokia` | Novo formato suportado, com consolidação de eventos `MTC`, `UCA`, `FOR`, `MOC` e tratamento específico para `POC` e `PTC`. |

## Principais Recursos

### Preprocessamento de números

O módulo `teleutils.preprocessing` fornece três funções públicas:

| Função | Descrição |
|--------|-----------|
| `normalize_number` | Normaliza um número telefônico brasileiro e retorna `(numero_formatado, numero_valido)`. |
| `normalize_number_pair` | Normaliza um par de números usando o primeiro como contexto para inferência de DDD. |
| `spark_normalize_number` | UDF vetorizada para Spark que retorna `numero_formatado` e `numero_valido` em lote. |

A normalização trata prefixos nacionais e internacionais, remove caracteres espúrios e valida números segundo padrões brasileiros compatíveis com ANATEL e E.164.

### Extração configurável de CDR

O módulo `teleutils.robocalls.extractors` é baseado em `CDRSchema`, uma dataclass que define:

- delimitador do arquivo;
- uso opcional de schema Spark para leitura;
- presença de cabeçalho;
- colunas a selecionar e seus nomes finais;
- filtro opcional por valor de coluna;
- descrição do job para monitoramento no Spark.

Os arquivos extraídos são gravados em parquet particionado por `tipo_de_chamada`, o que melhora leituras posteriores que dependem desse campo.

### Transformação e enriquecimento

O módulo `teleutils.robocalls.transformers` padroniza os registros extraídos em um schema único e adiciona indicadores operacionais para análise:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `referencia` | `string` | Identificador da chamada no CDR de origem. |
| `tipo_de_chamada` | `string` | Tipo do evento de chamada mantido da fonte. |
| `data_hora` | `timestamp` | Data e hora da chamada convertidas para timestamp Spark. |
| `numero_de_a_formatado` | `string` | Número originador normalizado. |
| `numero_de_b_formatado` | `string` | Número destinatário normalizado. |
| `hora_da_chamada` | `string` | Chave temporal no formato `YYYYMMDDHH`. |
| `duracao_da_chamada` | `integer` | Duração em segundos. |
| `chamada_curta` | `integer` | `1` quando a duração é menor ou igual ao limiar configurado. |
| `chamada_autenticada` | `integer` | `-1` para falha, `0` para não verificada, `1` para autenticada. |
| `chamada_caixa_postal` | `integer` | `1` quando o registro é identificado como encaminhamento para caixa postal. |

### Análise de padrões de chamadas abusivas

O módulo `teleutils.robocalls.analyzers` agrega o parquet transformado por `numero_de_a_formatado` e `hora_da_chamada` e calcula as métricas:

- `total_chamadas`
- `total_chamadas_curtas`
- `total_chamadas_caixa_postal`
- `total_chamadas_autenticadas`
- `total_chamadas_curtas_autenticadas`
- `total_chamadas_caixa_postal_autenticadas`

As métricas de caixa postal consideram apenas chamadas não curtas. O resultado final é ordenado por `total_chamadas_curtas` em ordem decrescente.

---

## Instalação

### Requisitos

- Python 3.9 ou superior
- Ambiente com Apache Spark disponível para execução das rotinas de pipeline

### Com `uv` (recomendado)

```bash
git clone https://github.com/InovaFiscaliza/teleutils.git
cd teleutils
uv sync
```

### Instalação a partir do código-fonte com `pip`

```bash
git clone https://github.com/InovaFiscaliza/teleutils.git
cd teleutils
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### Dependências principais

- `pandas >= 2.3.3`
- `pyarrow == 21.0.0`
- `pyspark >= 3.5.5`

Dependências de desenvolvimento:

- `jupyter`
- `matplotlib`
- `pre-commit`
- `pytest`

---

## Início Rápido

### Pipeline completo com TIM VoLTE

```python
from pyspark.sql import SparkSession

from teleutils.robocalls import (
    RoboCallsAnalyzer,
    RoboCallsExtractor,
    RoboCallsTransformer,
)

spark = SparkSession.builder.appName("cdr-pipeline").getOrCreate()

extractor = RoboCallsExtractor(spark)
df_extracted = extractor.extract_cdr_tim_volte(
    source_file="data/tim_volte.csv",
    target_file="processed/tim_volte_extracted",
)

transformer = RoboCallsTransformer(spark, limiar_chamada_ofensora=6)
df_transformed = transformer.transform_cdr_tim_volte(
    source_file="processed/tim_volte_extracted",
    target_file="processed/tim_volte_transformed",
)

analyzer = RoboCallsAnalyzer(spark)
df_analyzed = analyzer.analyze(
    source_file="processed/tim_volte_transformed",
    target_file="processed/tim_volte_analyzed",
)

df_analyzed.show(10)
```

### Normalização numérica em Python

```python
from teleutils.preprocessing import normalize_number, normalize_number_pair

numero, valido = normalize_number("(11) 99999-9999")
print(numero, valido)

a_fmt, a_ok, b_fmt, b_ok = normalize_number_pair("11999999999", "88888888")
print(a_fmt, a_ok, b_fmt, b_ok)
```

### Normalização numérica em Spark

```python
from pyspark.sql import functions as F

from teleutils.preprocessing import spark_normalize_number

df = spark.createDataFrame(
    [("11999999999",), ("0800-123-4567",), ("numero_invalido",)],
    ["numero"],
)

df = df.withColumn("normalizado", spark_normalize_number("numero"))
df = df.select(
    "numero",
    F.col("normalizado.numero_formatado").alias("numero_formatado"),
    F.col("normalizado.numero_valido").alias("numero_valido"),
)
```

### Exemplo com Claro Nokia

```python
from pyspark.sql import SparkSession

from teleutils.robocalls import RoboCallsExtractor, RoboCallsTransformer

spark = SparkSession.builder.appName("claro-nokia").getOrCreate()

extractor = RoboCallsExtractor(spark)
extractor.extract_cdr_claro_nokia(
    source_file="data/claro_nokia.csv",
    target_file="processed/claro_nokia_extracted",
)

transformer = RoboCallsTransformer(spark)
df = transformer.transform_cdr_claro_nokia(
    source_file="processed/claro_nokia_extracted",
    target_file="processed/claro_nokia_transformed",
)

df.show(5)
```

---

## Referência da API

| Módulo | Responsabilidade | Símbolos públicos |
|--------|------------------|-------------------|
| `teleutils.preprocessing.number_format` | Normalização e validação de números telefônicos brasileiros | `normalize_number`, `normalize_number_pair`, `spark_normalize_number` |
| `teleutils.robocalls.extractors` | Leitura de CDRs brutos e padronização intermediária | `CDRSchema`, `RoboCallsExtractor` |
| `teleutils.robocalls.transformers` | Conversão dos CDRs extraídos para schema analítico padronizado | `RoboCallsTransformer` |
| `teleutils.robocalls.analyzers` | Agregação e detecção de padrões de chamadas abusivas | `RoboCallsAnalyzer` |

Métodos públicos do extrator:

- `extract_cdr_ericsson(source_file, target_file)`
- `extract_cdr_tim_volte(source_file, target_file)`
- `extract_cdr_vivo_volte(source_file, target_file)`
- `extract_cdr_claro_nokia(source_file, target_file)`

Métodos públicos do transformador:

- `transform_cdr_ericsson(source_file, target_file)`
- `transform_cdr_tim_volte(source_file, target_file)`
- `transform_cdr_vivo_volte(source_file, target_file)`
- `transform_cdr_claro_nokia(source_file, target_file)`

Método público do analisador:

- `analyze(source_file, target_file="")`

---

## Organização do Projeto

```text
src
└── teleutils
    ├── __init__.py
    ├── _logging.py
    ├── preprocessing
    │   ├── __init__.py
    │   └── number_format.py
    └── robocalls
        ├── __init__.py
        ├── analyzers.py
        ├── extractors.py
        └── transformers.py
```

### Arquitetura em alto nível

- `preprocessing` concentra regras de normalização numérica reutilizáveis dentro e fora do pipeline.
- `extractors` define o contrato de leitura e mapeamento inicial por formato de CDR.
- `transformers` consolida heurísticas por operadora e produz um parquet analítico padrão.
- `analyzers` agrega o resultado transformado para evidenciar comportamento suspeito.

---

## Desenvolvimento

### Configuração do ambiente

```bash
git clone https://github.com/InovaFiscaliza/teleutils.git
cd teleutils
uv sync
source .venv/bin/activate
pre-commit install
```

### Diretrizes

- Mantenha docstrings e README alinhados com a API pública.
- Adicione testes sempre que alterar regras de normalização, extração ou heurísticas de transformação.
- Preserve os formatos de saída parquet esperados pelas etapas seguintes do pipeline.

---

## Licença

TeleUtils é licenciado sob **GNU General Public License v3.0**. Veja [LICENSE](LICENSE) para mais detalhes.

## Referências

- [ANATEL - Agência Nacional de Telecomunicações](https://www.anatel.gov.br/)
- [ITU-T E.164 - Numbering Plan for the ISDN Era](https://handle.itu.int/11.1002/1000/10688)
- [Apache Spark Documentation](https://spark.apache.org/documentation.html)
- [Conventional Commits](https://www.conventionalcommits.org/)

---

**Desenvolvido por:** [InovaFiscaliza](https://github.com/InovaFiscaliza)
**Última atualização:** Abril 2026