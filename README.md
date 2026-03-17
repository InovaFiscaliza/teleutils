[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/InovaFiscaliza/teleutils)

# TeleUtils


**Ferramentas utilitárias para processamento de registros de chamadas telefônicas (CDR) de operadoras brasileiras.**

---

## Visão Geral

O **TeleUtils** é uma biblioteca Python especializada em extração, transformação e análise de registros de chamadas telefônicas (CDR — Call Detail Records) provenientes de diferentes operadoras e tecnologias brasileiras. A biblioteca fornece um pipeline completo e eficiente para:

1. **Extração**: Ler e padronizar arquivos CDR em múltiplos formatos
2. **Transformação**: Normalizar dados, converter timestamps e enriquecer registros com indicadores
3. **Análise**: Agregar dados e identificar padrões indicadores de atividade abusiva

A biblioteca é construída sobre **Apache Spark** para processamento escalável de grandes volumes de dados, integrada com **Pandas** e **PyArrow** para otimização de I/O.

### Formatos Suportados

- **Ericsson** — Sistemas de telecomunicações Ericsson
- **TIM VoLTE** — Voice over LTE da TIM
- **TIM STIR** — STIR/SHAKEN da TIM com autenticação
- **Vivo VoLTE** — Voice over LTE da Vivo

---

## Instalação

### Com `uv` (recomendado)

```bash
uv pip install teleutils
```

Para ambiente de desenvolvimento:

```bash
git clone https://github.com/InovaFiscaliza/teleutils.git
cd teleutils
uv sync
```

### Com `pip`

```bash
pip install teleutils
```

### Dependências

- `pandas >= 2.3.3`
- `pyarrow == 21.0.0`
- `pyspark >= 3.5.5`

---

## Início Rápido

Exemplo completo cobrindo extração, transformação e análise:

```python
from pyspark.sql import SparkSession
from teleutils.robocalls import (
    RoboCallsExtractor,
    RoboCallsTransformer,
    RoboCallsAnalyzer
)

# Inicializar sessão Spark
spark = SparkSession.builder \
    .appName("CDR Pipeline") \
    .getOrCreate()

# 1. EXTRAÇÃO: Ler CDR em formato CSV
extractor = RoboCallsExtractor(spark)
df_extracted = extractor.extract_cdr_tim_volte(
    source_file="data/tim_volte_cdr.csv",
    target_file="parquet/tim_volte_extracted"
)

# 2. TRANSFORMAÇÃO: Normalizar e padronizar
transformer = RoboCallsTransformer(
    spark,
    limiar_chamada_ofensora=6  # chamadas curtas <= 6 segundos
)
df_transformed = transformer.transform_cdr_tim_volte(
    source_file="parquet/tim_volte_extracted",
    target_file="parquet/tim_volte_transformed"
)

# 3. ANÁLISE: Agregar e identificar padrões
analyzer = RoboCallsAnalyzer(spark)
df_analyzed = analyzer.analyze(
    source_file="parquet/tim_volte_transformed",
    target_file="parquet/tim_volte_analyzed"
)

# Visualizar top 10 originadores mais agressivos
df_analyzed.show(10)
```

---

## Referência da API

| Módulo | Responsabilidade | Detalhes |
|--------|------------------|----------|
| `preprocessing.number_format` | Normalização de números telefônicos brasileiros | [Ver detalhes](#number_format) |
| `robocalls.extractors` | Extração de dados brutos em formato CDR | [Ver detalhes](#extractors) |
| `robocalls.transformers` | Transformação e padronização dos dados | [Ver detalhes](#transformers) |
| `robocalls.analyzers` | Análise agregada e detecção de padrões | [Ver detalhes](#analyzers) |

---

## Detalhamento da API

<a id="number_format"></a>

### Formatação de Números Telefônicos (`preprocessing.number_format`)

O módulo de formatação de números implementa a normalização de números telefônicos brasileiros de acordo com os padrões ANATEL (Agência Nacional de Telecomunicações) e o padrão internacional ITU-T E.164.

**Funções Públicas:**

| Função | Parâmetros | Retorno | Descrição |
|--------|-----------|---------|-----------|
| `normalize_number(subscriber_number, national_destination_code="")` | `subscriber_number` (str/int): Número a normalizar<br/>`national_destination_code` (str): Código de área (opcional) | `(str, bool)`: Número normalizado e flag de validade | Normaliza um número telefônico brasileiro |
| `normalize_number_pair(number_a, number_b, national_destination_code="")` | `number_a` (str/int): Primeiro número<br/>`number_b` (str/int): Segundo número<br/>`national_destination_code` (str): Código de área (opcional) | `(str, bool, str, bool)`: Ambos normalizados com flags | Normaliza par de números com inferência contextual de área |

**Exemplo de Uso:**

```python
from teleutils.preprocessing import normalize_number, normalize_number_pair

# Normalizar número individual
numero_formatado, eh_valido = normalize_number("(11) 99999-9999")
print(f"Número: {numero_formatado}, Válido: {eh_valido}")
# Saída: Número: 11999999999, Válido: True

# Normalizar número local com código de área
numero_fmt, valido = normalize_number("88888888", "11")
print(f"Número com DDD: {numero_fmt}")
# Saída: Número com DDD: 1188888888

# Normalizar par de números (origem e destino)
a_fmt, a_ok, b_fmt, b_ok = normalize_number_pair("11999999999", "88888888")
print(f"A: {a_fmt} ({a_ok}), B: {b_fmt} ({b_ok})")
# Saída: A: 11999999999 (True), B: 1188888888 (True)
```

---

<a id="extractors"></a>

### Extração de CDR (`robocalls.extractors`)

O módulo de extração fornece ferramentas para ler e padronizar arquivos CDR em múltiplos formatos de operadoras brasileiras. A extração é baseada em esquemas configuráveis que definem delimitador, índices de coluna e nomes finais.

**Classes Públicas:**

| Classe | Responsabilidade |
|--------|-----------------|
| `RoboCallsExtractor(spark)` | Extrai dados brutos de arquivos CDR em diferentes formatos |
| `CDRSchema(name, delimiter, has_header, column_indices, column_names, job_description)` | Define esquema de mapeamento de colunas para um formato CDR específico |

**Métodos Públicos de `RoboCallsExtractor`:**

| Método | Parâmetros | Retorno | Descrição |
|--------|-----------|---------|-----------|
| `extract_cdr_ericsson(source_file, target_file)` | `source_file` (str): Caminho CSV<br/>`target_file` (str): Diretório parquet saída | `DataFrame` | Extrai CDR formato Ericsson |
| `extract_cdr_tim_volte(source_file, target_file)` | `source_file` (str): Caminho CSV<br/>`target_file` (str): Diretório parquet saída | `DataFrame` | Extrai CDR formato TIM VoLTE |
| `extract_cdr_tim_stir(source_file, target_file)` | `source_file` (str): Caminho CSV<br/>`target_file` (str): Diretório parquet saída | `DataFrame` | Extrai CDR formato TIM STIR |
| `extract_cdr_vivo_volte(source_file, target_file)` | `source_file` (str): Caminho CSV<br/>`target_file` (str): Diretório parquet saída | `DataFrame` | Extrai CDR formato Vivo VoLTE |

**Exemplo de Uso:**

```python
from pyspark.sql import SparkSession
from teleutils.robocalls import RoboCallsExtractor

spark = SparkSession.builder.appName("Extract CDR").getOrCreate()
extractor = RoboCallsExtractor(spark)

# Extrair CDR TIM VoLTE
df = extractor.extract_cdr_tim_volte(
    source_file="raw_data/cdr_tim_volte.csv",
    target_file="processed/tim_volte_extracted"
)

# Explorar dados extraídos
print(df.printSchema())
print(df.count())  # Total de registros
df.show(5)
```

---

<a id="transformers"></a>

### Transformação de Dados (`robocalls.transformers`)

O módulo de transformação padroniza dados CDR brutos, normalizando números telefônicos, parseando timestamps, e enriquecendo registros com indicadores binários de padrão de abuso (chamadas curtas, caixa postal, autenticação).

**Classes Públicas:**

| Classe | Responsabilidade |
|--------|-----------------|
| `RoboCallsTransformer(spark, limiar_chamada_ofensora=6)` | Transforma CDR brutos para formato padronizado e enriquecido |

**Métodos Públicos de `RoboCallsTransformer`:**

| Método | Parâmetros | Retorno | Descrição |
|--------|-----------|---------|-----------|
| `transform_cdr_ericsson(source_file, target_file)` | `source_file` (str): Parquet extraído<br/>`target_file` (str): Diretório parquet saída | `DataFrame` | Transforma CDR Ericsson |
| `transform_cdr_tim_volte(source_file, target_file)` | `source_file` (str): Parquet extraído<br/>`target_file` (str): Diretório parquet saída | `DataFrame` | Transforma CDR TIM VoLTE |
| `transform_cdr_tim_stir(source_file, target_file)` | `source_file` (str): Parquet extraído<br/>`target_file` (str): Diretório parquet saída | `DataFrame` | Transforma CDR TIM STIR |
| `transform_cdr_vivo_volte(source_file, target_file)` | `source_file` (str): Parquet extraído<br/>`target_file` (str): Diretório parquet saída | `DataFrame` | Transforma CDR Vivo VoLTE |

**Schema de Saída da Transformação:**

Todos os transformadores retornam um DataFrame com as seguintes colunas padronizadas:

| Coluna | Tipo | Descrição | Valores |
|--------|------|-----------|---------|
| `referencia` | `string` | Identificador único da chamada no CDR | Variável conforme fonte |
| `tipo_de_chamada` | `string` | Classificação da chamada | TER, FORv, 3, 4, 82, etc. |
| `data_hora` | `timestamp` | Data e hora exata da chamada | ISO 8601 format |
| `numero_de_a_formatado` | `string` | Número originador padronizado | Formato CN+PREFIXO+MCDU ou como informado |
| `numero_de_b_formatado` | `string` | Número destinatário padronizado | Formato CN+PREFIXO+MCDU ou como informado |
| `hora_da_chamada` | `string` | Hora cheia (sem minutos/segundos) | Formato `YYYYMMDDHH` |
| `duracao_da_chamada` | `integer` | Duração total da chamada | Segundos (0 se null) |
| `chamada_curta` | `integer` | Flag indicador de duração curta | 0 = duração > limiar, 1 = duração ≤ limiar |
| `chamada_caixa_postal` | `integer` | Flag indicador de encaminhamento ao correio de voz | 0 = não encaminhado, 1 = encaminhado |
| `chamada_autenticada` | `integer` | Status da autenticação STIR/SHAKEN | -1 = falhou, 0 = não verificada, 1 = bem-sucedida |

**Exemplo de Uso:**

```python
from pyspark.sql import SparkSession
from teleutils.robocalls import RoboCallsTransformer

spark = SparkSession.builder.appName("Transform CDR").getOrCreate()
transformer = RoboCallsTransformer(
    spark,
    limiar_chamada_ofensora=6  # Classificar como curta se duração <= 6s
)

# Transformar CDR TIM VoLTE
df_transformed = transformer.transform_cdr_tim_volte(
    source_file="processed/tim_volte_extracted",
    target_file="processed/tim_volte_transformed"
)

# Explorar dados transformados
df_transformed.printSchema()
print(f"Total de registros: {df_transformed.count()}")

# Filtrar chamadas curtas
chamadas_curtas = df_transformed.filter("chamada_curta == 1")
print(f"Chamadas curtas: {chamadas_curtas.count()}")

# Mostrar amostra
df_transformed.show(5)
```

---

<a id="analyzers"></a>

### Análise de Padrões (`robocalls.analyzers`)

O módulo de análise agrupa recursos CDR por número originador e hora da chamada, computando métricas que indicam atividade abusiva (robocalls, campanhas agressivas de telemarketing).

**Classes Públicas:**

| Classe | Responsabilidade |
|--------|-----------------|
| `RoboCallsAnalyzer(spark)` | Analisa CDR transformados e identifica padrões abusivos |

**Métodos Públicos de `RoboCallsAnalyzer`:**

| Método | Parâmetros | Retorno | Descrição |
|--------|-----------|---------|-----------|
| `analyze(source_file, target_file="")` | `source_file` (str): Parquet transformado<br/>`target_file` (str): Diretório parquet saída | `DataFrame` | Agrupa por (numero_de_a_formatado, hora_da_chamada) e calcula métricas |

**Schema de Saída da Análise:**

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `numero_de_a_formatado` | `string` | Número originador (agregação) |
| `hora_da_chamada` | `string` | Hora cheia em formato `YYYYMMDDHH` (agregação) |
| `total_chamadas` | `long` | Contagem total de chamadas no grupo |
| `total_chamadas_curtas` | `long` | Contagem de chamadas com duração ≤ limiar |
| `total_chamadas_caixa_postal` | `long` | Contagem de chamadas encaminhadas ao correio de voz |
| `total_chamadas_autenticadas` | `long` | Contagem de chamadas com autenticação bem-sucedida (score = 1) |

**Exemplo de Uso:**

```python
from pyspark.sql import SparkSession
from teleutils.robocalls import RoboCallsAnalyzer

spark = SparkSession.builder.appName("Analyze CDR").getOrCreate()
analyzer = RoboCallsAnalyzer(spark)

# Analisar padrões
df_analyzed = analyzer.analyze(
    source_file="processed/tim_volte_transformed",
    target_file="analyzed/tim_volte_analyzed"
)

# Top 10 originadores mais agressivos (mais chamadas curtas)
df_analyzed.show(10)

# Filtrar padrões com alta concentração de chamadas curtas
padroes_suspeitos = df_analyzed.filter("total_chamadas_curtas >= 50")
print(f"Padrões suspeitos identificados: {padroes_suspeitos.count()}")

# Estatísticas por hora
df_analyzed.groupBy("hora_da_chamada") \
    .agg({"total_chamadas_curtas": "sum"}) \
    .sort("hora_da_chamada") \
    .show(24)
```

---

## Contribuição

### Configuração do Ambiente de Desenvolvimento

1. Clone o repositório:
   ```bash
   git clone https://github.com/InovaFiscaliza/teleutils.git
   cd teleutils
   ```

2. Sincronize dependências com `uv`:
   ```bash
   uv sync
   ```

3. Ative o ambiente virtual:
   ```bash
   source .venv/bin/activate
   ```

4. Instale hooks de pré-commit:
   ```bash
   pre-commit install
   ```

### Padrões de Desenvolvimento

- **Commits**: Siga [Conventional Commits](https://www.conventionalcommits.org/) com [gitmoji](https://gitmoji.dev/)
  - Exemplos: `✨ feat: adicionar novo formato de CDR`, `🐛 fix: corrigir parsing de datas`
- **Código**: Respeite linting com `ruff` e formatação com `black`
- **Testes**: Escreva testes para novas funcionalidades
- **Documentação**: Mantenha docstrings em Google Style e atualize este README quando necessário

### Executando Testes

```bash
# Executar todos os testes
pytest tests/

# Com cobertura
pytest --cov=teleutils tests/

# Testes específicos
pytest tests/robocalls/
```

---

## Licença

TeleUtils é licenciado sob **GNU General Public License v3.0**. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

---

## Referências

- [ANATEL — Agência Nacional de Telecomunicações](https://www.anatel.gov.br/)
- [ITU-T E.164 — Numbering Plan for the ISDN Era](https://handle.itu.int/11.1002/1000/10688)
- [Apache Spark Documentation](https://spark.apache.org/documentation.html)
- [Conventional Commits](https://www.conventionalcommits.org/)

---

**Desenvolvido por:** [InovaFiscaliza](https://github.com/InovaFiscaliza)
**Última atualização:** Março 2026
