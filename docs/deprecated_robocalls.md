# Documentação da Linhagem e Transformação dos Dados

## 1. Visão geral do fluxo

Esta documentação descreve exclusivamente o pipeline implementado em `teleutils.robocalls` e as dependências internas importadas diretamente por ele que afetam seu comportamento: `teleutils.preprocessing.normalize_number` e `teleutils._logging.log_operation`.

O pipeline possui três camadas persistidas:

```text
CSV CDR de origem
→ RoboCallsExtractor._extract_cdr
→ Parquet extraído, particionado por tipo_de_chamada
→ transform_cdr_<formato>
→ Parquet transformado, com 10 colunas padronizadas
→ RoboCallsAnalyzer.analyze
→ Parquet analítico final, com 8 colunas agregadas
```

Formatos de origem implementados:

- Ericsson;
- TIM VoLTE;
- Vivo VoLTE;
- Claro Nokia.

O resultado terminal do pipeline é o parquet gravado por `RoboCallsAnalyzer.analyze`. Os parquets transformados também são resultados persistidos e constituem a entrada direta da análise; por isso, suas 10 colunas são documentadas antes das 8 colunas analíticas finais.

### Inventário independente das colunas

**Parquet transformado, na ordem de `_TRANSFORMED_COLUMNS`:**

1. `referencia`;
2. `tipo_de_chamada`;
3. `data_hora`;
4. `numero_de_a_formatado`;
5. `numero_de_b_formatado`;
6. `hora_da_chamada`;
7. `duracao_da_chamada`;
8. `chamada_curta`;
9. `chamada_autenticada`;
10. `chamada_caixa_postal`.

**Parquet analítico final, conforme `groupBy` e aliases de `RoboCallsAnalyzer.analyze`:**

1. `numero_de_a_formatado`;
2. `hora_da_chamada`;
3. `total_chamadas`;
4. `total_chamadas_curtas`;
5. `total_chamadas_caixa_postal`;
6. `total_chamadas_autenticadas`;
7. `total_chamadas_curtas_autenticadas`;
8. `total_chamadas_caixa_postal_autenticadas`.

## 2. Convenções utilizadas

| Nível | Descrição |
| :--- | :--- |
| Colunas Originais | Campos presentes no CSV antes da seleção. Quando o código não fixa o cabeçalho, são identificados pelo índice zero-based. |
| Colunas Extraídas | Campos selecionados e renomeados por `_extract_cdr`, persistidos no primeiro parquet. |
| Coluna Intermediária | Campo criado ou alterado durante transformação, join, filtro ou agregação. |
| Coluna Final Transformada | Campo selecionado por `_TRANSFORMED_COLUMNS` e gravado por `_write_parquet`. |
| Coluna Final Analítica | Campo gravado pelo método `RoboCallsAnalyzer.analyze`. |

Classificações usadas:

- **Renomeação:** apenas troca de nome por `toDF`, sem alteração do valor.
- **Cópia:** seleção, chave de agrupamento ou alias sem alteração relevante do conteúdo.
- **Transformação:** cálculo, parsing, normalização, condição, agregação ou mudança de tipo/conteúdo.
- **Valor constante:** criação sem coluna de origem.
- **Filtro/controle de população:** muda quais linhas chegam à saída, não o valor de uma coluna individual.

A leitura CSV usa `inferSchema=False`; portanto, as colunas extraídas são lidas como strings, salvo o schema explícito da TIM VoLTE, que também declara todas como `StringType`.

## 3. Transformações por formato de origem

### 3.1 `extract_cdr_ericsson` / `transform_cdr_ericsson`

#### Descrição

Lê CSV com `;` e cabeçalho, seleciona sete posições, mantém somente chamadas `TER`, aplica o pipeline comum e cria os indicadores de autenticação e caixa postal com valor constante zero.

#### Fluxo resumido

```text
CSV Ericsson com cabeçalho
→ índices [0, 1, 2, 3, 4, 9, 11]
→ renomeação para schema extraído
→ parquet particionado por tipo_de_chamada
→ filtro tipo_de_chamada = "TER"
→ formatação de data/hora, duração e números
→ indicadores
→ seleção das 10 colunas
→ cast final de tipo_de_chamada para string
→ parquet transformado
```

#### Tabela de mapeamento

| Colunas Originais | Colunas Extraídas | Coluna Intermediária | Coluna Final Transformada |
| :--- | :--- | :--- | :--- |
| índice 0, nome do cabeçalho conhecido somente em execução | `referencia` | Mantida sem alteração | `referencia` |
| índice 4, nome do cabeçalho conhecido somente em execução | `tipo_de_chamada` | filtro `== "TER"`; cast para string na escrita | `tipo_de_chamada` |
| índices 2 e 3 | `_data`, `_hora` | concatenação em `data_hora`; parsing `yyyy-MM-dd HH:mm:ss` | `data_hora` |
| índice 1 | `numero_de_a` | `_spark_normalize_number(...).numero_formatado` | `numero_de_a_formatado` |
| índice 9 | `numero_de_b` | `_spark_normalize_number(...).numero_formatado` | `numero_de_b_formatado` |
| índices 2 e 3 | `_data`, `_hora` | `data_hora` → `date_format("yyyyMMddHH")` | `hora_da_chamada` |
| índice 11 | `duracao_da_chamada` | cast para inteiro; nulo convertido em `0` | `duracao_da_chamada` |
| índice 11 | `duracao_da_chamada` | comparação com `limiar_chamada_ofensora` | `chamada_curta` |
| Não aplicável | Não aplicável | constante `0` | `chamada_autenticada` |
| Não aplicável | Não aplicável | constante `0` | `chamada_caixa_postal` |

#### Transformações detalhadas por coluna

##### `referencia`

**Linhagem:** índice original 0 → seleção → renomeação para `referencia` → seleção final. A única alteração é a **renomeação** na extração.

##### `tipo_de_chamada`

**Linhagem:** índice original 4 → renomeação para `tipo_de_chamada` → filtro `TER` → cast `StringType` → coluna final. O cast é **transformação de tipo**; o filtro controla a população.

##### `data_hora`

**Linhagem:** índices 2 e 3 → `_data` e `_hora` → `concat_ws(" ", _data, _hora)` → `to_timestamp(..., "yyyy-MM-dd HH:mm:ss")`. É uma **transformação** de duas strings em timestamp.

##### `numero_de_a_formatado`

**Linhagem:** índice 1 → `numero_de_a` → `_spark_normalize_number` → campo estruturado `numero_formatado`. É uma **transformação**, detalhada em “Funções auxiliares comuns”.

##### `numero_de_b_formatado`

**Linhagem:** índice 9 → `numero_de_b` → `_spark_normalize_number` → campo estruturado `numero_formatado`. É uma **transformação**.

##### `hora_da_chamada`

**Linhagem:** `_data` + `_hora` → `data_hora` timestamp → `date_format(data_hora, "yyyyMMddHH")`. É uma **transformação** para string de hora cheia.

##### `duracao_da_chamada`

**Linhagem:** índice 11 → coluna extraída homônima → cast para inteiro → `coalesce(..., 0)`. É uma **transformação de tipo e tratamento de nulo**.

##### `chamada_curta`

**Linhagem:** `duracao_da_chamada` já convertida → `1` quando duração `<= limiar_chamada_ofensora`, senão `0`. É uma **transformação condicional**.

##### `chamada_autenticada`

**Linhagem:** sem coluna original → `lit(0)`. É **valor constante**.

##### `chamada_caixa_postal`

**Linhagem:** sem coluna original → `lit(0)`. É **valor constante**.

#### Funções auxiliares específicas

Não há função auxiliar exclusiva do fluxo Ericsson. Ele usa `_extract_cdr`, `_apply_standard_pipeline` e `_write_parquet`, documentadas como auxiliares comuns.

### 3.2 `extract_cdr_tim_volte` / `transform_cdr_tim_volte`

#### Descrição

Lê CSV sem cabeçalho com `;` e schema `_c0` a `_c16`, remove a linha cujo `tipo_de_chamada` contém o texto de cabeçalho, separa registros principais `TERv` de evidências de caixa postal `FORv` e os relaciona por `referencia`.

#### Fluxo resumido

```text
CSV TIM VoLTE
→ índices [0, 1, 2, 3, 4, 7, 12, 16]
→ renomeação e exclusão da linha de cabeçalho lógico
→ parquet extraído
→ caminho principal TERv + caminho auxiliar FORv
→ pipeline comum + autenticação
→ left join por referencia
→ seleção final e escrita
```

#### Tabela de mapeamento

| Colunas Originais | Colunas Extraídas | Coluna Intermediária | Coluna Final Transformada |
| :--- | :--- | :--- | :--- |
| `_c12` (índice 12) | `referencia` | chave do join com `FORv` | `referencia` |
| `_c3` (índice 3) | `tipo_de_chamada` | filtro `TERv`; cast para string | `tipo_de_chamada` |
| `_c1`, `_c2` | `_data`, `_hora` | concatenação e parsing padrão `yyyy-MM-dd HH-mm-ss` | `data_hora` |
| `_c0` | `numero_de_a` | normalização | `numero_de_a_formatado` |
| `_c4` | `numero_de_b` | normalização | `numero_de_b_formatado` |
| `_c1`, `_c2` | `_data`, `_hora` | `data_hora` → `yyyyMMddHH` | `hora_da_chamada` |
| `_c7` | `duracao_da_chamada` | cast inteiro e nulo → `0` | `duracao_da_chamada` |
| `_c7` | `duracao_da_chamada` | comparação com limiar | `chamada_curta` |
| `_c16` | `autenticacao` | classificação em `-1`, `0` ou `1` | `chamada_autenticada` |
| `_c12`, `_c3`, `_c4` em registros `FORv` | `referencia`, `tipo_de_chamada`, `numero_de_b` | evidência distinta por referência; left join; nulo → `0` | `chamada_caixa_postal` |

#### Transformações detalhadas por coluna

##### `referencia`

**Linhagem:** `_c12` → renomeação para `referencia` → chave da chamada `TERv` e do left join com referências `FORv` distintas → coluna final. O valor é uma **renomeação/cópia**; o join pode alterar a multiplicidade conforme registrado nas ambiguidades.

##### `tipo_de_chamada`

**Linhagem:** `_c3` → renomeação → exclusão da linha igual a `TipodeCDR(role-of-Node)` → filtro do caminho final `TERv` → cast para string. Há filtro e **transformação de tipo**, não renomeação final.

##### `data_hora`

**Linhagem:** `_c1` + `_c2` → `_data` + `_hora` → concatenação com espaço → `to_timestamp` no formato padrão `yyyy-MM-dd HH-mm-ss`. É **transformação**.

##### `numero_de_a_formatado`

**Linhagem:** `_c0` → `numero_de_a` → normalização → `numero_de_a_formatado`. É **transformação**.

##### `numero_de_b_formatado`

**Linhagem:** `_c4` do registro `TERv` → `numero_de_b` → normalização → `numero_de_b_formatado`. É **transformação**. O `_c4` de registros `FORv` participa somente da detecção de caixa postal.

##### `hora_da_chamada`

**Linhagem:** `_c1` + `_c2` → `data_hora` → `date_format(..., "yyyyMMddHH")`. É **transformação**.

##### `duracao_da_chamada`

**Linhagem:** `_c7` → renomeação homônima → cast inteiro → nulo convertido em zero. É **transformação**.

##### `chamada_curta`

**Linhagem:** `_c7` → `duracao_da_chamada` inteira → comparação `<= limiar_chamada_ofensora` → `1` ou `0`. É **transformação condicional**.

##### `chamada_autenticada`

**Linhagem:** `_c16` → `autenticacao` → `0` se nulo; `1` se contém `TN-Validation-Pa`; `-1` para qualquer outro valor não nulo. É **transformação condicional**.

##### `chamada_caixa_postal`

**Linhagem:** registros auxiliares em que `_c3`/`tipo_de_chamada == "FORv"` e `_c4`/`numero_de_b` começa com `5505` e possui comprimento 6 → seleção distinta de `_c12`/`referencia` → constante `1` → left join com `TERv` por `referencia` → nulo convertido em `0`. É **transformação por filtro, existência e join**.

#### Funções auxiliares específicas

A detecção `df_voice_mail` é implementada dentro de `transform_cdr_tim_volte`, sem função separada. Entradas: `tipo_de_chamada`, `numero_de_b` e `referencia`; saída: pares distintos de `referencia` e `chamada_caixa_postal = 1`.

### 3.3 `extract_cdr_vivo_volte` / `transform_cdr_vivo_volte`

#### Descrição

Lê CSV sem cabeçalho delimitado por `|`. Usa registros tipo `4` como caminho principal e registros tipo `3` como evidência de caixa postal. No caminho principal, divide o campo original do número A por `;`: o primeiro item torna-se o número A e o segundo torna-se autenticação.

#### Fluxo resumido

```text
CSV Vivo VoLTE
→ índices [0, 2, 5, 12, 13, 31, 45]
→ parquet extraído
→ caminho principal tipo 4: split de numero_de_a
→ caminho auxiliar tipo 3: últimos 11 dígitos e igualdade A = B
→ formatação comum nos dois caminhos necessários
→ left join por referencia, data_hora e numero_de_b_formatado
→ seleção final e escrita
```

#### Tabela de mapeamento

| Colunas Originais | Colunas Extraídas | Coluna Intermediária | Coluna Final Transformada |
| :--- | :--- | :--- | :--- |
| `_c45` (índice 45) | `referencia` | chave de join | `referencia` |
| `_c0` | `tipo_de_chamada` | filtro do caminho principal `4`; cast string | `tipo_de_chamada` |
| `_c13`, `_c31` | `_data`, `_hora` | concatenação; parsing `yyyyMMdd HHmmss` | `data_hora` |
| `_c2` | `numero_de_a` | item 0 do split `;`; normalização | `numero_de_a_formatado` |
| `_c5` | `numero_de_b` | normalização | `numero_de_b_formatado` |
| `_c13`, `_c31` | `_data`, `_hora` | `data_hora` → `yyyyMMddHH` | `hora_da_chamada` |
| `_c12` | `duracao_da_chamada` | cast inteiro e nulo → `0` | `duracao_da_chamada` |
| `_c12` | `duracao_da_chamada` | comparação com limiar | `chamada_curta` |
| `_c2` | `numero_de_a` | item 1 do split `;` → `autenticacao` → classificação | `chamada_autenticada` |
| `_c45`, `_c13`, `_c31`, `_c2`, `_c5` em registros tipo `3` | campos extraídos correspondentes | últimos 11 dígitos, igualdade A/B, constante 1 e join | `chamada_caixa_postal` |

#### Transformações detalhadas por coluna

##### `referencia`

**Linhagem:** `_c45` → renomeação para `referencia` → chave de join entre tipo `4` e tipo `3` → coluna final. O conteúdo é **renomeado/copiado**.

##### `tipo_de_chamada`

**Linhagem:** `_c0` → `tipo_de_chamada` → filtro `== "4"` no caminho principal → cast string → final. O filtro controla a população e o cast é **transformação de tipo**.

##### `data_hora`

**Linhagem:** `_c13` + `_c31` → `_data` + `_hora` → concatenação → `to_timestamp(..., "yyyyMMdd HHmmss")`. A mesma transformação ocorre no caminho auxiliar tipo `3` para formar uma chave de join. É **transformação**.

##### `numero_de_a_formatado`

**Linhagem principal:** `_c2` → `numero_de_a` → item 0 de `split(";")` → normalização → final. São duas **transformações** sucessivas.

No caminho auxiliar de caixa postal, `_c2` é reduzido diretamente aos últimos 11 caracteres com `substr(-11, 11)`; essa intermediária não é levada como número A final.

##### `numero_de_b_formatado`

**Linhagem principal:** `_c5` → `numero_de_b` → normalização → final.

No caminho auxiliar, `_c5` → últimos 11 caracteres; esse valor participa da comparação A/B e da chave de join, mas o valor final continua sendo o normalizado do registro tipo `4`.

##### `hora_da_chamada`

**Linhagem:** `_c13` + `_c31` → `data_hora` → `date_format(..., "yyyyMMddHH")`. É **transformação**.

##### `duracao_da_chamada`

**Linhagem:** `_c12` → coluna extraída homônima → cast inteiro → nulo convertido em zero. É **transformação**.

##### `chamada_curta`

**Linhagem:** `_c12` → duração inteira → comparação com o limiar → `1` ou `0`. É **transformação condicional**.

##### `chamada_autenticada`

**Linhagem:** `_c2` → `numero_de_a` → item 1 de `split(";")` → `autenticacao` → `0` se nulo, `1` se contém `TN-Validation-Pa`, senão `-1`. É **transformação de extração e classificação**.

##### `chamada_caixa_postal`

**Linhagem:** registros tipo `3` → últimos 11 caracteres de `_c2` e `_c5` → filtro de igualdade → constante `1` → formatação de `data_hora` → seleção de `referencia`, `data_hora`, `numero_de_b_formatado` e flag → left join com tipo `4` pelas três primeiras colunas → nulo convertido em `0`. É **transformação por heurística e join**.

#### Funções auxiliares específicas

Os caminhos de split de autenticação e de detecção `df_voice_mail` estão implementados diretamente em `transform_cdr_vivo_volte`. Não existe função separada específica.

### 3.4 `extract_cdr_claro_nokia` / `transform_cdr_claro_nokia`

#### Descrição

Lê CSV com `;` e cabeçalho. Combina dois caminhos para a população principal: tipos `MTC`, `UCA`, `FOR` e `MOC`; e registros `POC` que não encontrem correspondência `PTC` por `referencia` e `numero_de_a`. Registros `FOR` também fornecem a evidência de caixa postal.

#### Fluxo resumido

```text
CSV Claro Nokia com cabeçalho
→ índices [0, 2, 3, 7, 8, 13, 15]
→ parquet extraído
→ PTC como conjunto de exclusão
→ POC sem PTC
→ union com MTC/UCA/FOR/MOC
→ deduplicação por referencia e numero_de_a
→ pipeline comum
→ join com evidências FOR
→ autenticação constante 0
→ seleção final e escrita
```

#### Tabela de mapeamento

| Colunas Originais | Colunas Extraídas | Coluna Intermediária | Coluna Final Transformada |
| :--- | :--- | :--- | :--- |
| índice 2, cabeçalho conhecido em execução | `referencia` | chaves de joins e deduplicação | `referencia` |
| índice 0 | `tipo_de_chamada` | filtros, union; cast string | `tipo_de_chamada` |
| índice 3 | `data_hora` | parsing `yyyy-MM-dd HH:mm:ss` | `data_hora` |
| índice 7 | `numero_de_a` | chaves auxiliares; normalização | `numero_de_a_formatado` |
| índice 8 | `numero_de_b` | normalização | `numero_de_b_formatado` |
| índice 3 | `data_hora` | timestamp → `yyyyMMddHH` | `hora_da_chamada` |
| índice 15 | `duracao_da_chamada` | cast inteiro e nulo → `0` | `duracao_da_chamada` |
| índice 15 | `duracao_da_chamada` | comparação com limiar | `chamada_curta` |
| Não aplicável | Não aplicável | constante `0` | `chamada_autenticada` |
| índices 0, 2 e 7 nos registros `FOR` | `tipo_de_chamada`, `referencia`, `numero_de_a` | constante 1 e join por referência/número A | `chamada_caixa_postal` |

O índice 13 é extraído como `numero_conectado`, mas não participa de nenhuma coluna transformada e é eliminado pela seleção final.

#### Transformações detalhadas por coluna

##### `referencia`

**Linhagem:** índice 2 → renomeação para `referencia` → chave nos caminhos PTC/POC, deduplicação e join de caixa postal → final. O valor é **renomeado/copiado**.

##### `tipo_de_chamada`

**Linhagem:** índice 0 → renomeação → seleção por múltiplos caminhos (`MTC`, `UCA`, `FOR`, `MOC` ou `POC` sem PTC correspondente) → cast string → final. O cast é **transformação de tipo**; filtros e union controlam a população.

##### `data_hora`

**Linhagem:** índice 3 → renomeação direta para `data_hora` → como a coluna já existe, não há concatenação de `_data`/`_hora` → `to_timestamp(..., "yyyy-MM-dd HH:mm:ss")`. É **renomeação na extração**, seguida de **transformação de tipo**.

##### `numero_de_a_formatado`

**Linhagem:** índice 7 → `numero_de_a` → participação nas chaves e deduplicação → normalização → final. É **transformação**.

##### `numero_de_b_formatado`

**Linhagem:** índice 8 → `numero_de_b` → normalização → final. É **transformação**.

##### `hora_da_chamada`

**Linhagem:** índice 3 → `data_hora` timestamp → `date_format(..., "yyyyMMddHH")`. É **transformação**.

##### `duracao_da_chamada`

**Linhagem:** índice 15 → coluna extraída homônima → cast inteiro → nulo convertido em zero. É **transformação**.

##### `chamada_curta`

**Linhagem:** índice 15 → duração inteira → comparação com o limiar → `1` ou `0`. É **transformação condicional**.

##### `chamada_autenticada`

**Linhagem:** sem coluna original → `lit(0)`. É **valor constante**.

##### `chamada_caixa_postal`

**Linhagem:** registros cujo índice 0/`tipo_de_chamada == "FOR"` → índice 2/`referencia` + índice 7/`numero_de_a` → constante `1` → left join com a população principal por essas duas colunas → nulo convertido em `0`. É **transformação por existência e join**.

#### Funções auxiliares específicas

Os DataFrames `df_ptc`, `df_poc_without_ptc` e `df_voice_mail` são estruturas intermediárias locais de `transform_cdr_claro_nokia`; não são funções separadas.

- `df_ptc`: chaves `referencia`/`numero_de_a` de registros `PTC`, com `chamada_ptc = 1`.
- `df_poc_without_ptc`: registros `POC` sem chave correspondente em `df_ptc`.
- `df_voice_mail`: chaves de registros `FOR`, com `chamada_caixa_postal = 1`.

## 4. Múltiplos caminhos para colunas finais

### `chamada_autenticada`

```text
Ericsson ou Claro Nokia:
sem coluna de origem → constante 0

TIM VoLTE:
índice 16 → autenticacao → classificação por nulo/conteúdo/outro

Vivo VoLTE:
índice 2 → segundo item após split por ";" → autenticacao → classificação
```

### `chamada_caixa_postal`

```text
Ericsson:
sem coluna de origem → constante 0

TIM VoLTE:
FORv + numero_de_b no padrão 5505xx → referencia → join com TERv → 1/0

Vivo VoLTE:
tipo 3 + últimos 11 dígitos de A iguais aos de B
→ join com tipo 4 por referencia/data_hora/B formatado → 1/0

Claro Nokia:
registro FOR → referencia + numero_de_a → join com população principal → 1/0
```

### População que alimenta o resultado

```text
Ericsson: somente TER
TIM VoLTE: somente TERv; FORv apenas enriquece caixa postal
Vivo VoLTE: somente tipo 4; tipo 3 apenas enriquece caixa postal
Claro Nokia: MTC/UCA/FOR/MOC, mais POC sem PTC correspondente
```

As demais colunas transformadas seguem os mesmos tipos de operações, mas partem das posições próprias de cada layout, registradas nas tabelas anteriores.

## 5. Etapa final de geração do Parquet

### 5.1 Parquet transformado

`RoboCallsTransformer._write_parquet` recebe o DataFrame já limitado por `.select(self._TRANSFORMED_COLUMNS)`, converte `tipo_de_chamada` para `StringType` e grava com modo `overwrite`, sem particionamento explícito. Cada método público relê o parquet e retorna essa releitura.

| Coluna anterior | Operação final | Coluna final transformada |
| :--- | :--- | :--- |
| `referencia` | cópia por seleção | `referencia` |
| `tipo_de_chamada` | cast para string | `tipo_de_chamada` |
| `data_hora` | cópia por seleção | `data_hora` |
| `numero_de_a_formatado` | cópia por seleção | `numero_de_a_formatado` |
| `numero_de_b_formatado` | cópia por seleção | `numero_de_b_formatado` |
| `hora_da_chamada` | cópia por seleção | `hora_da_chamada` |
| `duracao_da_chamada` | cópia por seleção | `duracao_da_chamada` |
| `chamada_curta` | cópia por seleção | `chamada_curta` |
| `chamada_autenticada` | cópia por seleção | `chamada_autenticada` |
| `chamada_caixa_postal` | cópia por seleção | `chamada_caixa_postal` |

Schema determinável estaticamente após as expressões Spark:

| Coluna | Tipo esperado pelo código |
| :--- | :--- |
| `referencia` | string |
| `tipo_de_chamada` | string, por cast explícito |
| `data_hora` | timestamp |
| `numero_de_a_formatado` | string |
| `numero_de_b_formatado` | string |
| `hora_da_chamada` | string |
| `duracao_da_chamada` | integer |
| `chamada_curta` | integer |
| `chamada_autenticada` | integer |
| `chamada_caixa_postal` | integer |

### 5.2 Parquet analítico final: `RoboCallsAnalyzer.analyze`

O método lê um parquet transformado, agrupa por `numero_de_a_formatado` e `hora_da_chamada`, calcula seis métricas, ordena por `total_chamadas_curtas` decrescente, grava com modo `overwrite` e retorna uma releitura do parquet gravado.

| Coluna anterior | Classificação e operação | Coluna final analítica |
| :--- | :--- | :--- |
| `numero_de_a_formatado` | cópia como chave de agrupamento | `numero_de_a_formatado` |
| `hora_da_chamada` | cópia como chave de agrupamento | `hora_da_chamada` |
| `referencia` | transformação: `count(referencia)` | `total_chamadas` |
| `chamada_curta` | transformação: `sum(chamada_curta)` | `total_chamadas_curtas` |
| `chamada_curta`, `chamada_caixa_postal` | transformação: soma de caixa postal apenas quando chamada curta = 0 | `total_chamadas_caixa_postal` |
| `chamada_autenticada` | transformação: soma do cast inteiro da condição `== 1` | `total_chamadas_autenticadas` |
| `chamada_autenticada`, `chamada_curta` | transformação: soma de chamada curta apenas quando autenticação = 1 | `total_chamadas_curtas_autenticadas` |
| `chamada_autenticada`, `chamada_curta`, `chamada_caixa_postal` | transformação: soma de caixa postal quando autenticação = 1 e chamada curta = 0 | `total_chamadas_caixa_postal_autenticadas` |

Não há renomeação por `withColumnRenamed` nessa etapa. Os seis nomes de métricas são **aliases de resultados agregados**, portanto representam transformações, não simples renomeações.

#### Transformações detalhadas das colunas analíticas finais

##### `numero_de_a_formatado`

**Origem:** `numero_de_a_formatado` do parquet transformado, cuja origem específica por formato está nas seções 3.1 a 3.4.

**Linhagem:** número original → extração como `numero_de_a` → `normalize_number` → chave de agrupamento → coluna analítica final. A passagem final é **cópia**.

##### `hora_da_chamada`

**Origem:** `_data`/`_hora` ou `data_hora`, conforme formato.

**Linhagem:** campos temporais originais → timestamp `data_hora` → string `yyyyMMddHH` → chave de agrupamento → final. A criação é **transformação**; a passagem pelo agrupamento é **cópia**.

##### `total_chamadas`

**Origem:** `referencia` dos registros de cada grupo.

**Linhagem:** posição original de referência → renomeação para `referencia` → `count(referencia)` → alias `total_chamadas`. É **agregação**. `count` ignora referências nulas.

##### `total_chamadas_curtas`

**Origem:** duração original.

**Linhagem:** duração original → cast inteiro/nulo para zero → `chamada_curta` (`1` se duração `<=` limiar; senão `0`) → `sum` → alias. É **transformação condicional seguida de agregação**.

##### `total_chamadas_caixa_postal`

**Origem:** depende do formato, conforme os quatro caminhos de `chamada_caixa_postal` na seção 4.

**Linhagem:** evidência/constante de caixa postal → flag `chamada_caixa_postal` → mantém a flag somente quando `chamada_curta == 0`, senão zero → soma → alias. Chamadas curtas são explicitamente excluídas desta métrica.

##### `total_chamadas_autenticadas`

**Origem:** constante zero em Ericsson/Claro; campo de autenticação em TIM/Vivo.

**Linhagem:** `chamada_autenticada` → comparação `== 1` → cast da condição para inteiro → soma → alias. Somente o estado de sucesso é contado.

##### `total_chamadas_curtas_autenticadas`

**Origem:** duração e autenticação pelos caminhos específicos do formato.

**Linhagem:** `chamada_autenticada == 1` → mantém `chamada_curta`, senão zero → soma → alias. É **transformação condicional e agregação**.

##### `total_chamadas_caixa_postal_autenticadas`

**Origem:** autenticação, duração e evidência de caixa postal.

**Linhagem:** quando `chamada_autenticada == 1` e `chamada_curta == 0`, mantém `chamada_caixa_postal`; nos demais casos usa zero → soma → alias. É **transformação condicional e agregação**.

A ordenação decrescente por `total_chamadas_curtas` muda apenas a ordem das linhas, não o schema nem o conteúdo das colunas.

## 6. Funções auxiliares específicas

As estruturas específicas de TIM, Vivo e Claro estão documentadas junto aos respectivos fluxos. Não existem métodos auxiliares exclusivos adicionais: a lógica específica está dentro dos quatro métodos `transform_cdr_*`.

# Funções auxiliares comuns

## Funções em nível de módulo

### `_spark_normalize_number`

**Localização:** `teleutils.robocalls.transformers`.

**Finalidade:** adaptar `normalize_number` para execução vetorizada como pandas UDF Spark.

**Entradas:** série pandas originada de `numero_de_a` ou `numero_de_b`.

**Saídas:** struct com `numero_formatado: string` e `numero_valido: boolean`. O pipeline seleciona somente `numero_formatado`; `numero_valido` é intermediário descartado.

**Colunas impactadas:** `numero_de_a_formatado`, `numero_de_b_formatado`.

**Fluxos:** todos os quatro formatos.

### `normalize_number`

**Localização:** `teleutils.preprocessing.number_format`, importada por `teleutils.preprocessing`.

**Finalidade e transformação:** para cada valor:

1. se o valor for falsy, retorna o sentinela `("5599999999999", False)`;
2. converte para string;
3. se houver `;`, mantém somente o primeiro segmento;
4. converte para minúsculas e remove o caractere `f`;
5. cria uma versão limpa removendo letras ASCII, pontuação e espaços;
6. remove no início os prefixos `90`/`9090`, `00` ou `0`;
7. valida por `E164_FULL_NUMBERS` quando restam pelo menos 10 dígitos, ou por `SMALL_NUMBERS` nos demais casos;
8. com exatamente um match, retorna o grupo numérico validado;
9. sem exatamente um match, retorna a string do passo 4, e não a versão totalmente limpa.

O pipeline de robocalls não fornece `national_destination_code`, portanto números locais não recebem DDD por contexto nesse fluxo.

**Colunas impactadas:** `numero_de_a_formatado`, `numero_de_b_formatado`.

**Fluxos:** todos os quatro formatos.

### `log_operation`

**Localização:** `teleutils._logging`.

**Finalidade:** registrar início, sucesso e exceção dos métodos públicos decorados, preservando seu retorno.

**Entradas/saídas:** envolve chamadas de extração e transformação e devolve o resultado do método original.

**Colunas impactadas:** nenhuma. Não transforma DataFrames nem altera a linhagem.

**Fluxos:** todos os métodos públicos de extração e transformação.

## Métodos auxiliares em classes

### `RoboCallsExtractor._extract_cdr`

**Finalidade:** leitura CSV, validação dos índices, seleção posicional, renomeação por `toDF`, filtro opcional e escrita do parquet extraído.

**Entradas:** `source_file`, `target_file`, `CDRSchema`.

**Saída:** releitura do parquet extraído, particionado por `tipo_de_chamada`.

**Colunas impactadas:** todas as colunas extraídas de cada formato.

### `CDRSchema.__post_init__`

**Finalidade:** validar tipos, filtro, cardinalidade entre índices e nomes, índices negativos/vazios e compatibilidade com schema explícito.

**Colunas impactadas:** nenhuma em tempo de transformação; impede configurações estruturalmente inválidas.

### `RoboCallsTransformer._format_columns`

**Finalidade:** criar `data_hora` quando ausente, converter duração para inteiro com zero para nulo, parsear timestamp e derivar `hora_da_chamada`.

**Entradas:** DataFrame e formato de data/hora.

**Saídas/colunas impactadas:** `duracao_da_chamada`, `data_hora`, `hora_da_chamada`.

### `RoboCallsTransformer._format_numbers`

**Finalidade:** aplicar `_spark_normalize_number` separadamente a A e B.

**Entradas:** `numero_de_a`, `numero_de_b`.

**Saídas:** `numero_de_a_formatado`, `numero_de_b_formatado`.

### `RoboCallsTransformer._add_chamada_curta`

**Finalidade:** comparar a duração já convertida com `self.limiar_chamada_ofensora`.

**Entrada:** `duracao_da_chamada`.

**Saída:** `chamada_curta` inteira, `1` ou `0`.

### `RoboCallsTransformer._add_chamada_autenticada`

**Finalidade:** classificar autenticação.

**Entrada:** `autenticacao`.

**Saída:** `chamada_autenticada`: `0` para nulo, `1` quando contém `TN-Validation-Pa`, `-1` nos demais valores não nulos.

**Fluxos:** TIM VoLTE e Vivo VoLTE.

### `RoboCallsTransformer._apply_standard_pipeline`

**Finalidade:** encadear `_format_columns`, `_format_numbers` e `_add_chamada_curta`, nessa ordem.

**Fluxos:** todos os quatro formatos.

### `RoboCallsTransformer._write_parquet`

**Finalidade:** converter `tipo_de_chamada` para string e gravar o parquet transformado em modo `overwrite`.

**Colunas impactadas:** somente o tipo de `tipo_de_chamada`; as demais são copiadas para escrita.

### `RoboCallsAnalyzer.analyze`

**Finalidade:** agregar o parquet transformado por número A formatado e hora, ordenar, gravar e reler o parquet analítico.

**Entradas:** as seis colunas transformadas efetivamente referenciadas: `numero_de_a_formatado`, `hora_da_chamada`, `referencia`, `chamada_curta`, `chamada_caixa_postal`, `chamada_autenticada`.

**Saídas:** as oito colunas analíticas inventariadas na seção 1.

# 8. Ambiguidades

### Nomes físicos de colunas com cabeçalho

- **Elemento afetado:** colunas originais Ericsson e Claro Nokia.
- **Interrupção:** antes da seleção posicional no CSV.
- **Motivo:** com `header=True`, o código usa os nomes encontrados no próprio arquivo e seleciona por índice; os nomes literais do cabeçalho não estão declarados no repositório analisado.
- **Evidência:** `columns_to_keep = [df.columns[i] ...]`.
- **Conclusão:** a posição e o nome extraído são seguros; o nome físico original não pode ser determinado estaticamente.

> ⚠️ Não foi possível determinar os nomes de cabeçalho originais de Ericsson e Claro Nokia com segurança apenas pela análise estática do código.

### Formato temporal TIM VoLTE

- **Elemento afetado:** `data_hora` e `hora_da_chamada`.
- **Ponto:** `_apply_standard_pipeline` é chamado sem formato explícito.
- **Evidência:** aplica-se o padrão literal `yyyy-MM-dd HH-mm-ss`, com hífens entre hora, minuto e segundo.
- **Ambiguidade:** sem amostra de entrada não é possível confirmar se os valores seguem esse padrão ou se o parsing produz nulos. A documentação registra o formato implementado, não um formato presumido.

### Correspondência de caixa postal Vivo VoLTE

- **Elemento afetado:** `chamada_caixa_postal`.
- **Ponto:** chave `numero_de_b_formatado` do join.
- **Motivo:** no caminho tipo `3`, o campo é `substr(-11, 11)`; no tipo `4`, é o resultado de `normalize_number`. O código exige igualdade entre resultados obtidos por regras diferentes.
- **Impacto:** não é possível determinar estaticamente quais representações casarão sem os dados.

### Multiplicidade dos joins

- **TIM:** `distinct` garante uma linha auxiliar por `referencia`, mas múltiplas linhas principais com a mesma referência permanecem.
- **Vivo:** o caminho auxiliar não usa `distinct` nem deduplicação das três chaves; múltiplas correspondências podem multiplicar linhas principais.
- **Claro:** `df_voice_mail` não usa `distinct`; múltiplos registros `FOR` com a mesma chave podem multiplicar linhas após o join.

A cardinalidade efetiva não pode ser determinada sem os dados de origem.

### Deduplicação Claro Nokia

- **Elemento afetado:** todas as colunas não usadas como chave após `dropDuplicates(["referencia", "numero_de_a"])`.
- **Motivo:** quando há mais de uma linha com a mesma chave e valores divergentes nas outras colunas, o código não define qual linha será preservada.
- **Impacto:** a origem posicional é conhecida, mas o registro específico sobrevivente é indeterminado estaticamente.

### Nulos e configuração da sessão Spark

- `to_timestamp` e `date_format` dependem dos valores e das regras da sessão Spark; entradas inválidas podem produzir nulo.
- `count(referencia)` não conta nulos, portanto `total_chamadas` pode ser menor que o número físico de linhas do grupo.
- Os tipos das seis agregações são determinados pelo Spark; contagens e somas de inteiros são normalmente materializadas como `long`, mas o documento não substitui a inspeção do schema em uma execução específica.

# Observações da revisão

## Inconsistências identificadas

1. **Localização:** `_TRANSFORM_MAP` em `RoboCallsTransformer`.
   **Descrição:** o mapa contém Ericsson, TIM VoLTE e Vivo VoLTE, mas não Claro Nokia, embora `transform_cdr_claro_nokia` exista.
   **Impacto potencial:** consumidores que dependam do mapa não descobrirão o fluxo Claro.
   **Sugestão:** avaliar a inclusão em alteração futura, sem modificação nesta revisão.

2. **Localização:** docstring de `transform_cdr_tim_volte`.
   **Descrição:** afirma que `autenticacao` é inicializada como zero, mas o código chama `_add_chamada_autenticada` e classifica a coluna extraída.
   **Impacto potencial:** interpretação incorreta da linhagem de autenticação.
   **Sugestão:** corrigir a descrição em revisão futura.

3. **Localização:** comentário de `_AUTH_NONE`.
   **Descrição:** o comentário diz “não autenticada (é nulo)”, enquanto a implementação representa campo nulo/não verificado com zero. O comportamento executável foi adotado como fonte da verdade.
   **Impacto potencial:** ambiguidade semântica para consumidores.
   **Sugestão:** alinhar terminologia em revisão futura.

## Pontos de atenção para manutenção

1. A ordem de `column_indices` e `column_names` é o contrato de extração; mudar uma lista sem a outra muda a linhagem.
2. `numero_valido`, produzido pela UDF, é descartado. O número retornado pode ser inválido sem flag de validade no parquet transformado.
3. A seleção por `_TRANSFORMED_COLUMNS` é a barreira que remove colunas originais e intermediárias, incluindo `_data`, `_hora`, `numero_de_a`, `numero_de_b`, `autenticacao`, `numero_conectado`, `chamada_ptc` e auxiliares de join.
4. Os joins de caixa postal são sensíveis a nulos e duplicatas nas chaves.
5. O argumento `target_file` de `RoboCallsAnalyzer.analyze` possui padrão vazio, mas é sempre usado em uma escrita parquet; o comportamento com string vazia depende do Spark/ambiente.

## Possíveis melhorias

1. Tornar explícitos os schemas e nomes físicos dos formatos com cabeçalho para permitir rastreabilidade independente de arquivos de amostra.
2. Adicionar validações/testes de unicidade das chaves auxiliares antes dos joins de Vivo e Claro.
3. Tornar determinística a escolha de registros na deduplicação Claro Nokia quando existirem divergências fora da chave.
4. Validar a taxa de `data_hora` nula após parsing, especialmente para o formato literal TIM VoLTE.
5. Expor ou persistir `numero_valido` caso a qualidade da normalização precise ser auditável.

# 10. Validação de cobertura das colunas finais

A cobertura foi conferida contra os contratos executáveis do código:

| Resultado | Fonte do contrato | Quantidade no código | Quantidade documentada | Colunas sem documentação |
| :--- | :--- | ---: | ---: | :--- |
| Parquet transformado | `RoboCallsTransformer._TRANSFORMED_COLUMNS` | 10 | 10 em cada um dos quatro fluxos | Nenhuma |
| Parquet analítico final | chaves de `groupBy` + aliases de `agg` | 8 | 8 | Nenhuma |

Todas as colunas finais foram rastreadas até uma posição de origem, uma constante ou uma transformação derivada. Onde o código não permite fechar a linhagem com segurança — nomes reais de cabeçalho, cardinalidade de joins, registro sobrevivente da deduplicação e compatibilidade de representações — a interrupção foi registrada explicitamente na seção de ambiguidades.
