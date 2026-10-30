# Documentação da Linhagem e Transformação dos Dados

## 1. Visão geral do fluxo

Este documento descreve exclusivamente o pipeline ativo em `src/teleutils/core` e
`src/teleutils/preprocessing`. O pacote `src/teleutils/robocalls` está depreciado e
foi deliberadamente excluído da análise. O código atual é a fonte da verdade. O módulo
`loaders` (carga do Parquet final em sistemas de destino) está fora do escopo, pois a linhagem termina no Parquet final.

Os layouts são selecionados pela chave `cdr_schema` nos catálogos dos extratores
(`PARQUET_DEFAULT_SCHEMAS` e `TEXT_DEFAULT_SCHEMAS`). `CDRParquetExtractor` e
`CDRTextExtractor` oferecem `extract` (persistido: chama `extract_dataframe` e grava
o DataFrame com `write.mode("overwrite").parquet`, sem partição) e `extract_dataframe`
(sem escrita intermediária). O orquestrador (`core/orchestrators/etl.py`) escolhe o
único extrator cujo catálogo contém a chave e resolve, por `getattr`, o método
`transform_<cdr_schema>` da instância de `CDRTransformer`; os aliases
`transform_stfc_tropico_claro_lega` e `transform_stfc_tropico_claro_legb` apontam para
`transform_stfc_tropico_claro`. O orquestrador chama a transformação apenas com a
origem e o caminho de saída, portanto `output_format` mantém o valor `default`.

Fluxo arquitetural:

```text
CDR de origem (Parquet do Teleparser ou texto/CSV/largura fixa)
→ seleção e renomeação segundo CDRParquetSchema ou CDRTextSchema
→ inclusão de esquema, prestadora, servico, tipo_cdr e arquivo_origem
→ Parquet intermediário (padrão) OU DataFrame extraído (modo direto)
→ pré-processamento específico do layout (transform_<cdr_schema>)
→ _apply_standard_pipeline
→ _select_transformed_columns (cast + alias de TARGET_SCHEMA)
→ _write_parquet (overwrite, particionado por no_tipo_chamada)
→ Parquet final
```

O modo direto é ativado por `materialize_intermediate=False` no orquestrador
(`paths.extracted_file is None`). Ele elimina a persistência entre extração e
transformação e não muda os mapeamentos, filtros ou renomeações. No modo
materializado, o extrator usa a sessão `spark` recebida e o transformador lê o
Parquet intermediário com a própria sessão (`spark.newSession()` configurada com
`spark.sql.timestampType=TIMESTAMP_NTZ`). No modo direto, ambos os componentes usam a
sessão isolada NTZ do transformador. No uso manual, construa o extrator com
`transformer.spark`; DataFrames de outra sessão são rejeitados por `_resolve_input`.
Overrides SQL de runtime do chamador não são copiados. A validação de metadados
inferidos do caminho (`add_source_metadata`) pode executar uma ação (`collect`) antes da
escrita final.

O contrato final é determinado pela ordem de `TARGET_SCHEMA`, em
`src/teleutils/_config.py`, e contém **38 colunas**. Colunas intermediárias fora
desse contrato (por exemplo `servico`, colunas com prefixo `_` e `status_chamada`)
são descartadas na seleção final.

### 1.1 Fluxos disponíveis

São 17 chaves `cdr_schema` (6 em `PARQUET_DEFAULT_SCHEMAS` e 11 em `TEXT_DEFAULT_SCHEMAS`); cada uma possui o método `transform_<cdr_schema>` em `CDRTransformer`.

| # | `cdr_schema` | Extrator | Método transformador | Máscara de `data_hora`/`data_hora_fim` | Máscara de `data_hora_referencia` |
| --: | :-- | :-- | :-- | :-- | :-- |
| 1 | `smp_ericsson_gsm` | `CDRParquetExtractor` | `transform_smp_ericsson_gsm` | `yy-MM-dd HH:mm:ss` | mesma máscara de `data_hora` |
| 2 | `smp_nokia_gsm` | `CDRParquetExtractor` | `transform_smp_nokia_gsm` | `dd/MM/yyyy HH:mm:ss` | mesma máscara de `data_hora` |
| 3 | `smp_huawei_volte_tim` | `CDRParquetExtractor` | `transform_smp_huawei_volte_tim` | `yyyy-MM-dd HH:mm:ss` | mesma máscara de `data_hora` |
| 4 | `smp_ericsson_volte_vivo` | `CDRParquetExtractor` | `transform_smp_ericsson_volte_vivo` | `yyyyMMdd HHmmss` | mesma máscara de `data_hora` |
| 5 | `stfc_italtel_ngn_tim` | `CDRParquetExtractor` | `transform_stfc_italtel_ngn_tim` | `dd/MM/yy HH:mm:ss` | `yyyy-MM-dd HH:mm:ss` |
| 6 | `stfc_sigma_claro` | `CDRParquetExtractor` | `transform_stfc_sigma_claro` | `ddMMyy HH:mm:ss` | mesma máscara de `data_hora` |
| 7 | `stfc_huawei_ngn` | `CDRTextExtractor` | `transform_stfc_huawei_ngn` | `ddMMyyyy HHmmss` | mesma máscara de `data_hora` |
| 8 | `stfc_huawei_ngn_tim` | `CDRTextExtractor` | `transform_stfc_huawei_ngn_tim` | `yyMMddHHmmss` | mesma máscara de `data_hora` |
| 9 | `stfc_fcdr_vivo` | `CDRTextExtractor` | `transform_stfc_fcdr_vivo` | `ddMMyy HHmmss` | mesma máscara de `data_hora` |
| 10 | `stfc_7n_oi` | `CDRTextExtractor` | `transform_stfc_7n_oi` | `ddMMyyyy HHmmss` | mesma máscara de `data_hora` |
| 11 | `stfc_tropico_oi` | `CDRTextExtractor` | `transform_stfc_tropico_oi` | `ddMMyy HHmmss` | mesma máscara de `data_hora` |
| 12 | `stfc_axe_claro` | `CDRTextExtractor` | `transform_stfc_axe_claro` | `yyMMdd HHmmss` | mesma máscara de `data_hora` |
| 13 | `stfc_pcl_claro` | `CDRTextExtractor` | `transform_stfc_pcl_claro` | `yyyyMMdd HHmmss` | mesma máscara de `data_hora` |
| 14 | `stfc_tropico_claro_lega` | `CDRTextExtractor` | `transform_stfc_tropico_claro_lega` (alias de `transform_stfc_tropico_claro`) | `ddMMyyyy HHmmss` | mesma máscara de `data_hora` |
| 15 | `stfc_tropico_claro_legb` | `CDRTextExtractor` | `transform_stfc_tropico_claro_legb` (alias de `transform_stfc_tropico_claro`) | `ddMMyyyy HHmmss` | mesma máscara de `data_hora` |
| 16 | `stfc_pit_claro` | `CDRTextExtractor` | `transform_stfc_pit_claro` | `yyyyMMddHHmmss` | mesma máscara de `data_hora` |
| 17 | `stfc_ss8bf_claro` | `CDRTextExtractor` | `transform_stfc_ss8bf_claro` | `yyyy-MM-dd'T'HH:mm:ss` | mesma máscara de `data_hora` |

## 2. Convenções utilizadas

| Nível | Descrição |
| :-- | :-- |
| Colunas Originais | Campos presentes no Parquet bruto, posições do CSV (base 0) ou posições/larguras de linhas de largura fixa (base 1, como em `substring`). |
| Colunas Extraídas | Nomes atribuídos pelo extrator, presentes no DataFrame e, no modo materializado, gravados no Parquet intermediário. |
| Coluna Intermediária | Coluna criada ou alterada durante a transformação (nomes de origem das chaves de `TARGET_SCHEMA`). |
| Coluna Final | Nome após o `cast` e o `alias` definidos em `TARGET_SCHEMA`. |

Classificações empregadas:

- **Renomeação**: somente o nome muda, durante a extração ou na seleção final.
- **Cópia**: o valor segue sem regra de conteúdo (“Mantida sem alteração”), embora possa receber o cast final.
- **Transformação**: o valor é calculado, analisado, normalizado, condicionado ou combinado.
- **Constante/metadado**: valor literal ou derivado do caminho do arquivo ou de parâmetros do extrator.
- **Não aplicável / ausente**: `_fill_missing_columns` cria `NULL`; se a coluna participa da chave
  primária, `_fill_primary_key_columns` (ou `_format_date_time`, nos timestamps) substitui o nulo antes da escrita.

Nas tabelas por fluxo, “Colunas Originais” descreve a posição ou o nome no dado bruto de cada coluna extraída
usada; quando o layout é posicional e não há nomes de origem, somente a posição é conhecida (ver Ambiguidades).

### 2.1 Contrato final e tipos

Lista independente: as 38 chaves de `TARGET_SCHEMA`, na ordem de declaração.

| # | Coluna anterior | Operação final | Coluna final | Tipo final | Chave primária |
| --: | :-- | :-- | :-- | :-- | :-: |
| 1 | `referencia` | cast + renomeação | `nu_referencia` | string | sim |
| 2 | `referencia_sip` | cast + renomeação | `nu_referencia_sip` | string | sim |
| 3 | `data_hora_referencia` | cast + renomeação | `dh_referencia` | timestamp_ntz | sim |
| 4 | `data_hora` | cast + renomeação | `dh_chamada` | timestamp_ntz | sim |
| 5 | `data_hora_fim` | cast + renomeação | `dh_fim_chamada` | timestamp_ntz | sim |
| 6 | `duracao` | cast + renomeação | `qt_duracao_segundos` | integer | sim |
| 7 | `numero_origem_extraido` | cast + renomeação | `nu_origem` | string | sim |
| 8 | `sucesso_extracao_origem` | cast + renomeação | `ic_origem_valido` | boolean | não |
| 9 | `numero_origem` | cast + renomeação | `nu_origem_original` | string | sim |
| 10 | `numero_destino_extraido` | cast + renomeação | `nu_destino` | string | sim |
| 11 | `sucesso_extracao_destino` | cast + renomeação | `ic_destino_valido` | boolean | não |
| 12 | `numero_destino` | cast + renomeação | `nu_destino_original` | string | sim |
| 13 | `resultado_chamada` | cast + renomeação | `no_resultado_chamada` | string | sim |
| 14 | `codigo_resposta_sip` | cast + renomeação | `co_resposta_sip` | string | não |
| 15 | `autenticacao` | cast + renomeação | `no_autenticacao` | string | não |
| 16 | `prestadora` | cast + renomeação | `no_prestadora` | string | não |
| 17 | `rota_entrada` | cast + renomeação | `no_rota_entrada` | string | sim |
| 18 | `rota_saida` | cast + renomeação | `no_rota_saida` | string | sim |
| 19 | `bilhetador` | cast + renomeação | `no_bilhetador` | string | sim |
| 20 | `celula_origem` | cast + renomeação | `nu_cgi_origem` | string | não |
| 21 | `celula_origem_hex` | cast + renomeação | `nu_cgi_origem_hex` | string | não |
| 22 | `tecnologia_celula_origem` | cast + renomeação | `no_tecnologia_celula_origem` | string | não |
| 23 | `imei_origem` | cast + renomeação | `nu_imei_origem` | string | não |
| 24 | `imsi_origem` | cast + renomeação | `nu_imsi_origem` | string | não |
| 25 | `ip_origem` | cast + renomeação | `nu_ip_origem` | string | não |
| 26 | `porta_ip_origem` | cast + renomeação | `nu_porta_ip_origem` | integer | não |
| 27 | `celula_destino` | cast + renomeação | `nu_cgi_destino` | string | não |
| 28 | `celula_destino_hex` | cast + renomeação | `nu_cgi_destino_hex` | string | não |
| 29 | `tecnologia_celula_destino` | cast + renomeação | `no_tecnologia_celula_destino` | string | não |
| 30 | `imei_destino` | cast + renomeação | `nu_imei_destino` | string | não |
| 31 | `imsi_destino` | cast + renomeação | `nu_imsi_destino` | string | não |
| 32 | `ip_destino` | cast + renomeação | `nu_ip_destino` | string | não |
| 33 | `porta_ip_destino` | cast + renomeação | `nu_porta_ip_destino` | integer | não |
| 34 | `agente_usuario` | cast + renomeação | `no_agente_usuario` | string | não |
| 35 | `esquema` | cast + renomeação | `no_esquema` | string | não |
| 36 | `tipo_cdr` | cast + renomeação | `no_tipo_cdr` | string | não |
| 37 | `arquivo_origem` | cast + renomeação | `no_arquivo_origem` | string | não |
| 38 | `tipo_chamada` | cast + renomeação | `no_tipo_chamada` | string | sim |

### 2.2 Transformações comuns anteriores à seleção final

`_apply_standard_pipeline` (em `CDRBaseTransformer`) executa, nesta ordem:

1. `_fill_missing_columns`: cria como `NULL` (sem cast) qualquer chave de `TARGET_SCHEMA`
   ausente no DataFrame, inclusive as colunas derivadas `numero_origem_extraido`,
   `sucesso_extracao_origem`, `numero_destino_extraido` e `sucesso_extracao_destino`
   (sobrescritas na etapa 3).
2. `_format_date_time`: `duracao` = `coalesce(cast(duracao as int), 0)`;
   `data_hora` e `data_hora_referencia` = `greatest(try_to_timestamp(coluna, máscara), MIN_SAFE_DATE)`
   (`data_hora_referencia` usa `ref_date_time_fmt` quando informado); `data_hora_fim`: se não nula,
   mesma normalização; se nula, `data_hora` (já normalizada) + `make_interval(secs=duracao)`.
   `MIN_SAFE_DATE` é `1901-01-01 00:00:00`. Um `data_hora_fim` não nulo, mas inválido, resulta
   em `MIN_SAFE_DATE` (não em `data_hora` + `duracao`).
3. `_extract_phone_numbers`: aplica a pandas UDF `_extract_numbers` (que chama `extract_number`,
   em `preprocessing/phone_number_extractor.py`) separadamente a `numero_origem` e `numero_destino`,
   expandindo o struct em `numero_origem_extraido`/`sucesso_extracao_origem` e
   `numero_destino_extraido`/`sucesso_extracao_destino`. Depois, se existirem as colunas de nome
   exato `_numero_origem_original` e/ou `_numero_destino_original`, elas **sobrescrevem**
   `numero_origem`/`numero_destino` (valor bruto) e são removidas. Regras de `extract_number`:
   entrada vazia (falsy) → `("5599999999999", False)`; sem extração válida → `(entrada original, False)`;
   com extração → `(número extraído, True)`.
4. `_add_tn_validation_status`: se existir `_autenticacao`, `autenticacao` =
   `TN-Validation-Passed` (prefixo `verstat=TN-Validation-P`), `TN-Validation-Failed` (`verstat=TN-Validation-F`),
   `No-TN-Validation` (`verstat=No-TN-Validation`) ou `NULL`; sem a coluna, `autenticacao = NULL`.
5. `_fill_primary_key_columns`: nas 15 colunas da chave (`PRIMARY_KEY_COLUMNS`, mapeadas para os nomes
   intermediários de `TARGET_SCHEMA`), substitui nulos por `MIN_SAFE_DATE` (timestamps), `0` (numéricos)
   e `"__NULL__"` (demais), após o cast ao tipo de `TARGET_SCHEMA`.

Assim, “ausente” não significa sempre `NULL` no Parquet final. Para as colunas da chave
(`no_tipo_chamada`, `nu_referencia`, `nu_referencia_sip`, `dh_referencia`, `dh_chamada`, `dh_fim_chamada`,
`qt_duracao_segundos`, `nu_origem`, `nu_origem_original`, `nu_destino`, `nu_destino_original`,
`no_resultado_chamada`, `no_rota_entrada`, `no_rota_saida`, `no_bilhetador`) vale o preenchimento acima.
Strings vazias não são nulas e não são substituídas. As demais colunas (`ic_*`, `co_resposta_sip`,
`no_autenticacao`, `no_prestadora`, `nu_cgi_*`, `nu_imei_*`, `nu_imsi_*`, `nu_ip_*`, `nu_porta_ip_*`,
`no_agente_usuario`, `no_tecnologia_celula_*`, `no_esquema`, `no_tipo_cdr`, `no_arquivo_origem`)
permanecem `NULL` quando ausentes.

### 2.3 Metadados adicionados pelos extratores

`add_source_metadata` (em `core/extractors/_extractors_utils.py`) é executado por ambos os extratores
após a seleção de colunas e antes de qualquer filtro de linhas do extrator de texto. Usa o caminho do arquivo
de cada registro (`F.input_file_name()` no leitor CSV/Parquet ou o par `(linha, caminho)` de
`read_text_with_source`); `servico` não pertence ao contrato e é descartada na seleção final.

| Coluna extraída | Origem |
| :-- | :-- |
| `esquema` | Literal com a chave `cdr_schema`. |
| `prestadora` | Argumento `operator`; se vazio, `url_decode` do segmento `-4` do caminho. |
| `servico` | Argumento `service`; se vazio, segmento `-3`. Não chega ao contrato final. |
| `tipo_cdr` | Se existir `_tipo_cdr` nos dados, esta coluna prevalece (inclusive nula); senão, argumento `cdr_type`; senão, segmento `-2`. `_tipo_cdr` é removida em seguida. |
| `arquivo_origem` | Se já existir no DataFrame (mapeada pelo schema), é mantida; senão, `url_decode` do segmento `-1`. |

O `+` do caminho é substituído por `%2B` antes de `url_decode`, de modo que `+` literal é preservado.
Quando algum valor inferido do caminho é nulo ou em branco, a extração falha com `ValueError`.

# 3. Transformações por formato de origem

Cada tópico descreve um método `transform_<cdr_schema>` (e o extrator correspondente). A tabela de cada fluxo possui uma linha para cada uma das 38 colunas finais, na ordem de `TARGET_SCHEMA`.

## 3.1 `transform_smp_ericsson_gsm`

### Descrição

Catálogo: `smp_ericsson_gsm` (`PARQUET_DEFAULT_SCHEMAS`). Layout Parquet Ericsson GSM. Converte a duração `HH:mm:ss` em segundos, compõe início e término a partir de `_data`, `_hora` e `_hora_fim` e monta células, IMSIs e IMEIs a partir de componentes. O parâmetro `output_format` é validado, mas não altera nenhuma coluna alcançável (ver Observações).

### Fluxo resumido

```text
Arquivo de origem
→ `CDRParquetExtractor` (`smp_ericsson_gsm`): `select` + alias do `column_mapping` e metadados do caminho
→ `_resolve_input` (Parquet ou DataFrame)
→ `duracao` `HH:mm:ss` → segundos
→ `_concat_date_time(stop_date='_data')`
→ `_build_composite_column` para células, IMSIs e IMEIs
→ `_apply_standard_pipeline(df, 'yy-MM-dd HH:mm:ss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| `networkCallReference` | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| `dateForStartOfCharge`; `timeForStartOfCharge` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; nulo se algum componente for nulo ou em branco; `try_to_timestamp(..., 'yy-MM-dd HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| `dateForStartOfCharge`; `timeForStopOfCharge` | `_data`, `_hora_fim` | `_concat_date_time(stop_date='_data')`: `_data` + espaço + `_hora_fim` (a data de início é usada também no término); se não nulo: `try_to_timestamp(..., 'yy-MM-dd HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`); se nulo: `data_hora` + `duracao` s | `dh_fim_chamada` |
| `chargeableDuration` | `duracao` | `HH:mm:ss` → `substring(1,2)*3600 + substring(4,2)*60 + substring(7,2)`; nulo → `0`; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| `callingPartyNumber.digits` | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| `callingPartyNumber.digits` | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| `callingPartyNumber.digits` | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| `calledPartyNumber.digits` | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| `calledPartyNumber.digits` | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| `calledPartyNumber.digits` | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| `callPosition` | `status_chamada` | `callPosition` é extraída como `status_chamada`, mas nenhuma etapa a copia para `resultado_chamada`; `status_chamada` não pertence a `TARGET_SCHEMA` e é descartada. `resultado_chamada` é criada nula; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| `incomingRoute` | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| `outgoingRoute` | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| `exchangeIdentity` | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| `firstCallingLocationInformation.mcc`; `firstCallingLocationInformation.mnc`; `firstCallingLocationInformation.lac`; `firstCallingLocationInformation.ci_sac` | `celula_origem_mcc`, `celula_origem_mnc`, `celula_origem_lac`, `celula_origem_ci_sac` | `_build_composite_column('-', ...)`: componentes em branco → nulo; `lpad(celula_origem_lac, 5, '0')` e `lpad(celula_origem_ci_sac, 5, '0')`; qualquer componente nulo → `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| `callingSubscriberIMEI.type_allocation_code`; `callingSubscriberIMEI.serial_number` | `imei_origem_tac`, `imei_origem_sn` | `_build_composite_column('', ...)`: TAC + SN sem separador; componente em branco ou nulo → `NULL` | `nu_imei_origem` |
| `callingSubscriberIMSI.mcc`; `callingSubscriberIMSI.mnc`; `callingSubscriberIMSI.msin` | `imsi_origem_mcc`, `imsi_origem_mnc`, `imsi_origem_msin` | `_build_composite_column('', ...)`: concatenação sem separador; componente em branco ou nulo → `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| `firstCalledLocationInformation.mcc`; `firstCalledLocationInformation.mnc`; `firstCalledLocationInformation.lac`; `firstCalledLocationInformation.ci_sac` | `celula_destino_mcc`, `celula_destino_mnc`, `celula_destino_lac`, `celula_destino_ci_sac` | `_build_composite_column('-', ...)`: mesmas regras da origem, com os componentes de destino | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| `calledSubscriberIMEI.type_allocation_code`; `calledSubscriberIMEI.serial_number` | `imei_destino_tac`, `imei_destino_sn` | `_build_composite_column('', ...)`: mesmas regras da origem | `nu_imei_destino` |
| `calledSubscriberIMSI.mcc`; `calledSubscriberIMSI.mnc`; `calledSubscriberIMSI.msin` | `imsi_destino_mcc`, `imsi_destino_mnc`, `imsi_destino_msin` | `_build_composite_column('', ...)`: mesmas regras da origem | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'smp_ericsson_gsm'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| `CallModule` | `tipo_chamada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

Cobertura: 38 de 38 colunas finais documentadas; 24 com origem efetiva neste fluxo e 14 criadas nulas.

## 3.2 `transform_smp_nokia_gsm`

### Descrição

Catálogo: `smp_nokia_gsm` (`PARQUET_DEFAULT_SCHEMAS`). Layout Parquet Nokia GSM. Escolhe valores por precedência (`coalesce`) para duração, início e números, trata os tipos `UCA` e `FORW`, imputa o MNC pela prestadora e classifica a causa de término por faixas.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRParquetExtractor` (`smp_nokia_gsm`)
→ `_resolve_input`
→ `duracao` = primeiro valor não nulo entre as colunas `_duracao*` (sentinela `FFFFFF` → nulo)
→ `data_hora`, `data_hora_fim`, `numero_origem`, `numero_destino` por precedência/condição
→ `celula_origem` e `celula_destino` com MCC `724` e MNC imputado
→ `resultado_chamada` por faixas de `_resultado_chamada`
→ `_apply_standard_pipeline(df, 'dd/MM/yyyy HH:mm:ss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| `call_reference` | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| `call_reference_time` | `data_hora_referencia` | Mantida sem alteração; `try_to_timestamp(..., 'dd/MM/yyyy HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_referencia` |
| `in_channel_allocated_time`; `call_reference_time` | `data_hora_alocacao_canal`, `data_hora_referencia` | `coalesce(data_hora_alocacao_canal, data_hora_referencia)`; `try_to_timestamp(..., 'dd/MM/yyyy HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| `charging_end_time`; `release_time`; `record_type` | `data_hora_fim`, `data_hora_desconexao`, `tipo_chamada` | se `data_hora_desconexao` existe: `tipo_chamada == 'UCA'` → `coalesce(data_hora_desconexao, data_hora_fim)`; demais tipos → `data_hora_fim`; se não nulo: `try_to_timestamp(..., 'dd/MM/yyyy HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`); se nulo: `data_hora` + `duracao` s | `dh_fim_chamada` |
| `orig_mcz_duration`; `term_mcz_duration`; `forw_mcz_duration`; `roam_mcz_duration`; `iaz_duration`; `oaz_duration`; `chargeable_duration`; `char_band_duration` | `_duracao_orig_mcz`, `_duracao_term_mcz`, `_duracao_forw_mcz`, `_duracao_roam_mcz`, `_duracao_iaz`, `_duracao_oaz`, `_duracao_tarifavel`, `_duracao_banda_tarifavel` | colunas cujo nome começa com `_duracao`: valor `FFFFFF` → nulo; `coalesce` na ordem das colunas do DataFrame; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| `calling_number`; `orig_calling_number` | `numero_origem`, `numero_origem_original` | pré-processamento: `coalesce(numero_origem, numero_origem_original)`; `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| `calling_number`; `orig_calling_number` | `numero_origem`, `numero_origem_original` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| `calling_number`; `orig_calling_number` | `numero_origem`, `numero_origem_original` | `numero_origem` após o pré-processamento (`coalesce(numero_origem, numero_origem_original)`); não há `_numero_origem_original`; nulo → `__NULL__` (chave) | `nu_origem_original` |
| `called_number`; `forwarding_number` | `numero_destino`, `numero_origem_encaminhamento` | pré-processamento: `tipo_chamada == 'FORW'` → `numero_origem_encaminhamento`; demais → `numero_destino`; `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| `called_number`; `forwarding_number` | `numero_destino`, `numero_origem_encaminhamento` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| `called_number`; `forwarding_number` | `numero_destino`, `numero_origem_encaminhamento` | `numero_destino` após o pré-processamento (`tipo_chamada == 'FORW'` → `numero_origem_encaminhamento`; demais → `numero_destino`); não há `_numero_destino_original`; nulo → `__NULL__` (chave) | `nu_destino_original` |
| `cause_for_termination` | `_resultado_chamada` | faixas numéricas de `_resultado_chamada` (ver “Faixas de `_resultado_chamada`” abaixo); fora das faixas → `NULL`; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| `in_circuit_group` | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| `out_circuit_group` | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| `exchange_id` | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| `calling_subs_first_lac`; `calling_subs_first_ci`; metadado (caminho do arquivo ou parâmetro do extrator) | `celula_origem_lac`, `celula_origem_ci`, `prestadora` | `_build_composite_column('-', ...)` com `DEFAULT_MCC` (`724`), `_nokia_mnc`, `lpad(lac, 5, '0')` e `lpad(ci, 5, '0')`; `_nokia_mnc`: `prestadora == 'claro'` → `05`, `'algar'` → `34`, outro valor → `NULL` (composto `NULL`) | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| `calling_imei` | `imei_origem` | Mantida sem alteração | `nu_imei_origem` |
| `calling_imsi` | `imsi_origem` | Mantida sem alteração | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| `called_subs_first_lac`; `called_subs_first_ci`; metadado (caminho do arquivo ou parâmetro do extrator) | `celula_destino_lac`, `celula_destino_ci`, `prestadora` | mesma regra da origem, com `celula_destino_lac` e `celula_destino_ci` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| `called_imei` | `imei_destino` | Mantida sem alteração | `nu_imei_destino` |
| `called_imsi` | `imsi_destino` | Mantida sem alteração | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'smp_nokia_gsm'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| `record_type` | `tipo_chamada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **Faixas de `_resultado_chamada`** (comparação direta com literais inteiros; limites hexadecimais convertidos em Python): `0x0000–0x03FF` → `normal clearing`; `0x0400–0x07FF` → `internal congestion`; `0x0800–0x0BFF` → `external congestion`; `0x0C00–0x0FFF` → `subscriber errors`; `>= 0x1000` → `event codes`; demais → `NULL`.
- **Colunas extraídas sem consumo:** `numero_destino_original`, `numero_conectado`, `numero_destino_encaminhamento` e `perfil_prestadora` não são lidas por nenhuma etapa posterior e não pertencem a `TARGET_SCHEMA`.
- **Restauração do bruto:** o transformador não cria `_numero_origem_original` nem `_numero_destino_original`; por isso `nu_origem_original` e `nu_destino_original` refletem `numero_origem` e `numero_destino` após o `coalesce` e a regra `FORW`.

Cobertura: 38 de 38 colunas finais documentadas; 26 com origem efetiva neste fluxo e 12 criadas nulas.

## 3.3 `transform_smp_huawei_volte_tim`

### Descrição

Catálogo: `smp_huawei_volte_tim` (`PARQUET_DEFAULT_SCHEMAS`). Layout Parquet Huawei VoLTE TIM. O tipo de registro (`tipo_cdr`, vindo de `recordType`) define a origem dos números: `aTSRecord` usa `tEL-URI` com dígitos invertidos de dois em dois; `iBCFRecord` usa `sIP-URI`. O papel do nó (`tipo_chamada`) decide se célula, tecnologia, IMEI e IMSI pertencem à origem ou ao destino.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRParquetExtractor` (`smp_huawei_volte_tim`); `_tipo_cdr` (`recordType`) vira `tipo_cdr` e é removida
→ `_resolve_input`
→ `left(data_hora, 19)` e `left(data_hora_fim, 19)`
→ números origem/destino por `tipo_cdr` (ATS ou IBCF) + `_numero_*_original`
→ `_autenticacao` por regex `verstat=...`
→ `_extract_cell_info` + `_format_cell_id(..., 'smp_huawei_volte_tim')`
→ `_imei` por `_info_imei == 'iMEI'`; `_imsi` por JSON `eND-USER-IMSI`
→ atribuição origem/destino por `tipo_chamada`
→ `_resultado_chamada` (inteiro) → `codigo_resposta_sip` e `resultado_chamada`
→ `_apply_standard_pipeline(df, 'yyyy-MM-dd HH:mm:ss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| `network-Call-Reference` | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| `iMS-Charging-Identifier` | `referencia_sip` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| `serviceRequestTimeStamp` | `data_hora` | `left(data_hora, 19)` (remove o sufixo de fuso); `try_to_timestamp(..., 'yyyy-MM-dd HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| `serviceDeliveryEndTimeStamp` | `data_hora_fim` | `left(data_hora_fim, 19)`; se não nulo: `try_to_timestamp(..., 'yyyy-MM-dd HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`); se nulo: `data_hora` + `duracao` s | `dh_fim_chamada` |
| `duration` | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| `list-Of-Calling-Party-Address`; `recordType` | `_numero_origem_ats_ibcf`, `_tipo_cdr` | pré-processamento: `tipo_cdr == 'aTSRecord'`: `get_json_object(..., '$[0].tEL-URI')`, `substr(3, 9999)` e `regexp_replace('(.)(.)', '$2$1')`; `tipo_cdr == 'iBCFRecord'`: `regexp_extract(get_json_object(..., '$[0].sIP-URI'), 'sip:([0-9]+)[@;]', 1)`; outros tipos → `NULL`; `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| `list-Of-Calling-Party-Address`; `recordType` | `_numero_origem_ats_ibcf`, `_tipo_cdr` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| `list-Of-Calling-Party-Address`; `recordType` | `_numero_origem_ats_ibcf`, `_tipo_cdr` | `_numero_origem_original`: ATS → valor bruto de `$[0].tEL-URI`; demais tipos → o `numero_origem` calculado acima; restaurada em `numero_origem` após a extração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| `called-Party-Address_tEL-URI`; `called-Party-Address_sIP-URI`; `recordType` | `_numero_destino_ats`, `_numero_destino_ibcf`, `_tipo_cdr` | pré-processamento: ATS: `_numero_destino_ats.substr(3, 9999)` + inversão de pares `(.)(.)` → `$2$1`; IBCF: `regexp_extract(_numero_destino_ibcf, 'sip:([0-9]+)[@;]', 1)`; outros tipos → `NULL`; `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| `called-Party-Address_tEL-URI`; `called-Party-Address_sIP-URI`; `recordType` | `_numero_destino_ats`, `_numero_destino_ibcf`, `_tipo_cdr` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| `called-Party-Address_tEL-URI`; `called-Party-Address_sIP-URI`; `recordType` | `_numero_destino_ats`, `_numero_destino_ibcf`, `_tipo_cdr` | `_numero_destino_original`: ATS → `_numero_destino_ats` bruto; demais tipos → o `numero_destino` calculado; restaurada em `numero_destino` após a extração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| `serviceReasonReturnCode`; `recordType` | `_resultado_chamada`, `_tipo_cdr` | `_resultado_chamada` inteiro (IBCF: `regexp_extract('SIP;cause=([0-9]+);', 1)`; demais: cast direto) e tabela de códigos (ver “Códigos de `_resultado_chamada`” abaixo); nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| `serviceReasonReturnCode`; `recordType` | `_resultado_chamada`, `_tipo_cdr` | `_resultado_chamada` inteiro (regra acima) mantido somente se `>= 200`; senão `NULL` | `co_resposta_sip` |
| `calling-Party-Address-Generic`; `list-Of-Calling-Party-Address`; `recordType` | `_numero_origem_auth`, `_numero_origem_ats_ibcf`, `_tipo_cdr` | `_autenticacao`: ATS → `regexp_extract(_numero_origem_auth, '(verstat=[a-zA-Z\-]+)', 0)`; demais → mesma regex sobre `_numero_origem_ats_ibcf`; depois `_add_tn_validation_status` | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| `specifiedTreatmentField_incoming-Route` | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| `specifiedTreatmentField_outgoing-Route` | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| `nodeAddress_domainName` | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| `accessNetworkInformation`; `role-of-Node` | `_informacao_rede`, `tipo_chamada` | `_extract_cell_info` (`regexp_extract('3gpp=([0-9a-fA-F]+);?', 1)` → `_id_celula_hex`) e `_format_cell_id` (formato `smp_huawei_volte_tim`) → `_id_celula`; mantida somente se `tipo_chamada == 'oRIGINATING-ROLE'`; senão `NULL` | `nu_cgi_origem` |
| `accessNetworkInformation`; `role-of-Node` | `_informacao_rede`, `tipo_chamada` | `_id_celula_hex` (`regexp_extract`, vazio se não houver correspondência) somente se `tipo_chamada == 'oRIGINATING-ROLE'` | `nu_cgi_origem_hex` |
| `accessNetworkInformation`; `role-of-Node` | `_informacao_rede`, `tipo_chamada` | `_tecnologia_celula` = `split(_informacao_rede, ';')[0]` somente se `tipo_chamada == 'oRIGINATING-ROLE'` | `no_tecnologia_celula_origem` |
| `private-User-Equipment-Info_private-User-Equipment-Info-Type`; `private-User-Equipment-Info_private-User-Equipment-Info-Value`; `role-of-Node` | `_info_imei`, `_imei`, `tipo_chamada` | `_imei` = `_imei` se `_info_imei == 'iMEI'`, senão `NULL`; atribuída somente se `tipo_chamada == 'oRIGINATING-ROLE'` | `nu_imei_origem` |
| `list-of-subscription-ID`; `role-of-Node` | `_info_imsi`, `tipo_chamada` | `_imsi` = `from_json(_info_imsi, array<struct<info_type,info_value>>)[0].info_value` se `info_type == 'eND-USER-IMSI'`; atribuída somente se `tipo_chamada == 'oRIGINATING-ROLE'` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| `accessNetworkInformation`; `role-of-Node` | `_informacao_rede`, `tipo_chamada` | mesma derivação de `_id_celula`, atribuída somente se `tipo_chamada == 'tERMINATING-ROLE'`; senão `NULL` | `nu_cgi_destino` |
| `accessNetworkInformation`; `role-of-Node` | `_informacao_rede`, `tipo_chamada` | `_id_celula_hex` somente se `tipo_chamada == 'tERMINATING-ROLE'` | `nu_cgi_destino_hex` |
| `accessNetworkInformation`; `role-of-Node` | `_informacao_rede`, `tipo_chamada` | `_tecnologia_celula` somente se `tipo_chamada == 'tERMINATING-ROLE'` | `no_tecnologia_celula_destino` |
| `private-User-Equipment-Info_private-User-Equipment-Info-Type`; `private-User-Equipment-Info_private-User-Equipment-Info-Value`; `role-of-Node` | `_info_imei`, `_imei`, `tipo_chamada` | mesma derivação de `_imei`, atribuída somente se `tipo_chamada == 'tERMINATING-ROLE'` | `nu_imei_destino` |
| `list-of-subscription-ID`; `role-of-Node` | `_info_imsi`, `tipo_chamada` | mesma derivação de `_imsi`, atribuída somente se `tipo_chamada == 'tERMINATING-ROLE'` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| `user-Agent-Value` | `agente_usuario` | Mantida sem alteração | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'smp_huawei_volte_tim'`); mantida sem alteração | `no_esquema` |
| `recordType` | `_tipo_cdr` → `tipo_cdr` | `add_source_metadata`: `_tipo_cdr` prevalece sobre `cdr_type` e sobre o caminho; `_tipo_cdr` é removida em seguida | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| `role-of-Node` | `tipo_chamada` | Mantida sem alteração (valor bruto de `role-of-Node`); nulo → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **Códigos de `_resultado_chamada`** (inteiro): `(-400, -300]` → `Redirection`; `(-300, -200]` → `Final Response`; `-3` → `End of REGISTER dialog`; `-2` → `End of SUBSCRIBE dialog`; `-1` → `Successful transaction`; `0` → `Normal end of session`; `1` → `Unspecified error`; `2` → `Unsuccessful session setup`; `3` → `Internal error`; `4` → `Session timer timeout`; `5` → `CAC_REJECT`; `200` → `Normal end of session`; `201–299` → `Final Response`; `300–399` → `Redirection`; `400–499` → `Request failure`; `500–599` → `Server failure`; `600–699` → `Global failure`; demais → `NULL`.
- **Formato de célula:** `_format_cell_id` identifica a tecnologia pelo comprimento do hexadecimal (13 → 3G, 16 → 4G, 20 → 5G). Para 4G no formato `smp_huawei_volte_tim` o resultado é `mcc-mnc-eci` com o ECI em decimal; para 3G usa-se a forma detalhada com `lpad` 5; para 5G, `gnb_id_bits=26`. Comprimento desconhecido mantém o valor bruto; resultado vazio → `NULL`.
- **Somente o primeiro elemento** dos arrays JSON `list-Of-Calling-Party-Address` (`$[0]`) e `list-of-subscription-ID` (`getItem(0)`) é considerado.
- **`tipo_cdr`:** é a coluna `recordType` e prevalece sobre o argumento `cdr_type` e sobre o caminho; os literais comparados são `aTSRecord` e `iBCFRecord`.

Cobertura: 38 de 38 colunas finais documentadas; 33 com origem efetiva neste fluxo e 5 criadas nulas.

## 3.4 `transform_smp_ericsson_volte_vivo`

### Descrição

Catálogo: `smp_ericsson_volte_vivo` (`PARQUET_DEFAULT_SCHEMAS`). Layout Parquet Ericsson VoLTE Vivo. Separa o marcador de autenticação do número de origem, traduz códigos de tipo e resultado, remove hífens dos IMEIs, decodifica as células hexadecimais e extrai a tecnologia das informações de rede.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRParquetExtractor` (`smp_ericsson_volte_vivo`)
→ `_resolve_input`
→ `_concat_date_time(stop_date='_data')`
→ `_extract_cell_info(..., out_col_cell_id=None)` para origem e destino
→ `split(_numero_origem_original, ';')` → `numero_origem` e `_autenticacao`
→ tradução de `_tipo_chamada` e `_resultado_chamada`; `translate` de hífens nos IMEIs
→ `_format_cell_id` (formato `default`) sobre `celula_origem_hex` e `celula_destino_hex`
→ `_apply_standard_pipeline(df, 'yyyyMMdd HHmmss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| `networkCallReference` | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| `imsChargingIdentifier` | `referencia_sip` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| `dateForStartOfCharge`; `timeForStartOfCharge` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; nulo se algum componente for nulo ou em branco; `try_to_timestamp(..., 'yyyyMMdd HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| `dateForStartOfCharge`; `timeForStopOfCharge` | `_data`, `_hora_fim` | `_concat_date_time(stop_date='_data')`: `_data` + espaço + `_hora_fim`; se não nulo: `try_to_timestamp(..., 'yyyyMMdd HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`); se nulo: `data_hora` + `duracao` s | `dh_fim_chamada` |
| `chargeableDurat` | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| `callingPartyNumber` | `_numero_origem_original` | pré-processamento: `split(_numero_origem_original, ';')[0]` (número sem o marcador); `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| `callingPartyNumber` | `_numero_origem_original` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| `callingPartyNumber` | `_numero_origem_original` | `_numero_origem_original` (texto completo, inclusive `;verstat=...`) restaurada em `numero_origem` após a extração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| `calledPartyNumber` | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| `calledPartyNumber` | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| `calledPartyNumber` | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| `callPosition` | `_resultado_chamada` | `'1'` → `callHasReachedCongestionOrBusyState`; `'2'` → `callHasOnlyReachedThroughConnection`; `'3'` → `b-AnswerHasBeenReceived`; outro valor → mantém o valor de `_resultado_chamada`; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| `callingPartyNumber` | `_numero_origem_original` | `_autenticacao` = `split(_numero_origem_original, ';')[1]`; depois `_add_tn_validation_status` | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| `incomingRoute` | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| `outgoingRoute` | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| `exchangeIdentity` | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| `firstCallingLocInf` | `celula_origem_hex` | `_format_cell_id(celula_origem_hex → celula_origem)`, formato `default`: comprimento 13 → 3G, 16 → 4G, 20 → 5G; outro comprimento mantém o valor bruto; vazio → `NULL` | `nu_cgi_origem` |
| `firstCallingLocInf` | `celula_origem_hex` | Mantida sem alteração | `nu_cgi_origem_hex` |
| `firstCallingAccessNetInf` | `_informacao_rede_origem` | `split(_informacao_rede_origem, ';')[0]` | `no_tecnologia_celula_origem` |
| `callingSubscriberIMEI` | `imei_origem` | `translate(imei_origem, '-', '')` (remove hífens) | `nu_imei_origem` |
| `callingSubscriberIMSI` | `imsi_origem` | Mantida sem alteração | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| `firstCalledLocInfo` | `celula_destino_hex` | `_format_cell_id(celula_destino_hex → celula_destino)`, formato `default` (mesmas regras da origem) | `nu_cgi_destino` |
| `firstCalledLocInfo` | `celula_destino_hex` | Mantida sem alteração | `nu_cgi_destino_hex` |
| `firstCalledAccessNetInf` | `_informacao_rede_destino` | `split(_informacao_rede_destino, ';')[0]` | `no_tecnologia_celula_destino` |
| `calledSubscriberIMEI` | `imei_destino` | `translate(imei_destino, '-', '')` (remove hífens) | `nu_imei_destino` |
| `calledSubscriberIMSI` | `imsi_destino` | Mantida sem alteração | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'smp_ericsson_volte_vivo'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| metadado (caminho do arquivo ou parâmetro do extrator) | `arquivo_origem` | Mantida sem alteração (como a coluna existe, o nome não é inferido do caminho; se ausente na origem, fica `NULL`) | `no_arquivo_origem` |
| `callModule` | `_tipo_chamada` | `'1'` → `msOriginating`; `'3'` → `callForwarding`; `'4'` → `msTerminating`; outro valor → mantém `_tipo_chamada`; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **`arquivo_origem`** é extraída de `source_filename`; como a coluna passa a existir no DataFrame, o extrator não infere o nome pelo caminho. Se `source_filename` não existir no Parquet de origem, `arquivo_origem` fica `NULL`.
- **Sem marcador:** quando `_numero_origem_original` não contém `;`, `split(...)[1]` é nulo e `no_autenticacao` fica `NULL`.

Cobertura: 38 de 38 colunas finais documentadas; 31 com origem efetiva neste fluxo e 7 criadas nulas.

## 3.5 `transform_stfc_italtel_ngn_tim`

### Descrição

Catálogo: `stfc_italtel_ngn_tim` (`PARQUET_DEFAULT_SCHEMAS`). Layout Parquet Italtel NGN TIM. Compõe a data de início, normaliza o tipo de chamada substituindo espaços por `_` e usa uma máscara própria (`yyyy-MM-dd HH:mm:ss`) para `data_hora_referencia`.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRParquetExtractor` (`stfc_italtel_ngn_tim`)
→ `_resolve_input`
→ `tipo_chamada` = `regexp_replace(_tipo_chamada, ' ', '_')`
→ `_concat_date_time` (somente início; `_data_fim` ausente)
→ `_apply_standard_pipeline(df, 'dd/MM/yy HH:mm:ss', 'yyyy-MM-dd HH:mm:ss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| `network_call_reference_sequence_number` | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| `call_id_number_time` | `data_hora_referencia` | Mantida sem alteração; `try_to_timestamp(..., 'yyyy-MM-dd HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_referencia` |
| `starting_date_of_call`; `starting_time_of_call` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; `try_to_timestamp(..., 'dd/MM/yy HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_format_date_time` calcula `data_hora` + `duracao` s | `dh_fim_chamada` |
| `duration_of_call_seconds` | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| `calling_information_calling_number` | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| `calling_information_calling_number` | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| `calling_information_calling_number` | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| `called_information_called_number` | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| `called_information_called_number` | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| `called_information_called_number` | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| `call_final_status_information_final_status` | `resultado_chamada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| `trunking_group_identity_information_incoming_trunk_group_identity` | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| `trunking_group_identity_information_outgoing_trunk_group_identity` | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| `network_call_reference_switch_identity` | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_italtel_ngn_tim'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| `type_of_call` | `_tipo_chamada` | `regexp_replace(_tipo_chamada, ' ', '_')`; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

Cobertura: 38 de 38 colunas finais documentadas; 19 com origem efetiva neste fluxo e 19 criadas nulas.

## 3.6 `transform_stfc_sigma_claro`

### Descrição

Catálogo: `stfc_sigma_claro` (`PARQUET_DEFAULT_SCHEMAS`). Layout Parquet Sigma Claro. O extrator remove hífens finais de todas as colunas mapeadas (`fill_char='-'`). As datas são compostas com `concat_ws` (que ignora nulos). Nenhuma etapa produz `tipo_chamada`.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRParquetExtractor` (`stfc_sigma_claro`): `regexp_replace('-+$', '')` em cada coluna mapeada; `format` vira `tipo_cdr`
→ `_resolve_input`
→ `data_hora` e `data_hora_fim` por `concat_ws(' ', ...)`
→ `_apply_standard_pipeline(df, 'ddMMyy HH:mm:ss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| `a_sequence_number` | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| `a_start_date`; `a_incoming_seizure_time` | `_data`, `_hora` | `concat_ws(' ', _data, _hora)` (componentes nulos são ignorados, sem anular o composto); `try_to_timestamp(..., 'ddMMyy HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| `a_start_date`; `a_call_release_time` | `_data`, `_hora_fim` | `concat_ws(' ', _data, _hora_fim)` (usa a data de início); se não nulo: `try_to_timestamp(..., 'ddMMyy HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`); se nulo: `data_hora` + `duracao` s | `dh_fim_chamada` |
| `a_duration_seconds` | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| `a_calling_party` | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| `a_calling_party` | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| `a_calling_party` | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| `a_called_party` | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| `a_called_party` | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| `a_called_party` | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| `a_call_class` | `resultado_chamada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| `a_incoming_route_name` | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| `a_outgoing_route_name` | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| `a_exchange_id` | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_sigma_claro'`); mantida sem alteração | `no_esquema` |
| `format` | `_tipo_cdr` → `tipo_cdr` | `add_source_metadata`: `_tipo_cdr` prevalece sobre `cdr_type` e sobre o caminho; `_tipo_cdr` é removida em seguida | `no_tipo_cdr` |
| metadado (caminho do arquivo ou parâmetro do extrator) | `arquivo_origem` | Mantida sem alteração (como a coluna existe, o nome não é inferido do caminho; se ausente na origem, fica `NULL`) | `no_arquivo_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **`tipo_chamada`:** não há coluna extraída nem derivação; `tipo_chamada` é criada nula e recebe `__NULL__` (chave primária), que também passa a ser o valor da partição `no_tipo_chamada`.

Cobertura: 38 de 38 colunas finais documentadas; 18 com origem efetiva neste fluxo e 20 criadas nulas.

## 3.7 `transform_stfc_huawei_ngn`

### Descrição

Catálogo: `stfc_huawei_ngn` (`TEXT_DEFAULT_SCHEMAS`). Layout CSV Huawei NGN (Algar/Claro), sem cabeçalho, delimitado por `,`. Compõe início e término, traduz os códigos de tipo e de resultado e copia o código de resposta SIP.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRTextExtractor` (`stfc_huawei_ngn`): seleção posicional + metadados
→ `_resolve_input`
→ `_concat_date_time` (início e término)
→ `tipo_chamada` e `resultado_chamada` por tabelas de códigos
→ `_apply_standard_pipeline(df, 'ddMMyyyy HHmmss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| campo CSV 0 (base 0) | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| campo CSV 3 (base 0); campo CSV 4 (base 0) | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; `try_to_timestamp(..., 'ddMMyyyy HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| campo CSV 5 (base 0); campo CSV 6 (base 0) | `_data_fim`, `_hora_fim` | `_concat_date_time`: `_data_fim` + espaço + `_hora_fim`; se não nulo: `try_to_timestamp(..., 'ddMMyyyy HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`); se nulo: `data_hora` + `duracao` s | `dh_fim_chamada` |
| campo CSV 7 (base 0) | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| campo CSV 8 (base 0) | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| campo CSV 8 (base 0) | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| campo CSV 8 (base 0) | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| campo CSV 9 (base 0) | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| campo CSV 9 (base 0) | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| campo CSV 9 (base 0) | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| campo CSV 22 (base 0) | `_resultado_chamada` | `'00'` → `caller party on-hook`; `'01'` → `called party on-hook`; `'02'` → `abnormal`; demais (inclusive nulo) → `unknown`; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| campo CSV 21 (base 0) | `codigo_resposta_sip` | Mantida sem alteração | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| campo CSV 17 (base 0) | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| campo CSV 18 (base 0) | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| campo CSV 1 (base 0) | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_huawei_ngn'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| campo CSV 19 (base 0) | `_tipo_chamada` | `'01'` → `intra_office`; `'02'` → `incoming_office`; `'03'` → `outgoing_office`; `'04'` → `tandem`; `'05'` → `new_service`; demais (inclusive nulo) → `unknown`; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

Cobertura: 38 de 38 colunas finais documentadas; 20 com origem efetiva neste fluxo e 18 criadas nulas.

## 3.8 `transform_stfc_huawei_ngn_tim`

### Descrição

Catálogo: `stfc_huawei_ngn_tim` (`TEXT_DEFAULT_SCHEMAS`). Layout CSV Huawei NGN TIM, sem cabeçalho, delimitado por `;`. Não há pré-processamento específico: o transformador apenas aplica o pipeline comum. As colunas `data_hora` e `data_hora_fim` já chegam compostas do CSV.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRTextExtractor` (`stfc_huawei_ngn_tim`)
→ `_resolve_input`
→ `_apply_standard_pipeline(df, 'yyMMddHHmmss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| campo CSV 18 (base 0) | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| campo CSV 29 (base 0) | `data_hora` | Mantida sem alteração; `try_to_timestamp(..., 'yyMMddHHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| campo CSV 30 (base 0) | `data_hora_fim` | Mantida sem alteração; se não nulo: `try_to_timestamp(..., 'yyMMddHHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`); se nulo: `data_hora` + `duracao` s | `dh_fim_chamada` |
| campo CSV 5 (base 0) | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| campo CSV 10 (base 0) | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| campo CSV 10 (base 0) | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| campo CSV 10 (base 0) | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| campo CSV 11 (base 0) | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| campo CSV 11 (base 0) | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| campo CSV 11 (base 0) | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| campo CSV 6 (base 0) | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| campo CSV 8 (base 0) | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| campo CSV 17 (base 0) | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_huawei_ngn_tim'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **Sem `resultado_chamada` e sem `tipo_chamada`:** nenhuma coluna extraída ou derivada alimenta essas colunas; ambas recebem `__NULL__` (chave primária), e `__NULL__` é o valor da partição `no_tipo_chamada`.

Cobertura: 38 de 38 colunas finais documentadas; 17 com origem efetiva neste fluxo e 21 criadas nulas.

## 3.9 `transform_stfc_fcdr_vivo`

### Descrição

Catálogo: `stfc_fcdr_vivo` (`TEXT_DEFAULT_SCHEMAS`). Layout CSV FCDR Vivo, sem cabeçalho, delimitado por `;`. O campo de posição 1 alimenta `tipo_cdr`. Traduz tipo e resultado da chamada; `_resultado_chamada` passa por `abs(cast int)` antes da tabela de códigos.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRTextExtractor` (`stfc_fcdr_vivo`); `_tipo_cdr` vira `tipo_cdr` e é removida
→ `_resolve_input`
→ `_concat_date_time` (somente início)
→ `_resultado_chamada` = `abs(cast int)`
→ `tipo_chamada` e `resultado_chamada` por tabelas de códigos
→ `_apply_standard_pipeline(df, 'ddMMyy HHmmss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| campo CSV 68 (base 0) | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| campo CSV 28 (base 0); campo CSV 25 (base 0) | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; `try_to_timestamp(..., 'ddMMyy HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_format_date_time` calcula `data_hora` + `duracao` s | `dh_fim_chamada` |
| campo CSV 27 (base 0) | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| campo CSV 78 (base 0) | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| campo CSV 78 (base 0) | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| campo CSV 78 (base 0) | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| campo CSV 22 (base 0) | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| campo CSV 22 (base 0) | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| campo CSV 22 (base 0) | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| campo CSV 24 (base 0) | `_resultado_chamada` | `abs(cast int)` e tabela de códigos (ver “Códigos de `_resultado_chamada`” abaixo); fora da tabela → `NULL`; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| campo CSV 33 (base 0) | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| campo CSV 34 (base 0) | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| campo CSV 0 (base 0) | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_fcdr_vivo'`); mantida sem alteração | `no_esquema` |
| campo CSV 1 (base 0) | `_tipo_cdr` → `tipo_cdr` | `add_source_metadata`: `_tipo_cdr` prevalece sobre `cdr_type` e sobre o caminho; `_tipo_cdr` é removida em seguida | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| campo CSV 2 (base 0) | `_tipo_chamada` | `'E'` → `Entrada`; `'S'` → `Saída`; `'T'` → `Transporte`; demais → `NULL`; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **Códigos de `_resultado_chamada`:** `0` → `Desconexão prematura do assinante A`; `1` → `Normal`; `2` → `Linha ocupada`; `3` → `Número Mudado`; `4` → `Congestionamento interno`; `5` → `Assinante Livre, sem Tarifação`; `6` → `Assinante livre, com Dupla Desconexão`; `7` → `Número inexistente, nível vago`; `8–15` → `Congestionamento interno`; `20–25` → `Congestionamento (outros tipos)`; demais → `NULL`.

Cobertura: 38 de 38 colunas finais documentadas; 18 com origem efetiva neste fluxo e 20 criadas nulas.

## 3.10 `transform_stfc_7n_oi`

### Descrição

Catálogo: `stfc_7n_oi` (`TEXT_DEFAULT_SCHEMAS`). Layout de largura fixa 7N Oi (`read_rdd=True`, `fill_char='-'`). Apenas linhas com `_tipo_registro == '<'` são mantidas. Cada campo é obtido por `substring(value, posição, largura)`, sem hífens finais e com `trim`.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRTextExtractor` (`stfc_7n_oi`): `read_text_with_source`, `substring`, `regexp_replace('-+$', '')`, `trim`, metadados e filtro `_tipo_registro == '<'`
→ `_resolve_input`
→ `_concat_date_time` (somente início)
→ `resultado_chamada` por tabela de códigos
→ `_apply_standard_pipeline(df, 'ddMMyyyy HHmmss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| posição 2, largura 32 (linha de largura fixa) | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| posição 194, largura 8 (linha de largura fixa); posição 202, largura 6 (linha de largura fixa) | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; `try_to_timestamp(..., 'ddMMyyyy HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_format_date_time` calcula `data_hora` + `duracao` s | `dh_fim_chamada` |
| posição 208, largura 6 (linha de largura fixa) | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| posição 34, largura 16 (linha de largura fixa) | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| posição 34, largura 16 (linha de largura fixa) | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| posição 34, largura 16 (linha de largura fixa) | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| posição 85, largura 30 (linha de largura fixa) | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| posição 85, largura 30 (linha de largura fixa) | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| posição 85, largura 30 (linha de largura fixa) | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| posição 247, largura 2 (linha de largura fixa) | `_resultado_chamada` | `'10'` → `Chamada completada`; `'31'` → `Não responde`; `'32'` → `linha ocupada`; `'44'` → `Congestionamento`; `'48'` → `Falha`; `'50'`, `'51'`, `'52'`, `'53'` → `Outros`; `'99'` → `Não identificado`; demais → `NULL`; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| posição 222, largura 8 (linha de largura fixa) | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| posição 235, largura 8 (linha de largura fixa) | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| posição 255, largura 4 (linha de largura fixa) | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_7n_oi'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **Sem `tipo_chamada`:** nenhuma coluna alimenta `tipo_chamada`; recebe `__NULL__` (chave primária) e define a partição `no_tipo_chamada=__NULL__`.

Cobertura: 38 de 38 colunas finais documentadas; 17 com origem efetiva neste fluxo e 21 criadas nulas.

## 3.11 `transform_stfc_tropico_oi`

### Descrição

Catálogo: `stfc_tropico_oi` (`TEXT_DEFAULT_SCHEMAS`). Layout de largura fixa Trópico Oi (`read_rdd=True`, `fill_char='-'`), filtrado por `_tipo_registro == '#'`. `_resultado_chamada` passa por `abs(cast int)` antes da tabela de códigos.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRTextExtractor` (`stfc_tropico_oi`): largura fixa, `fill_char`, metadados e filtro `_tipo_registro == '#'`
→ `_resolve_input`
→ `_concat_date_time` (somente início)
→ `_resultado_chamada` = `abs(cast int)`
→ `tipo_chamada` e `resultado_chamada` por tabelas de códigos
→ `_apply_standard_pipeline(df, 'ddMMyy HHmmss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| posição 9, largura 25 (linha de largura fixa) | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| posição 147, largura 6 (linha de largura fixa); posição 153, largura 6 (linha de largura fixa) | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; `try_to_timestamp(..., 'ddMMyy HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_format_date_time` calcula `data_hora` + `duracao` s | `dh_fim_chamada` |
| posição 159, largura 6 (linha de largura fixa) | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| posição 45, largura 16 (linha de largura fixa) | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| posição 45, largura 16 (linha de largura fixa) | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| posição 45, largura 16 (linha de largura fixa) | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| posição 79, largura 18 (linha de largura fixa) | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| posição 79, largura 18 (linha de largura fixa) | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| posição 79, largura 18 (linha de largura fixa) | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| posição 142, largura 3 (linha de largura fixa) | `_resultado_chamada` | `abs(cast int)` e tabela de códigos (ver “Códigos de `_resultado_chamada`” abaixo); fora da tabela → `NULL`; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| posição 191, largura 7 (linha de largura fixa) | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| posição 205, largura 7 (linha de largura fixa) | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| posição 175, largura 4 (linha de largura fixa) | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_tropico_oi'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| posição 173, largura 1 (linha de largura fixa) | `_tipo_chamada` | `0` → `Interna`; `1` → `Saída`; `2` → `Entrada`; demais → `NULL`; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **Códigos de `_resultado_chamada`** (comparados com literais string, p. ex. `'10'`): `10` → `Chamada completada com tarifação`; `20` → `Chamada completada sem tarifação`; `31` → `Não responde`; `32` → `linha ocupada`; `43` e `44` → `Congestionamento a frente`; `48` e `49` → `Falha tecnica`; `50` → `Desistencia pela origem`; `51` → `Assinante com Defeito ou Fora de Serviço`; `52` → `Acesso Barrado ou chamada rejeitada`; `53` → `Evento não especificado`; `54` → `Assinante com número mudado`; `55` → `Chamada não tarifada, sem o sinal de atendimento`; `56` → `Acesso inesistente ou número vago`; demais → `NULL`.

Cobertura: 38 de 38 colunas finais documentadas; 18 com origem efetiva neste fluxo e 20 criadas nulas.

## 3.12 `transform_stfc_axe_claro`

### Descrição

Catálogo: `stfc_axe_claro` (`TEXT_DEFAULT_SCHEMAS`). Layout de largura fixa AXE Claro, lido por `spark.read.csv` com `schema='value string'`. Compõe início e término com a mesma data (`_data`), traduz o tipo de chamada e classifica o resultado por código.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRTextExtractor` (`stfc_axe_claro`): `substring(value, posição, largura)` + `trim` + metadados
→ `_resolve_input`
→ `_concat_date_time(start_date='_data', start_time='_hora', stop_date='_data', stop_time='_hora_fim')`
→ `tipo_chamada` por código; `_resultado_chamada` → inteiro; `resultado_chamada` por código
→ `_apply_standard_pipeline(df, 'yyMMdd HHmmss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| posição 142, largura 8 (linha de largura fixa) | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| posição 80, largura 6 (linha de largura fixa); posição 62, largura 6 (linha de largura fixa) | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; `try_to_timestamp(..., 'yyMMdd HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| posição 80, largura 6 (linha de largura fixa); posição 68, largura 6 (linha de largura fixa) | `_data`, `_hora_fim` | `_concat_date_time`: `_data` + espaço + `_hora_fim` (usa a data de início); se não nulo: `try_to_timestamp(..., 'yyMMdd HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`); se nulo: `data_hora` + `duracao` s | `dh_fim_chamada` |
| posição 74, largura 6 (linha de largura fixa) | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| posição 15, largura 18 (linha de largura fixa) | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| posição 15, largura 18 (linha de largura fixa) | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| posição 15, largura 18 (linha de largura fixa) | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| posição 36, largura 24 (linha de largura fixa) | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| posição 36, largura 24 (linha de largura fixa) | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| posição 36, largura 24 (linha de largura fixa) | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| posição 60, largura 2 (linha de largura fixa) | `_resultado_chamada` | `cast int` e tabela de códigos (ver “Códigos de `_resultado_chamada`” abaixo); qualquer valor fora das faixas (inclusive nulo) → `Desconexão Prematura`; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| posição 92, largura 4 (linha de largura fixa) | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| posição 103, largura 4 (linha de largura fixa) | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| posição 2, largura 8 (linha de largura fixa) | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_axe_claro'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| posição 13, largura 2 (linha de largura fixa) | `_tipo_chamada` | tabela de códigos `01`–`0B` (ver “Códigos de `_tipo_chamada`” abaixo); demais → `NULL`; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **Códigos de `_tipo_chamada`:** `01` → `POTS_efetiva`; `02` → `POTS_inefetiva`; `03` → `RDSI_efetiva`; `04` → `RDSI_inefetiva`; `05` → `redirecionada_efetiva`; `06` → `redirecionada_inefetiva`; `07` → `Procedimento_de_assinante`; `08` → `Evento_não_relativo_a_chamada`; `09` → `Comando_de_serviço_de_assinante`; `0A` → `RDSI_E_efetiva`; `0B` → `RDSI_E_inefetiva`.
- **Códigos de `_resultado_chamada`** (inteiro): `1` → `Assinante Livre com Tarifação`; `2` → `Assinante Ocupado`; `3` → `Assinante com Número Mudado`; `4` → `Reserva`; `5` → `Assinante Livre com Tarifação`; `6` → `Assinante Livre Com Tarifação Retenção Ass. B`; `7` → `Número inexistente`; `8` → `Número com defeito`; `9–19` → `Reserva`; `20` → `Temporização na Entrada  ( CO0 )`; `21` → `Falha  de Sinalização na Entrada  ( CO0 )`; `22–23` → `Reserva`; `24` → `Congestionamento no Destino ( CO2 )`; `25` → `Reserva`; `26` → `Congestionamento Interno   ( CO1 )`; `27` → `Falha Interna na Central   ( CO1 )`; `28` → `Temporização na Saída   ( CO3 )`; `29` → `Falha de Sinalização na Saída (CO3 )`; `30–99` → `Reserva`; demais (inclusive `0`, `>= 100` e nulo) → `Desconexão Prematura`.

Cobertura: 38 de 38 colunas finais documentadas; 19 com origem efetiva neste fluxo e 19 criadas nulas.

## 3.13 `transform_stfc_pcl_claro`

### Descrição

Catálogo: `stfc_pcl_claro` (`TEXT_DEFAULT_SCHEMAS`). Layout de largura fixa PCL Claro, lido por `spark.read.csv` com `schema='value string'`. Preserva o código de tipo de chamada e rotula apenas o resultado `'3'`.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRTextExtractor` (`stfc_pcl_claro`): `substring(value, posição, largura)` + `trim` + metadados
→ `_resolve_input`
→ `_concat_date_time` (somente início)
→ `tipo_chamada` = cast string de `_tipo_chamada`; `resultado_chamada` somente para `'3'`
→ `_apply_standard_pipeline(df, 'yyyyMMdd HHmmss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| posição 519, largura 10 (linha de largura fixa) | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| posição 499, largura 10 (linha de largura fixa) | `referencia_sip` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| posição 90, largura 8 (linha de largura fixa); posição 98, largura 6 (linha de largura fixa) | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; `try_to_timestamp(..., 'yyyyMMdd HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_format_date_time` calcula `data_hora` + `duracao` s | `dh_fim_chamada` |
| posição 110, largura 6 (linha de largura fixa) | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| posição 116, largura 22 (linha de largura fixa) | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| posição 116, largura 22 (linha de largura fixa) | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| posição 116, largura 22 (linha de largura fixa) | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| posição 138, largura 28 (linha de largura fixa) | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| posição 138, largura 28 (linha de largura fixa) | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| posição 138, largura 28 (linha de largura fixa) | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| posição 550, largura 1 (linha de largura fixa) | `_resultado_chamada` | `'3'` → `b-AnswerHasBeenReceived`; demais → `NULL`; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| posição 297, largura 8 (linha de largura fixa) | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| posição 313, largura 8 (linha de largura fixa) | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| posição 24, largura 8 (linha de largura fixa) | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_pcl_claro'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| posição 32, largura 2 (linha de largura fixa) | `_tipo_chamada` | `cast string` do código recebido, sem rótulo de domínio; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **Colunas extraídas sem consumo:** `tecnologia_fornecedor` não pertence a `TARGET_SCHEMA` e é descartada.
- **`referencia` e `referencia_sip`:** ambas são extraídas de posições distintas (no catálogo, `referencia_sip` está comentada como Nokia e `referencia` como Ericsson); nenhuma etapa escolhe entre elas.

Cobertura: 38 de 38 colunas finais documentadas; 19 com origem efetiva neste fluxo e 19 criadas nulas.

## 3.14 `transform_stfc_tropico_claro_lega`

### Descrição

Catálogo: `stfc_tropico_claro_lega` (`TEXT_DEFAULT_SCHEMAS`). Layout CSV Trópico Claro LEGA (`,`, sem cabeçalho). Usa `transform_stfc_tropico_claro` (alias `transform_stfc_tropico_claro_lega`). Difere da LEGB apenas nas posições de `rota_entrada` (17), `rota_saida` (18) e `referencia` (42).

### Fluxo resumido

```text
Arquivo de origem
→ `CDRTextExtractor`: seleção posicional + metadados
→ `_resolve_input`
→ `_concat_date_time` (início e término)
→ `tipo_chamada` = literal `'tandem'`; `resultado_chamada` por tabela de códigos de três dígitos
→ `_apply_standard_pipeline(df, 'ddMMyyyy HHmmss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| campo CSV 42 (base 0) | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| campo CSV 3 (base 0); campo CSV 4 (base 0) | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; `try_to_timestamp(..., 'ddMMyyyy HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| campo CSV 5 (base 0); campo CSV 6 (base 0) | `_data_fim`, `_hora_fim` | `_concat_date_time`: `_data_fim` + espaço + `_hora_fim`; se não nulo: `try_to_timestamp(..., 'ddMMyyyy HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`); se nulo: `data_hora` + `duracao` s | `dh_fim_chamada` |
| campo CSV 7 (base 0) | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| campo CSV 8 (base 0) | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| campo CSV 8 (base 0) | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| campo CSV 8 (base 0) | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| campo CSV 9 (base 0) | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| campo CSV 9 (base 0) | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| campo CSV 9 (base 0) | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| campo CSV 2 (base 0) | `_resultado_chamada` | tabela de códigos de três dígitos (ver “Códigos de `_resultado_chamada`” abaixo); qualquer outro valor (inclusive nulo) → `unknown`; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| campo CSV 25 (base 0) | `codigo_resposta_sip` | Mantida sem alteração | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| campo CSV 17 (base 0) | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| campo CSV 18 (base 0) | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| campo CSV 1 (base 0) | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_tropico_claro_lega'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| Não aplicável (constante) | Não aplicável | constante `'tandem'` (`F.lit`), sem origem em coluna; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **Códigos de `_resultado_chamada`:** `003` e `006` → `chamada com atendimento e desligamento lado A ou B`; `013` → `assinante B não atendeu`; `014` → `assinante B ocupado`; `015` → `número mudado`; `016` → `assinante B fora de serviço`; `017` → `unknown destination`; `018` → `denied acess`; `019` → `route failure`; `020` → `congestion`; `021` → `tecnical fail`; `022` → `dest congestion`; `023` → `signal`; `024` → `controller restart`; `025` → `application intervation`; `026` → `calling party idle timeout`; `027` → `calling party abandon`; `028` → `internal error`; `029` → `unmapped event`; `030` → `error application intervation`; `031` → `demais causas`; `032` → `normal disconnection`; `033` → `erro atuação aplicação`; `102` e `104` → `chamada que não atingiu a fase de conversação`; `201` e `202` → `fatia da chamada`; demais → `unknown`.
- **Alias:** `transform_stfc_tropico_claro_lega` e `transform_stfc_tropico_claro_legb` apontam para `transform_stfc_tropico_claro`. O método `transform_stfc_tropico_claro` não é chave de nenhum catálogo de extração.

Cobertura: 38 de 38 colunas finais documentadas; 20 com origem efetiva neste fluxo e 18 criadas nulas.

## 3.15 `transform_stfc_tropico_claro_legb`

### Descrição

Catálogo: `stfc_tropico_claro_legb` (`TEXT_DEFAULT_SCHEMAS`). Layout CSV Trópico Claro LEGB (`,`, sem cabeçalho). Usa `transform_stfc_tropico_claro` (alias `transform_stfc_tropico_claro_legb`). Difere da LEGA nas posições de `rota_entrada` (14), `rota_saida` (15) e `referencia` (45).

### Fluxo resumido

```text
Arquivo de origem
→ `CDRTextExtractor`: seleção posicional + metadados
→ `_resolve_input`
→ `_concat_date_time` (início e término)
→ `tipo_chamada` = literal `'tandem'`; `resultado_chamada` por tabela de códigos de três dígitos
→ `_apply_standard_pipeline(df, 'ddMMyyyy HHmmss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| campo CSV 45 (base 0) | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| campo CSV 3 (base 0); campo CSV 4 (base 0) | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; `try_to_timestamp(..., 'ddMMyyyy HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| campo CSV 5 (base 0); campo CSV 6 (base 0) | `_data_fim`, `_hora_fim` | `_concat_date_time`: `_data_fim` + espaço + `_hora_fim`; se não nulo: `try_to_timestamp(..., 'ddMMyyyy HHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`); se nulo: `data_hora` + `duracao` s | `dh_fim_chamada` |
| campo CSV 7 (base 0) | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| campo CSV 8 (base 0) | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| campo CSV 8 (base 0) | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| campo CSV 8 (base 0) | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| campo CSV 9 (base 0) | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| campo CSV 9 (base 0) | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| campo CSV 9 (base 0) | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| campo CSV 2 (base 0) | `_resultado_chamada` | tabela de códigos de três dígitos (ver “Códigos de `_resultado_chamada`” abaixo); qualquer outro valor (inclusive nulo) → `unknown`; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| campo CSV 25 (base 0) | `codigo_resposta_sip` | Mantida sem alteração | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| campo CSV 14 (base 0) | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| campo CSV 15 (base 0) | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| campo CSV 1 (base 0) | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_tropico_claro_legb'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| Não aplicável (constante) | Não aplicável | constante `'tandem'` (`F.lit`), sem origem em coluna; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **Códigos de `_resultado_chamada`:** `003` e `006` → `chamada com atendimento e desligamento lado A ou B`; `013` → `assinante B não atendeu`; `014` → `assinante B ocupado`; `015` → `número mudado`; `016` → `assinante B fora de serviço`; `017` → `unknown destination`; `018` → `denied acess`; `019` → `route failure`; `020` → `congestion`; `021` → `tecnical fail`; `022` → `dest congestion`; `023` → `signal`; `024` → `controller restart`; `025` → `application intervation`; `026` → `calling party idle timeout`; `027` → `calling party abandon`; `028` → `internal error`; `029` → `unmapped event`; `030` → `error application intervation`; `031` → `demais causas`; `032` → `normal disconnection`; `033` → `erro atuação aplicação`; `102` e `104` → `chamada que não atingiu a fase de conversação`; `201` e `202` → `fatia da chamada`; demais → `unknown`.
- **Alias:** `transform_stfc_tropico_claro_lega` e `transform_stfc_tropico_claro_legb` apontam para `transform_stfc_tropico_claro`. O método `transform_stfc_tropico_claro` não é chave de nenhum catálogo de extração.

Cobertura: 38 de 38 colunas finais documentadas; 20 com origem efetiva neste fluxo e 18 criadas nulas.

## 3.16 `transform_stfc_pit_claro`

### Descrição

Catálogo: `stfc_pit_claro` (`TEXT_DEFAULT_SCHEMAS`). Layout CSV PIT Claro, lido com o `StructType` `stfc_pit_claro_schema` (110 campos); apenas registros com `_tipo_registro == '5'` são mantidos. Trunca as datas em 14 caracteres e deriva o tipo de chamada da prestadora de origem.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRTextExtractor` (`stfc_pit_claro`): `spark.read.csv` com `schema=stfc_pit_claro_schema`, seleção por índice, metadados e filtro `_tipo_registro == '5'`
→ `_resolve_input`
→ `data_hora` e `data_hora_fim` = `substring(..., 1, 14)`
→ `tipo_chamada` por `_prestadora_origem`
→ `_apply_standard_pipeline(df, 'yyyyMMddHHmmss')`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| `networkcallreference` (campo 42 de `stfc_pit_claro_schema`) | `referencia` | Mantida sem alteração; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `greatest(NULL, MIN_SAFE_DATE)` → `MIN_SAFE_DATE` | `dh_referencia` |
| `setuptime` (campo 43 de `stfc_pit_claro_schema`) | `_data_hora` | `substring(_data_hora, 1, 14)`; `try_to_timestamp(..., 'yyyyMMddHHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| `releasetime` (campo 9 de `stfc_pit_claro_schema`) | `_data_hora_fim` | `substring(_data_hora_fim, 1, 14)`; se não nulo: `try_to_timestamp(..., 'yyyyMMddHHmmss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`); se nulo: `data_hora` + `duracao` s | `dh_fim_chamada` |
| `callduration` (campo 10 de `stfc_pit_claro_schema`) | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| `callingnumber` (campo 4 de `stfc_pit_claro_schema`) | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| `callingnumber` (campo 4 de `stfc_pit_claro_schema`) | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| `callingnumber` (campo 4 de `stfc_pit_claro_schema`) | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| `callednumber` (campo 5 de `stfc_pit_claro_schema`) | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| `callednumber` (campo 5 de `stfc_pit_claro_schema`) | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| `callednumber` (campo 5 de `stfc_pit_claro_schema`) | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| `causeforterm` (campo 11 de `stfc_pit_claro_schema`) | `resultado_chamada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| `mscincomingroute` (campo 2 de `stfc_pit_claro_schema`) | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| `mscoutgoingroute` (campo 3 de `stfc_pit_claro_schema`) | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| `recordingentity` (campo 1 de `stfc_pit_claro_schema`) | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_pit_claro'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| `originatingoperator` (campo 108 de `stfc_pit_claro_schema`) | `_prestadora_origem` | `_prestadora_origem == 'BRAEBT'` → `Originating`; qualquer outro valor (inclusive nulo) → `Terminating`; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

Cobertura: 38 de 38 colunas finais documentadas; 19 com origem efetiva neste fluxo e 19 criadas nulas.

## 3.17 `transform_stfc_ss8bf_claro`

### Descrição

Catálogo: `stfc_ss8bf_claro` (`TEXT_DEFAULT_SCHEMAS`). Layout CSV SS8BF Claro, lido com o `StructType` `stfc_ss8bf_claro_schema` (222 campos); registros com `start_time` (`_data_hora`) nulo são descartados. `record_id` (`_referencia`) alimenta tanto `referencia` quanto `data_hora_referencia`.

### Fluxo resumido

```text
Arquivo de origem
→ `CDRTextExtractor` (`stfc_ss8bf_claro`): `spark.read.csv` com `schema=stfc_ss8bf_claro_schema`, seleção por índice, metadados e filtro `_data_hora is not null`
→ `_resolve_input`
→ `data_hora` = `substring(_data_hora, 1, 19)`; `data_hora_referencia` = `substring(_referencia, 1, 19)`; `referencia` = `substring(_referencia, 52, 8)`
→ `resultado_chamada` por código; `tipo_chamada` por identificadores e encaminhamento
→ `_apply_standard_pipeline(df, "yyyy-MM-dd'T'HH:mm:ss")`
→ `_write_parquet`
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| `record_id` (campo 5 de `stfc_ss8bf_claro_schema`) | `_referencia` | `substring(_referencia, 52, 8)`; nulo → `__NULL__` (chave) | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns` → `__NULL__` (chave) | `nu_referencia_sip` |
| `record_id` (campo 5 de `stfc_ss8bf_claro_schema`) | `_referencia` | `substring(_referencia, 1, 19)`; `try_to_timestamp(..., 'yyyy-MM-dd'T'HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_referencia` |
| `start_time` (campo 2 de `stfc_ss8bf_claro_schema`) | `_data_hora` | `substring(_data_hora, 1, 19)`; `try_to_timestamp(..., 'yyyy-MM-dd'T'HH:mm:ss')` e `greatest(..., MIN_SAFE_DATE)` (nulo/inválido → `MIN_SAFE_DATE`) | `dh_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_format_date_time` calcula `data_hora` + `duracao` s | `dh_fim_chamada` |
| `duration_of_call` (campo 3 de `stfc_ss8bf_claro_schema`) | `duracao` | Mantida sem alteração; `cast int` e `coalesce(..., 0)` (chave) | `qt_duracao_segundos` |
| `originating_number_calling_party` (campo 11 de `stfc_ss8bf_claro_schema`) | `numero_origem` | `numero_origem` → `_extract_numbers` (`extract_number`) → `numero_origem_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_origem` |
| `originating_number_calling_party` (campo 11 de `stfc_ss8bf_claro_schema`) | `numero_origem` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_origem)` (mesma entrada de `nu_origem`) | `ic_origem_valido` |
| `originating_number_calling_party` (campo 11 de `stfc_ss8bf_claro_schema`) | `numero_origem` | `numero_origem` sem restauração (não há `_numero_origem_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_origem_original` |
| `terminating_number_called_party` (campo 10 de `stfc_ss8bf_claro_schema`) | `numero_destino` | `numero_destino` → `_extract_numbers` (`extract_number`) → `numero_destino_extraido`; entrada vazia → `5599999999999`; extração sem sucesso → entrada original; nulo → `__NULL__` (chave) | `nu_destino` |
| `terminating_number_called_party` (campo 10 de `stfc_ss8bf_claro_schema`) | `numero_destino` | campo `extracao_sucesso` do struct de `_extract_numbers(numero_destino)` (mesma entrada de `nu_destino`) | `ic_destino_valido` |
| `terminating_number_called_party` (campo 10 de `stfc_ss8bf_claro_schema`) | `numero_destino` | `numero_destino` sem restauração (não há `_numero_destino_original`): mantida sem alteração; nulo → `__NULL__` (chave) | `nu_destino_original` |
| `attempt_indicator` (campo 17 de `stfc_ss8bf_claro_schema`) | `_resultado_chamada` | `'0'`–`'9'` → mensagens `Call was ...` (ver “Códigos de `_resultado_chamada`” abaixo); demais → `NULL`; nulo → `__NULL__` (chave) | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `co_resposta_sip` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; `_add_tn_validation_status` mantém `NULL` (sem `_autenticacao`) | `no_autenticacao` |
| Não aplicável (metadado) | `prestadora` | `add_source_metadata`: argumento `operator` ou `url_decode` do segmento `-4` do caminho; mantida sem alteração | `no_prestadora` |
| `ingress_sip_endpoint_address` (campo 125 de `stfc_ss8bf_claro_schema`) | `rota_entrada` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_entrada` |
| `egress_sip_endpoint_address` (campo 126 de `stfc_ss8bf_claro_schema`) | `rota_saida` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_rota_saida` |
| `switch_id` (campo 4 de `stfc_ss8bf_claro_schema`) | `bilhetador` | Mantida sem alteração; nulo → `__NULL__` (chave) | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_tecnologia_celula_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula por `_fill_missing_columns`; permanece `NULL` | `no_agente_usuario` |
| Não aplicável (constante do extrator) | `esquema` | `add_source_metadata`: literal com a chave `cdr_schema` (`'stfc_ss8bf_claro'`); mantida sem alteração | `no_esquema` |
| Não aplicável (metadado) | `tipo_cdr` | `add_source_metadata`: argumento `cdr_type` ou `url_decode` do segmento `-2` do caminho; mantida sem alteração | `no_tipo_cdr` |
| Não aplicável (metadado) | `arquivo_origem` | `add_source_metadata`: `url_decode` do segmento `-1` do caminho; mantida sem alteração | `no_arquivo_origem` |
| `originating_party_identifier` (campo 39 de `stfc_ss8bf_claro_schema`); `terminating_party_identifier` (campo 40 de `stfc_ss8bf_claro_schema`); `forwarded_from_dn` (campo 64 de `stfc_ss8bf_claro_schema`) | `_identificador_origem`, `_identificador_destino`, `_numero_encaminhado` | `originating == '900'` → `Originating`; `'901'` + destino `'902'`/`'903'` + `_numero_encaminhado` não nulo → `Forwarding`; `'901'` + `'902'` sem encaminhamento → `Terminating`; `'901'` + `'903'` sem encaminhamento → `Transit`; demais → `Unknown`; nulo → `__NULL__` (chave) | `no_tipo_chamada` |

### Regras e tabelas específicas do fluxo

- **Códigos de `_resultado_chamada`:** `0` → `Call was completed`; `1` → `Call was not completed due to called party busy`; `2` → `Call was not completed due to invalid dialed number`; `3` → `Call was not completed due to lack of available lines/trunks to complete the call`; `4` → `Call was not completed due to calling party aborting the call prior to answer`; `5` → `Call was not completed due to called party not answering the call`; `6` → `Call was not completed due to a network problem`; `7` → `Call was not completed due to unknown reasons`; `8` → `Call was not completed due to no subscriber account`; `9` → `Call was not completed due to unauthorized subscriber`; demais → `NULL`.
- **Máscara de `data_hora_referencia`:** como `ref_date_time_fmt` não é informado, a mesma máscara de `data_hora` é usada.

Cobertura: 38 de 38 colunas finais documentadas; 19 com origem efetiva neste fluxo e 19 criadas nulas.

# 4. Múltiplos caminhos para colunas finais

As tabelas abaixo comparam, por coluna final, a origem e o tratamento em cada um dos 17 fluxos. Uma origem
identificada em um fluxo não é universal: cada linha vale somente para o fluxo indicado. Os textos de tratamento são os
mesmos da seção 3; as regras comuns da seção 2.2 (máscaras de data, `MIN_SAFE_DATE`, sentinelas da chave, cast final) valem para todos os fluxos
e não são repetidas aqui.

### `nu_referencia_sip`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_huawei_volte_tim` | `referencia_sip` | Mantida sem alteração |
| `smp_ericsson_volte_vivo` | `referencia_sip` | Mantida sem alteração |
| `stfc_pcl_claro` | `referencia_sip` | Mantida sem alteração |

Fluxos sem origem (coluna criada nula): `smp_ericsson_gsm`, `smp_nokia_gsm`, `stfc_italtel_ngn_tim`, `stfc_sigma_claro`, `stfc_huawei_ngn`, `stfc_huawei_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_axe_claro`, `stfc_tropico_claro_lega`, `stfc_tropico_claro_legb`, `stfc_pit_claro`, `stfc_ss8bf_claro`.

### `dh_referencia`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_nokia_gsm` | `data_hora_referencia` | Mantida sem alteração |
| `stfc_italtel_ngn_tim` | `data_hora_referencia` | Mantida sem alteração |
| `stfc_ss8bf_claro` | `_referencia` | `substring(_referencia, 1, 19)` |

Fluxos sem origem (coluna criada nula): `smp_ericsson_gsm`, `smp_huawei_volte_tim`, `smp_ericsson_volte_vivo`, `stfc_sigma_claro`, `stfc_huawei_ngn`, `stfc_huawei_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_axe_claro`, `stfc_pcl_claro`, `stfc_tropico_claro_lega`, `stfc_tropico_claro_legb`, `stfc_pit_claro`.

### `dh_chamada`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_ericsson_gsm` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; nulo se algum componente for nulo ou em branco |
| `smp_nokia_gsm` | `data_hora_alocacao_canal`, `data_hora_referencia` | `coalesce(data_hora_alocacao_canal, data_hora_referencia)` |
| `smp_huawei_volte_tim` | `data_hora` | `left(data_hora, 19)` (remove o sufixo de fuso) |
| `smp_ericsson_volte_vivo` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora`; nulo se algum componente for nulo ou em branco |
| `stfc_italtel_ngn_tim` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora` |
| `stfc_sigma_claro` | `_data`, `_hora` | `concat_ws(' ', _data, _hora)` (componentes nulos são ignorados, sem anular o composto) |
| `stfc_huawei_ngn` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora` |
| `stfc_huawei_ngn_tim` | `data_hora` | Mantida sem alteração |
| `stfc_fcdr_vivo` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora` |
| `stfc_7n_oi` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora` |
| `stfc_tropico_oi` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora` |
| `stfc_axe_claro` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora` |
| `stfc_pcl_claro` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora` |
| `stfc_tropico_claro_lega` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora` |
| `stfc_tropico_claro_legb` | `_data`, `_hora` | `_concat_date_time`: `_data` + espaço + `_hora` |
| `stfc_pit_claro` | `_data_hora` | `substring(_data_hora, 1, 14)` |
| `stfc_ss8bf_claro` | `_data_hora` | `substring(_data_hora, 1, 19)` |

### `dh_fim_chamada`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_ericsson_gsm` | `_data`, `_hora_fim` | `_concat_date_time(stop_date='_data')`: `_data` + espaço + `_hora_fim` (a data de início é usada também no término) |
| `smp_nokia_gsm` | `data_hora_fim`, `data_hora_desconexao`, `tipo_chamada` | se `data_hora_desconexao` existe: `tipo_chamada == 'UCA'` → `coalesce(data_hora_desconexao, data_hora_fim)`; demais tipos → `data_hora_fim` |
| `smp_huawei_volte_tim` | `data_hora_fim` | `left(data_hora_fim, 19)` |
| `smp_ericsson_volte_vivo` | `_data`, `_hora_fim` | `_concat_date_time(stop_date='_data')`: `_data` + espaço + `_hora_fim` |
| `stfc_sigma_claro` | `_data`, `_hora_fim` | `concat_ws(' ', _data, _hora_fim)` (usa a data de início) |
| `stfc_huawei_ngn` | `_data_fim`, `_hora_fim` | `_concat_date_time`: `_data_fim` + espaço + `_hora_fim` |
| `stfc_huawei_ngn_tim` | `data_hora_fim` | Mantida sem alteração |
| `stfc_axe_claro` | `_data`, `_hora_fim` | `_concat_date_time`: `_data` + espaço + `_hora_fim` (usa a data de início) |
| `stfc_tropico_claro_lega` | `_data_fim`, `_hora_fim` | `_concat_date_time`: `_data_fim` + espaço + `_hora_fim` |
| `stfc_tropico_claro_legb` | `_data_fim`, `_hora_fim` | `_concat_date_time`: `_data_fim` + espaço + `_hora_fim` |
| `stfc_pit_claro` | `_data_hora_fim` | `substring(_data_hora_fim, 1, 14)` |

Fluxos sem origem (coluna criada nula): `stfc_italtel_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_pcl_claro`, `stfc_ss8bf_claro`.

### `qt_duracao_segundos`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_ericsson_gsm` | `duracao` | `HH:mm:ss` → `substring(1,2)*3600 + substring(4,2)*60 + substring(7,2)`; nulo → `0` |
| `smp_nokia_gsm` | `_duracao_orig_mcz`, `_duracao_term_mcz`, `_duracao_forw_mcz`, `_duracao_roam_mcz`, `_duracao_iaz`, `_duracao_oaz`, `_duracao_tarifavel`, `_duracao_banda_tarifavel` | colunas cujo nome começa com `_duracao`: valor `FFFFFF` → nulo; `coalesce` na ordem das colunas do DataFrame |
| `smp_huawei_volte_tim` | `duracao` | Mantida sem alteração |
| `smp_ericsson_volte_vivo` | `duracao` | Mantida sem alteração |
| `stfc_italtel_ngn_tim` | `duracao` | Mantida sem alteração |
| `stfc_sigma_claro` | `duracao` | Mantida sem alteração |
| `stfc_huawei_ngn` | `duracao` | Mantida sem alteração |
| `stfc_huawei_ngn_tim` | `duracao` | Mantida sem alteração |
| `stfc_fcdr_vivo` | `duracao` | Mantida sem alteração |
| `stfc_7n_oi` | `duracao` | Mantida sem alteração |
| `stfc_tropico_oi` | `duracao` | Mantida sem alteração |
| `stfc_axe_claro` | `duracao` | Mantida sem alteração |
| `stfc_pcl_claro` | `duracao` | Mantida sem alteração |
| `stfc_tropico_claro_lega` | `duracao` | Mantida sem alteração |
| `stfc_tropico_claro_legb` | `duracao` | Mantida sem alteração |
| `stfc_pit_claro` | `duracao` | Mantida sem alteração |
| `stfc_ss8bf_claro` | `duracao` | Mantida sem alteração |

### `nu_origem_original`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_ericsson_gsm` | `numero_origem` | Mantida sem alteração |
| `smp_nokia_gsm` | `numero_origem`, `numero_origem_original` | valor pré-processado: `coalesce(numero_origem, numero_origem_original)` |
| `smp_huawei_volte_tim` | `_numero_origem_ats_ibcf`, `_tipo_cdr` | `_numero_origem_original`: ATS → valor bruto de `$[0].tEL-URI`; demais tipos → o `numero_origem` calculado acima; restaurada em `numero_origem` após a extração |
| `smp_ericsson_volte_vivo` | `_numero_origem_original` | `_numero_origem_original` (texto completo, inclusive `;verstat=...`) restaurada em `numero_origem` após a extração |
| `stfc_italtel_ngn_tim` | `numero_origem` | Mantida sem alteração |
| `stfc_sigma_claro` | `numero_origem` | Mantida sem alteração |
| `stfc_huawei_ngn` | `numero_origem` | Mantida sem alteração |
| `stfc_huawei_ngn_tim` | `numero_origem` | Mantida sem alteração |
| `stfc_fcdr_vivo` | `numero_origem` | Mantida sem alteração |
| `stfc_7n_oi` | `numero_origem` | Mantida sem alteração |
| `stfc_tropico_oi` | `numero_origem` | Mantida sem alteração |
| `stfc_axe_claro` | `numero_origem` | Mantida sem alteração |
| `stfc_pcl_claro` | `numero_origem` | Mantida sem alteração |
| `stfc_tropico_claro_lega` | `numero_origem` | Mantida sem alteração |
| `stfc_tropico_claro_legb` | `numero_origem` | Mantida sem alteração |
| `stfc_pit_claro` | `numero_origem` | Mantida sem alteração |
| `stfc_ss8bf_claro` | `numero_origem` | Mantida sem alteração |

`nu_origem` (`numero_origem_extraido`) e `ic_origem_valido` derivam sempre da coluna `numero_origem` **antes** da restauração; `nu_origem_original` recebe o valor restaurado quando há `_numero_origem_original` (Huawei TIM e Ericsson VoLTE Vivo) ou o valor pré-processado nos demais fluxos. As mesmas regras valem, espelhadas, para `numero_destino` (restauração de `_numero_destino_original` somente no Huawei TIM).

### `nu_destino_original`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_ericsson_gsm` | `numero_destino` | Mantida sem alteração |
| `smp_nokia_gsm` | `numero_destino`, `numero_origem_encaminhamento` | valor pré-processado: `tipo_chamada == 'FORW'` → `numero_origem_encaminhamento`; demais → `numero_destino` |
| `smp_huawei_volte_tim` | `_numero_destino_ats`, `_numero_destino_ibcf`, `_tipo_cdr` | `_numero_destino_original`: ATS → `_numero_destino_ats` bruto; demais tipos → o `numero_destino` calculado; restaurada em `numero_destino` após a extração |
| `smp_ericsson_volte_vivo` | `numero_destino` | Mantida sem alteração |
| `stfc_italtel_ngn_tim` | `numero_destino` | Mantida sem alteração |
| `stfc_sigma_claro` | `numero_destino` | Mantida sem alteração |
| `stfc_huawei_ngn` | `numero_destino` | Mantida sem alteração |
| `stfc_huawei_ngn_tim` | `numero_destino` | Mantida sem alteração |
| `stfc_fcdr_vivo` | `numero_destino` | Mantida sem alteração |
| `stfc_7n_oi` | `numero_destino` | Mantida sem alteração |
| `stfc_tropico_oi` | `numero_destino` | Mantida sem alteração |
| `stfc_axe_claro` | `numero_destino` | Mantida sem alteração |
| `stfc_pcl_claro` | `numero_destino` | Mantida sem alteração |
| `stfc_tropico_claro_lega` | `numero_destino` | Mantida sem alteração |
| `stfc_tropico_claro_legb` | `numero_destino` | Mantida sem alteração |
| `stfc_pit_claro` | `numero_destino` | Mantida sem alteração |
| `stfc_ss8bf_claro` | `numero_destino` | Mantida sem alteração |

### `no_resultado_chamada`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_ericsson_gsm` | `status_chamada` | `callPosition` é extraída como `status_chamada`, mas nenhuma etapa a copia para `resultado_chamada`; `status_chamada` não pertence a `TARGET_SCHEMA` e é descartada. `resultado_chamada` é criada nula |
| `smp_nokia_gsm` | `_resultado_chamada` | faixas numéricas de `_resultado_chamada` (ver “Faixas de `_resultado_chamada`” abaixo); fora das faixas → `NULL` |
| `smp_huawei_volte_tim` | `_resultado_chamada`, `_tipo_cdr` | `_resultado_chamada` inteiro (IBCF: `regexp_extract('SIP;cause=([0-9]+);', 1)`; demais: cast direto) e tabela de códigos (ver “Códigos de `_resultado_chamada`” abaixo) |
| `smp_ericsson_volte_vivo` | `_resultado_chamada` | `'1'` → `callHasReachedCongestionOrBusyState`; `'2'` → `callHasOnlyReachedThroughConnection`; `'3'` → `b-AnswerHasBeenReceived`; outro valor → mantém o valor de `_resultado_chamada` |
| `stfc_italtel_ngn_tim` | `resultado_chamada` | Mantida sem alteração |
| `stfc_sigma_claro` | `resultado_chamada` | Mantida sem alteração |
| `stfc_huawei_ngn` | `_resultado_chamada` | `'00'` → `caller party on-hook`; `'01'` → `called party on-hook`; `'02'` → `abnormal`; demais (inclusive nulo) → `unknown` |
| `stfc_fcdr_vivo` | `_resultado_chamada` | `abs(cast int)` e tabela de códigos (ver “Códigos de `_resultado_chamada`” abaixo); fora da tabela → `NULL` |
| `stfc_7n_oi` | `_resultado_chamada` | `'10'` → `Chamada completada`; `'31'` → `Não responde`; `'32'` → `linha ocupada`; `'44'` → `Congestionamento`; `'48'` → `Falha`; `'50'`, `'51'`, `'52'`, `'53'` → `Outros`; `'99'` → `Não identificado`; demais → `NULL` |
| `stfc_tropico_oi` | `_resultado_chamada` | `abs(cast int)` e tabela de códigos (ver “Códigos de `_resultado_chamada`” abaixo); fora da tabela → `NULL` |
| `stfc_axe_claro` | `_resultado_chamada` | `cast int` e tabela de códigos (ver “Códigos de `_resultado_chamada`” abaixo); qualquer valor fora das faixas (inclusive nulo) → `Desconexão Prematura` |
| `stfc_pcl_claro` | `_resultado_chamada` | `'3'` → `b-AnswerHasBeenReceived`; demais → `NULL` |
| `stfc_tropico_claro_lega` | `_resultado_chamada` | tabela de códigos de três dígitos (ver “Códigos de `_resultado_chamada`” abaixo); qualquer outro valor (inclusive nulo) → `unknown` |
| `stfc_tropico_claro_legb` | `_resultado_chamada` | tabela de códigos de três dígitos (ver “Códigos de `_resultado_chamada`” abaixo); qualquer outro valor (inclusive nulo) → `unknown` |
| `stfc_pit_claro` | `resultado_chamada` | Mantida sem alteração |
| `stfc_ss8bf_claro` | `_resultado_chamada` | `'0'`–`'9'` → mensagens `Call was ...` (ver “Códigos de `_resultado_chamada`” abaixo); demais → `NULL` |

Fluxos sem origem (coluna criada nula): `stfc_huawei_ngn_tim`.

### `co_resposta_sip`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_huawei_volte_tim` | `_resultado_chamada`, `_tipo_cdr` | `_resultado_chamada` inteiro (regra acima) mantido somente se `>= 200`; senão `NULL` |
| `stfc_huawei_ngn` | `codigo_resposta_sip` | Mantida sem alteração |
| `stfc_tropico_claro_lega` | `codigo_resposta_sip` | Mantida sem alteração |
| `stfc_tropico_claro_legb` | `codigo_resposta_sip` | Mantida sem alteração |

Fluxos sem origem (coluna criada nula): `smp_ericsson_gsm`, `smp_nokia_gsm`, `smp_ericsson_volte_vivo`, `stfc_italtel_ngn_tim`, `stfc_sigma_claro`, `stfc_huawei_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_axe_claro`, `stfc_pcl_claro`, `stfc_pit_claro`, `stfc_ss8bf_claro`.

### `no_autenticacao`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_huawei_volte_tim` | `_numero_origem_auth`, `_numero_origem_ats_ibcf`, `_tipo_cdr` | `_autenticacao`: ATS → `regexp_extract(_numero_origem_auth, '(verstat=[a-zA-Z\-]+)', 0)`; demais → mesma regex sobre `_numero_origem_ats_ibcf`; depois `_add_tn_validation_status` |
| `smp_ericsson_volte_vivo` | `_numero_origem_original` | `_autenticacao` = `split(_numero_origem_original, ';')[1]`; depois `_add_tn_validation_status` |

Fluxos sem origem (coluna criada nula): `smp_ericsson_gsm`, `smp_nokia_gsm`, `stfc_italtel_ngn_tim`, `stfc_sigma_claro`, `stfc_huawei_ngn`, `stfc_huawei_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_axe_claro`, `stfc_pcl_claro`, `stfc_tropico_claro_lega`, `stfc_tropico_claro_legb`, `stfc_pit_claro`, `stfc_ss8bf_claro`.

### `nu_cgi_origem`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_ericsson_gsm` | `celula_origem_mcc`, `celula_origem_mnc`, `celula_origem_lac`, `celula_origem_ci_sac` | `_build_composite_column('-', ...)`: componentes em branco → nulo; `lpad(celula_origem_lac, 5, '0')` e `lpad(celula_origem_ci_sac, 5, '0')`; qualquer componente nulo → `NULL` |
| `smp_nokia_gsm` | `celula_origem_lac`, `celula_origem_ci`, `prestadora` | `_build_composite_column('-', ...)` com `DEFAULT_MCC` (`724`), `_nokia_mnc`, `lpad(lac, 5, '0')` e `lpad(ci, 5, '0')`; `_nokia_mnc`: `prestadora == 'claro'` → `05`, `'algar'` → `34`, outro valor → `NULL` (composto `NULL`) |
| `smp_huawei_volte_tim` | `_informacao_rede`, `tipo_chamada` | `_extract_cell_info` (`regexp_extract('3gpp=([0-9a-fA-F]+);?', 1)` → `_id_celula_hex`) e `_format_cell_id` (formato `smp_huawei_volte_tim`) → `_id_celula`; mantida somente se `tipo_chamada == 'oRIGINATING-ROLE'`; senão `NULL` |
| `smp_ericsson_volte_vivo` | `celula_origem_hex` | `_format_cell_id(celula_origem_hex → celula_origem)`, formato `default`: comprimento 13 → 3G, 16 → 4G, 20 → 5G; outro comprimento mantém o valor bruto; vazio → `NULL` |

Fluxos sem origem (coluna criada nula): `stfc_italtel_ngn_tim`, `stfc_sigma_claro`, `stfc_huawei_ngn`, `stfc_huawei_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_axe_claro`, `stfc_pcl_claro`, `stfc_tropico_claro_lega`, `stfc_tropico_claro_legb`, `stfc_pit_claro`, `stfc_ss8bf_claro`.

As colunas de destino (`celula_destino*`, `tecnologia_celula_destino`, `imei_destino`, `imsi_destino`) seguem as mesmas regras com os componentes de destino ou `tERMINATING-ROLE` (Huawei TIM).

### `nu_cgi_origem_hex`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_huawei_volte_tim` | `_informacao_rede`, `tipo_chamada` | `_id_celula_hex` (`regexp_extract`, vazio se não houver correspondência) somente se `tipo_chamada == 'oRIGINATING-ROLE'` |
| `smp_ericsson_volte_vivo` | `celula_origem_hex` | Mantida sem alteração |

Fluxos sem origem (coluna criada nula): `smp_ericsson_gsm`, `smp_nokia_gsm`, `stfc_italtel_ngn_tim`, `stfc_sigma_claro`, `stfc_huawei_ngn`, `stfc_huawei_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_axe_claro`, `stfc_pcl_claro`, `stfc_tropico_claro_lega`, `stfc_tropico_claro_legb`, `stfc_pit_claro`, `stfc_ss8bf_claro`.

### `no_tecnologia_celula_origem`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_huawei_volte_tim` | `_informacao_rede`, `tipo_chamada` | `_tecnologia_celula` = `split(_informacao_rede, ';')[0]` somente se `tipo_chamada == 'oRIGINATING-ROLE'` |
| `smp_ericsson_volte_vivo` | `_informacao_rede_origem` | `split(_informacao_rede_origem, ';')[0]` |

Fluxos sem origem (coluna criada nula): `smp_ericsson_gsm`, `smp_nokia_gsm`, `stfc_italtel_ngn_tim`, `stfc_sigma_claro`, `stfc_huawei_ngn`, `stfc_huawei_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_axe_claro`, `stfc_pcl_claro`, `stfc_tropico_claro_lega`, `stfc_tropico_claro_legb`, `stfc_pit_claro`, `stfc_ss8bf_claro`.

### `nu_imei_origem`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_ericsson_gsm` | `imei_origem_tac`, `imei_origem_sn` | `_build_composite_column('', ...)`: TAC + SN sem separador; componente em branco ou nulo → `NULL` |
| `smp_nokia_gsm` | `imei_origem` | Mantida sem alteração |
| `smp_huawei_volte_tim` | `_info_imei`, `_imei`, `tipo_chamada` | `_imei` = `_imei` se `_info_imei == 'iMEI'`, senão `NULL`; atribuída somente se `tipo_chamada == 'oRIGINATING-ROLE'` |
| `smp_ericsson_volte_vivo` | `imei_origem` | `translate(imei_origem, '-', '')` (remove hífens) |

Fluxos sem origem (coluna criada nula): `stfc_italtel_ngn_tim`, `stfc_sigma_claro`, `stfc_huawei_ngn`, `stfc_huawei_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_axe_claro`, `stfc_pcl_claro`, `stfc_tropico_claro_lega`, `stfc_tropico_claro_legb`, `stfc_pit_claro`, `stfc_ss8bf_claro`.

### `nu_imsi_origem`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_ericsson_gsm` | `imsi_origem_mcc`, `imsi_origem_mnc`, `imsi_origem_msin` | `_build_composite_column('', ...)`: concatenação sem separador; componente em branco ou nulo → `NULL` |
| `smp_nokia_gsm` | `imsi_origem` | Mantida sem alteração |
| `smp_huawei_volte_tim` | `_info_imsi`, `tipo_chamada` | `_imsi` = `from_json(_info_imsi, array<struct<info_type,info_value>>)[0].info_value` se `info_type == 'eND-USER-IMSI'`; atribuída somente se `tipo_chamada == 'oRIGINATING-ROLE'` |
| `smp_ericsson_volte_vivo` | `imsi_origem` | Mantida sem alteração |

Fluxos sem origem (coluna criada nula): `stfc_italtel_ngn_tim`, `stfc_sigma_claro`, `stfc_huawei_ngn`, `stfc_huawei_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_axe_claro`, `stfc_pcl_claro`, `stfc_tropico_claro_lega`, `stfc_tropico_claro_legb`, `stfc_pit_claro`, `stfc_ss8bf_claro`.

### `no_agente_usuario`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_huawei_volte_tim` | `agente_usuario` | Mantida sem alteração |

Fluxos sem origem (coluna criada nula): `smp_ericsson_gsm`, `smp_nokia_gsm`, `smp_ericsson_volte_vivo`, `stfc_italtel_ngn_tim`, `stfc_sigma_claro`, `stfc_huawei_ngn`, `stfc_huawei_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_axe_claro`, `stfc_pcl_claro`, `stfc_tropico_claro_lega`, `stfc_tropico_claro_legb`, `stfc_pit_claro`, `stfc_ss8bf_claro`.

### `no_tipo_chamada`

| Fluxo | Colunas extraídas | Tratamento |
| :-- | :-- | :-- |
| `smp_ericsson_gsm` | `tipo_chamada` | Mantida sem alteração |
| `smp_nokia_gsm` | `tipo_chamada` | Mantida sem alteração |
| `smp_huawei_volte_tim` | `tipo_chamada` | Mantida sem alteração (valor bruto de `role-of-Node`) |
| `smp_ericsson_volte_vivo` | `_tipo_chamada` | `'1'` → `msOriginating`; `'3'` → `callForwarding`; `'4'` → `msTerminating`; outro valor → mantém `_tipo_chamada` |
| `stfc_italtel_ngn_tim` | `_tipo_chamada` | `regexp_replace(_tipo_chamada, ' ', '_')` |
| `stfc_huawei_ngn` | `_tipo_chamada` | `'01'` → `intra_office`; `'02'` → `incoming_office`; `'03'` → `outgoing_office`; `'04'` → `tandem`; `'05'` → `new_service`; demais (inclusive nulo) → `unknown` |
| `stfc_fcdr_vivo` | `_tipo_chamada` | `'E'` → `Entrada`; `'S'` → `Saída`; `'T'` → `Transporte`; demais → `NULL` |
| `stfc_tropico_oi` | `_tipo_chamada` | `0` → `Interna`; `1` → `Saída`; `2` → `Entrada`; demais → `NULL` |
| `stfc_axe_claro` | `_tipo_chamada` | tabela de códigos `01`–`0B` (ver “Códigos de `_tipo_chamada`” abaixo); demais → `NULL` |
| `stfc_pcl_claro` | `_tipo_chamada` | `cast string` do código recebido, sem rótulo de domínio |
| `stfc_tropico_claro_lega` | Não aplicável | constante `'tandem'` (`F.lit`), sem origem em coluna |
| `stfc_tropico_claro_legb` | Não aplicável | constante `'tandem'` (`F.lit`), sem origem em coluna |
| `stfc_pit_claro` | `_prestadora_origem` | `_prestadora_origem == 'BRAEBT'` → `Originating`; qualquer outro valor (inclusive nulo) → `Terminating` |
| `stfc_ss8bf_claro` | `_identificador_origem`, `_identificador_destino`, `_numero_encaminhado` | `originating == '900'` → `Originating`; `'901'` + destino `'902'`/`'903'` + `_numero_encaminhado` não nulo → `Forwarding`; `'901'` + `'902'` sem encaminhamento → `Terminating`; `'901'` + `'903'` sem encaminhamento → `Transit`; demais → `Unknown` |

Fluxos sem origem (coluna criada nula): `stfc_sigma_claro`, `stfc_huawei_ngn_tim`, `stfc_7n_oi`.

### Colunas sem variação entre fluxos

- `nu_referencia`: sempre `referencia` (cópia), exceto SS8BF Claro (`substring(_referencia, 52, 8)`).
- `no_prestadora` e `no_esquema`: metadados do extrator em todos os fluxos.
- `no_tipo_cdr`: segmento `-2` do caminho (ou `cdr_type`), exceto nos fluxos que mapeiam `_tipo_cdr`: `smp_huawei_volte_tim`, `stfc_sigma_claro`, `stfc_fcdr_vivo`.
- `no_arquivo_origem`: segmento `-1` do caminho, exceto nos fluxos que mapeiam `arquivo_origem` no schema: `smp_ericsson_volte_vivo`, `stfc_sigma_claro`.
- `nu_origem`, `ic_origem_valido`, `nu_destino`, `ic_destino_valido`: sempre `_extract_numbers` sobre `numero_origem`/`numero_destino`.
- `no_rota_entrada`, `no_rota_saida`, `no_bilhetador`: cópia das colunas de mesmo nome em todos os fluxos.
- `nu_ip_origem`, `nu_ip_destino`, `nu_porta_ip_origem`, `nu_porta_ip_destino`: nenhum fluxo as extrai ou deriva; são criadas nulas em todos.
- `nu_cgi_*`, `nu_cgi_*_hex`, `no_tecnologia_celula_*`, `nu_imei_*`, `nu_imsi_*`: preenchidas apenas por Ericsson GSM, Nokia GSM, Huawei VoLTE TIM e Ericsson VoLTE Vivo (ver tabelas acima); nulas nos 13 fluxos restantes.

# 5. Etapa final de geração do Parquet

**Responsáveis:** `CDRBaseTransformer._select_transformed_columns` e `CDRBaseTransformer._write_parquet`
(`core/transformers/base_transformer.py`), chamados no final de cada método `transform_<cdr_schema>`.

**DataFrame gravado:** o DataFrame resultante de `_apply_standard_pipeline`, ainda com os nomes intermediários
(chaves de `TARGET_SCHEMA`) e eventuais colunas auxiliares.

**Seleção, cast e renomeação:** `df.select(*[F.col(origem).cast(tipo).alias(destino) for origem, (destino, tipo) in TARGET_SCHEMA.items()])`.
Todas as colunas fora de `TARGET_SCHEMA` (por exemplo `servico`, `status_chamada`, `numero_origem_original`,
`numero_conectado`, `perfil_prestadora`, `tecnologia_fornecedor` e colunas iniciadas por `_`) são excluídas por esta projeção.
A ordem das colunas finais é a ordem de `TARGET_SCHEMA`. Não há `drop` explícito nem `withColumnRenamed` nesta etapa.

Operações por coluna (todas são cast seguido de renomeação por `alias`; o conteúdo não é alterado nesta etapa):

| # | Coluna anterior | Operação | Coluna final |
| --: | :-- | :-- | :-- |
| 1 | `referencia` | cast `string` + renomeação (`alias`) | `nu_referencia` |
| 2 | `referencia_sip` | cast `string` + renomeação (`alias`) | `nu_referencia_sip` |
| 3 | `data_hora_referencia` | cast `timestamp_ntz` + renomeação (`alias`) | `dh_referencia` |
| 4 | `data_hora` | cast `timestamp_ntz` + renomeação (`alias`) | `dh_chamada` |
| 5 | `data_hora_fim` | cast `timestamp_ntz` + renomeação (`alias`) | `dh_fim_chamada` |
| 6 | `duracao` | cast `integer` + renomeação (`alias`) | `qt_duracao_segundos` |
| 7 | `numero_origem_extraido` | cast `string` + renomeação (`alias`) | `nu_origem` |
| 8 | `sucesso_extracao_origem` | cast `boolean` + renomeação (`alias`) | `ic_origem_valido` |
| 9 | `numero_origem` | cast `string` + renomeação (`alias`) | `nu_origem_original` |
| 10 | `numero_destino_extraido` | cast `string` + renomeação (`alias`) | `nu_destino` |
| 11 | `sucesso_extracao_destino` | cast `boolean` + renomeação (`alias`) | `ic_destino_valido` |
| 12 | `numero_destino` | cast `string` + renomeação (`alias`) | `nu_destino_original` |
| 13 | `resultado_chamada` | cast `string` + renomeação (`alias`) | `no_resultado_chamada` |
| 14 | `codigo_resposta_sip` | cast `string` + renomeação (`alias`) | `co_resposta_sip` |
| 15 | `autenticacao` | cast `string` + renomeação (`alias`) | `no_autenticacao` |
| 16 | `prestadora` | cast `string` + renomeação (`alias`) | `no_prestadora` |
| 17 | `rota_entrada` | cast `string` + renomeação (`alias`) | `no_rota_entrada` |
| 18 | `rota_saida` | cast `string` + renomeação (`alias`) | `no_rota_saida` |
| 19 | `bilhetador` | cast `string` + renomeação (`alias`) | `no_bilhetador` |
| 20 | `celula_origem` | cast `string` + renomeação (`alias`) | `nu_cgi_origem` |
| 21 | `celula_origem_hex` | cast `string` + renomeação (`alias`) | `nu_cgi_origem_hex` |
| 22 | `tecnologia_celula_origem` | cast `string` + renomeação (`alias`) | `no_tecnologia_celula_origem` |
| 23 | `imei_origem` | cast `string` + renomeação (`alias`) | `nu_imei_origem` |
| 24 | `imsi_origem` | cast `string` + renomeação (`alias`) | `nu_imsi_origem` |
| 25 | `ip_origem` | cast `string` + renomeação (`alias`) | `nu_ip_origem` |
| 26 | `porta_ip_origem` | cast `integer` + renomeação (`alias`) | `nu_porta_ip_origem` |
| 27 | `celula_destino` | cast `string` + renomeação (`alias`) | `nu_cgi_destino` |
| 28 | `celula_destino_hex` | cast `string` + renomeação (`alias`) | `nu_cgi_destino_hex` |
| 29 | `tecnologia_celula_destino` | cast `string` + renomeação (`alias`) | `no_tecnologia_celula_destino` |
| 30 | `imei_destino` | cast `string` + renomeação (`alias`) | `nu_imei_destino` |
| 31 | `imsi_destino` | cast `string` + renomeação (`alias`) | `nu_imsi_destino` |
| 32 | `ip_destino` | cast `string` + renomeação (`alias`) | `nu_ip_destino` |
| 33 | `porta_ip_destino` | cast `integer` + renomeação (`alias`) | `nu_porta_ip_destino` |
| 34 | `agente_usuario` | cast `string` + renomeação (`alias`) | `no_agente_usuario` |
| 35 | `esquema` | cast `string` + renomeação (`alias`) | `no_esquema` |
| 36 | `tipo_cdr` | cast `string` + renomeação (`alias`) | `no_tipo_cdr` |
| 37 | `arquivo_origem` | cast `string` + renomeação (`alias`) | `no_arquivo_origem` |
| 38 | `tipo_chamada` | cast `string` + renomeação (`alias`); coluna de partição da escrita | `no_tipo_chamada` |

**Gravação:** `_write_parquet` valida que a sessão mantém `spark.sql.timestampType=TIMESTAMP_NTZ`
(`_validate_timestamp_type`) e executa `df.write.mode("overwrite").partitionBy("no_tipo_chamada").parquet(target_file)`.
Esta é a ação Spark que materializa o plano; `overwrite` substitui o conteúdo existente em `target_file`.
`no_tipo_chamada` é coluna de partição: valores nulos nunca ocorrem (chave primária), mas o sentinela `__NULL__`
pode se tornar um diretório de partição.

**Schema final:** as 38 colunas da tabela 2.1, com os tipos `string`, `timestamp_ntz`, `integer` e `boolean`
nela indicados.

# 6. Funções auxiliares específicas

Funções e estruturas usadas por apenas um fluxo ou por um subconjunto restrito de layouts. Os helpers de transformação
compartilhados estão em “Funções auxiliares comuns”.

### `PARQUET_DEFAULT_SCHEMAS` e `CDRParquetSchema`

**Localização:** `src/teleutils/core/extractors/schemas/parquet.py`; `CDRParquetSchema` é uma dataclass imutável.

**Finalidade:** declarar, por layout Parquet, o `column_mapping` (coluna de origem → coluna extraída), a descrição do job e o `fill_char`.

**Entradas:** chave `cdr_schema` entre as 6 do catálogo (`smp_ericsson_gsm`, `smp_nokia_gsm`, `smp_huawei_volte_tim`,
`smp_ericsson_volte_vivo`, `stfc_italtel_ngn_tim`, `stfc_sigma_claro`).

**Saídas:** pares `(origem, destino)` usados por `CDRParquetExtractor.extract_dataframe`.

**Colunas impactadas:** todas as colunas extraídas dos fluxos Parquet. Colunas de origem ausentes no arquivo viram `NULL` (`cast string`) com aviso em log.
Nomes de origem com `.` (por exemplo `callingPartyNumber.digits`) são lidos literalmente, com crases.

**Transformação realizada:** renomeação (`alias`). Com `fill_char` (somente `stfc_sigma_claro`, valor `-`), aplica `regexp_replace(coluna, '-+$', '')`
a todas as colunas mapeadas.

### `TEXT_DEFAULT_SCHEMAS` e `CDRTextSchema`

**Localização:** `src/teleutils/core/extractors/schemas/text.py`.

**Finalidade:** declarar, por layout texto, o delimitador, o schema de leitura, os índices/larguras/nomes das colunas, o filtro de linhas
(`lines_to_keep`), `read_rdd` e `fill_char`.

**Entradas:** chave `cdr_schema` entre as 11 do catálogo.

**Saídas:** DataFrame com as colunas nomeadas em `column_names`, após `CDRTextExtractor.extract_dataframe`.

**Colunas impactadas:** todas as colunas extraídas dos fluxos texto.

**Transformação realizada:** com `column_sizes` (largura fixa), `F.trim(F.substring(value, índice, largura))` (com `regexp_replace('-+$', '')`
antes do `trim` quando há `fill_char`; `stfc_7n_oi` e `stfc_tropico_oi`); sem `column_sizes`, seleção posicional (`F.col(colunas_lidas[índice]).alias(nome)`),
com validação de que o maior índice existe. `lines_to_keep` aplica `filter` após `add_source_metadata`: `col == valor`, `is not null` ou `is null`.

### `stfc_pit_claro_schema` e `stfc_ss8bf_claro_schema`

**Localização:** `src/teleutils/core/extractors/schemas/_claro.py` (constantes `StructType`, com 110 e 222 campos).

**Finalidade:** fornecer nomes e tipos de leitura do CSV para PIT Claro e SS8BF Claro (todos os campos são `string`).

**Colunas impactadas:** os índices selecionados em `TEXT_DEFAULT_SCHEMAS` — PIT: `recordtype`, `recordingentity`, `mscincomingroute`, `mscoutgoingroute`,
`callingnumber`, `callednumber`, `releasetime`, `callduration`, `causeforterm`, `networkcallreference`, `setuptime`, `originatingoperator`;
SS8BF: `start_time`, `duration_of_call`, `switch_id`, `record_id`, `terminating_number_called_party`, `originating_number_calling_party`,
`attempt_indicator`, `originating_party_identifier`, `terminating_party_identifier`, `forwarded_from_dn`, `ingress_sip_endpoint_address`, `egress_sip_endpoint_address`.

**Transformação realizada:** nenhuma; apenas nomeação dos campos de leitura.

### `read_text_with_source`

**Localização:** `src/teleutils/core/extractors/_extractors_utils.py`.

**Finalidade:** ler linhas de texto preservando o caminho do arquivo por linha (coluna interna `__teleutils_source_path`) sem o leitor CSV; usada quando `read_rdd=True`
(`stfc_7n_oi` e `stfc_tropico_oi`).

**Entradas:** sessão Spark e caminho(s)/glob(s); falha com `FileNotFoundError` se um caminho não existir; ignora arquivos cujo nome começa com `.` ou `_`.

**Saídas:** DataFrame `value string, __teleutils_source_path string`.

**Colunas impactadas:** `value` (linha bruta) e o caminho usado por `add_source_metadata` (`prestadora`, `servico`, `tipo_cdr`, `arquivo_origem`).

**Transformação realizada:** leitura via `textFile` com par `(linha, URI do arquivo)`; sem alteração de conteúdo.

### `_check_output_format` e `_VALID_OUTPUT_FORMATS`

**Localização:** `src/teleutils/core/transformers/transformers.py` (nível de módulo).

**Finalidade:** validar `output_format` contra `default`, `smp_ericsson_gsm_tim`, `smp_huawei_volte_tim` e `smp_nokia_algar`.

**Entradas/saídas:** string; levanta `ValueError` se inválida; sem retorno.

**Colunas impactadas:** nenhuma diretamente. É chamada por `transform_smp_ericsson_gsm` e por `_format_cell_id`.

**Transformação realizada:** nenhuma. No fluxo `smp_ericsson_gsm`, `output_format == "tim"` não pertence ao conjunto válido e o ramo correspondente é inalcançável (ver Observações).

### `CDRTransformer.__init__` (constantes de MCC/MNC)

**Localização:** `CDRTransformer.__init__` em `transformers.py`.

**Finalidade:** criar as expressões literais `default_mcc` (`DEFAULT_MCC = "724"`), `algar_mnc` (`"34"`) e `claro_mnc` (`"05"`) de `teleutils._config`.

**Colunas impactadas:** `nu_cgi_origem` e `nu_cgi_destino` do fluxo `smp_nokia_gsm`.

**Transformação realizada:** valores literais; sem origem em coluna.

### `CDRTransformer.transform`

**Localização:** `CDRTransformer.transform` em `transformers.py`.

**Finalidade:** despachante por `cdr_schema` que valida `output_format` e procura `transform_<cdr_schema>` em `globals()` do módulo.

**Colunas impactadas:** nenhuma. Como os transformadores de layout são métodos da classe e não funções globais, este método não os encontra e levanta
`ValueError`; o orquestrador não o utiliza (usa `getattr` na instância).

# Funções auxiliares comuns

Classificadas como comuns por serem reutilizadas por mais de um fluxo ou por realizarem operações genéricas.

## Funções em nível de módulo

### `_null_if_blank`

- **Localização:** `core/transformers/transformers.py`.
- **Finalidade:** converter string vazia ou só com espaços (`trim`) em `NULL`.
- **Entradas/saídas:** nome de coluna → `Column`.
- **Colunas impactadas:** os componentes nomeados de `_build_composite_column` (e, por ele, `data_hora`/`data_hora_fim` de `_concat_date_time`).
- **Fluxos:** `smp_ericsson_gsm`, `smp_nokia_gsm`, `smp_ericsson_volte_vivo`, `stfc_italtel_ngn_tim`, `stfc_huawei_ngn`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`,
  `stfc_axe_claro`, `stfc_pcl_claro`, `stfc_tropico_claro_lega`, `stfc_tropico_claro_legb`.

### `_concat_or_null`

- **Localização:** `core/transformers/transformers.py`.
- **Finalidade:** `concat_ws` com separador que devolve `NULL` se qualquer componente for nulo.
- **Entradas/saídas:** separador e colunas (`str` ou `Column`) → `Column`; `TypeError` se não houver componentes.
- **Colunas impactadas:** compostos de data/hora, células, IMSIs e IMEIs.
- **Fluxos:** os mesmos de `_build_composite_column`.

### `_build_composite_column`

- **Localização:** `core/transformers/transformers.py`.
- **Finalidade:** montar identificador composto: componentes nomeados passam por `_null_if_blank`, os listados em `padded_columns` recebem `lpad(..., 5, '0')`, e o resultado vem de `_concat_or_null`. Componentes `Column` não passam por `_null_if_blank` nem por `lpad`.
- **Entradas/saídas:** separador, componentes, colunas com preenchimento → `Column`.
- **Colunas impactadas:** `celula_origem`, `celula_destino`, `imsi_*`, `imei_*` (Ericsson GSM); `celula_origem`, `celula_destino` (Nokia); `data_hora` e `data_hora_fim` via `_concat_date_time`.
- **Fluxos:** `smp_ericsson_gsm`, `smp_nokia_gsm` e todos os que usam `_concat_date_time`.

### `_concat_date_time`

- **Localização:** `core/transformers/transformers.py`.
- **Finalidade:** criar `data_hora` = `start_date` + espaço + `start_time` e, se `stop_date` e `stop_time` existirem no DataFrame, `data_hora_fim` = `stop_date` + espaço + `stop_time`; nulo se algum componente for nulo ou em branco. A conversão para timestamp ocorre em `_format_date_time`.
- **Entradas/saídas:** DataFrame e nomes de coluna (padrões `_data`, `_hora`, `_data_fim`, `_hora_fim`) → DataFrame.
- **Colunas impactadas:** `data_hora`, `data_hora_fim` (→ `dh_chamada`, `dh_fim_chamada`).
- **Fluxos:** `smp_ericsson_gsm` e `smp_ericsson_volte_vivo` (com `stop_date='_data'`); `stfc_huawei_ngn` e `stfc_tropico_claro_lega/legb` (início e término); `stfc_axe_claro` (`stop_date='_data'`);
  `stfc_italtel_ngn_tim`, `stfc_fcdr_vivo`, `stfc_7n_oi`, `stfc_tropico_oi`, `stfc_pcl_claro` (somente início, pois `_data_fim`/`_hora_fim` não existem).

### `_extract_cell_info`

- **Localização:** `core/transformers/transformers.py`.
- **Finalidade:** extrair tecnologia (`split(coluna, ';')[0]`) e identificador hexadecimal (`regexp_extract(coluna, '3gpp=([0-9a-fA-F]+);?', 1)`; vazio se não houver correspondência) da informação de rede.
- **Entradas/saídas:** DataFrame, coluna de rede e nomes das colunas de saída (qualquer uma pode ser `None`) → DataFrame; `ValueError` se faltar parâmetro ou a coluna não existir.
- **Colunas impactadas:** `_tecnologia_celula` e `_id_celula_hex` (Huawei TIM); `tecnologia_celula_origem` e `tecnologia_celula_destino` (Ericsson VoLTE Vivo).
- **Fluxos:** `smp_huawei_volte_tim`, `smp_ericsson_volte_vivo`.

### `_format_cell_id`

- **Localização:** `core/transformers/transformers.py`.
- **Finalidade:** decodificar identificadores de célula hexadecimais em `mcc-mnc-...` conforme o comprimento: 13 caracteres (3G: tac e ci em decimal; `lpad` 5 no formato detalhado), 16 (4G: ECI da posição 10, largura 7; formato `smp_huawei_volte_tim` produz `mcc-mnc-eci`; demais: eNB e célula com `lpad` 7 e 3) e 20 (5G: NCGI com `gnb_id_bits=26`, `lpad` 8 e 4). Outro comprimento mantém o valor; resultado vazio → `NULL`.
- **Entradas/saídas:** DataFrame, coluna hexadecimal, coluna de saída, `gnb_id_bits`, `output_format` → DataFrame.
- **Colunas impactadas:** `_id_celula` (Huawei TIM, formato `smp_huawei_volte_tim`); `celula_origem` e `celula_destino` (Ericsson VoLTE Vivo, formato `default`).
- **Fluxos:** `smp_huawei_volte_tim`, `smp_ericsson_volte_vivo`.

### `_extract_numbers` (pandas UDF) e `extract_number`

- **Localização:** `_extract_numbers` em `core/transformers/base_transformer.py`; `extract_number` e auxiliares (`_extract_number`, `_search_number`, `_is_rejected_number`, `ExtractionResult`) em `preprocessing/phone_number_extractor.py`.
- **Finalidade:** localizar e validar o número telefônico em `numero_origem`/`numero_destino`.
- **Entradas/saídas:** `pd.Series` de números brutos → `pd.DataFrame` com `numero_extraido` (string) e `extracao_sucesso` (boolean). `extract_number` recebe o valor e devolve `ExtractionResult(number, success)`.
- **Regra:** valor falsy → `("5599999999999", False)`. Caso contrário: minúsculas, remoção de um `f` final, `CLEAN_PATTERN` (`^0+|(\d)\1{7,}$`: zeros à esquerda e sufixo de oito ou mais dígitos iguais) e tentativa inicial por comprimento (3–7: serviços; 8–9: fixo/móvel sem CN ou CNG curto; 10–11: com CN ou CNG). Se não houver resultado, `FALLBACK_PATTERNS` em ordem (móvel no fim, móvel no início, fixo no fim, fixo no início, CNG no fim, CNG no início, chamada a cobrar `9090`, móvel em qualquer posição, fixo em qualquer posição, CNG em qualquer posição). Números que casam `REJECTED_NUMBERS_PATTERN` (dígitos repetidos) não são aceitos. Sem extração válida devolve o valor original e `False`.
- **Colunas impactadas:** `nu_origem`, `ic_origem_valido`, `nu_destino`, `ic_destino_valido`.
- **Fluxos:** todos os 17.

### `add_source_metadata` e `validate_metadata_arguments`

- **Localização:** `core/extractors/_extractors_utils.py`.
- **Finalidade:** validar `operator`, `service`, `cdr_type` (`str` ou `None`, sem valores só com espaços) e adicionar `esquema`, `prestadora`, `servico`, `tipo_cdr` e `arquivo_origem` (ver 2.3).
- **Entradas/saídas:** DataFrame com `__teleutils_source_path` e argumentos → DataFrame sem essa coluna.
- **Colunas impactadas:** `no_esquema`, `no_prestadora`, `no_tipo_cdr`, `no_arquivo_origem` (e `servico`, descartada).
- **Fluxos:** todos os 17.

### `resolve_cdr_schema`

- **Localização:** `core/extractors/schemas/_schema_utils.py`.
- **Finalidade:** obter o contrato pela chave exata (`TypeError` se não for `str`; `ValueError` se ausente).
- **Colunas impactadas:** nenhuma diretamente; define quais colunas são extraídas.
- **Fluxos:** todos os 17.

### Funções sem impacto em colunas

`suppress_spark_loggers` (ajusta temporariamente o nível de dois loggers do Spark durante `CDRParquetExtractor.extract`), `log_operation` (registra início, duração e falhas, relançando exceções) e
`format_source_file_for_log` não alteram valores, nomes ou existência de colunas.

## Métodos auxiliares em classes

### `CDRParquetExtractor.extract_dataframe` e `CDRParquetExtractor.extract`

- **Localização:** `core/extractors/parquet_extractors.py`.
- **Finalidade:** ler o(s) Parquet(s) (`mergeSchema=true`, `ignoreCorruptFiles=true`), criar `__teleutils_source_path` com `input_file_name()` se necessário, selecionar e renomear as colunas do `column_mapping` (ausentes → `NULL` string; `fill_char` opcional), adicionar metadados, opcionalmente `dropDuplicates` (`unique=True`) e remover `_tipo_cdr`. `extract` grava o resultado com `overwrite`.
- **Entradas/saídas:** caminho(s), `cdr_schema`, `unique`, `operator`, `service`, `cdr_type` → DataFrame (`extract_dataframe`) ou caminho gravado (`extract`).
- **Colunas impactadas:** todas as colunas extraídas dos fluxos Parquet.
- **Fluxos:** `smp_ericsson_gsm`, `smp_nokia_gsm`, `smp_huawei_volte_tim`, `smp_ericsson_volte_vivo`, `stfc_italtel_ngn_tim`, `stfc_sigma_claro`. O orquestrador não repassa `unique`; o padrão é `False`.

### `CDRTextExtractor.extract_dataframe` e `CDRTextExtractor.extract`

- **Localização:** `core/extractors/text_extractors.py`.
- **Finalidade:** ler texto (`read_text_with_source` se `read_rdd`, senão `spark.read.csv` com `sep`, `header`, `schema`, `inferSchema=False` e remoção de espaços laterais), selecionar colunas por largura fixa ou posição, adicionar metadados, aplicar `lines_to_keep` e remover `_tipo_cdr`. `extract` grava o resultado com `overwrite`.
- **Entradas/saídas:** caminho(s), `cdr_schema`, `operator`, `service`, `cdr_type` → DataFrame ou caminho gravado.
- **Colunas impactadas:** todas as colunas extraídas dos fluxos texto.
- **Fluxos:** os 11 fluxos texto.

### `CDRBaseTransformer.__init__`, `_validate_timestamp_type` e `_resolve_input`

- **Localização:** `core/transformers/base_transformer.py`.
- **Finalidade:** criar a sessão `spark.newSession()` com `TIMESTAMP_NTZ`, as expressões `min_safe_date` e `null_sentinel_value`; rejeitar sessões com tipo de timestamp alterado; ler Parquet (`str`) ou aceitar DataFrame da própria sessão (`TypeError`/`ValueError` nos demais casos).
- **Colunas impactadas:** nenhuma diretamente (definem a entrada e o tipo de timestamp do pipeline).
- **Fluxos:** todos os 17 (cada `transform_*` chama `_resolve_input`).

### `CDRBaseTransformer._fill_missing_columns`, `_format_date_time`, `_extract_phone_numbers`, `_add_tn_validation_status`, `_fill_primary_key_columns`, `_apply_standard_pipeline`

| Método | Finalidade | Colunas impactadas | Fluxos |
| :-- | :-- | :-- | :-- |
| `_fill_missing_columns` | criar como `NULL` toda chave de `TARGET_SCHEMA` ausente | qualquer coluna ausente | todos |
| `_format_date_time` | duração inteira e três timestamps com limite mínimo | `duracao`, `data_hora`, `data_hora_fim`, `data_hora_referencia` | todos |
| `_extract_phone_numbers` | extrair números, indicadores de sucesso e restaurar o bruto quando houver `_numero_*_original` | `numero_origem_extraido`, `sucesso_extracao_origem`, `numero_destino_extraido`, `sucesso_extracao_destino`, `numero_origem`, `numero_destino` | todos (restauração: Huawei TIM e Ericsson VoLTE Vivo) |
| `_add_tn_validation_status` | classificar `verstat` em `autenticacao` | `autenticacao` | todos (com `_autenticacao`: Huawei TIM e Ericsson VoLTE Vivo) |
| `_fill_primary_key_columns` | substituir nulos nos 15 componentes da chave | referências, datas, duração, números, resultado, rotas, bilhetador e tipo de chamada | todos |
| `_apply_standard_pipeline` | encadear as cinco etapas acima, nesta ordem | contrato intermediário | todos |

### `CDRBaseTransformer._select_transformed_columns` e `_write_parquet`

- **Localização:** `core/transformers/base_transformer.py`.
- **Finalidade:** projeção/cast/alias de `TARGET_SCHEMA` e escrita `overwrite` particionada por `no_tipo_chamada` (ver seção 5).
- **Colunas impactadas:** as 38 colunas finais.
- **Fluxos:** todos os 17.

# 8. Ambiguidades

Esta seção registra o que não pode ser determinado com segurança apenas pela análise estática do código.
Quando indicado, a resposta depende de dados reais ou de execução.

### Tipo físico de `cause_for_termination` (Nokia)

**Elemento afetado:** `no_resultado_chamada` do fluxo `smp_nokia_gsm`.

**Ponto do rastreamento:** o extrator preserva o tipo físico do Parquet e o transformador compara `_resultado_chamada`
diretamente com literais inteiros (`int("0000", 16)` … `int("1000", 16)`).

**Motivo:** ⚠️ Não foi possível determinar esta linhagem com segurança apenas pela análise estática do código. A análise estática não
determina se a origem chega como inteiro, decimal ou texto hexadecimal, nem o comportamento efetivo sob `spark.sql.ansi.enabled=true`.

**Evidência:** `("cause_for_termination", "_resultado_chamada")` em `PARQUET_DEFAULT_SCHEMAS["smp_nokia_gsm"]` e as comparações em `transform_smp_nokia_gsm`.

### Semântica da precedência nos campos Nokia

**Elemento afetado:** `dh_chamada`, `dh_fim_chamada`, `qt_duracao_segundos`, `nu_origem*` e `nu_destino*` do fluxo `smp_nokia_gsm`.

**Ponto do rastreamento:** os `coalesce` e os ramos `UCA`/`FORW` são determinísticos, mas o código não contém evidência
suficiente para afirmar por que uma fonte tem prioridade semântica sobre outra.

**Evidência:** ordem das colunas `_duracao*` (ordem do DataFrame), `coalesce(data_hora_alocacao_canal, data_hora_referencia)` e as condições `tipo_chamada == 'UCA'` e `'FORW'`.

### Nomes originais em layouts posicionais

**Elemento afetado:** todas as colunas originais dos fluxos `stfc_huawei_ngn`, `stfc_huawei_ngn_tim`, `stfc_fcdr_vivo`, `stfc_tropico_claro_lega`, `stfc_tropico_claro_legb`
(CSV sem cabeçalho) e dos layouts de largura fixa (`stfc_7n_oi`, `stfc_tropico_oi`, `stfc_axe_claro`, `stfc_pcl_claro`).

**Ponto do rastreamento:** nesses catálogos só existem posições (e larguras); os nomes dos campos nos documentos de layout dos fornecedores não estão no código.
Não é possível determinar com segurança o significado de cada posição sem documentação externa.

**Evidência:** `schema=None`/`schema="value string"`, `has_header=False`, `column_indices` e `column_sizes` em `TEXT_DEFAULT_SCHEMAS`.

### Leitura de largura fixa com `spark.read.csv` (AXE e PCL Claro)

**Elemento afetado:** `stfc_axe_claro` e `stfc_pcl_claro` (`read_rdd=False`, `delimiter=None`, `schema="value string"`).

**Ponto do rastreamento:** o extrator chama `spark.read.csv(sep=None, schema="value string")` e depois recorta `value` por `substring`. Não é possível confirmar
estaticamente como o leitor CSV trata linhas que contenham o separador padrão (`,`) quando o schema possui uma única coluna.

**Evidência:** `read_rdd` é `True` apenas em `stfc_7n_oi` e `stfc_tropico_oi`; o comentário do extrator associa o ramo RDD a falhas do leitor CSV com caminhos contendo `@`. Verificar com amostras reais.

### Casts e comparações dependentes de configuração ANSI

**Elemento afetado:** `qt_duracao_segundos` (todos os fluxos), `codigo_resposta_sip`/`no_resultado_chamada` (Huawei TIM, FCDR Vivo, Trópico Oi, AXE Claro),
`no_resultado_chamada` (Nokia) e comparações de `_resultado_chamada` inteiro com literais string (Trópico Oi).

**Ponto do rastreamento:** `cast("int")` de strings não numéricas e a coerção implícita nas comparações produzem `NULL` ou erro conforme `spark.sql.ansi.enabled`. Os testes do repositório
exercitam as duas configurações apenas para a normalização de timestamps. O ambiente instalado usa PySpark 3.5.5, mas `pyproject.toml` declara `pyspark>=3.5.5`.

**Evidência:** `_format_date_time` (`normalize_duration`), `transform_smp_huawei_volte_tim`, `transform_stfc_fcdr_vivo`, `transform_stfc_tropico_oi`, `transform_stfc_axe_claro` e `tests/test_dataframe_pipeline.py`.

### Valores nulos entregues à pandas UDF `_extract_numbers`

**Elemento afetado:** `nu_origem`, `ic_origem_valido`, `nu_destino`, `ic_destino_valido`.

**Ponto do rastreamento:** `extract_number` trata valores falsy como entrada vazia (`5599999999999`, `False`). A análise estática não confirma se um nulo Spark chega à UDF como `None`
(falsy) ou como outro valor (por exemplo `NaN`, que é truthy e seria convertido em texto); isso depende da conversão Arrow → pandas em execução.

**Evidência:** `_extract_numbers` itera a `pd.Series` e chama `extract_number(subscriber_number=...)`.

### Conteúdo real dos campos JSON e URIs do Huawei VoLTE TIM

**Elemento afetado:** `nu_origem_original`, `nu_destino_original`, `nu_origem`, `nu_destino`, `no_autenticacao`, `nu_imsi_*` do fluxo `smp_huawei_volte_tim`.

**Ponto do rastreamento:** o código assume `list-Of-Calling-Party-Address` como JSON com `$[0].tEL-URI`/`$[0].sIP-URI`, `list-of-subscription-ID` como array de `{info_type, info_value}` e
`tipo_cdr` iguais a `aTSRecord`/`iBCFRecord`. O formato real desses campos e o conjunto completo de valores de `recordType` não estão no código.

**Evidência:** `get_json_object`, `from_json` e as comparações literais em `transform_smp_huawei_volte_tim`. Registros com outro `recordType` têm `numero_origem`/`numero_destino` `NULL`.

### Hierarquia dos caminhos de entrada

**Elemento afetado:** `no_prestadora`, `no_tipo_cdr` (quando não vem de `_tipo_cdr`) e `no_arquivo_origem` (quando não vem do schema).

**Ponto do rastreamento:** quando `operator`, `cdr_type` e `service` não são informados, a extração assume que os segmentos `-4`, `-2` e `-1` do caminho
representam, respectivamente, prestadora, tipo de CDR e arquivo (`-3` é `servico`). Só há validação de valores nulos ou em branco, não da estrutura do caminho.
No fluxo Nokia, a imputação do MNC compara `prestadora` com os literais `claro` e `algar` (comparação exata).

**Evidência:** `add_source_metadata` e `transform_smp_nokia_gsm`.

### Colunas `arquivo_origem` ausentes no Parquet (Ericsson VoLTE Vivo e Sigma Claro)

**Elemento afetado:** `no_arquivo_origem` dos fluxos `smp_ericsson_volte_vivo` (`source_filename`) e `stfc_sigma_claro` (`source_file`).

**Ponto do rastreamento:** a coluna é mapeada no schema; se não existir no Parquet de origem, é criada nula e, como já existe, o nome não é inferido do caminho.
Não é possível saber estaticamente se os arquivos reais sempre trazem essas colunas.

**Evidência:** `column_mapping` dos dois fluxos e a regra `if "arquivo_origem" not in df.columns` em `add_source_metadata`.

# Observações da revisão

Nenhuma alteração de código foi feita; as sugestões abaixo são apenas registros para avaliação futura.

## Inconsistências identificadas

1. **Ericsson GSM: `status_chamada` não alimenta o contrato.**
   - **Localização:** `("callPosition", "status_chamada")` em `PARQUET_DEFAULT_SCHEMAS["smp_ericsson_gsm"]`; `transform_smp_ericsson_gsm`; `TARGET_SCHEMA` (`resultado_chamada`).
   - **Descrição:** a coluna extraída `status_chamada` não é lida por nenhuma etapa e não pertence a `TARGET_SCHEMA`, cujo nome intermediário é `resultado_chamada`.
   - **Impacto potencial:** `no_resultado_chamada` recebe `__NULL__` em todos os registros desse fluxo.
   - **Sugestão:** confirmar a intenção do contrato e, em alteração separada, alinhar o nome extraído ou criar a derivação.
2. **Fluxos sem `tipo_chamada`.**
   - **Localização:** `transform_stfc_sigma_claro`, `transform_stfc_huawei_ngn_tim`, `transform_stfc_7n_oi`.
   - **Descrição:** nenhuma coluna extraída ou derivada alimenta `tipo_chamada`.
   - **Impacto potencial:** `no_tipo_chamada` = `__NULL__` e a partição `no_tipo_chamada=__NULL__` na escrita.
   - **Sugestão:** definir o valor de domínio ou documentar que o sentinela é esperado.
3. **Huawei NGN TIM sem `resultado_chamada`.**
   - **Localização:** `CDRTextSchema` `stfc_huawei_ngn_tim` e `transform_stfc_huawei_ngn_tim`.
   - **Descrição:** o schema não mapeia nenhuma coluna de resultado e o transformador só executa o pipeline comum.
   - **Impacto potencial:** `no_resultado_chamada` = `__NULL__` em todos os registros.
   - **Sugestão:** verificar se o layout fornece o campo.
4. **Despachante `CDRTransformer.transform` não encontra os transformadores.**
   - **Localização:** `CDRTransformer.transform` (`globals().get(f"transform_{cdr_schema}")`).
   - **Descrição:** os transformadores são métodos da classe, não funções globais; o método levanta `ValueError` para qualquer schema.
   - **Impacto potencial:** nenhum no pipeline do orquestrador (usa `getattr`), mas o método público é inutilizável.
   - **Sugestão:** alinhar o despachante à resolução por `getattr`.
5. **Ramos e formatos inalcançáveis.**
   - **Localização:** `transform_smp_ericsson_gsm` (`output_format == "tim"` rejeitado por `_check_output_format`); `_format_cell_id` (formatos `smp_ericsson_gsm_tim` e `smp_nokia_algar`).
   - **Descrição:** o orquestrador nunca passa `output_format` diferente de `default`; nenhum fluxo chama `_format_cell_id` com `smp_ericsson_gsm_tim` ou `smp_nokia_algar`, e `"tim"` não é um formato válido.
   - **Impacto potencial:** código sem efeito observável e documentação enganosa sobre `output_format`.
   - **Sugestão:** remover ou ligar esses ramos de forma explícita.
6. **Mensagem de aviso de `_fill_missing_columns`.**
   - **Localização:** `CDRBaseTransformer._fill_missing_columns`.
   - **Descrição:** o log afirma criar as colunas com `NULL_SENTINEL_VALUE`, mas elas são criadas com `F.lit(None)`.
   - **Impacto potencial:** diagnóstico incorreto; o sentinela só entra depois, para as colunas da chave.
   - **Sugestão:** corrigir o texto do log.
7. **Colunas finais nunca populadas.**
   - **Localização:** `TARGET_SCHEMA` (`ip_origem`, `ip_destino`, `porta_ip_origem`, `porta_ip_destino`).
   - **Descrição:** nenhum catálogo ou transformador cria essas colunas.
   - **Impacto potencial:** `nu_ip_*` e `nu_porta_ip_*` são sempre `NULL`.
   - **Sugestão:** confirmar se há fonte de dados para elas.
8. **Colunas extraídas sem consumo.**
   - **Localização:** Nokia (`numero_destino_original`, `numero_conectado`, `numero_destino_encaminhamento`, `perfil_prestadora`), PCL Claro (`tecnologia_fornecedor`), Ericsson GSM (`status_chamada`) e `servico` (todos os fluxos).
   - **Impacto potencial:** informação disponível é descartada.
   - **Sugestão:** avaliar se devem compor o contrato.
9. **Natureza heterogênea de `nu_origem_original`/`nu_destino_original` no Huawei TIM.**
   - **Localização:** `_numero_origem_original` e `_numero_destino_original` em `transform_smp_huawei_volte_tim`.
   - **Descrição:** em registros `aTSRecord` o valor é o campo bruto (`tEL-URI` ou `_numero_destino_ats`); nos demais é o número já extraído da `sIP-URI`/`_numero_destino_ibcf`.
   - **Impacto potencial:** a mesma coluna final mistura formatos.
   - **Sugestão:** documentar o contrato esperado do valor “original”.
10. **Taxonomias de `no_resultado_chamada` e valores de fallback divergentes entre fluxos.**
    - **Localização:** `transform_stfc_huawei_ngn` e `transform_stfc_tropico_claro` (`unknown`), `transform_stfc_axe_claro` (`Desconexão Prematura`), `transform_smp_ericsson_volte_vivo` (mantém o código), demais (`NULL` → `__NULL__`).
    - **Impacto potencial:** comparações entre fornecedores não são diretamente equivalentes; valores nulos ou desconhecidos recebem rótulos diferentes.
    - **Sugestão:** consolidar uma taxonomia comum mantendo também o código bruto.
11. **Término com a data de início.**
    - **Localização:** `transform_smp_ericsson_gsm`, `transform_smp_ericsson_volte_vivo`, `transform_stfc_axe_claro` (`stop_date='_data'`) e `transform_stfc_sigma_claro` (`_data` + `_hora_fim`).
    - **Descrição:** `data_hora_fim` combina `_data` com `_hora_fim`; não há coluna de data de término nesses layouts extraídos.
    - **Impacto potencial:** se uma chamada atravessar a meia-noite, o término compõe a data de início com uma hora menor, resultando em `dh_fim_chamada` anterior a `dh_chamada` (a ocorrência real depende dos dados).
    - **Sugestão:** validar com amostras reais.
12. **Tipo de chamada por exclusão (PIT Claro) e constante (Trópico Claro).**
    - **Localização:** `transform_stfc_pit_claro` e `transform_stfc_tropico_claro`.
    - **Descrição:** qualquer `_prestadora_origem` diferente de `BRAEBT` (inclusive nulo) vira `Terminating`; ambos os lados do Trópico Claro recebem `tandem`.
    - **Impacto potencial:** classificação sem evidência por registro.
    - **Sugestão:** confirmar as regras de negócio.

## Pontos de atenção para manutenção

- Alterar `TARGET_SCHEMA` muda simultaneamente seleção, tipo, nome e ordem das colunas finais; a documentação e as 38 linhas de cada tabela de fluxo devem ser revisadas junto com essa constante.
- `PRIMARY_KEY_COLUMNS` usa nomes finais, enquanto o DataFrame intermediário usa nomes de origem; `_fill_primary_key_columns` faz o mapeamento. As 15 entradas recebem sentinelas e influenciam a deduplicação feita fora do transformador.
- Um novo layout exige uma chave em um dos catálogos (única entre os dois) e um método `transform_<cdr_schema>` em `CDRTransformer`, pois o orquestrador resolve por `getattr`.
- A restauração do número bruto depende de nomes exatos: somente `_numero_origem_original` e `_numero_destino_original` sobrescrevem `numero_origem` e `numero_destino`; o `numero_origem_original` (sem sublinhado) do Nokia não tem esse efeito.
- `_tipo_cdr`, quando mapeada, prevalece sobre o argumento `cdr_type` e sobre o caminho (Huawei TIM, Sigma Claro e FCDR Vivo), inclusive quando nula.
- O parsing Huawei TIM considera somente o primeiro elemento dos arrays JSON.
- A partição final depende de `no_tipo_chamada`; o sentinela `__NULL__` pode se tornar diretório de partição.
- O extrator Parquet suporta `unique=True` (`dropDuplicates`), mas o orquestrador não repassa o parâmetro e o extrator de texto não o oferece.
- No modo materializado, extração e transformação usam sessões diferentes; no modo direto, ambas usam a sessão do transformador. A equivalência de resultados entre os modos não foi verificada por execução.
- `concat_ws` (Sigma Claro) ignora componentes nulos, ao contrário de `_concat_or_null` (demais fluxos com composição de datas): uma hora nula em Sigma produz apenas a data, que não é analisável com a máscara e resulta em `MIN_SAFE_DATE`.

## Possíveis melhorias

Estas sugestões não foram implementadas:

- adicionar testes que comparem exatamente as 38 colunas finais e os nomes de `TARGET_SCHEMA` por fluxo;
- adicionar testes de integração por fluxo para datas, sentinelas, metadados e partição `no_tipo_chamada`;
- mapear `status_chamada` (Ericsson GSM) ou ajustar o nome intermediário, e definir `tipo_chamada`/`resultado_chamada` nos fluxos que hoje recebem `__NULL__`;
- tornar explícitos os casts antes das comparações de status (Nokia, Trópico Oi) e documentar o comportamento esperado sob ANSI;
- validar a estrutura de diretórios antes de derivar metadados;
- documentar externamente o layout posicional completo dos CSV e arquivos de largura fixa;
- corrigir ou remover `CDRTransformer.transform` e os ramos/formatos inalcançáveis de `output_format`;
- consolidar uma taxonomia comum de status mantendo também o código bruto.

# 10. Validação de cobertura

Lista independente de colunas finais: as 38 chaves de `TARGET_SCHEMA` (tabela 2.1). Cada uma aparece uma vez na tabela 2.1, uma vez na tabela da etapa final (seção 5) e uma vez em cada tabela de fluxo (seção 3).

| Fluxo | Colunas esperadas | Colunas documentadas | Com origem efetiva | Criadas nulas |
| :-- | --: | --: | --: | --: |
| `smp_ericsson_gsm` | 38 | 38 | 24 | 14 |
| `smp_nokia_gsm` | 38 | 38 | 26 | 12 |
| `smp_huawei_volte_tim` | 38 | 38 | 33 | 5 |
| `smp_ericsson_volte_vivo` | 38 | 38 | 31 | 7 |
| `stfc_italtel_ngn_tim` | 38 | 38 | 19 | 19 |
| `stfc_sigma_claro` | 38 | 38 | 18 | 20 |
| `stfc_huawei_ngn` | 38 | 38 | 20 | 18 |
| `stfc_huawei_ngn_tim` | 38 | 38 | 17 | 21 |
| `stfc_fcdr_vivo` | 38 | 38 | 18 | 20 |
| `stfc_7n_oi` | 38 | 38 | 17 | 21 |
| `stfc_tropico_oi` | 38 | 38 | 18 | 20 |
| `stfc_axe_claro` | 38 | 38 | 19 | 19 |
| `stfc_pcl_claro` | 38 | 38 | 19 | 19 |
| `stfc_tropico_claro_lega` | 38 | 38 | 20 | 18 |
| `stfc_tropico_claro_legb` | 38 | 38 | 20 | 18 |
| `stfc_pit_claro` | 38 | 38 | 19 | 19 |
| `stfc_ss8bf_claro` | 38 | 38 | 19 | 19 |

A coluna “Com origem efetiva” inclui as colunas derivadas pelo pipeline comum (quatro colunas de extração de números) e os metadados do extrator.

**Confirmado por análise estática do código:** a lista de 38 colunas finais (`TARGET_SCHEMA`), a existência de um método
`transform_<cdr_schema>` para cada uma das 17 chaves dos catálogos, as colunas extraídas e suas origens (verificadas programaticamente contra os catálogos
`PARQUET_DEFAULT_SCHEMAS` e `TEXT_DEFAULT_SCHEMAS`), as transformações, condições e tabelas de códigos de cada fluxo, a etapa final e as regras do pipeline comum. Regras de cinco fluxos (`smp_ericsson_volte_vivo`, `smp_huawei_volte_tim`, `smp_nokia_gsm`, `stfc_sigma_claro` e `stfc_ss8bf_claro`) também foram conferidas de forma pontual executando os transformadores sobre amostras sintéticas; isso não substitui a validação com dados reais.

**Depende de dados reais ou execução (não confirmado):** o tipo físico e o conteúdo dos campos brutos, o comportamento de casts sob `spark.sql.ansi.enabled`,
o tratamento de nulos pela pandas UDF, a leitura CSV de linhas de largura fixa que contenham `,` (AXE e PCL Claro), o significado semântico das posições dos layouts sem nomes
e a equivalência entre os modos materializado e direto. Estas lacunas estão detalhadas na seção 8.

**Lacunas remanescentes:** nenhuma coluna final ficou sem linha de documentação em nenhum fluxo. A rastreabilidade até a coluna original é completa para os fluxos Parquet
(nomes originais) e, para os fluxos texto, limita-se à posição (e largura) do campo, conforme as ambiguidades registradas.
Os fluxos `stfc_tropico_claro_lega` e `stfc_tropico_claro_legb` compartilham a mesma implementação e diferem apenas nas posições de três campos.
