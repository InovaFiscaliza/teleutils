# Documentação da Linhagem e Transformação dos Dados

## 1. Visão geral do fluxo

Este documento descreve exclusivamente o pipeline ativo em `src/teleutils/core` e
`src/teleutils/preprocessing`. O pacote `src/teleutils/robocalls` está depreciado e
foi deliberadamente excluído da análise.

Há cinco fluxos de produção do mesmo contrato final:

1. Parquet Ericsson: `CDRParquetExtractor.extract_cdr_ericsson` →
   `CDRTransformer.transform_cdr_ericsson`.
2. Parquet LTE Huawei TIM: `CDRParquetExtractor.extract_cdr_lte_huawei_tim` →
   `CDRTransformer.transform_cdr_lte_huawei_tim`.
3. Parquet LTE Ericsson Vivo: `CDRParquetExtractor.extract_cdr_lte_ericsson_vivo` →
   `CDRTransformer.transform_cdr_lte_ericsson_vivo`.
4. Parquet Nokia: `CDRParquetExtractor.extract_cdr_nokia` →
   `CDRTransformer.transform_cdr_nokia`.
5. Texto/CSV Algar Huawei: `CDRTextExtractor.extract_cdr_algar_huawei` →
   `CDRTransformer.transform_cdr_algar_huawei`.

Fluxo arquitetural:

```text
CDR de origem
→ seleção e renomeação segundo CDRParquetSchema ou CDRTextSchema
→ inclusão de prestadora, tipo_cdr e arquivo_origem
→ Parquet extraído/intermediário
→ pré-processamento específico do layout
→ _apply_standard_pipeline
→ _select_transformed_columns (cast + alias)
→ _write_parquet (overwrite, particionado por no_tipo_chamada)
→ Parquet final
```

O contrato final é determinado pela ordem de `TARGET_SCHEMA`, em
`src/teleutils/_config.py`, e contém **35 colunas**. Colunas intermediárias fora
desse contrato são descartadas na seleção final.

## 2. Convenções utilizadas

| Nível | Descrição |
| :-- | :-- |
| Colunas Originais | Campos presentes no Parquet bruto ou posições do CSV bruto. |
| Colunas Extraídas | Nomes atribuídos pelo extrator e gravados no Parquet intermediário. |
| Coluna Intermediária | Coluna criada ou alterada durante a transformação. |
| Coluna Final | Nome após o `cast` e o `alias` definidos em `TARGET_SCHEMA`. |

Classificações empregadas:

- **Renomeação**: somente o nome muda, durante a extração ou na seleção final.
- **Cópia**: o valor segue sem regra de conteúdo, embora possa receber o cast final.
- **Transformação**: o valor é calculado, analisado, normalizado, condicionado ou combinado.
- **Constante/metadado**: valor literal ou derivado do caminho do arquivo.
- **Ausente**: `_fill_missing_columns` cria `NULL`; se a coluna participa da chave
  primária, `_fill_primary_key_columns` substitui o nulo antes da escrita.

### 2.1 Contrato final e tipos

| # | Coluna anterior | Operação final | Coluna final | Tipo final |
| --: | :-- | :-- | :-- | :-- |
| 1 | `referencia` | cast + renomeação | `nu_referencia` | string |
| 2 | `referencia_sip` | cast + renomeação | `nu_referencia_sip` | string |
| 3 | `data_hora_referencia` | cast + renomeação | `dh_referencia` | timestamp_ntz |
| 4 | `data_hora` | cast + renomeação | `dh_chamada` | timestamp_ntz |
| 5 | `data_hora_fim` | cast + renomeação | `dh_fim_chamada` | timestamp_ntz |
| 6 | `duracao` | cast + renomeação | `qt_duracao_segundos` | integer |
| 7 | `numero_origem_formatado` | cast + renomeação | `nu_origem` | string |
| 8 | `numero_origem_valido` | cast + renomeação | `ic_origem_valido` | boolean |
| 9 | `numero_origem` | cast + renomeação | `nu_origem_original` | string |
| 10 | `numero_destino_formatado` | cast + renomeação | `nu_destino` | string |
| 11 | `numero_destino_valido` | cast + renomeação | `ic_destino_valido` | boolean |
| 12 | `numero_destino` | cast + renomeação | `nu_destino_original` | string |
| 13 | `status_chamada` | cast + renomeação | `no_resultado_chamada` | string |
| 14 | `codigo_resposta_sip` | cast + renomeação | `co_resposta_sip` | string |
| 15 | `autenticacao` | cast + renomeação | `no_autenticacao` | string |
| 16 | `prestadora` | cast + renomeação | `no_prestadora` | string |
| 17 | `rota_entrada` | cast + renomeação | `no_rota_entrada` | string |
| 18 | `rota_saida` | cast + renomeação | `no_rota_saida` | string |
| 19 | `bilhetador` | cast + renomeação | `no_bilhetador` | string |
| 20 | `celula_origem` | cast + renomeação | `nu_cgi_origem` | string |
| 21 | `celula_origem_hex` | cast + renomeação | `nu_cgi_origem_hex` | string |
| 22 | `imei_origem` | cast + renomeação | `nu_imei_origem` | string |
| 23 | `imsi_origem` | cast + renomeação | `nu_imsi_origem` | string |
| 24 | `ip_origem` | cast + renomeação | `nu_ip_origem` | string |
| 25 | `porta_ip_origem` | cast + renomeação | `nu_porta_ip_origem` | integer |
| 26 | `celula_destino` | cast + renomeação | `nu_cgi_destino` | string |
| 27 | `celula_destino_hex` | cast + renomeação | `nu_cgi_destino_hex` | string |
| 28 | `imei_destino` | cast + renomeação | `nu_imei_destino` | string |
| 29 | `imsi_destino` | cast + renomeação | `nu_imsi_destino` | string |
| 30 | `ip_destino` | cast + renomeação | `nu_ip_destino` | string |
| 31 | `porta_ip_destino` | cast + renomeação | `nu_porta_ip_destino` | integer |
| 32 | `agente_usuario` | cast + renomeação | `no_agente_usuario` | string |
| 33 | `tipo_cdr` | cast + renomeação | `no_tipo_cdr` | string |
| 34 | `arquivo_origem` | cast + renomeação | `no_arquivo_origem` | string |
| 35 | `tipo_chamada` | cast + renomeação | `no_tipo_chamada` | string |

### 2.2 Transformações comuns anteriores à seleção final

`_apply_standard_pipeline` executa, nesta ordem:

1. `_fill_missing_columns`: cria como `NULL` qualquer chave de `TARGET_SCHEMA`
   ausente no DataFrame.
2. `_format_date_time`: converte `duracao` para inteiro, usando `0` quando nula
   ou não conversível; analisa as três datas com `try_to_timestamp` e aplica
   `greatest(..., MIN_SAFE_DATE)`, cujo limite é `1901-01-01 00:00:00`.
3. `_format_numbers`: aplica `spark_normalize_number` separadamente a
   `numero_origem` e `numero_destino`, gerando número formatado e indicador de
   validade. Depois restaura os valores brutos quando existem as colunas com
   nomes exatos `_numero_origem_original` e `_numero_destino_original`.
4. `_add_tn_validation_status`: transforma prefixos de `_autenticacao` em
   `TN-Validation-Passed`, `TN-Validation-Failed` ou `No-TN-Validation`; sem a
   coluna de entrada, cria `autenticacao = NULL`.
5. `_fill_primary_key_columns`: nas 15 colunas da chave, substitui nulos por
   `MIN_SAFE_DATE` para timestamps, `0` para números e `"__NULL__"` para os
   demais tipos.

Assim, “ausente” não significa sempre `NULL` no Parquet final. Para as colunas
da chave (`no_tipo_chamada`, referências, datas, duração, números formatados e
originais, resultado, rotas e bilhetador), vale o preenchimento descrito acima.

## 3. Transformações por formato de origem

## 3.1 `transform_cdr_ericsson`

### Descrição

Lê o Parquet intermediário Ericsson, converte duração `HH:mm:ss` em segundos e
monta células, IMSIs e IMEIs a partir de componentes. Aplica o formato temporal
`yy-MM-dd HH:mm:ss` ao pipeline comum.

### Fluxo resumido

```text
Parquet bruto Ericsson
→ mapeamento nominal do schema Ericsson + metadados do caminho
→ duração e identificadores compostos
→ pipeline comum
→ cast/renomeação TARGET_SCHEMA
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| `networkCallReference` | `referencia` | cópia; nulo vira `"__NULL__"` pela chave | `nu_referencia` |
| Não aplicável | Não aplicável | `referencia_sip` criada nula; chave → `"__NULL__"` | `nu_referencia_sip` |
| Não aplicável | Não aplicável | `data_hora_referencia` criada nula; normalização → `MIN_SAFE_DATE` | `dh_referencia` |
| `dateForStartOfCharge`, `timeForStartOfCharge` | `_data`, `_hora` | **não utilizadas**; `data_hora` é criada nula → `MIN_SAFE_DATE` | `dh_chamada` |
| `timeForStopOfCharge` | `_hora_fim` | **não utilizada**; `data_hora_fim` é criada nula → `MIN_SAFE_DATE` | `dh_fim_chamada` |
| `chargeableDuration` | `duracao` | `HH*3600 + mm*60 + ss`; nulo → `0`; cast inteiro | `qt_duracao_segundos` |
| `callingPartyNumber.digits` | `numero_origem` | `spark_normalize_number(...).numero_formatado`; chave → sentinela se nulo | `nu_origem` |
| mesma origem de `nu_origem` | `numero_origem` | `spark_normalize_number(...).numero_valido` | `ic_origem_valido` |
| `callingPartyNumber.digits` | `numero_origem` | valor bruto mantido; chave → `"__NULL__"` se nulo | `nu_origem_original` |
| `calledPartyNumber.digits` | `numero_destino` | `spark_normalize_number(...).numero_formatado`; chave → sentinela se nulo | `nu_destino` |
| mesma origem de `nu_destino` | `numero_destino` | `spark_normalize_number(...).numero_valido` | `ic_destino_valido` |
| `calledPartyNumber.digits` | `numero_destino` | valor bruto mantido; chave → `"__NULL__"` se nulo | `nu_destino_original` |
| `callPosition` | `status_chamada` | cópia; chave → `"__NULL__"` se nulo | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula; mantida nula | `co_resposta_sip` |
| Não aplicável | Não aplicável | `_autenticacao` ausente; `autenticacao = NULL` | `no_autenticacao` |
| caminho do arquivo, segmento `-3` | `prestadora` | metadado mantido | `no_prestadora` |
| `incomingRoute` | `rota_entrada` | cópia; chave → `"__NULL__"` se nulo | `no_rota_entrada` |
| `outgoingRoute` | `rota_saida` | cópia; chave → `"__NULL__"` se nulo | `no_rota_saida` |
| `exchangeIdentity` | `bilhetador` | cópia; chave → `"__NULL__"` se nulo | `no_bilhetador` |
| `firstCallingLocationInformation.{mcc,mnc,lac,ci_sac}` | quatro componentes `celula_origem_*` | branco → nulo; LAC/CI com `lpad(5)`; concatenação `-`, ou nulo | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula | `nu_cgi_origem_hex` |
| `callingSubscriberIMEI.{type_allocation_code,serial_number}` | `imei_origem_tac`, `imei_origem_sn` | branco → nulo; concatenação sem separador, ou nulo | `nu_imei_origem` |
| `callingSubscriberIMSI.{mcc,mnc,msin}` | três componentes `imsi_origem_*` | branco → nulo; concatenação sem separador, ou nulo | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula; cast inteiro | `nu_porta_ip_origem` |
| `firstCalledLocationInformation.{mcc,mnc,lac,ci_sac}` | quatro componentes `celula_destino_*` | branco → nulo; LAC/CI com `lpad(5)`; concatenação `-`, ou nulo | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula | `nu_cgi_destino_hex` |
| `calledSubscriberIMEI.{type_allocation_code,serial_number}` | `imei_destino_tac`, `imei_destino_sn` | branco → nulo; concatenação sem separador, ou nulo | `nu_imei_destino` |
| `calledSubscriberIMSI.{mcc,mnc,msin}` | três componentes `imsi_destino_*` | branco → nulo; concatenação sem separador, ou nulo | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula; cast inteiro | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula | `no_agente_usuario` |
| caminho do arquivo, segmento `-2` | `tipo_cdr` | metadado mantido | `no_tipo_cdr` |
| caminho do arquivo, segmento `-1` | `arquivo_origem` | metadado mantido sem `url_decode` | `no_arquivo_origem` |
| `CallModule` | `tipo_chamada` | cópia; chave → `"__NULL__"` se nulo | `no_tipo_chamada` |

### Funções auxiliares específicas

Não há auxiliar exclusiva deste fluxo. Ele reutiliza `_build_composite_column`,
`_concat_or_null` e `_null_if_blank`.

## 3.2 `transform_cdr_lte_huawei_tim`

### Descrição

O extrator remove duplicatas do conjunto completo (`dropDuplicates`) e substitui
o `tipo_cdr` derivado do caminho pelo conteúdo original de `recordType`. A
transformação diferencia `aTSRecord` e `iBCFRecord`, além dos papéis
`oRIGINATING-ROLE` e `tERMINATING-ROLE`.

### Fluxo resumido

```text
Parquet bruto Huawei TIM
→ mapeamento + dropDuplicates + recordType como tipo_cdr
→ JSON/regex para números, célula, IMSI, IMEI e status
→ atribuição por tipo de CDR e papel
→ pipeline comum (yyyy-MM-dd HH:mm:ss)
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| `network-Call-Reference` | `referencia` | cópia; chave → `"__NULL__"` se nulo | `nu_referencia` |
| `iMS-Charging-Identifier` | `referencia_sip` | cópia; chave → `"__NULL__"` se nulo | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula; normalização → `MIN_SAFE_DATE` | `dh_referencia` |
| `serviceRequestTimeStamp` | `data_hora` | primeiros 19 caracteres → parse → limite `MIN_SAFE_DATE` | `dh_chamada` |
| `serviceDeliveryEndTimeStamp` | `data_hora_fim` | primeiros 19 caracteres → parse → limite `MIN_SAFE_DATE` | `dh_fim_chamada` |
| `duration` | `duracao` | cast inteiro; nulo/inválido → `0` | `qt_duracao_segundos` |
| origens descritas para `nu_origem_original` | campos `_numero_origem*` | ATS: extração do genérico ou valor TEL-URI com troca de cada par e `substr(3)`; demais: segunda regex `sip:` sobre `_numero_origem_ibcf`; depois normalização | `nu_origem` |
| mesmas origens de `nu_origem` | campos `_numero_origem*` | indicador retornado por `spark_normalize_number` | `ic_origem_valido` |
| `calling-Party-Address-Generic`; `list-Of-Calling-Party-Address` | `_numero_origem_ats_auth`, `_numero_origem` | ATS: valor genérico se presente, senão TEL-URI extraída; não ATS: SIP-URI extraída; restauração do bruto | `nu_origem_original` |
| origens descritas para `nu_destino_original` | `_numero_destino_ats`, `_numero_destino_ibcf` | número discável condicional → `spark_normalize_number`; chave → sentinela | `nu_destino` |
| mesmas origens de `nu_destino` | `_numero_destino_ats`, `_numero_destino_ibcf` | indicador retornado por `spark_normalize_number` | `ic_destino_valido` |
| `called-Party-Address_tEL-URI`; `called-Party-Address_sIP-URI` | `_numero_destino_ats`, `_numero_destino_ibcf` | ATS preserva TEL-URI bruta; não ATS preserva SIP-URI bruta; restauração do bruto | `nu_destino_original` |
| `serviceReasonReturnCode` | `_status_chamada` | IBCF extrai `SIP;cause=N;`; demais fazem cast inteiro; classificação por código/faixa | `no_resultado_chamada` |
| `serviceReasonReturnCode` | `_status_chamada` | valor inteiro quando `>= 200`, senão nulo; cast final string | `co_resposta_sip` |
| `calling-Party-Address-Generic`; `list-Of-Calling-Party-Address` | `_numero_origem_ats_auth`, `_numero_origem` | regex `verstat=...` por tipo; classificação comum em três rótulos | `no_autenticacao` |
| caminho do arquivo, segmento `-3` | `prestadora` | metadado mantido | `no_prestadora` |
| `specifiedTreatmentField_incoming-Route` | `rota_entrada` | cópia; chave → sentinela se nulo | `no_rota_entrada` |
| `specifiedTreatmentField_outgoing-Route` | `rota_saida` | cópia; chave → sentinela se nulo | `no_rota_saida` |
| `nodeAddress_domainName` | `bilhetador` | cópia; chave → sentinela se nulo | `no_bilhetador` |
| `accessNetworkInformation` | `_informacao_rede` | regex de `utran-cell-id-3gpp`; `_format_cell_id`; somente papel originante | `nu_cgi_origem` |
| `accessNetworkInformation` | `_informacao_rede` | regex de `utran-cell-id-3gpp`; somente papel originante | `nu_cgi_origem_hex` |
| tipo e valor de `private-User-Equipment-Info` | `_info_imei`, `_imei` | aceita valor só quando tipo=`iMEI`; somente papel originante | `nu_imei_origem` |
| `list-of-subscription-ID` | `_info_imsi` | primeiro objeto JSON; aceita `eND-USER-IMSI`; somente papel originante | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula; cast inteiro | `nu_porta_ip_origem` |
| `accessNetworkInformation` | `_informacao_rede` | regex + `_format_cell_id`; somente papel terminante | `nu_cgi_destino` |
| `accessNetworkInformation` | `_informacao_rede` | regex; somente papel terminante | `nu_cgi_destino_hex` |
| tipo e valor de `private-User-Equipment-Info` | `_info_imei`, `_imei` | aceita valor só quando tipo=`iMEI`; somente papel terminante | `nu_imei_destino` |
| `list-of-subscription-ID` | `_info_imsi` | primeiro objeto JSON; aceita `eND-USER-IMSI`; somente papel terminante | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula; cast inteiro | `nu_porta_ip_destino` |
| `user-Agent-Value` | `agente_usuario` | cópia | `no_agente_usuario` |
| `recordType` | `_tipo_cdr` → `tipo_cdr` | substitui o metadado inicialmente derivado do caminho | `no_tipo_cdr` |
| caminho do arquivo, segmento `-1` | `arquivo_origem` | metadado mantido sem `url_decode` | `no_arquivo_origem` |
| `role-of-Node` | `tipo_chamada` | cópia; chave → sentinela se nulo | `no_tipo_chamada` |

### Caminhos de status

- `-399..-300` → `Redirection`; `-299..-200` → `Final Response`.
- `-3`, `-2`, `-1`, `0`, `1`, `2`, `3`, `4`, `5` possuem rótulos individuais.
- `200` → `Normal end of session`; `201..299` → `Final Response`.
- `300..399` → `Redirection`; `400..499` → `Request failure`;
  `500..599` → `Server failure`; `600..699` → `Global failure`.
- Qualquer outro valor → nulo antes do preenchimento de chave; como
  `status_chamada` integra a chave, o resultado final passa a `"__NULL__"`.

### Funções auxiliares específicas

`_AUTH_EXTRACT_PATTERN` é a expressão regular
`(verstat=[a-zA-Z\-]+)`. O parsing do IMSI usa um `ArrayType` de objetos
`{info_type, info_value}` declarado dentro do método e considera somente o
primeiro elemento.

## 3.3 `transform_cdr_lte_ericsson_vivo`

### Descrição

Separa o número e a autenticação embutidos em `callingPartyNumber`, traduz
códigos conhecidos de tipo/status, remove hífens dos IMEIs e decodifica células
hexadecimais. O formato temporal configurado é `yyyyMMdd HHmmss`.

### Fluxo resumido

```text
Parquet bruto Vivo Ericsson
→ mapeamento + metadados
→ split de callingPartyNumber, enums, IMEIs e células
→ pipeline comum
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| `networkCallReference` | `referencia` | cópia; chave → sentinela se nulo | `nu_referencia` |
| `imsChargingIdentifier` | `referencia_sip` | cópia; chave → sentinela se nulo | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula → `MIN_SAFE_DATE` | `dh_referencia` |
| `dateForStartOfCharge`, `timeForStartOfCharge` | `_data`, `_hora` | **não utilizadas**; `data_hora` criada nula → `MIN_SAFE_DATE` | `dh_chamada` |
| `timeForStopOfCharge` | `_hora_fim` | **não utilizada**; `data_hora_fim` criada nula → `MIN_SAFE_DATE` | `dh_fim_chamada` |
| `chargeableDurat` | `duracao` | cast inteiro; nulo/inválido → `0` | `qt_duracao_segundos` |
| `callingPartyNumber` | `_numero_origem_original` | `split(';')[0]` → normalização; chave → sentinela | `nu_origem` |
| mesma origem de `nu_origem` | `_numero_origem_original` | indicador retornado pela normalização | `ic_origem_valido` |
| `callingPartyNumber` | `_numero_origem_original` | valor bruto completo restaurado depois da normalização | `nu_origem_original` |
| `calledPartyNumber` | `numero_destino` | normalização; chave → sentinela | `nu_destino` |
| mesma origem de `nu_destino` | `numero_destino` | indicador retornado pela normalização | `ic_destino_valido` |
| `calledPartyNumber` | `numero_destino` | cópia do valor bruto | `nu_destino_original` |
| `callPosition` | `_status_chamada` | `1`, `2`, `3` → rótulos; demais mantidos | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula | `co_resposta_sip` |
| `callingPartyNumber` | `_numero_origem_original` | `split(';')[1]` → classificação `verstat` | `no_autenticacao` |
| caminho do arquivo, segmento `-3` | `prestadora` | metadado mantido | `no_prestadora` |
| `incomingRoute` | `rota_entrada` | cópia; chave → sentinela se nulo | `no_rota_entrada` |
| `outgoingRoute` | `rota_saida` | cópia; chave → sentinela se nulo | `no_rota_saida` |
| `exchangeIdentity` | `bilhetador` | cópia; chave → sentinela se nulo | `no_bilhetador` |
| `firstCallingLocInf` | `celula_origem_hex` | `_format_cell_id` conforme comprimento | `nu_cgi_origem` |
| `firstCallingLocInf` | `celula_origem_hex` | cópia | `nu_cgi_origem_hex` |
| `callingSubscriberIMEI` | `imei_origem` | remove todos os hífens com `translate` | `nu_imei_origem` |
| `callingSubscriberIMSI` | `imsi_origem` | cópia | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula; cast inteiro | `nu_porta_ip_origem` |
| `firstCalledLocInfo` | `celula_destino_hex` | `_format_cell_id` conforme comprimento | `nu_cgi_destino` |
| `firstCalledLocInfo` | `celula_destino_hex` | cópia | `nu_cgi_destino_hex` |
| `calledSubscriberIMEI` | `imei_destino` | remove todos os hífens com `translate` | `nu_imei_destino` |
| `calledSubscriberIMSI` | `imsi_destino` | cópia | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula; cast inteiro | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula | `no_agente_usuario` |
| caminho do arquivo, segmento `-2` | `tipo_cdr` | metadado mantido | `no_tipo_cdr` |
| caminho do arquivo, segmento `-1` | `arquivo_origem` | metadado mantido sem `url_decode` | `no_arquivo_origem` |
| `callModule` | `_tipo_chamada` | `1`→`msOriginating`; `3`→`callForwarding`; `4`→`msTerminating`; demais mantidos | `no_tipo_chamada` |

### Funções auxiliares específicas

Não há auxiliar exclusiva. `_format_cell_id` é aplicada duas vezes, uma para
cada direção.

## 3.4 `transform_cdr_nokia`

### Descrição

Consolida oito possíveis campos de duração, escolhe timestamps e números por
precedência/tipo de chamada, compõe células com MCC/MNC imputados e agrupa o
status em faixas. O formato temporal é `dd/MM/yyyy HH:mm:ss`.

### Fluxo resumido

```text
Parquet bruto Nokia
→ mapeamento + metadados
→ coalescências, regras UCA/FORW, células e status
→ pipeline comum
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| `call_reference` | `referencia` | cópia; chave → sentinela se nulo | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula; chave → `"__NULL__"` | `nu_referencia_sip` |
| `call_reference_time` | `data_hora_referencia` | parse + limite `MIN_SAFE_DATE` | `dh_referencia` |
| `in_channel_allocated_time`, `call_reference_time` | `data_hora_alocacao_canal`, `data_hora_referencia` | `coalesce` nessa ordem → parse + limite | `dh_chamada` |
| `release_time`, `charging_end_time` | `data_hora_desconexao`, `data_hora_fim` | UCA: `coalesce(release, charging_end)`; demais: `charging_end`; parse + limite | `dh_fim_chamada` |
| oito campos `*_duration` | oito colunas `_duracao*` | `FFFFFF` → nulo; `coalesce` na ordem do schema; cast inteiro; fallback `0` | `qt_duracao_segundos` |
| `calling_number`, `orig_calling_number` | `numero_origem`, `numero_origem_original` | `coalesce` → normalização; chave → sentinela | `nu_origem` |
| mesmas origens de `nu_origem` | mesmas extraídas | indicador retornado pela normalização | `ic_origem_valido` |
| `calling_number`, `orig_calling_number` | `numero_origem`, `numero_origem_original` | resultado bruto do `coalesce`; coluna sem sublinhado não é restaurada pelo pipeline | `nu_origem_original` |
| `forwarding_number`, `called_number` | `numero_origem_encaminhamento`, `numero_destino` | FORW usa `forwarding_number`; demais usam `called_number`; normalização | `nu_destino` |
| mesmas origens de `nu_destino` | mesmas extraídas | indicador retornado pela normalização | `ic_destino_valido` |
| `forwarding_number`, `called_number` | mesmas extraídas | resultado bruto da seleção condicional | `nu_destino_original` |
| `cause_for_termination` | `_status_chamada` | comparação com limites inteiros hexadecimais; agrupamento em cinco faixas | `no_resultado_chamada` |
| Não aplicável | Não aplicável | criada nula | `co_resposta_sip` |
| Não aplicável | Não aplicável | `_autenticacao` ausente; criada nula | `no_autenticacao` |
| caminho do arquivo, segmento `-3` | `prestadora` | metadado; também decide MNC das células | `no_prestadora` |
| `in_circuit_group` | `rota_entrada` | cópia; chave → sentinela se nulo | `no_rota_entrada` |
| `out_circuit_group` | `rota_saida` | cópia; chave → sentinela se nulo | `no_rota_saida` |
| `exchange_id` | `bilhetador` | cópia; chave → sentinela se nulo | `no_bilhetador` |
| `calling_subs_first_lac`, `calling_subs_first_ci` e caminho | `celula_origem_lac`, `celula_origem_ci`, `prestadora` | `724`; MNC `05` para `claro`, `34` para `algar`, senão nulo; LAC/CI `lpad(5)`; concatenação | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula | `nu_cgi_origem_hex` |
| `calling_imei` | `imei_origem` | cópia | `nu_imei_origem` |
| `calling_imsi` | `imsi_origem` | cópia | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula; cast inteiro | `nu_porta_ip_origem` |
| `called_subs_first_lac`, `called_subs_first_ci` e caminho | `celula_destino_lac`, `celula_destino_ci`, `prestadora` | mesma regra de MCC/MNC/padding da origem | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula | `nu_cgi_destino_hex` |
| `called_imei` | `imei_destino` | cópia | `nu_imei_destino` |
| `called_imsi` | `imsi_destino` | cópia | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula; cast inteiro | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula | `no_agente_usuario` |
| caminho do arquivo, segmento `-2` | `tipo_cdr` | metadado mantido | `no_tipo_cdr` |
| caminho do arquivo, segmento `-1` | `arquivo_origem` | metadado mantido sem `url_decode` | `no_arquivo_origem` |
| `record_type` | `tipo_chamada` | cópia; chave → sentinela se nulo | `no_tipo_chamada` |

### Caminhos de status

- `0x0000..0x03FF` → `normal clearing`.
- `0x0400..0x07FF` → `internal congestion`.
- `0x0800..0x0BFF` → `external congestion`.
- `0x0C00..0x0FFF` → `subscriber errors`.
- `>= 0x1000` → `event codes`.
- Fora dessas condições → nulo e, por integrar a chave, `"__NULL__"` no final.

### Funções auxiliares específicas

Não há função exclusiva; as listas de duração e respectivas expressões são
construídas localmente no método. A seleção usa todas as colunas cujo nome
começa por `_duracao`, na ordem corrente do DataFrame.

## 3.5 `transform_cdr_algar_huawei`

### Descrição

O único fluxo texto/CSV seleciona campos por índice zero-based, sem cabeçalho,
com delimitador vírgula. Não há schema explícito de leitura. A grafia `huawei`
é a usada nas APIs e chaves atuais do código.

### Fluxo resumido

```text
CSV Algar Huawei
→ seleção posicional + aliases + metadados do caminho
→ Parquet intermediário
→ composição temporal e tradução de códigos
→ pipeline comum (ddMMyyyy HHmmss)
→ Parquet final
```

### Tabela de mapeamento e transformações detalhadas por coluna

| Colunas Originais | Colunas Extraídas | Coluna Intermediária / transformação | Coluna Final |
| :-- | :-- | :-- | :-- |
| posição 0 | `referencia` | cópia; chave → sentinela se nulo | `nu_referencia` |
| Não aplicável | Não aplicável | criada nula; chave → `"__NULL__"` | `nu_referencia_sip` |
| Não aplicável | Não aplicável | criada nula → `MIN_SAFE_DATE` | `dh_referencia` |
| posições 3 e 4 | `_data`, `_hora` | `concat_ws(' ', _data, _hora)`; vazio → nulo; parse + limite | `dh_chamada` |
| posições 5 e **4** | `_data_fim`, `_hora` | `concat_ws(' ', _data_fim, _hora)`; `_hora_fim` da posição 6 não é usada | `dh_fim_chamada` |
| posição 7 | `duracao` | cast inteiro; nulo/inválido → `0` | `qt_duracao_segundos` |
| posição 8 | `numero_origem` | normalização; chave → sentinela | `nu_origem` |
| posição 8 | `numero_origem` | indicador retornado pela normalização | `ic_origem_valido` |
| posição 8 | `numero_origem` | valor bruto mantido | `nu_origem_original` |
| posição 9 | `numero_destino` | normalização; chave → sentinela | `nu_destino` |
| posição 9 | `numero_destino` | indicador retornado pela normalização | `ic_destino_valido` |
| posição 9 | `numero_destino` | valor bruto mantido | `nu_destino_original` |
| posição 22 | `_status_chamada` | `00`→`caller party on-hook`; `01`→`called party on-hook`; `02`→`abnormal`; demais→`unknown` | `no_resultado_chamada` |
| posição 21 | `codigo_resposta_sip` | cópia + cast string | `co_resposta_sip` |
| Não aplicável | Não aplicável | `_autenticacao` ausente; criada nula | `no_autenticacao` |
| caminho do arquivo, segmento `-3` | `prestadora` | metadado mantido | `no_prestadora` |
| posição 17 | `rota_entrada` | cópia; chave → sentinela se nulo | `no_rota_entrada` |
| posição 18 | `rota_saida` | cópia; chave → sentinela se nulo | `no_rota_saida` |
| posição 1 | `bilhetador` | cópia; chave → sentinela se nulo | `no_bilhetador` |
| Não aplicável | Não aplicável | criada nula | `nu_cgi_origem` |
| Não aplicável | Não aplicável | criada nula | `nu_cgi_origem_hex` |
| Não aplicável | Não aplicável | criada nula | `nu_imei_origem` |
| Não aplicável | Não aplicável | criada nula | `nu_imsi_origem` |
| Não aplicável | Não aplicável | criada nula | `nu_ip_origem` |
| Não aplicável | Não aplicável | criada nula; cast inteiro | `nu_porta_ip_origem` |
| Não aplicável | Não aplicável | criada nula | `nu_cgi_destino` |
| Não aplicável | Não aplicável | criada nula | `nu_cgi_destino_hex` |
| Não aplicável | Não aplicável | criada nula | `nu_imei_destino` |
| Não aplicável | Não aplicável | criada nula | `nu_imsi_destino` |
| Não aplicável | Não aplicável | criada nula | `nu_ip_destino` |
| Não aplicável | Não aplicável | criada nula; cast inteiro | `nu_porta_ip_destino` |
| Não aplicável | Não aplicável | criada nula | `no_agente_usuario` |
| caminho do arquivo, segmento `-2` | `tipo_cdr` | metadado mantido | `no_tipo_cdr` |
| caminho do arquivo, segmento `-1` | `arquivo_origem` | `url_decode` aplicado ao nome | `no_arquivo_origem` |
| posição 19 | `_tipo_chamada` | `01`→`intra_office`; `02`→`incoming_office`; `03`→`outgoing_office`; `04`→`tandem`; `05`→`new_service`; demais→`unknown` | `no_tipo_chamada` |

### Funções auxiliares específicas

Não há função auxiliar exclusiva; as duas expressões temporais e os dois
mapeamentos de códigos estão definidos diretamente no método.

## 4. Múltiplos caminhos para colunas finais

### `nu_origem_original` e `nu_origem` no Huawei TIM

```text
aTSRecord com calling-Party-Address-Generic não nulo
→ regex ":+?dígitos" para normalização
→ valor genérico bruto como nu_origem_original

aTSRecord sem campo genérico
→ primeiro tEL-URI do JSON → regexp_replace global que troca cada par de caracteres → substr(3)
→ valor tEL-URI transformado como nu_origem_original

demais tipos, inclusive iBCFRecord
→ primeiro sIP-URI do JSON → regex `sip:([0-9]+)[@;]` produz somente dígitos
→ uma segunda regex que exige prefixo `sip:` é aplicada a esses dígitos para normalização
→ valor extraído para _numero_origem_ibcf como nu_origem_original
```

O ramo `otherwise` não testa explicitamente `is_ibcf`; tipos diferentes de
`aTSRecord` também seguem o caminho IBCF. Pela composição estática das duas
regexes, o valor usado para normalizar a origem IBCF tende a ser string vazia;
o valor original final continua sendo a saída em dígitos da primeira regex.

### Dispositivo no Huawei TIM

```text
oRIGINATING-ROLE → célula/IMEI/IMSI somente nas colunas de origem
tERMINATING-ROLE → célula/IMEI/IMSI somente nas colunas de destino
qualquer outro papel → ambas as direções nulas
```

### Datas e números Nokia

```text
dh_chamada: in_channel_allocated_time → fallback call_reference_time
dh_fim_chamada UCA: release_time → fallback charging_end_time
dh_fim_chamada não UCA: charging_end_time

nu_origem_original: calling_number → fallback orig_calling_number
nu_destino_original FORW: forwarding_number
nu_destino_original não FORW: called_number
```

`orig_called_number`, `connected_to_number` e `forwarded_to_number` são
extraídas no fluxo Nokia, mas não alimentam nenhuma coluna final.

### Células Nokia

```text
prestadora == "claro" → MCC 724 + MNC 05 + LAC + CI
prestadora == "algar" → MCC 724 + MNC 34 + LAC + CI
outra prestadora → MNC nulo → composto inteiro nulo
```

### Células Vivo e Huawei TIM

`_format_cell_id` escolhe o caminho pelo comprimento da string:

- 13 caracteres: MCC/MNC textuais + TAC e CI hexadecimais convertidos para
  decimal e preenchidos até 5 posições.
- 16 caracteres: MCC/MNC + valor ECGI hexadecimal; separa eNodeB por divisão
  inteira por 256 e Cell ID por módulo 256.
- 20 caracteres: MCC/MNC + NCGI hexadecimal de 36 bits; separa gNB ID com
  deslocamento de 10 bits e Cell ID com máscara `0x3FF`.
- outro comprimento: mantém o valor original.
- resultado vazio: nulo.

## 5. Etapa final de geração do Parquet

### Responsáveis

- `CDRBaseTransformer._select_transformed_columns`: percorre `TARGET_SCHEMA` na
  ordem declarada, aplica `cast(data_type)` e `alias(target_column)`.
- `CDRBaseTransformer._write_parquet`: grava o DataFrame selecionado com modo
  `overwrite`, particionado por `no_tipo_chamada`.

Não há `drop` explícito nessa etapa. A projeção por `select` exclui todas as
colunas que não pertencem ao contrato. O particionamento Spark normalmente
materializa `no_tipo_chamada` como coluna de partição no diretório, recuperada
na leitura do dataset Parquet.

### Schema final completo

O schema final, com 35 entradas, está enumerado na seção 2.1. Toda entrada
executa **cast + renomeação**; isso não deve ser confundido com as transformações
de conteúdo executadas antes dessa etapa.

## 6. Funções auxiliares específicas

As funções específicas locais já foram indicadas ao fim de cada fluxo. Os
seguintes artefatos declarativos controlam a extração:

### `PARQUET_DEFAULT_SCHEMAS`

**Localização:** `src/teleutils/core/extractors/schemas/parquet.py`.

**Finalidade:** mapear nomes originais para colunas do Parquet intermediário nos
quatro layouts Parquet.

**Entradas e saídas:** pares `(source_col, target_col)`; campos originais
ausentes são representados por literal nulo tipado inicialmente como string.

### `TEXT_DEFAULT_SCHEMAS["algar_huawei"]`

**Localização:** `src/teleutils/core/extractors/schemas/text.py`.

**Finalidade:** selecionar as posições `(0, 1, 3, 4, 5, 6, 7, 8, 9, 17, 18,
19, 21, 22)` do CSV e atribuir os nomes intermediários documentados na seção
3.5.

# Funções auxiliares comuns

## Funções em nível de módulo

### `_null_if_blank`

**Localização:** `src/teleutils/core/transformers/transformers.py`.

Recebe o nome de uma coluna e retorna o valor original, exceto quando seu cast
para string, após `trim`, é vazio; nesse caso retorna nulo. Impacta componentes
de células, IMSIs e IMEIs nos fluxos Ericsson e Nokia.

### `_concat_or_null`

**Localização:** `src/teleutils/core/transformers/transformers.py`.

Converte nomes em expressões `Column`, testa se qualquer componente é nulo e
só então aplica `concat_ws`. Evita identificadores compostos parciais.

### `_build_composite_column`

**Localização:** `src/teleutils/core/transformers/transformers.py`.

Combina `_null_if_blank`, `lpad(5)` opcional e `_concat_or_null`. É usada para
células, IMSIs e IMEIs Ericsson e para células Nokia.

### `_format_cell_id`

**Localização:** `src/teleutils/core/transformers/transformers.py`.

Decodifica identificadores 3G, 4G ou 5G conforme o comprimento. É usada pelos
fluxos Huawei TIM e Vivo. Entradas: DataFrame, coluna hexadecimal, coluna de
saída e, opcionalmente, largura do gNB (26 bits por padrão). Saída: DataFrame
com a coluna formatada.

### `normalize_number` e `spark_normalize_number`

**Localização:** `src/teleutils/preprocessing/number_format.py`.

`normalize_number` retém o primeiro trecho antes de `;`, remove `f`, letras,
pontuação, espaços e prefixos de discagem (`90`/`9090`, `00` ou `0`), e valida
o resultado contra padrões brasileiros. Em sucesso retorna o trecho capturado
e `True`; em falha, o valor de entrada e `False`; entrada vazia produz
`("5599999999999", False)`. `spark_normalize_number` expõe essa regra como UDF
Pandas de struct. Afeta `nu_origem`, `ic_origem_valido`, `nu_destino` e
`ic_destino_valido` em todos os cinco fluxos.

## Métodos auxiliares em classes

### `CDRParquetExtractor.extract_cdr`

Lê um ou vários Parquets com `mergeSchema=true`, seleciona/renomeia os campos
do contrato, cria como string nula os campos originais ausentes, acrescenta
metadados do caminho, opcionalmente elimina duplicatas e grava o Parquet
intermediário em `overwrite`.

### `CDRTextExtractor.extract_cdr`

Lê CSV conforme o contrato, valida o maior índice, seleciona por posição,
adiciona metadados do caminho, aplica filtro opcional e grava o Parquet
intermediário em `overwrite`.

### Métodos de `CDRBaseTransformer`

| Método | Finalidade | Colunas impactadas | Fluxos |
| :-- | :-- | :-- | :-- |
| `_fill_missing_columns` | garantir todas as chaves de `TARGET_SCHEMA` | qualquer coluna ausente | todos |
| `_format_date_time` | duração inteira e timestamps com limite mínimo | `duracao`, três datas | todos |
| `_format_numbers` | normalização e validade; restauração opcional do bruto | seis colunas de números, incluindo temporárias | todos |
| `_add_tn_validation_status` | classificar `verstat` | `autenticacao` | todos |
| `_fill_primary_key_columns` | substituir nulos nos 15 componentes da chave | referências, datas, duração, números, status, rotas, bilhetador e tipo | todos |
| `_apply_standard_pipeline` | ordenar as cinco etapas anteriores | contrato intermediário | todos |
| `_select_transformed_columns` | cast, alias e projeção do contrato | todas as 35 colunas | todos |
| `_write_parquet` | persistência `overwrite` particionada | dataset final | todos |

# 8. Ambiguidades

### Semântica dos campos Nokia escolhidos por precedência

**Elemento afetado:** `dh_chamada`, `dh_fim_chamada`, duração e números.

**Ponto do rastreamento:** as expressões `coalesce` e os ramos UCA/FORW em
`transform_cdr_nokia` são determinísticos, mas o código não contém evidência
suficiente para afirmar por que uma fonte tem prioridade semântica sobre outra.

**Evidência:** a implementação usa a ordem documentada nas seções 3.4 e 4.

### Layout posicional Algar Huawei

**Elemento afetado:** todas as colunas provenientes do CSV.

**Ponto do rastreamento:** o schema não possui cabeçalho nem nomes originais;
somente posições. Não é possível determinar com segurança nomes de campos
anteriores aos aliases sem documentação externa.

**Evidência:** `schema=None`, `has_header=False` e `column_indices` em
`TEXT_DEFAULT_SCHEMAS`.

### Tipo físico de `cause_for_termination`

**Elemento afetado:** `no_resultado_chamada` Nokia.

**Ponto do rastreamento:** o extrator preserva o tipo físico do Parquet e o
transformador compara `_status_chamada` diretamente com literais inteiros. A
análise estática não determina se a origem chega como inteiro, decimal ou texto
hexadecimal, nem o comportamento efetivo sob todas as configurações ANSI.

### Hierarquia dos caminhos de entrada

**Elemento afetado:** `no_prestadora`, `no_tipo_cdr` e `no_arquivo_origem`.

**Ponto do rastreamento:** a extração assume que os segmentos `-3`, `-2` e `-1`
de `input_file_name()` representam, respectivamente, prestadora, tipo e arquivo.
Não há validação estrutural do caminho.

### Linhagem temporal Ericsson e Vivo

**Elemento afetado:** `dh_chamada` e `dh_fim_chamada`.

**Ponto do rastreamento:** `_data`, `_hora` e `_hora_fim` são extraídas, mas não
há expressão que as combine nesses dois métodos. Portanto, a fonte temporal
pretendida não pode ser confirmada como fonte efetiva; o resultado implementado
é `MIN_SAFE_DATE`.

# Observações da revisão

## Inconsistências identificadas

1. **Ericsson e Vivo, datas:** os campos temporais extraídos não alimentam
   `data_hora` nem `data_hora_fim`. Impacto potencial: todas essas datas finais
   recebem `1901-01-01 00:00:00`. Sugestão: confirmar o contrato temporal e,
   em alteração futura separada, compor os campos antes do pipeline comum.
2. **Algar Huawei, hora final:** `data_hora_fim` usa `_data_fim` com `_hora`,
   embora `_hora_fim` seja extraída. Impacto potencial: horário final igual ao
   horário inicial. Sugestão: validar a intenção com amostras e especificação.
3. **Huawei TIM, número ATS:** o comentário afirma remoção de três caracteres,
   mas `substr(3, ...)` no Spark inicia na terceira posição e remove dois. O
   exemplo do próprio código também mostra remoção de dois. Sugestão: alinhar
   comentário e regra de negócio após validação funcional.
4. **Nokia, campos extraídos sem consumo:** `orig_called_number`,
   `connected_to_number`, `forwarded_to_number` e `perfil_prestadora` não chegam
   ao contrato final. Impacto potencial: informação disponível é descartada.
5. **Taxonomia de status:** os cinco fluxos produzem domínios diferentes em
   `no_resultado_chamada`; comparações entre fornecedores não são diretamente
   equivalentes.

## Pontos de atenção para manutenção

- Alterar `TARGET_SCHEMA` muda simultaneamente seleção, tipo, nome e ordem das
  colunas finais; a documentação deve ser revisada junto com essa constante.
- As 15 entradas de `PRIMARY_KEY_COLUMNS` recebem sentinelas e influenciam a
  deduplicação realizada fora deste transformador.
- `tipo_cdr` Huawei TIM vem de `recordType`; nos demais fluxos vem do caminho.
- O parsing Huawei TIM considera somente o primeiro elemento dos arrays JSON.
- A partição final depende de `no_tipo_chamada`; o valor sentinela pode se
  tornar um diretório de partição quando o tipo estiver ausente.
- O nome público `algar_huawei` contém grafia divergente de “Huawei” e deve ser
  preservado enquanto fizer parte da API.

## Possíveis melhorias

Estas sugestões não foram implementadas:

- adicionar testes de schema que comparem exatamente as 35 colunas finais;
- adicionar testes de integração por fluxo para datas, sentinelas e metadados;
- tornar explícitos os casts Nokia antes das comparações de status;
- validar a estrutura de diretórios antes de derivar metadados;
- documentar externamente o layout posicional completo do CSV Algar Huawei;
- consolidar uma taxonomia comum de status, mantendo também o código bruto.

# 10. Validação de cobertura

Foi usada como lista independente a sequência das 35 chaves de `TARGET_SCHEMA`.
Cada uma aparece uma vez na tabela de contrato e uma vez em cada tabela de
fluxo:

| Fluxo | Colunas esperadas | Colunas documentadas | Sem linhagem silenciosa |
| :-- | --: | --: | :-- |
| Ericsson | 35 | 35 | Sim |
| LTE Huawei TIM | 35 | 35 | Sim |
| LTE Ericsson Vivo | 35 | 35 | Sim |
| Nokia | 35 | 35 | Sim |
| Algar Huawei | 35 | 35 | Sim |

As colunas sem origem efetiva estão marcadas como “Não aplicável” e têm seu
comportamento de nulo ou sentinela descrito. As ambiguidades de origem ou
semântica foram mantidas explícitas, sem completar lacunas por semelhança de
nomes.
