# Documentação da Linhagem e Transformação dos Dados

## 1. Visão geral do fluxo

O projeto contém dois fluxos de CDR independentes quanto à origem: **Teleparser/Parquet** e **texto/CSV**. Em ambos, o módulo `extractors` lê o artefato de origem, seleciona e renomeia campos para um contrato intermediário e grava um Parquet extraído. O módulo `transformers` relê esse Parquet, executa pré-processamentos específicos do layout, chama o pipeline comum da classe `CDRBaseTransformer` e grava o Parquet final.

```text
CDR original
-> Extração
-> Parquet extraído
-> Transformações específicas
-> DataFrame intermediário padronizado
-> Padronização comum
-> Seleção/renomeação final
-> Parquet final
```

Não há uma função de orquestração de alto nível no código analisado que conecte extractor e transformer. A ligação é contratual: o `target_file` escrito pelo extractor deve ser fornecido como `source_file` ao transformer compatível.

Pontos de entrada identificados:

| Camada | Classe/função | Entrada | Saída |
| :-- | :-- | :-- | :-- |
| Extração Parquet | `CDRTeleparserExtractor.extract_cdr` | Parquet bruto | Parquet extraído |
| Extração Parquet | `extract_cdr_ericsson`, `extract_cdr_lte_huawei_tim`, `extract_cdr_lte_ericsson_vivo`, `extract_cdr_nokia` | Layout Teleparser | Delegam para `extract_cdr` |
| Extração texto | `CDRTextExtractor.extract_cdr` | CSV/texto | Parquet extraído |
| Extração texto | `extract_cdr_ericsson`, `extract_cdr_tim_huawei`, `extract_cdr_vivo_fcdr`, `extract_cdr_nokia` | Layout textual | Delegam para `extract_cdr` |
| Transformação Parquet | `CDRTeleparserTransformer.transform_cdr_*` | Parquet extraído | Parquet final de 37 colunas |
| Transformação texto | `CDRTextTransformer.transform_cdr_*` | Parquet extraído | Contrato final declarado de 16 colunas |
| Escrita final | `CDRBaseTransformer._write_parquet` | DataFrame transformado | `mode("overwrite").partitionBy("no_tipo_chamada").parquet(target_file)` |

## 2. Convenções utilizadas

| Nível | Descrição |
| :-- | :-- |
| Colunas Originais | Campos do Parquet bruto ou posições do CSV/texto de origem. |
| Colunas Extraídas | Campos do Parquet escrito pelo extractor, após `select`/`alias` ou seleção por índice/`toDF`. |
| Coluna Intermediária | Campo criado ou sobrescrito nos transformadores antes da seleção final. |
| Coluna Final | Campo projetado por `_select_transformed_columns` e persistido no Parquet final. |

`Não aplicável` significa que não existe etapa adicional entre os níveis. `Constante` ou `nulo tipado` identifica uma coluna criada pelo código sem dependência de uma coluna de origem. `__NULL__` é o sentinela configurado em `NULL_SENTINEL_VALUE`.

O nome da coluna por si só não foi usado para estabelecer relações: todo mapeamento abaixo é derivado de `column_mapping`, `column_indices`, `withColumn(s)`, `select`, `alias` e chamadas auxiliares efetivas.

## 2.1 Parquets intermediários e descarte na extração

Os dois métodos genéricos `extract_cdr` escrevem `target_file` em modo `overwrite` e retornam uma releitura com `spark.read.parquet(target_file)`. O caminho concreto não é fixado pelo repositório: é argumento de quem invoca os métodos. No Teleparser, todo campo fora de `schema.column_mapping` é descartado pelo `select`; no texto, todo índice fora de `column_indices` é descartado pela seleção posicional. Em ambos, `prestadora`, `tipo_cdr` e `arquivo_origem` são criadas do caminho retornado por `input_file_name()`.

| Formato | Função de extração | Parquet intermediário | Campos extraídos sem projeção final direta |
| :-- | :-- | :-- | :-- |
| Teleparser Ericsson | `extract_cdr_ericsson` | `target_file` | componentes `celula_*_{mcc,mnc,lac,ci_sac}`, `imsi_*_{mcc,mnc,msin}`, `imei_*_{tac,sn}` são consumidos para compor campos finais; `_data`, `_hora`, `_hora_fim` são consumidos nas datas. |
| Teleparser LTE Huawei TIM | `extract_cdr_lte_huawei_tim` | `target_file`, após `dropDuplicates()` | `_numero_*`, `_informacao_rede`, `_info_imei`, `_imei`, `_info_imsi`, `_status_chamada`, `_tipo_cdr` são campos de trabalho; `_tipo_cdr` substitui `tipo_cdr` e então é descartado. |
| Teleparser LTE Ericsson Vivo | `extract_cdr_lte_ericsson_vivo` | `target_file` | `_tipo_chamada`, `_status_chamada`, `_numero_origem_original`, `_data`, `_hora`, `_hora_fim` alimentam colunas posteriores. |
| Teleparser Nokia | `extract_cdr_nokia` | `target_file` | `numero_conectado`, `numero_destino_original`, `numero_destino_encaminhamento`, `perfil_prestadora` e os campos de duração/data auxiliares não têm projeção final direta; alguns são usados como alternativas no transformer. |
| Texto Ericsson | `extract_cdr_ericsson` | `target_file` | `_data`, `_hora`, `_tipo_chamada`, `rota_entrada`, `rota_saida` são campos de trabalho. |
| Texto TIM Huawei | `extract_cdr_tim_huawei` | `target_file`, após filtro | `_autenticacao` é extraído mas não usado pelo transformer textual; `_data`, `_hora`, `_tipo_chamada` são campos de trabalho. |
| Texto Vivo FCDR | `extract_cdr_vivo_fcdr` | `target_file` | `_numero_origem`, `_data`, `_hora`, `_tipo_chamada`, rotas são campos de trabalho, mas a chamada ao pré-processamento não resolvida interrompe a linhagem. |
| Texto Claro Nokia | `extract_cdr_nokia` | `target_file` | `_referencia` e `_rota` são consumidos pelo transformer; `numero_conectado` não é projetado ao contrato textual. |

Em extração Teleparser, quando um campo declarado no schema não existe no DataFrame original, ele não é descartado: é criado no Parquet intermediário como `NULL` com cast para `string`. Em extração textual, índice ausente gera `ValueError` antes da escrita; o formato TIM Huawei também remove as linhas cujo `_tipo_chamada` seja igual ao cabeçalho textual `TipodeCDR(role-of-Node)`.

## 3. Transformações por formato de CDR

### `CDRTeleparserTransformer.transform_cdr_ericsson`

**Descrição:** transforma o Parquet Ericsson produzido por `CDRTeleparserExtractor.extract_cdr_ericsson`. O extractor usa o schema `TELEPARSER_DEFAULT_SCHEMAS["ericsson"]`; o transformer converte duração, compõe identificadores de rede e dispositivo e aplica o pipeline comum.

**Fluxo resumido**

```text
Parquet Ericsson bruto
-> extract_cdr_ericsson / extract_cdr
-> Parquet extraído Ericsson
-> conversão de duração e composição de CGI/IMSI/IMEI
-> _apply_standard_pipeline
-> _select_transformed_columns da classe base
-> Parquet final Teleparser
```

| Colunas Originais | Colunas Extraídas | Coluna Intermediária | Coluna Final |
| :-- | :-- | :-- | :-- |
| `networkCallReference` | `referencia` | `referencia` com `coalesce(..., "__NULL__")` | `nu_referencia` |
| Não aplicável | Não aplicável | criada como `"__NULL__"` | `nu_referencia_sip` |
| Não aplicável | Não aplicável | `data_hora_referencia = MIN_SAFE_DATE` | `dh_referencia` |
| `dateForStartOfCharge`, `timeForStartOfCharge` | `_data`, `_hora` | `data_hora` | `dh_chamada` |
| `dateForStartOfCharge`, `timeForStopOfCharge` | `_data`, `_hora_fim` | `data_hora_fim` | `dh_fim_chamada` |
| `chargeableDuration` | `duracao` | segundos inteiros | `qt_duracao_segundos` |
| `callingPartyNumber.digits` | `numero_origem` | `numero_origem_formatado` | `nu_origem` |
| `callingPartyNumber.digits` | `numero_origem` | `numero_origem_valido` | `ic_origem_valido` |
| `callingPartyNumber.digits` | `numero_origem` | `numero_origem` (preenchido com sentinela quando nulo) | `nu_origem_original` |
| `calledPartyNumber.digits` | `numero_destino` | `numero_destino_formatado` | `nu_destino` |
| `calledPartyNumber.digits` | `numero_destino` | `numero_destino_valido` | `ic_destino_valido` |
| `calledPartyNumber.digits` | `numero_destino` | `numero_destino` (preenchido com sentinela quando nulo) | `nu_destino_original` |
| `callPosition` | `status_chamada` | `status_chamada` (sentinela se nulo) | `no_resultado_chamada` |
| Constante `NULL` | criada no transformer | `codigo_resposta_sip` | `co_resposta_sip` |
| Não aplicável | Não aplicável | `autenticacao = NULL` | `no_autenticacao` |
| caminho do arquivo (`-3`) | `prestadora` | `prestadora` | `no_prestadora` |
| `incomingRoute` | `rota_entrada` | `rota_entrada` (sentinela se nula) | `no_rota_entrada` |
| `outgoingRoute` | `rota_saida` | `rota_saida` (sentinela se nula) | `no_rota_saida` |
| `exchangeIdentity` | `bilhetador` | `bilhetador` (sentinela se nulo) | `no_bilhetador` |
| `firstCallingLocationInformation.{mcc,mnc,lac,ci_sac}` | `celula_origem_{mcc,mnc,lac,ci_sac}` | `celula_origem` | `nu_cgi_origem` |
| `callingSubscriberIMEI.{type_allocation_code,serial_number}` | `imei_origem_{tac,sn}` | `imei_origem` | `nu_imei_origem` |
| `callingSubscriberIMSI.{mcc,mnc,msin}` | `imsi_origem_{mcc,mnc,msin}` | `imsi_origem` | `nu_imsi_origem` |
| Constante `NULL` | criada no transformer | `ip_origem` | `nu_ip_origem` |
| Constante `NULL` | criada no transformer | `porta_ip_origem` | `nu_porta_ip_origem` |
| `firstCalledLocationInformation.{mcc,mnc,lac,ci_sac}` | `celula_destino_{mcc,mnc,lac,ci_sac}` | `celula_destino` | `nu_cgi_destino` |
| `calledSubscriberIMEI.{type_allocation_code,serial_number}` | `imei_destino_{tac,sn}` | `imei_destino` | `nu_imei_destino` |
| `calledSubscriberIMSI.{mcc,mnc,msin}` | `imsi_destino_{mcc,mnc,msin}` | `imsi_destino` | `nu_imsi_destino` |
| Constante `NULL` | criada no transformer | `ip_destino` | `nu_ip_destino` |
| Constante `NULL` | criada no transformer | `porta_ip_destino` | `nu_porta_ip_destino` |
| Constante `NULL` | criada no transformer | `agente_usuario` | `no_agente_usuario` |
| caminho do arquivo (`-2`) | `tipo_cdr` | `tipo_cdr` | `no_tipo_cdr` |
| caminho do arquivo (`-1`) | `arquivo_origem` | `arquivo_origem` | `no_arquivo_origem` |
| `CallModule` | `tipo_chamada` | cast para `string` | `no_tipo_chamada` |

**Transformações detalhadas por coluna:** `dh_chamada` concatena `_data + " " + _hora` quando não existia `data_hora`, aplica `try_to_timestamp("yy-MM-dd HH:mm:ss")` e `greatest(..., MIN_SAFE_DATE)`. `dh_fim_chamada` usa `_data + " " + _hora_fim` e o mesmo parsing. `qt_duracao_segundos` calcula `HH*3600 + mm*60 + ss`, ou `0` se a duração extraída for nula; o pipeline comum volta a aplicar `cast(int)` e `coalesce(..., 0)`. `nu_cgi_*` usa `_build_composite_column`: branco vira nulo, LAC/CI recebem `lpad(5, "0")`, qualquer componente nulo invalida todo o composto e os valores restantes são unidos por `-`. `nu_imsi_*` e `nu_imei_*` seguem a mesma regra de completude, sem separador. `nu_origem`, `nu_destino`, suas flags e os respectivos campos originais são produzidos por `_format_numbers`; a pandas UDF `spark_normalize_number` retorna struct com número limpo e flag, e não substitui os originais. `no_autenticacao` é nulo pois não existe `_autenticacao`. As demais colunas sem operação de conteúdo são aliases finais; as colunas obrigatórias nulas recebem `__NULL__` antes da projeção.

### `CDRTeleparserTransformer.transform_cdr_lte_huawei_tim`

**Descrição:** transforma o Parquet Teleparser do layout LTE Huawei TIM. O extractor deduplica o conjunto inteiro com `dropDuplicates()` e sobrescreve `tipo_cdr` pelo valor de `_tipo_cdr`, quando presente.

**Fluxo resumido**

```text
Parquet LTE Huawei TIM
-> extract_cdr_lte_huawei_tim (unique=True)
-> Parquet extraído
-> parsing JSON/SIP, célula, IMSI e IMEI; status SIP
-> _apply_standard_pipeline
-> seleção final base
-> Parquet final Teleparser
```

| Colunas Originais | Colunas Extraídas | Coluna Intermediária | Coluna Final |
| :-- | :-- | :-- | :-- |
| `network-Call-Reference` | `referencia` | `coalesce(referencia, "__NULL__")` | `nu_referencia` |
| `iMS-Charging-Identifier` | `referencia_sip` | `coalesce(referencia_sip, "__NULL__")` | `nu_referencia_sip` |
| Constante `MIN_SAFE_DATE` | criada no transformer | `data_hora_referencia` | `dh_referencia` |
| `serviceRequestTimeStamp` | `data_hora` | timestamp com formato `yyyy-MM-dd HH:mm:ssXXX` | `dh_chamada` |
| `serviceDeliveryEndTimeStamp` | `data_hora_fim` | timestamp com formato `yyyy-MM-dd HH:mm:ssXXX` | `dh_fim_chamada` |
| `duration` | `duracao` | `cast(int)`/`coalesce(0)` | `qt_duracao_segundos` |
| `calling-Party-Address-Generic`, `list-Of-Calling-Party-Address`, `recordType` | `_numero_origem_ats_auth`, `_numero_origem`, `_tipo_cdr`/`tipo_cdr` | `numero_origem_formatado` | `nu_origem` |
| Mesmas colunas de `nu_origem` | Mesmas | `numero_origem_valido` | `ic_origem_valido` |
| Mesmas colunas de `nu_origem` | Mesmas | `_numero_origem_original` restaurada como `numero_origem`, depois sentinela se nula | `nu_origem_original` |
| `called-Party-Address_tEL-URI`, `called-Party-Address_sIP-URI`, `recordType` | `_numero_destino_ats`, `_numero_destino_ibcf`, `_tipo_cdr`/`tipo_cdr` | `numero_destino_formatado` | `nu_destino` |
| Mesmas colunas de `nu_destino` | Mesmas | `numero_destino_valido` | `ic_destino_valido` |
| Mesmas colunas de `nu_destino` | Mesmas | `_numero_destino_original` restaurada como `numero_destino`, depois sentinela se nula | `nu_destino_original` |
| `serviceReasonReturnCode`, `recordType` | `_status_chamada`, `_tipo_cdr`/`tipo_cdr` | `status_chamada` | `no_resultado_chamada` |
| `serviceReasonReturnCode`, `recordType` | `_status_chamada`, `_tipo_cdr`/`tipo_cdr` | `codigo_resposta_sip` | `co_resposta_sip` |
| `calling-Party-Address-Generic`, `list-Of-Calling-Party-Address`, `recordType` | `_numero_origem_ats_auth`, `_numero_origem`, `_tipo_cdr`/`tipo_cdr` | `_autenticacao` -> `autenticacao` | `no_autenticacao` |
| caminho (`-3`) | `prestadora` | `prestadora` | `no_prestadora` |
| `specifiedTreatmentField_incoming-Route` | `rota_entrada` | sentinela se nula | `no_rota_entrada` |
| `specifiedTreatmentField_outgoing-Route` | `rota_saida` | sentinela se nula | `no_rota_saida` |
| `nodeAddress_domainName` | `bilhetador` | sentinela se nulo | `no_bilhetador` |
| `accessNetworkInformation`, `role-of-Node` | `_informacao_rede`, `tipo_chamada` | `_cell_id` -> `celula_origem` se originante | `nu_cgi_origem` |
| `private-User-Equipment-Info_*`, `role-of-Node` | `_info_imei`, `_imei`, `tipo_chamada` | `imei_origem` se originante | `nu_imei_origem` |
| `list-of-subscription-ID`, `role-of-Node` | `_info_imsi`, `tipo_chamada` | `_imsi` -> `imsi_origem` se originante | `nu_imsi_origem` |
| Constante `NULL` | criada no transformer | `ip_origem` | `nu_ip_origem` |
| Constante `NULL` | criada no transformer | `porta_ip_origem` | `nu_porta_ip_origem` |
| `accessNetworkInformation`, `role-of-Node` | `_informacao_rede`, `tipo_chamada` | `_cell_id` -> `celula_destino` se terminante | `nu_cgi_destino` |
| `private-User-Equipment-Info_*`, `role-of-Node` | `_info_imei`, `_imei`, `tipo_chamada` | `imei_destino` se terminante | `nu_imei_destino` |
| `list-of-subscription-ID`, `role-of-Node` | `_info_imsi`, `tipo_chamada` | `_imsi` -> `imsi_destino` se terminante | `nu_imsi_destino` |
| Constante `NULL` | criada no transformer | `ip_destino` | `nu_ip_destino` |
| Constante `NULL` | criada no transformer | `porta_ip_destino` | `nu_porta_ip_destino` |
| `user-Agent-Value` | `agente_usuario` | `agente_usuario` | `no_agente_usuario` |
| `recordType` | `_tipo_cdr`, depois `tipo_cdr` | `tipo_cdr` | `no_tipo_cdr` |
| caminho (`-1`) | `arquivo_origem` | `arquivo_origem` | `no_arquivo_origem` |
| `role-of-Node` | `tipo_chamada` | cast para `string` | `no_tipo_chamada` |

**Transformações detalhadas por coluna:** para `aTSRecord`, `nu_origem` vem de `_numero_origem_ats_auth` por `regexp_extract(:\\+?([0-9]+))`, se presente; caso contrário, do JSON `$[0].tEL-URI`, cujos pares de caracteres são invertidos por `regexp_replace("(.)(.)", "$2$1")`, e recebe `substr(3, 9999)`. Para `iBCFRecord`, vem de `$[0].sIP-URI` e regex SIP. `nu_destino` usa `substr(3,9999)` no ATS ou regex SIP no IBCF. Os originais são preservados nas colunas `_numero_*_original` e restaurados após a UDF. `_autenticacao` usa a regex `verstat=[a-zA-Z\\-]+`; `autenticacao` só classifica os prefixos Passed, Failed e No-TN-Validation. `_cell_id` é extraído de `utran-cell-id-3gpp=...;` e `_format_cell_id` o decodifica por comprimento 13/16/20 (3G/4G/5G); é direcionado para origem ou destino conforme `tipo_chamada`. IMEI é mantido apenas se `_info_imei == "iMEI"`; IMSI é lido por `from_json`, somente quando `info_type == "eND-USER-IMSI"`. `co_resposta_sip` é `_status_chamada` inteiro somente se `>=200`; `no_resultado_chamada` agrupa os valores e intervalos definidos na cadeia `when`.

### `CDRTeleparserTransformer.transform_cdr_lte_ericsson_vivo`

**Descrição:** transforma o Parquet LTE Ericsson Vivo. Se uma coluna mapeada estiver ausente na origem, o extractor a cria como string nula; portanto, a existência da coluna intermediária não prova que ela existia no arquivo bruto.

**Fluxo resumido**

```text
Parquet LTE Ericsson Vivo
-> extract_cdr_lte_ericsson_vivo
-> Parquet extraído
-> separação por ';', códigos de chamada/status, IMEI e CGI
-> _apply_standard_pipeline
-> seleção final base
-> Parquet final Teleparser
```

| Colunas Originais | Colunas Extraídas | Coluna Intermediária | Coluna Final |
| :-- | :-- | :-- | :-- |
| `networkCallReference` | `referencia` | sentinela se nula | `nu_referencia` |
| `imsChargingIdentifier` | `referencia_sip` | sentinela se nula | `nu_referencia_sip` |
| Constante `MIN_SAFE_DATE` | criada no transformer | `data_hora_referencia` | `dh_referencia` |
| `dateForStartOfCharge`, `timeForStartOfCharge` | `_data`, `_hora` | `data_hora` timestamp | `dh_chamada` |
| `dateForStartOfCharge`, `timeForStopOfCharge` | `_data`, `_hora_fim` | `data_hora_fim` timestamp | `dh_fim_chamada` |
| `chargeableDurat` | `duracao` | `cast(int)`/`coalesce(0)` | `qt_duracao_segundos` |
| `callingPartyNumber` | `_numero_origem_original` | split item 0 -> normalização -> `numero_origem_formatado` | `nu_origem` |
| `callingPartyNumber` | `_numero_origem_original` | UDF -> `numero_origem_valido` | `ic_origem_valido` |
| `callingPartyNumber` | `_numero_origem_original` | `_numero_origem_original` restaurada após UDF | `nu_origem_original` |
| `calledPartyNumber` | `numero_destino` | `numero_destino_formatado` | `nu_destino` |
| `calledPartyNumber` | `numero_destino` | `numero_destino_valido` | `ic_destino_valido` |
| `calledPartyNumber` | `numero_destino` | sentinela se nulo | `nu_destino_original` |
| `callPosition` | `_status_chamada` | `status_chamada` mapeado | `no_resultado_chamada` |
| Constante `NULL` | criada no transformer | `codigo_resposta_sip` | `co_resposta_sip` |
| sufixo após `;` de `callingPartyNumber` | `_numero_origem_original` | `_autenticacao` -> `autenticacao` | `no_autenticacao` |
| caminho (`-3`) | `prestadora` | `prestadora` | `no_prestadora` |
| `incomingRoute` | `rota_entrada` | sentinela se nula | `no_rota_entrada` |
| `outgoingRoute` | `rota_saida` | sentinela se nula | `no_rota_saida` |
| `exchangeIdentity` | `bilhetador` | sentinela se nulo | `no_bilhetador` |
| `firstCallingLocInf` | `celula_origem` | `_format_cell_id(celula_origem)` | `nu_cgi_origem` |
| `callingSubscriberIMEI` | `imei_origem` | `translate("-", "")` | `nu_imei_origem` |
| `callingSubscriberIMSI` | `imsi_origem` | `imsi_origem` | `nu_imsi_origem` |
| Constante `NULL` | criada no transformer | `ip_origem` | `nu_ip_origem` |
| Constante `NULL` | criada no transformer | `porta_ip_origem` | `nu_porta_ip_origem` |
| `firstCalledLocInfo` | `celula_destino` | `_format_cell_id(celula_destino)` | `nu_cgi_destino` |
| `calledSubscriberIMEI` | `imei_destino` | `translate("-", "")` | `nu_imei_destino` |
| `calledSubscriberIMSI` | `imsi_destino` | `imsi_destino` | `nu_imsi_destino` |
| Constante `NULL` | criada no transformer | `ip_destino` | `nu_ip_destino` |
| Constante `NULL` | criada no transformer | `porta_ip_destino` | `nu_porta_ip_destino` |
| Constante `NULL` | criada no transformer | `agente_usuario` | `no_agente_usuario` |
| caminho (`-2`) | `tipo_cdr` | `tipo_cdr` | `no_tipo_cdr` |
| caminho (`-1`) | `arquivo_origem` | `arquivo_origem` | `no_arquivo_origem` |
| `callModule` | `_tipo_chamada` | códigos `1/3/4` mapeados, depois cast string | `no_tipo_chamada` |

**Transformações detalhadas por coluna:** o split de `_numero_origem_original` em `;` cria `numero_origem` e `_autenticacao`. Como o pipeline preserva o original quando `_numero_origem_original` existe, `nu_origem_original` recebe o valor integral antes do split e `nu_origem` recebe a normalização do primeiro item. `no_tipo_chamada` converte `1`, `3`, `4` em `msOriginating`, `callForwarding`, `msTerminating`. `no_resultado_chamada` converte `_status_chamada` `1`, `2`, `3` nos três textos definidos; valores diferentes passam sem transformação. CGI segue o algoritmo 3G/4G/5G de `_format_cell_id`; IMEIs removem hífens. Datas são parseadas como `yyyyMMdd HHmmss` e protegidas por `MIN_SAFE_DATE`.

### `CDRTeleparserTransformer.transform_cdr_nokia`

**Descrição:** transforma o Parquet Nokia e tolera campos ausentes na extração: todo campo ausente no Parquet bruto é projetado como string nula pelo extractor. Consolida duração, datas e identificação de célula antes do pipeline comum.

**Fluxo resumido**

```text
Parquet Nokia
-> extract_cdr_nokia / extract_cdr
-> Parquet extraído (ausentes como NULL string)
-> consolidação de duração/data, rota, célula e status
-> _apply_standard_pipeline
-> seleção final base
-> Parquet final Teleparser
```

| Colunas Originais | Colunas Extraídas | Coluna Intermediária | Coluna Final |
| :-- | :-- | :-- | :-- |
| `call_reference` | `referencia` | sentinela se nula | `nu_referencia` |
| Não aplicável | Não aplicável | criada como `"__NULL__"` | `nu_referencia_sip` |
| `call_reference_time` | `data_hora_referencia` | timestamp `dd/MM/yyyy HH:mm:ss`, limitado por `MIN_SAFE_DATE` | `dh_referencia` |
| `in_channel_allocated_time`, `call_reference_time` | `data_hora_alocacao_canal`, `data_hora_referencia` | `coalesce(alocacao, referencia, MIN_SAFE_DATE)` -> `data_hora` | `dh_chamada` |
| `charging_end_time`, `release_time`, `record_type` | `data_hora_fim`, `data_hora_desconexao`, `tipo_chamada` | UCA: `coalesce(desconexao, fim)`; demais: fim | `dh_fim_chamada` |
| `orig_mcz_duration`, `term_mcz_duration`, `forw_mcz_duration`, `roam_mcz_duration`, `iaz_duration`, `oaz_duration`, `chargeable_duration`, `char_band_duration` | todos os `_duracao_*` | primeiro não nulo, ignorando `"FFFFFF"`; `cast(int)`/0 | `qt_duracao_segundos` |
| `calling_number`, `orig_calling_number` | `numero_origem`, `numero_origem_original` | `coalesce(numero_origem, numero_origem_original)` -> UDF | `nu_origem` |
| Mesmas colunas de `nu_origem` | Mesmas | `numero_origem_valido` | `ic_origem_valido` |
| `calling_number`, `orig_calling_number` | `numero_origem`, `numero_origem_original` | coalesce, sentinela se nulo | `nu_origem_original` |
| `called_number`, `forwarding_number`, `record_type` | `numero_destino`, `numero_origem_encaminhamento`, `tipo_chamada` | FORW usa encaminhamento; demais usam destino; UDF | `nu_destino` |
| Mesmas colunas de `nu_destino` | Mesmas | `numero_destino_valido` | `ic_destino_valido` |
| Mesmas colunas de `nu_destino` | Mesmas | destino selecionado, sentinela se nulo | `nu_destino_original` |
| `cause_for_termination` | `_status_chamada` | faixas hexadecimais -> `status_chamada` | `no_resultado_chamada` |
| Constante `NULL` | criada no transformer | `codigo_resposta_sip` | `co_resposta_sip` |
| Não aplicável | Não aplicável | `autenticacao = NULL` | `no_autenticacao` |
| caminho (`-3`) | `prestadora` | `prestadora` | `no_prestadora` |
| `in_circuit_group`, `record_type` | `rota_entrada`, `tipo_chamada` | somente POC: `_rota` não existe neste schema; ver ambiguidade | `no_rota_entrada` |
| `out_circuit_group`, `record_type` | `rota_saida`, `tipo_chamada` | somente PTC/FOR ou UCA: `_rota` não existe neste schema; ver ambiguidade | `no_rota_saida` |
| `exchange_id` | `bilhetador` | sentinela se nulo | `no_bilhetador` |
| `calling_subs_first_lac`, `calling_subs_first_ci`, caminho/prestadora | `celula_origem_lac`, `celula_origem_ci`, `prestadora` | MCC `724` + MNC por prestadora + LAC/CI | `nu_cgi_origem` |
| `calling_imei` | `imei_origem` | `imei_origem` | `nu_imei_origem` |
| `calling_imsi` | `imsi_origem` | `imsi_origem` | `nu_imsi_origem` |
| Constante `NULL` | criada no transformer | `ip_origem` | `nu_ip_origem` |
| Constante `NULL` | criada no transformer | `porta_ip_origem` | `nu_porta_ip_origem` |
| `called_subs_first_lac`, `called_subs_first_ci`, caminho/prestadora | `celula_destino_lac`, `celula_destino_ci`, `prestadora` | MCC `724` + MNC por prestadora + LAC/CI | `nu_cgi_destino` |
| `called_imei` | `imei_destino` | `imei_destino` | `nu_imei_destino` |
| `called_imsi` | `imsi_destino` | `imsi_destino` | `nu_imsi_destino` |
| Constante `NULL` | criada no transformer | `ip_destino` | `nu_ip_destino` |
| Constante `NULL` | criada no transformer | `porta_ip_destino` | `nu_porta_ip_destino` |
| Constante `NULL` | criada no transformer | `agente_usuario` | `no_agente_usuario` |
| caminho (`-2`) | `tipo_cdr` | `tipo_cdr` | `no_tipo_cdr` |
| caminho (`-1`) | `arquivo_origem` | `arquivo_origem` | `no_arquivo_origem` |
| `record_type` | `tipo_chamada` | cast para string | `no_tipo_chamada` |

**Transformações detalhadas por coluna:** `dh_chamada` prioriza alocação de canal, depois referência e então `MIN_SAFE_DATE`; ambas as datas são parseadas no pipeline comum. `qt_duracao_segundos` faz `when(valor == "FFFFFF", NULL)` em cada coluna `_duracao*`, faz `coalesce` na ordem das colunas existentes do DataFrame e converte para inteiro. Para `FORW`, o código usa `numero_origem_encaminhamento` como destino, embora o nome extraído venha de `forwarding_number`. CGI usa MCC literal `724`, MNC `05` para `prestadora == "claro"` e `34` para `"algar"`, além de LAC/CI com `lpad(5,"0")`. O status compara `_status_chamada` às faixas `0x0000..0x03FF`, `0x0400..0x07FF`, `0x0800..0x0BFF`, `0x0C00..0x0FFF` e `>=0x1000`.

### `CDRTextTransformer.transform_cdr_ericsson`

**Descrição:** pretende transformar o CSV Ericsson extraído por índice pelo `CDRTextExtractor`. O contrato final declarado é o override de 16 colunas de `CDRTextTransformer`.

**Fluxo resumido**

```text
CSV Ericsson
-> extract_cdr_ericsson / extract_cdr
-> Parquet extraído
-> _apply_standard_pipeline
-> mapeamento de _tipo_chamada
-> seleção final textual
-> Parquet final textual declarado
```

| Colunas Originais (índice) | Colunas Extraídas | Coluna Intermediária | Coluna Final |
| :-- | :-- | :-- | :-- |
| `0` | `referencia` | sentinela se nula | `nu_referencia` |
| `1` | `numero_origem` | `numero_origem` | `nu_origem_original` |
| `9` | `numero_destino` | `numero_destino` | `nu_destino_original` |
| `1` | `numero_origem` | UDF -> `numero_origem_formatado` | `nu_origem` |
| `1` | `numero_origem` | UDF -> `numero_origem_valido` | `ic_origem_valido` |
| `9` | `numero_destino` | UDF -> `numero_destino_formatado` | `nu_destino` |
| `9` | `numero_destino` | UDF -> `numero_destino_valido` | `ic_destino_valido` |
| `2`, `3` | `_data`, `_hora` | `data_hora` timestamp `yyyy-MM-dd HH:mm:ss` | `dh_chamada` |
| `11` | `duracao` | `cast(int)`/`coalesce(0)` | `qt_duracao_segundos` |
| `4` | `_tipo_chamada` | TER/TRA/ORI/ROA/FOR mapeados | `no_tipo_chamada` |
| Não aplicável | Não aplicável | `autenticacao = NULL` | `no_autenticacao` |
| `8` | `rota_entrada` | sentinela se nula | `no_rota_entrada` |
| `8` | `rota_saida` | sentinela se nula | `no_rota_saida` |
| caminho (`-3`) | `prestadora` | `prestadora` | `no_prestadora` |
| caminho (`-2`) | `tipo_cdr` | `tipo_cdr` | `no_tipo_cdr` |
| caminho (`-1`) | `arquivo_origem` | `arquivo_origem` | `no_arquivo_origem` |

**Transformações detalhadas por coluna:** datas e números usariam as mesmas auxiliares do fluxo Teleparser. `no_tipo_chamada` converte TER para `msTerminating`, TRA para `transit`, ORI para `msOriginating`, ROA para `roamingCallForwarding` e FOR para `callForwarding`. ⚠️ Não foi possível determinar uma saída final executável com segurança: antes de chegar à seleção final, `_format_date_time` referencia `data_hora_referencia` e `_hora_fim`, ausentes no schema extraído deste formato. O Spark deve falhar na resolução dessas colunas.

### `CDRTextTransformer.transform_cdr_tim_huawei`

**Descrição:** pretende transformar o CSV TIM Huawei filtrado para registros cujo `_tipo_chamada` não seja `TipodeCDR(role-of-Node)`.

| Colunas Originais (índice) | Colunas Extraídas | Coluna Intermediária | Coluna Final |
| :-- | :-- | :-- | :-- |
| `12` | `referencia` | sentinela se nula | `nu_referencia` |
| `0` | `numero_origem` | `numero_origem` | `nu_origem_original` |
| `4` | `numero_destino` | `numero_destino` | `nu_destino_original` |
| `0` | `numero_origem` | UDF -> formatado | `nu_origem` |
| `0` | `numero_origem` | UDF -> válido | `ic_origem_valido` |
| `4` | `numero_destino` | UDF -> formatado | `nu_destino` |
| `4` | `numero_destino` | UDF -> válido | `ic_destino_valido` |
| `1`, `2` | `_data`, `_hora` | `data_hora` com formato padrão `yyyy-MM-dd HH-mm-ss` | `dh_chamada` |
| `7` | `duracao` | `cast(int)`/`coalesce(0)` | `qt_duracao_segundos` |
| `3` | `_tipo_chamada` | TERv/ORIv/FORv convertidos | `no_tipo_chamada` |
| Não aplicável | Não aplicável | `autenticacao = NULL` | `no_autenticacao` |
| Constante `NULL` | criada no transformer | `rota_entrada` | `no_rota_entrada` |
| Constante `NULL` | criada no transformer | `rota_saida` | `no_rota_saida` |
| caminho (`-3`) | `prestadora` | direta | `no_prestadora` |
| caminho (`-2`) | `tipo_cdr` | direta | `no_tipo_cdr` |
| caminho (`-1`) | `arquivo_origem` | direta | `no_arquivo_origem` |

**Transformações detalhadas por coluna:** `no_tipo_chamada` converte TERv, ORIv e FORv, enquanto rotas são sobrescritas por nulos string. ⚠️ Não foi possível determinar uma saída final executável com segurança: o Parquet extraído não cria `_hora_fim` nem `data_hora_referencia`, que são usados incondicionalmente por `_format_date_time` antes de a seleção de saída ocorrer.

### `CDRTextTransformer.transform_cdr_vivo_fcdr`

**Descrição:** pretende transformar o CSV Vivo FCDR depois do pré-processamento herdado `_preprocess_cdr_vivo_fcdr` mencionado pela chamada. ⚠️ Essa função não existe em `CDRBaseTransformer` no código analisado; a chamada não pode ser resolvida estaticamente para uma implementação.

| Colunas Originais (índice) | Colunas Extraídas | Coluna Intermediária | Coluna Final |
| :-- | :-- | :-- | :-- |
| `45` | `referencia` | sentinela se nula | `nu_referencia` |
| `2` | `_numero_origem` | Não determinado: depende de método inexistente | `nu_origem_original` |
| `5` | `numero_destino` | Não determinado: depende de método inexistente | `nu_destino_original` |
| `2` | `_numero_origem` | Não determinado | `nu_origem` |
| `2` | `_numero_origem` | Não determinado | `ic_origem_valido` |
| `5` | `numero_destino` | Não determinado | `nu_destino` |
| `5` | `numero_destino` | Não determinado | `ic_destino_valido` |
| `12`, `13` | `_data`, `_hora` | `data_hora` pretendida no formato `yyyyMMdd HHmmss` | `dh_chamada` |
| `31` | `duracao` | `cast(int)`/`coalesce(0)` pretendido | `qt_duracao_segundos` |
| `0` | `_tipo_chamada` | Não determinado | `no_tipo_chamada` |
| Não aplicável | Não aplicável | Não determinado | `no_autenticacao` |
| `65` | `rota_entrada` | sentinela se nula pretendida | `no_rota_entrada` |
| `66` | `rota_saida` | sentinela se nula pretendida | `no_rota_saida` |
| caminho (`-3`) | `prestadora` | direta | `no_prestadora` |
| caminho (`-2`) | `tipo_cdr` | direta | `no_tipo_cdr` |
| caminho (`-1`) | `arquivo_origem` | direta | `no_arquivo_origem` |

**Transformações detalhadas por coluna:** ⚠️ Não foi possível determinar com segurança esta linhagem apenas pela análise do código. O rastreamento é interrompido em `self._preprocess_cdr_vivo_fcdr(df)`: não há definição desse método na classe base nem na classe textual. Em execução, a chamada deve produzir `AttributeError` antes de qualquer coluna final. Também faltam `_hora_fim` e `data_hora_referencia` para o pipeline padrão.

### `CDRTextTransformer.transform_cdr_nokia`

**Descrição:** pretende transformar o CSV Claro Nokia. O extractor não cria as mesmas colunas que o método subsequente usa para referência e rota.

| Colunas Originais (índice) | Colunas Extraídas | Coluna Intermediária | Coluna Final |
| :-- | :-- | :-- | :-- |
| `2` | `_referencia` | BCD invertido reordenado -> `referencia` | `nu_referencia` |
| `7` | `numero_origem` | direta | `nu_origem_original` |
| `8` | `numero_destino` | direta | `nu_destino_original` |
| `7` | `numero_origem` | UDF -> formatado | `nu_origem` |
| `7` | `numero_origem` | UDF -> válido | `ic_origem_valido` |
| `8` | `numero_destino` | UDF -> formatado | `nu_destino` |
| `8` | `numero_destino` | UDF -> válido | `ic_destino_valido` |
| `3` | `data_hora` | timestamp `yyyy-MM-dd HH:mm:ss` pretendido | `dh_chamada` |
| `15` | `duracao` | `cast(int)`/`coalesce(0)` pretendido | `qt_duracao_segundos` |
| `0` | `_tipo_chamada` | FOR -> FORW; demais preservados | `no_tipo_chamada` |
| Não aplicável | Não aplicável | `autenticacao = NULL` | `no_autenticacao` |
| `21` | `_rota` | somente POC: `_rota` | `no_rota_entrada` |
| `21` | `_rota` | PTC/FOR: direto; UCA: último item de split `&&` | `no_rota_saida` |
| caminho (`-3`) | `prestadora` | direta | `no_prestadora` |
| caminho (`-2`) | `tipo_cdr` | direta | `no_tipo_cdr` |
| caminho (`-1`) | `arquivo_origem` | direta | `no_arquivo_origem` |

**Transformações detalhadas por coluna:** `nu_referencia` concatena caracteres de `_referencia` nas posições `4,3,2,1,8,7,6,5,10,9`. As rotas são derivadas de `_rota` pelas condições documentadas na tabela. ⚠️ Não foi possível determinar uma saída final executável com segurança: `_format_date_time` exige `data_hora_referencia` e, quando `data_hora_fim` não existe, exige `_data` e `_hora_fim`; nenhuma dessas colunas é extraída. Além disso, `_apply_standard_pipeline` é chamado antes de o método criar `referencia` a partir de `_referencia`.

## 4. Múltiplas origens para a mesma coluna final

### `dh_chamada`

```text
Teleparser Ericsson:
dateForStartOfCharge + timeForStartOfCharge
-> _data + _hora
-> concat_ws(" ") -> try_to_timestamp
-> data_hora
-> dh_chamada

Teleparser LTE Huawei TIM:
serviceRequestTimeStamp
-> data_hora -> try_to_timestamp
-> dh_chamada

Teleparser LTE Ericsson Vivo:
dateForStartOfCharge + timeForStartOfCharge
-> _data + _hora -> data_hora
-> dh_chamada

Teleparser Nokia:
in_channel_allocated_time ou call_reference_time
-> data_hora_alocacao_canal ou data_hora_referencia
-> coalesce(..., MIN_SAFE_DATE) -> data_hora
-> dh_chamada
```

### `nu_origem`

```text
Ericsson: callingPartyNumber.digits -> numero_origem -> pandas UDF -> nu_origem
LTE Huawei TIM: JSON/SIP de list-Of-Calling-Party-Address ou calling-Party-Address-Generic
                 -> numero_origem -> pandas UDF -> nu_origem
LTE Ericsson Vivo: callingPartyNumber -> _numero_origem_original -> split(';')[0]
                     -> numero_origem -> pandas UDF -> nu_origem
Nokia: calling_number ou orig_calling_number -> coalesce -> numero_origem
       -> pandas UDF -> nu_origem
```

### `nu_cgi_origem`

```text
Ericsson: MCC + MNC + LAC + CI/SAC extraídos -> composto com padding -> nu_cgi_origem
LTE Huawei TIM: accessNetworkInformation -> regex utran-cell-id-3gpp -> decodificação 3G/4G/5G
                 -> (somente papel originante) -> nu_cgi_origem
LTE Ericsson Vivo: firstCallingLocInf -> decodificação 3G/4G/5G -> nu_cgi_origem
Nokia: literal 724 + MNC inferido da prestadora + LAC + CI -> composto com padding -> nu_cgi_origem
```

## 5. Etapa final de geração do Parquet

**Função responsável:** `CDRBaseTransformer._write_parquet`.

Recebe o DataFrame pós-transformação, chama despacho polimórfico a `_select_transformed_columns(df)` e persiste o resultado com `mode("overwrite")` e `partitionBy("no_tipo_chamada")`. O transformador Teleparser herda a seleção base de 37 colunas; `CDRTextTransformer` substitui esse método e declara 16 colunas. Em ambos, o particionamento é aplicado pela implementação base de `_write_parquet`.

| Coluna antes da etapa final | Operação | Coluna final |
| :-- | :-- | :-- |
| `referencia` | Renomeação | `nu_referencia` |
| `referencia_sip` | Renomeação | `nu_referencia_sip` |
| `data_hora_referencia` | Renomeação | `dh_referencia` |
| `data_hora` | Renomeação | `dh_chamada` |
| `data_hora_fim` | Renomeação | `dh_fim_chamada` |
| `duracao` | Renomeação | `qt_duracao_segundos` |
| `numero_origem_formatado` | Renomeação | `nu_origem` |
| `numero_origem_valido` | Renomeação | `ic_origem_valido` |
| `numero_origem` | Renomeação | `nu_origem_original` |
| `numero_destino_formatado` | Renomeação | `nu_destino` |
| `numero_destino_valido` | Renomeação | `ic_destino_valido` |
| `numero_destino` | Renomeação | `nu_destino_original` |
| `status_chamada` | Renomeação | `no_resultado_chamada` |
| `codigo_resposta_sip` | Renomeação | `co_resposta_sip` |
| `autenticacao` | Renomeação | `no_autenticacao` |
| `prestadora`, `rota_entrada`, `rota_saida`, `bilhetador` | Renomeação | `no_prestadora`, `no_rota_entrada`, `no_rota_saida`, `no_bilhetador` |
| `celula_*`, `imei_*`, `imsi_*`, `ip_*`, `porta_ip_*`, `agente_usuario` | Renomeação | campos técnicos `nu_*`/`no_agente_usuario` |
| `tipo_cdr`, `arquivo_origem` | Renomeação | `no_tipo_cdr`, `no_arquivo_origem` |
| `tipo_chamada` | `cast(string)` e renomeação | `no_tipo_chamada` |

O `cast` de `tipo_chamada` é transformação de tipo. Todos os demais itens da tabela são apenas projeções com `alias`; transformações de conteúdo ocorrem antes desta etapa.

## 6. Funções auxiliares utilizadas por cada fluxo

### `_apply_standard_pipeline`

**Localização:** `teleutils.core.transformers.base_transformer.CDRBaseTransformer`.

**Finalidade:** executa `_format_date_time`, `_format_numbers`, `_add_tn_validation_status`, `_add_missing_reference_columns` e `_fill_missing_columns` nessa ordem.

**Entradas:** DataFrame com as colunas necessárias a cada subetapa e máscara temporal opcional.

**Saídas e colunas impactadas:** `duracao`, `data_hora`, `data_hora_fim`, `data_hora_referencia`, `numero_*_formatado`, `numero_*_valido`, `autenticacao`, `referencia`, `referencia_sip`, e os campos obrigatórios preenchidos com sentinela.

**Funções que a utilizam:** todos os oito métodos `transform_cdr_*` das classes Teleparser e texto.

### `_format_cell_id`

**Localização:** `teleutils.core.transformers.teleparser_transformers` (nível de módulo).

**Finalidade:** converte uma célula hexadecimal em representação textual MCC-MNC-área-célula para comprimentos 13, 16 ou 20; preserva o valor original em outros comprimentos.

**Entradas/Saídas:** DataFrame, campo de entrada e campo de saída; impacta `_cell_id` no Huawei e `celula_origem`/`celula_destino` no LTE Ericsson Vivo.

**Funções que a utilizam:** `transform_cdr_lte_huawei_tim` e `transform_cdr_lte_ericsson_vivo`.

### `_build_composite_column`

**Localização:** `teleutils.core.transformers.teleparser_transformers` (nível de módulo).

**Finalidade:** normaliza brancos como nulos, aplica padding opcional e concatena somente se todos os componentes existirem.

**Entradas/Saídas:** componentes Spark e separador; produz CGI, IMSI e IMEI compostos.

**Funções que a utilizam:** `transform_cdr_ericsson` e `transform_cdr_nokia` do Teleparser.

## 7. Funções auxiliares comuns

## Funções em nível de módulo

### `spark_normalize_number`

**Localização:** `teleutils.preprocessing.number_format`.

**Finalidade:** pandas UDF vetorizada que chama `normalize_number` e retorna o struct `numero_formatado`, `numero_valido`.

**Entradas/Saídas:** uma série de número bruto; struct string/booleano. **Colunas impactadas:** gera `_numero_origem_formatado` e `_numero_destino_formatado`, dos quais saem as quatro colunas normalizadas. **Funções que a utilizam:** `_format_numbers` para todos os transformers.

### `normalize_number` e `_clean_numbers`

**Localização:** `teleutils.preprocessing.number_format`.

**Finalidade:** retém a parte anterior a `;`, remove `f`, caracteres ASCII não numéricos e prefixos `90`, `9090`, `00` ou `0`; valida contra os regex de numeração e retorna número/flag. **Uso no fluxo:** chamado indiretamente pela pandas UDF.

### `_null_if_blank` e `_concat_or_null`

**Localização:** `teleutils.core.transformers.teleparser_transformers`.

**Finalidade:** a primeira converte string vazia em nulo; a segunda impede composto parcial quando algum componente é nulo. **Colunas impactadas:** composições de CGI, IMSI e IMEI. **Funções que as utilizam:** `_build_composite_column`.

## Métodos auxiliares em classes

### `CDRBaseTransformer._format_date_time`

**Entradas:** `_data`/`_hora` e `_hora_fim` quando `data_hora`/`data_hora_fim` estiverem ausentes; `data_hora_referencia`, `duracao`. **Saídas:** timestamps protegidos por `MIN_SAFE_DATE` e duração inteira. **Usado por:** `_apply_standard_pipeline`.

### `CDRBaseTransformer._format_numbers`

**Entradas:** `numero_origem`, `numero_destino` e, opcionalmente, `_numero_*_original`. **Saídas:** quatro campos de normalização e restauração dos originais quando disponíveis. **Usado por:** `_apply_standard_pipeline`.

### `CDRBaseTransformer._add_tn_validation_status`

**Entradas:** `_autenticacao` opcional. **Saída:** `autenticacao`. **Usado por:** `_apply_standard_pipeline`.

### `CDRBaseTransformer._add_missing_reference_columns` e `_fill_missing_columns`

**Entradas:** referências e lista de campos obrigatórios. **Saídas:** referências e campos obrigatórios preenchidos por `__NULL__` quando ausentes/nulos. **Usado por:** `_apply_standard_pipeline`.

### `CDRBaseTransformer._select_transformed_columns` e `CDRTextTransformer._select_transformed_columns`

**Finalidade:** contratos finais de 37 e 16 colunas, respectivamente. **Usado por:** `_write_parquet` via despacho da instância concreta.

## 8. Ambiguidades e limitações

1. ⚠️ Não foi possível determinar com segurança a linhagem operacional dos quatro fluxos textuais até um Parquet final. `_format_date_time` sempre processa `data_hora_referencia` e cria `data_hora_fim` a partir de `_data`/`_hora_fim` quando necessário; os schemas textuais não fornecem todas essas colunas. A análise indica `AnalysisException` por coluna não resolvida antes da escrita.
2. ⚠️ O fluxo `CDRTextTransformer.transform_cdr_vivo_fcdr` chama `_preprocess_cdr_vivo_fcdr`, mas esse método não existe no arquivo da classe base nem na classe textual analisados. A linhagem após `_numero_origem` não é determinável e a chamada tende a produzir `AttributeError`.
3. ⚠️ No Teleparser Nokia, `rota_entrada` e `rota_saida` são inicialmente extraídas de `in_circuit_group` e `out_circuit_group`, mas o transformer as sobrescreve usando `_rota`, que não é produzido pelo schema Teleparser Nokia. A intenção de linhagem da rota após essa sobrescrita não é suportada pelo código; a operação tende a falhar por coluna não resolvida.
4. Para o Teleparser, colunas ausentes na origem são projetadas pelo extractor como `F.lit(None).cast("string")`. Assim, a origem de tais valores é `NULL` criado na extração, não o arquivo bruto.
5. Metadados `prestadora`, `tipo_cdr` e `arquivo_origem` dependem de `input_file_name()` e das posições `-3`, `-2` e `-1` no caminho. O código não valida esse formato de path.

## 9. Observações da revisão

## Inconsistências identificadas

| Localização | Descrição | Impacto potencial | Sugestão |
| :-- | :-- | :-- | :-- |
| `base_transformer._format_date_time` + extractors textuais | Consome colunas temporais que não são garantidas pelos schemas textuais. | Falha de análise Spark em todos os fluxos textuais documentados. | Alinhar contrato dos extractors e pré-condições do método base; cobrir por teste de integração. |
| `text_transformers.transform_cdr_vivo_fcdr` | Chama `_preprocess_cdr_vivo_fcdr` sem definição disponível. | `AttributeError` antes da transformação. | Definir claramente a implementação pretendida ou retirar a dependência, após decisão de produto. |
| `teleparser_transformers.transform_cdr_nokia` | Rota é sobrescrita a partir de `_rota`, que não existe no schema Teleparser Nokia. | Falha de resolução ou perda da rota originalmente extraída. | Revisar o campo de origem efetivo da regra de rota. |
| Contratos finais | Texto projeta 16 colunas; Teleparser projeta 37. | Consumidores não podem assumir schema único entre os dois pipelines. | Documentar formalmente como contratos distintos ou convergir deliberadamente. |

## Pontos de atenção para manutenção

| Localização | Descrição | Impacto potencial | Sugestão |
| :-- | :-- | :-- | :-- |
| `extract_cdr` Teleparser | `tipo_cdr` extraído do path é substituído por `_tipo_cdr` sempre que a coluna existir, mesmo nula. | Perda do metadado do diretório. | Decidir e testar a precedência desejada. |
| `_format_cell_id` | O particionamento de bits 5G assume `gnb_id_bits=26`. | CGI incorreto se o layout da operadora usar outra divisão. | Parametrizar por fonte após validação com dados reais. |
| `_build_composite_column` | Qualquer componente ausente anula o identificador completo. | Redução de cobertura de CGI/IMEI/IMSI parcialmente conhecidos. | Confirmar se a completude total é regra de negócio. |
| `NULL_SENTINEL_VALUE` | A string `__NULL__` representa ausência em campos operacionais. | Colisão se esse texto for um dado legítimo; semântica de nulo alterada. | Reservar/validar o sentinela no contrato de dados. |

## Possíveis melhorias

| Localização | Descrição | Impacto potencial | Sugestão |
| :-- | :-- | :-- | :-- |
| Testes | Não há teste de integração executável cobrindo cada par extractor/transformer. | Regressões de schema e parsing chegam ao runtime Spark. | Criar fixtures mínimas por formato e validar schema/linhagem esperados. |
| Orquestração | Não existe ligação explícita entre nomes de extratores e transformadores. | É possível combinar layouts incompatíveis. | Registrar um catálogo de pares suportados e contratos de entrada/saída. |
| Documentação de origem textual | A extração por índice perde o nome semântico original quando há header ou schema implícito. | Auditoria do CSV fica dependente da posição. | Versionar layouts com índice, nome fornecido pelo fabricante e exemplos representativos. |