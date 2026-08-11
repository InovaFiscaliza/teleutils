# Caminho das colunas dos CDRs

Este documento descreve como as colunas percorrem o pipeline de CDR do
Teleparser, desde o layout original até o contrato final gravado em parquet.
O fluxo é composto por três etapas:

1. **Extração:** `CDRTeleparserExtractor` lê o parquet bruto e aplica o
   `column_mapping` do schema do fornecedor.
2. **Transformação:** `CDRTeleparserTransformer` aplica regras específicas do
   layout e o pipeline comum de `CDRBaseTransformer`.
3. **Persistência:** `_select_transformed_columns` seleciona as colunas do
   contrato final, renomeia-as e `_write_parquet` grava o parquet particionado
   por `no_tipo_chamada`.

## Visão geral do fluxo

```mermaid
flowchart LR
    A[CDR bruto Teleparser] --> B[Mapeamento do schema]
    B --> C[Parquet intermediário]
    C --> D[Regras específicas do layout]
    D --> E[Pipeline comum]
    E --> F[Contrato final]
    F --> G[Parquet final]
```

No extrator, cada par `(coluna_original, coluna_intermediaria)` é convertido
em uma seleção Spark. Por exemplo:

```text
dateForStartOfCharge -> _data
timeForStartOfCharge -> _hora
```

Depois, o transformador opera sobre as colunas intermediárias. O alias final
é aplicado somente no momento da seleção do contrato de saída.

## Pipeline comum de transformação

O `CDRBaseTransformer._apply_standard_pipeline` executa estas operações:

| Ordem | Operação | Efeito principal |
|---|---|---|
| 1 | `_format_date_time` | Cria `data_hora` a partir de `_data` e `_hora` quando necessário; cria `data_hora_fim` a partir de `_data` e `_hora_fim`; converte ambas para timestamp; normaliza `duracao`. |
| 2 | `_format_numbers` | Normaliza `numero_origem` e `numero_destino`, criando os campos formatados e os indicadores de validade. |
| 3 | `_add_tn_validation_status` | Converte `_autenticacao` em `autenticacao`; quando a origem não fornece autenticação, cria o campo com valor nulo. |

## Ericsson

O Ericsson é o layout atualmente documentado de ponta a ponta.

### Mapeamento final do Ericsson

As setas abaixo devem ser lidas da esquerda para a direita. Quando houver
mais de uma origem, a coluna é derivada pela combinação das etapas indicadas.

| Coluna final | Caminho desde o CDR original | Regra aplicada |
|---|---|---|
| `nu_referencia` | `networkCallReference` -> `referencia` -> `nu_referencia` | Seleção e alias final. |
| `dh_referencia` | Não existe no schema Ericsson -> `data_hora_referencia` -> `dh_referencia` | Criada como timestamp nulo para atender ao contrato final. |
| `dh_chamada` | `dateForStartOfCharge` -> `_data`; `timeForStartOfCharge` -> `_hora`; `_data` + `_hora` -> `data_hora` -> `dh_chamada` | Concatenação, parsing para timestamp e alias final. |
| `dh_fim_chamada` | `dateForStartOfCharge` -> `_data`; `timeForStopOfCharge` -> `_hora_fim`; `_data` + `_hora_fim` -> `data_hora_fim` -> `dh_fim_chamada` | Concatenação, parsing para timestamp e alias final. |
| `qt_duracao_segundos` | `chargeableDuration` -> `duracao` -> `qt_duracao_segundos` | Ericsson converte `HH:mm:ss` em segundos inteiros antes do pipeline comum. |
| `nu_origem` | `callingPartyNumber.digits` -> `numero_origem` -> `numero_origem_formatado` -> `nu_origem` | Normalização do número. |
| `ic_origem_valido` | `callingPartyNumber.digits` -> `numero_origem` -> `numero_origem_valido` -> `ic_origem_valido` | Indicador produzido pela normalização. |
| `nu_origem_original` | `callingPartyNumber.digits` -> `numero_origem` -> `nu_origem_original` | Valor intermediário antes da formatação final. |
| `nu_destino` | `calledPartyNumber.digits` -> `numero_destino` -> `numero_destino_formatado` -> `nu_destino` | Substitui `#` por `c` e `*` por `b`, depois normaliza. |
| `ic_destino_valido` | `calledPartyNumber.digits` -> `numero_destino` -> `numero_destino_valido` -> `ic_destino_valido` | Indicador produzido pela normalização. |
| `nu_destino_original` | `calledPartyNumber.digits` -> `numero_destino` -> `nu_destino_original` | Valor intermediário após a substituição de caracteres especiais. |
| `no_resultado_chamada` | `callPosition` -> `status_chamada` -> `no_resultado_chamada` | Seleção e alias final. |
| `co_resposta_sip` | Não existe no schema Ericsson -> `codigo_resposta_sip` -> `co_resposta_sip` | Criada como inteiro nulo. |
| `no_autenticacao` | Não existe no Ericsson -> `autenticacao` -> `no_autenticacao` | Como `_autenticacao` não é fornecida, permanece nula. |
| `no_prestadora` | Caminho do arquivo -> `prestadora` -> `no_prestadora` | Obtida do terceiro componente anterior ao nome do arquivo no path. |
| `no_rota_entrada` | `incomingRoute` -> `rota_entrada` -> `no_rota_entrada` | Seleção e alias final. |
| `no_rota_saida` | `outgoingRoute` -> `rota_saida` -> `no_rota_saida` | Seleção e alias final. |
| `no_bilhetador` | `exchangeIdentity` -> `bilhetador` -> `no_bilhetador` | Seleção e alias final. |
| `nu_cgi_origem` | `firstCallingLocationInformation.{mcc,mnc,lac,ci_sac}` -> `celula_origem_{...}` -> `celula_origem` -> `nu_cgi_origem` | Componentes unidos por `-`; `ci_sac` recebe preenchimento à esquerda até cinco posições. |
| `nu_imei_origem` | `callingSubscriberIMEI.{type_allocation_code,serial_number}` -> `imei_origem_{tac,sn}` -> `imei_origem` -> `nu_imei_origem` | Componentes concatenados sem separador. |
| `nu_imsi_origem` | `callingSubscriberIMSI.{mcc,mnc,msin}` -> `imsi_origem_{mcc,mnc,msin}` -> `imsi_origem` -> `nu_imsi_origem` | Componentes concatenados sem separador. |
| `nu_ip_origem` | Não existe no schema Ericsson -> `ip_origem` -> `nu_ip_origem` | Criada como string nula. |
| `nu_cgi_destino` | `firstCalledLocationInformation.{mcc,mnc,lac,ci_sac}` -> `celula_destino_{...}` -> `celula_destino` -> `nu_cgi_destino` | Componentes unidos por `-`; `ci_sac` recebe preenchimento à esquerda até cinco posições. |
| `nu_imei_destino` | `calledSubscriberIMEI.{type_allocation_code,serial_number}` -> `imei_destino_{tac,sn}` -> `imei_destino` -> `nu_imei_destino` | Componentes concatenados sem separador. |
| `nu_imsi_destino` | `calledSubscriberIMSI.{mcc,mnc,msin}` -> `imsi_destino_{mcc,mnc,msin}` -> `imsi_destino` -> `nu_imsi_destino` | Componentes concatenados sem separador. |
| `nu_ip_destino` | Não existe no schema Ericsson -> `ip_destino` -> `nu_ip_destino` | Criada como string nula. |
| `no_agente_usuario` | Não existe no schema Ericsson -> `agente_usuario` -> `no_agente_usuario` | Criada como string nula. |
| `no_tipo_cdr` | Path do arquivo -> `tipo_cdr` -> `no_tipo_cdr` | Derivada do path; só é substituída quando o extrator recebe `_tipo_cdr`. |
| `no_arquivo_origem` | Nome do parquet lido -> `arquivo_origem` -> `no_arquivo_origem` | Derivada do nome do arquivo em `input_file_name()`. |
| `no_tipo_chamada` | `CallModule` -> `tipo_chamada` -> `no_tipo_chamada` | Convertida para string e usada também como partição do parquet final. |

### Exemplo detalhado: `dh_chamada`

O caminho completo da coluna de início da chamada é:

```text
dateForStartOfCharge
  -> _data
timeForStartOfCharge
  -> _hora
_data + " " + _hora
  -> data_hora
parse(data_hora, "yy-MM-dd HH:mm:ss")
  -> data_hora (timestamp)
data_hora
  -> dh_chamada
```

O mesmo campo `data_hora` também é usado como base para o contrato de saída
`dh_chamada`; a etapa de alias não cria uma nova regra de negócio, apenas
seleciona o campo transformado com o nome final.

## TIM Huawei

Esta tabela registra o estado atual do mapeamento do layout TIM Huawei. As
colunas finais que ainda não possuem caminho implementado são identificadas
explicitamente.

| Coluna final | Caminho desde o CDR original | Regra aplicada |
|---|---|---|
| `nu_referencia` | `network-Call-Reference` -> `referencia` -> `nu_referencia` | Seleção e alias final. |
| `dh_referencia` | coluna não mapeada | Em backlog de desenvolvimento: não há coluna intermediária de referência temporal no schema TIM Huawei. |
| `dh_chamada` | `recordOpeningTime` -> `data_hora` -> `dh_chamada` | Parsing com `yyyy-MM-dd HH:mm:ssxxx` e alias final. |
| `dh_fim_chamada` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece data/hora de fim. |
| `qt_duracao_segundos` | `duration` -> `duracao` -> `qt_duracao_segundos` | Conversão defensiva para inteiro no pipeline comum. |
| `nu_origem` | `list-Of-Calling-Party-Address_tEL-URI` -> `numero_origem` -> `numero_origem_formatado` -> `nu_origem` | Remove os dois primeiros caracteres antes da normalização. |
| `ic_origem_valido` | `numero_origem` -> `numero_origem_valido` -> `ic_origem_valido` | Indicador produzido pela normalização. |
| `nu_origem_original` | `numero_origem` -> `nu_origem_original` | Valor intermediário após a remoção do prefixo. |
| `nu_destino` | `called-Party-Address_tEL-URI` -> `numero_destino` -> `numero_destino_formatado` -> `nu_destino` | Remove os dois primeiros caracteres antes da normalização. |
| `ic_destino_valido` | `numero_destino` -> `numero_destino_valido` -> `ic_destino_valido` | Indicador produzido pela normalização. |
| `nu_destino_original` | `numero_destino` -> `nu_destino_original` | Valor intermediário após a remoção do prefixo. |
| `no_resultado_chamada` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece `status_chamada`. |
| `co_resposta_sip` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece código de resposta SIP. |
| `no_autenticacao` | `calling-Party-Address-Generic` -> `_numero_origem_generico` -> `_autenticacao` -> `autenticacao` -> `no_autenticacao` | Extrai o token `verstat=...` por expressão regular. |
| `no_prestadora` | Caminho do arquivo -> `prestadora` -> `no_prestadora` | Derivada do path de entrada. |
| `no_rota_entrada` | `specifiedTreatmentField_incoming-Route` -> `rota_entrada` -> `no_rota_entrada` | Seleção e alias final. |
| `no_rota_saida` | `specifiedTreatmentField_outgoing-Route` -> `rota_saida` -> `no_rota_saida` | Seleção e alias final. |
| `no_bilhetador` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece bilhetador. |
| `nu_cgi_origem` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece dados de célula de origem. |
| `nu_imei_origem` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IMEI de origem. |
| `nu_imsi_origem` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IMSI de origem. |
| `nu_ip_origem` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IP de origem. |
| `nu_cgi_destino` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece dados de célula de destino. |
| `nu_imei_destino` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IMEI de destino. |
| `nu_imsi_destino` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IMSI de destino. |
| `nu_ip_destino` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IP de destino. |
| `no_agente_usuario` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece agente do usuário. |
| `no_tipo_cdr` | `recordType` -> `_tipo_cdr` -> `tipo_cdr` -> `no_tipo_cdr` | O extrator substitui o valor derivado do path pelo valor de `_tipo_cdr`. |
| `no_arquivo_origem` | Nome do parquet lido -> `arquivo_origem` -> `no_arquivo_origem` | Derivada de `input_file_name()`. |
| `no_tipo_chamada` | `role-of-Node` -> `tipo_chamada` -> `no_tipo_chamada` | Convertida para string e usada como partição do parquet final. |

## Vivo FCDR

| Coluna final | Caminho desde o CDR original | Regra aplicada |
|---|---|---|
| `nu_referencia` | `networkCallReference` -> `referencia` -> `nu_referencia` | Seleção e alias final. |
| `dh_referencia` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece data/hora de referência. |
| `dh_chamada` | `dateForStartOfCharge` -> `_data`; `timeForStartOfCharge` -> `_hora`; `_data` + `_hora` -> `data_hora` -> `dh_chamada` | Parsing com `yyyyMMdd HHmmss` e alias final. |
| `dh_fim_chamada` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece horário de fim. |
| `qt_duracao_segundos` | `chargeableDurat` -> `duracao` -> `qt_duracao_segundos` | Conversão defensiva para inteiro no pipeline comum. |
| `nu_origem` | `callingPartyNumber` -> `_numero_origem` -> `numero_origem` -> `numero_origem_formatado` -> `nu_origem` | O pré-processamento separa o número do token de autenticação antes da normalização. |
| `ic_origem_valido` | `numero_origem` -> `numero_origem_valido` -> `ic_origem_valido` | Indicador produzido pela normalização. |
| `nu_origem_original` | `callingPartyNumber` -> `_numero_origem` -> `numero_origem` -> `nu_origem_original` | Valor após a separação do token de autenticação. |
| `nu_destino` | `calledPartyNumber` -> `numero_destino` -> `numero_destino_formatado` -> `nu_destino` | Normalização do número. |
| `ic_destino_valido` | `numero_destino` -> `numero_destino_valido` -> `ic_destino_valido` | Indicador produzido pela normalização. |
| `nu_destino_original` | `calledPartyNumber` -> `numero_destino` -> `nu_destino_original` | Valor intermediário antes da formatação final. |
| `no_resultado_chamada` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece status de chamada. |
| `co_resposta_sip` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece código de resposta SIP. |
| `no_autenticacao` | `callingPartyNumber` -> `_numero_origem` -> `_autenticacao` -> `autenticacao` -> `no_autenticacao` | O token após `;` é classificado pelo pipeline comum. |
| `no_prestadora` | Caminho do arquivo -> `prestadora` -> `no_prestadora` | Derivada do path de entrada. |
| `no_rota_entrada` | `incomingRoute` -> `rota_entrada` -> `no_rota_entrada` | Seleção e alias final. |
| `no_rota_saida` | `outgoingRoute` -> `rota_saida` -> `no_rota_saida` | Seleção e alias final. |
| `no_bilhetador` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece bilhetador. |
| `nu_cgi_origem` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece dados de célula de origem. |
| `nu_imei_origem` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IMEI de origem. |
| `nu_imsi_origem` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IMSI de origem. |
| `nu_ip_origem` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IP de origem. |
| `nu_cgi_destino` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece dados de célula de destino. |
| `nu_imei_destino` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IMEI de destino. |
| `nu_imsi_destino` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IMSI de destino. |
| `nu_ip_destino` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IP de destino. |
| `no_agente_usuario` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece agente do usuário. |
| `no_tipo_cdr` | Caminho do arquivo -> `tipo_cdr` -> `no_tipo_cdr` | Não há `_tipo_cdr` no schema Vivo; usa o valor derivado do path. |
| `no_arquivo_origem` | Nome do parquet lido -> `arquivo_origem` -> `no_arquivo_origem` | Derivada de `input_file_name()`. |
| `no_tipo_chamada` | `callModule` -> `_tipo_chamada` -> `tipo_chamada` -> `no_tipo_chamada` | Os códigos `1`, `3` e `4` são convertidos para rótulos funcionais. |

## Nokia

| Coluna final | Caminho desde o CDR original | Regra aplicada |
|---|---|---|
| `nu_referencia` | `call_reference` -> `referencia` -> `nu_referencia` | Seleção e alias final. |
| `dh_referencia` | `call_reference_time` -> `data_hora_referencia` -> `dh_referencia` | Seleção e alias final. |
| `dh_chamada` | `in_channel_allocated_time` -> `data_hora_alocacao_canal`; fallback `data_hora_referencia`; `coalesce(...)` -> `data_hora` -> `dh_chamada` | Usa o horário de alocação do canal e recorre ao horário de referência quando necessário. |
| `dh_fim_chamada` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece horário de fim. |
| `qt_duracao_segundos` | `orig_mcz_duration`, `term_mcz_duration`, `forw_mcz_duration`, `roam_mcz_duration`, `iaz_duration`, `oaz_duration`, `chargeable_duration`, `char_band_duration` -> `_duracao_*` -> `duracao` -> `qt_duracao_segundos` | Consolida os campos de duração com `coalesce`; o primeiro valor disponível é usado. |
| `nu_origem` | `calling_number` -> `numero_origem`; fallback `orig_calling_number` -> `numero_origem_original`; `coalesce(...)` -> `numero_origem` -> `numero_origem_formatado` -> `nu_origem` | Para chamadas `FORW`, o destino usa a regra específica de encaminhamento. |
| `ic_origem_valido` | `numero_origem` -> `numero_origem_valido` -> `ic_origem_valido` | Indicador produzido pela normalização. |
| `nu_origem_original` | `orig_calling_number` -> `numero_origem_original` -> `nu_origem_original` | Seleção e alias final. |
| `nu_destino` | `called_number` -> `numero_destino`; para `FORW`, `forwarding_number` -> `numero_origem_encaminhamento`; seleção -> `numero_destino_formatado` -> `nu_destino` | O destino de chamadas encaminhadas é derivado do número de encaminhamento. |
| `ic_destino_valido` | `numero_destino` -> `numero_destino_valido` -> `ic_destino_valido` | Indicador produzido pela normalização. |
| `nu_destino_original` | `orig_called_number` -> `numero_destino_original` -> `nu_destino_original` | Seleção e alias final. |
| `no_resultado_chamada` | coluna não mapeada | Em backlog de desenvolvimento: o layout Nokia não possui `status_chamada`; `record_type` representa o tipo da chamada. |
| `co_resposta_sip` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece código de resposta SIP. |
| `no_autenticacao` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece `_autenticacao`. |
| `no_prestadora` | Caminho do arquivo -> `prestadora` -> `no_prestadora` | Derivada do path de entrada. |
| `no_rota_entrada` | `in_circuit_group` -> `rota_entrada` -> `no_rota_entrada` | O transformador também possui regras específicas para layouts que usam `_rota`. |
| `no_rota_saida` | `out_circuit_group` -> `rota_saida` -> `no_rota_saida` | O transformador também possui regras específicas para layouts que usam `_rota`. |
| `no_bilhetador` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece bilhetador. |
| `nu_cgi_origem` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece dados de célula de origem. |
| `nu_imei_origem` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IMEI de origem. |
| `nu_imsi_origem` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IMSI de origem. |
| `nu_ip_origem` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IP de origem. |
| `nu_cgi_destino` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece dados de célula de destino. |
| `nu_imei_destino` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IMEI de destino. |
| `nu_imsi_destino` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IMSI de destino. |
| `nu_ip_destino` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece IP de destino. |
| `no_agente_usuario` | coluna não mapeada | Em backlog de desenvolvimento: o schema não fornece agente do usuário. |
| `no_tipo_cdr` | Caminho do arquivo -> `tipo_cdr` -> `no_tipo_cdr` | Não há `_tipo_cdr` no schema Nokia; usa o valor derivado do path. |
| `no_arquivo_origem` | Nome do parquet lido -> `arquivo_origem` -> `no_arquivo_origem` | Derivada de `input_file_name()`. |
| `no_tipo_chamada` | `record_type` -> `tipo_chamada` -> `no_tipo_chamada` | Para `FOR`, o transformador normaliza o valor para `FORW`; o resultado também particiona o parquet final. |

As tabelas acima representam o estado atual da implementação. As colunas sem
caminho permanecem explicitamente identificadas como `coluna não mapeada` e
estão em backlog de desenvolvimento. Essa marcação deve ser substituída por um
caminho completo quando o layout correspondente for evoluído e validado.

## Contrato final

O contrato de saída é definido em
`teleutils/core/transformers/base_transformer.py`, no método
`_select_transformed_columns`. Os nomes finais seguem a convenção:

- `nu_`: identificadores e números;
- `dh_`: datas e horários;
- `qt_`: quantidades, como duração em segundos;
- `ic_`: indicadores booleanos;
- `no_`: nomes, descrições e categorias;
- `co_`: códigos.

Essa convenção deve ser usada ao completar os mapeamentos dos demais CDRs.
Cada novo caminho deve informar, de forma explícita, a coluna original, a
coluna intermediária, as regras de derivação e o nome final.

## Referências de implementação

- `src/teleutils/core/extractors/schemas.py`: schemas e mapeamentos da entrada.
- `src/teleutils/core/extractors/teleparser_extractors.py`: leitura, seleção e
  enriquecimento do parquet intermediário.
- `src/teleutils/core/transformers/base_transformer.py`: pipeline comum e
  contrato final.
- `src/teleutils/core/transformers/teleparser_transformers.py`: regras
  específicas dos layouts Teleparser.

## Prompt para atualização futura

Use o prompt abaixo quando um novo tipo de CDR estiver completamente mapeado
ou desenvolvido no código:

```text
Atualize o documento `colunas.md` com base na implementação atual do CDR
<NOME_DO_CDR>.

Antes de editar, analise os módulos relacionados ao fluxo completo:

- `src/teleutils/core/extractors/schemas.py`
- `src/teleutils/core/extractors/teleparser_extractors.py`
- `src/teleutils/core/transformers/base_transformer.py`
- `src/teleutils/core/transformers/teleparser_transformers.py`

Considere também testes, fixtures ou outros transformadores específicos que
comprovem as regras do layout. Não invente caminhos de colunas: documente
somente mapeamentos, derivações, fallbacks e preenchimentos que estejam
implementados ou comprovados por testes.

Na seção `## <NOME_DO_CDR>`, mantenha uma tabela com exatamente estas colunas:

| Coluna final | Caminho desde o CDR original | Regra aplicada |

Para cada coluna do contrato final definido em
`CDRBaseTransformer._select_transformed_columns`, registre:

1. A coluna original do CDR.
2. A coluna intermediária criada pelo schema do extrator.
3. Todas as colunas derivadas durante o pré-processamento ou pipeline comum.
4. A coluna final e seu alias.
5. A regra aplicada, incluindo conversões, concatenações, normalizações,
   fallbacks, valores nulos, filtros e regras específicas do layout.

Quando o mapeamento ainda não existir, mantenha `coluna não mapeada` no campo
`Caminho desde o CDR original` e escreva `Em backlog de desenvolvimento:` no
início da respectiva célula `Regra aplicada`, preservando a justificativa da
pendência.

Se a coluna final for preenchida com `NULL` por exigência do contrato, não a
classifique como backlog: documente a origem como inexistente no layout e a
regra de preenchimento nulo implementada.

Ao atualizar o documento:

- preserve as seções e tabelas dos demais tipos de CDR;
- não altere mapeamentos já documentados sem verificar a implementação atual;
- remova o status de backlog somente das colunas que passaram a ter caminho
  implementado e validado;
- mantenha a nomenclatura e a convenção de prefixos do contrato final;
- atualize diagramas ou exemplos somente quando o novo fluxo exigir;
- ajuste a seção `Contrato final` apenas se o contrato de saída tiver mudado;
- mantenha as referências de implementação atualizadas.

Depois da edição, valide:

1. Que o novo CDR possui uma seção própria.
2. Que a tabela cobre todas as colunas do contrato final, sem duplicatas.
3. Que cada caminho citado existe no schema ou no transformador analisado.
4. Que toda coluna não mapeada contém o status `Em backlog de desenvolvimento`.
5. Que `git diff --check` não apresenta erros.

Informe ao final quais colunas foram concluídas, quais continuam em backlog e
quais validações foram executadas.
```