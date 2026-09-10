# Relatório de Auditoria da Documentação de Linhagem de Dados

## 1. Resumo executivo

A análise foi feita na ordem `código -> reconstrução de linhagem -> comparação com documentação`, usando os contratos efetivos de `_select_transformed_columns` e a escrita de `_write_parquet` como referência. Foram identificadas 37 colunas no contrato final Teleparser e 16 no contrato textual; as 16 são um subconjunto das 37. A cobertura abaixo contabiliza as 37 colunas distintas, considerando todos os caminhos que as produzem.

| Resultado | Quantidade |
| :-- | --: |
| Confirmada | 32 |
| Parcialmente correta | 0 |
| Incorreta | 4 |
| Não documentada | 0 |
| Ambígua | 1 |

**Avaliação geral: Necessita correções.** A documentação identifica corretamente a arquitetura, os dois contratos finais, os auxiliares centrais e a maior parte das linhagens Teleparser. As divergências incluem uma linhagem efetivamente vazia no ramo IBCF e uma origem posicional incorreta no CSV Ericsson. Além disso, a documentação aponta corretamente falhas estáticas que impedem a execução dos fluxos textuais e do Nokia Teleparser, mas não separa com precisão o que é impossibilidade de execução do que é uma linhagem de dados ambígua.

## 2. Cobertura das colunas finais

| Coluna Final | Status | Resultado da Auditoria |
| :-- | :-- | :-- |
| `nu_referencia` | Confirmada | Alias de `referencia`; as origens e sentinelas por fluxo foram documentadas. |
| `nu_referencia_sip` | Confirmada | Alias de `referencia_sip` e sentinela quando ausente, conforme pipeline base. |
| `dh_referencia` | Confirmada | Alias de `data_hora_referencia`; a documentação registra as constantes por fluxo Teleparser. |
| `dh_chamada` | Confirmada | Concatenação condicional ou fonte direta, seguida de `try_to_timestamp` e `greatest`, corretamente descrita. |
| `dh_fim_chamada` | Confirmada | Construção condicional e parsing temporal descritos corretamente. |
| `qt_duracao_segundos` | Confirmada | Conversões específicas e `cast(int)`/`coalesce(0)` do pipeline base estão documentados. |
| `nu_origem` | Incorreta | O caminho IBCF do Huawei é apresentado como número extraído, mas o segundo regex procura `sip:` em uma string que já contém apenas dígitos. |
| `ic_origem_valido` | Incorreta | Deriva da mesma entrada incorreta de `nu_origem` no ramo IBCF Huawei. |
| `nu_origem_original` | Confirmada | O restauro de `_numero_origem_original` depois da UDF está corretamente documentado. |
| `nu_destino` | Confirmada | Os ramos ATS e IBCF e a normalização via UDF foram identificados. |
| `ic_destino_valido` | Confirmada | É o campo booleano retornado pela mesma UDF aplicada a `numero_destino`. |
| `nu_destino_original` | Confirmada | Alias após eventual restauro de `_numero_destino_original`, corretamente descrito. |
| `no_resultado_chamada` | Confirmada | Mapeamentos por fornecedor e preenchimento do pipeline base foram documentados. |
| `co_resposta_sip` | Confirmada | Constante nula ou código Huawei `>= 200`, conforme o fluxo. |
| `no_autenticacao` | Incorreta | Para TIM Huawei textual, a documentação informa nulo/não usado, embora `_add_tn_validation_status` consuma `_autenticacao`. |
| `no_prestadora` | Confirmada | Derivação de `input_file_name()` na posição `-3` está registrada. |
| `no_rota_entrada` | Incorreta | O caminho Ericsson textual aponta o índice `8`, mas a configuração mapeia o índice `20` para `rota_entrada`. |
| `no_rota_saida` | Ambígua | A documentação aponta que Nokia Teleparser referencia `_rota` não extraída; portanto não há saída executável desse ramo, embora os demais estejam corretos. |
| `no_bilhetador` | Confirmada | Alias com preenchimento por sentinela no pipeline base. |
| `nu_cgi_origem` | Confirmada | Composição Ericsson/Nokia e decodificação Huawei/Vivo foram rastreadas. |
| `nu_imei_origem` | Confirmada | Regras de composição, filtragem ou remoção de hífen estão corretas. |
| `nu_imsi_origem` | Confirmada | Fontes diretas, composição ou parsing JSON foram documentados. |
| `nu_ip_origem` | Confirmada | Coluna nula tipada nos fluxos Teleparser que a selecionam. |
| `nu_porta_ip_origem` | Confirmada | Coluna nula tipada nos fluxos Teleparser que a selecionam. |
| `nu_cgi_destino` | Confirmada | Linhagens específicas por fornecedor documentadas corretamente. |
| `nu_imei_destino` | Confirmada | Linhagens específicas por fornecedor documentadas corretamente. |
| `nu_imsi_destino` | Confirmada | Linhagens específicas por fornecedor documentadas corretamente. |
| `nu_ip_destino` | Confirmada | Coluna nula tipada nos fluxos Teleparser que a selecionam. |
| `nu_porta_ip_destino` | Confirmada | Coluna nula tipada nos fluxos Teleparser que a selecionam. |
| `no_agente_usuario` | Confirmada | Valor direto Huawei ou nulo tipado nos demais fluxos Teleparser. |
| `no_tipo_cdr` | Confirmada | Campo do caminho, com substituição por `_tipo_cdr` no extractor Huawei Teleparser. |
| `no_arquivo_origem` | Confirmada | Derivação de `input_file_name()` na posição `-1` está correta. |
| `no_tipo_chamada` | Confirmada | `cast(string)` final e mapeamentos anteriores por formato estão documentados. |

## 3. Divergências encontradas

## `nu_origem`

### Classificação

Incorreta.

### Documentação anterior

No LTE Huawei TIM, afirma que o ramo IBCF extrai o número do JSON `$[0].sIP-URI` e aplica regex SIP, resultando em `numero_origem` para normalização.

### Evidência encontrada no código

`_numero_origem_ibcf` já é criado por `regexp_extract(..., r"sip:([0-9]+)[@;]", 1)`, portanto contém somente o grupo numérico. Em seguida, `ibcf_calling_party` executa `regexp_extract(_numero_origem_ibcf, r"sip:\+?([0-9]+)", 1)`. Como a entrada já não contém o prefixo `sip:`, a expressão retorna a string vazia. `numero_origem` usa esse resultado quando o registro não é ATS.

### Linhagem identificada

```text
list-Of-Calling-Party-Address
-> _numero_origem
-> get_json_object($[0].sIP-URI)
-> regexp_extract(sip:([0-9]+)[@;]) -> _numero_origem_ibcf
-> regexp_extract(sip:+?([0-9]+)) -> ""
-> numero_origem
-> spark_normalize_number
-> nu_origem
```

### Problema identificado

A documentação descreve a intenção aparente da extração, mas não a expressão efetivamente composta. O resultado do ramo IBCF não é o número extraído anteriormente.

### Correção recomendada na documentação

Registrar o segundo `regexp_extract` e seu efeito sobre a string já sem `sip:`; marcar o resultado como string vazia para IBCF, sujeito ao comportamento da UDF de normalização.

## `ic_origem_valido`

### Classificação

Incorreta.

### Documentação anterior

Associa a flag à normalização do número IBCF extraído do SIP.

### Evidência encontrada no código

`numero_origem_valido` é expandida de `_numero_origem_formatado.numero_valido`, e esse struct recebe o resultado de `spark_normalize_number("numero_origem")`. No ramo IBCF, `numero_origem` é a string vazia indicada na divergência anterior.

### Linhagem identificada

```text
_numero_origem_ibcf
-> regexp_extract(sip:+?([0-9]+)) -> ""
-> numero_origem
-> spark_normalize_number(...).numero_valido
-> numero_origem_valido
-> ic_origem_valido
```

### Problema identificado

A documentação herda a linhagem incorreta atribuída a `nu_origem` e, por consequência, atribui a flag a uma entrada que o código não fornece.

### Correção recomendada na documentação

Vincular a flag ao resultado real do segundo regex no ramo IBCF e não ao valor originalmente extraído do JSON.

## `no_autenticacao`

### Classificação

Incorreta.

### Documentação anterior

Para o fluxo textual TIM Huawei, registra `autenticacao = NULL` e também afirma que `_autenticacao` é extraída, mas não usada.

### Evidência encontrada no código

O schema textual TIM Huawei atribui o índice `16` a `_autenticacao`. Antes da seleção final, `transform_cdr_tim_huawei` chama `_apply_standard_pipeline`, que chama `_add_tn_validation_status`. Quando `_autenticacao` existe, este método usa três `when` com `startswith("verstat=...")` para criar `autenticacao`; o ramo nulo só é usado se a coluna não existir.

### Linhagem identificada

```text
CSV índice 16
-> _autenticacao
-> _add_tn_validation_status
-> autenticacao
-> no_autenticacao
```

### Problema identificado

Há contradição com a chamada efetiva da função comum. O fluxo possui outros defeitos de colunas temporais que impedem sua execução, mas isso não torna `_autenticacao` uma coluna não usada na análise estática.

### Correção recomendada na documentação

Descrever a classificação `Passed`/`Failed`/`No-TN-Validation` e, separadamente, registrar que o fluxo textual falha antes da materialização do Parquet final devido a colunas temporais ausentes.

## `no_rota_entrada`

### Classificação

Incorreta.

### Documentação anterior

No fluxo textual Ericsson, associa `no_rota_entrada` ao índice CSV `8`.

### Evidência encontrada no código

`CDRTextExtractor._SCHEMAS["ericsson"]` define `column_indices=[0,1,2,3,4,8,9,11,20]` e `column_names=[..., "rota_saida", "numero_destino", "duracao", "rota_entrada"]`. Logo, `rota_saida` é o índice `8`, enquanto `rota_entrada` é o índice `20`.

### Linhagem identificada

```text
CSV índice 20
-> rota_entrada
-> _fill_missing_columns (coalesce com "__NULL__")
-> no_rota_entrada
```

### Problema identificado

A documentação trocou a posição de origem de `rota_entrada` pela posição de `rota_saida`.

### Correção recomendada na documentação

Substituir a origem de `no_rota_entrada` por índice `20`. O índice `8` deve permanecer somente para `no_rota_saida`.

## 4. Funções auxiliares

| Função | Status da documentação | Problema |
| :-- | :-- | :-- |
| `_apply_standard_pipeline` | Correta | Ordem e principais efeitos foram registrados. |
| `_format_date_time` | Correta | A documentação identifica os campos que tornam os fluxos textuais não executáveis. |
| `_format_numbers` | Correta | UDF, struct temporário, expansão e restauro estão documentados. |
| `_add_tn_validation_status` | Parcialmente correta | A função está documentada, mas foi contradita no fluxo TIM Huawei textual, onde `_autenticacao` foi declarada como não usada. |
| `_add_missing_reference_columns` | Correta | Sentinela para referências ausentes documentada. |
| `_fill_missing_columns` | Correta | Campos e uso de `__NULL__` documentados. |
| `spark_normalize_number` | Correta | Localização, retorno estruturado e consumidores estão corretos. |
| `normalize_number` e `_clean_numbers` | Correta | São dependências indiretas da UDF, corretamente classificadas. |
| `_null_if_blank`, `_concat_or_null`, `_build_composite_column` | Correta | Relação de composição e tratamento de nulos está correta. |
| `_format_cell_id` | Correta | Chamadores, entradas e formatos 3G/4G/5G identificados. |
| `_preprocess_cdr_vivo_fcdr` | Correta como problema | A documentação não a inventa e registra que não há definição no código analisado. |
| `normalize_number_pair`, `validar_cnpj`, `spark_validar_cnpj` | Não aplicável | Estão disponíveis no pacote, mas não participam das cadeias CDR auditadas; sua ausência não é omissão. |

## 5. Auditoria das renomeações finais

`CDRBaseTransformer._write_parquet` chama `_select_transformed_columns` por despacho dinâmico e aplica `mode("overwrite").partitionBy("no_tipo_chamada").parquet(target_file)`. O Teleparser usa a implementação base; o texto usa o override com 16 aliases. A única transformação dentro da seleção final é `tipo_chamada.cast(StringType())`; os demais itens são `alias`.

| Coluna antes da etapa final | Operação identificada | Coluna final | Documentação correta? |
| :-- | :-- | :-- | :-- |
| `referencia` | Alias | `nu_referencia` | Sim |
| `referencia_sip` | Alias (somente base) | `nu_referencia_sip` | Sim |
| `data_hora_referencia` | Alias (somente base) | `dh_referencia` | Sim |
| `data_hora` | Alias | `dh_chamada` | Sim |
| `data_hora_fim` | Alias (somente base) | `dh_fim_chamada` | Sim |
| `duracao` | Alias | `qt_duracao_segundos` | Sim |
| `numero_origem_formatado` | Alias | `nu_origem` | Sim |
| `numero_origem_valido` | Alias | `ic_origem_valido` | Sim, exceto ramo IBCF documentado incorretamente |
| `numero_origem` | Alias | `nu_origem_original` | Sim |
| `numero_destino_formatado` | Alias | `nu_destino` | Sim |
| `numero_destino_valido` | Alias | `ic_destino_valido` | Sim |
| `numero_destino` | Alias | `nu_destino_original` | Sim |
| `status_chamada` | Alias (somente base) | `no_resultado_chamada` | Sim |
| `codigo_resposta_sip` | Alias (somente base) | `co_resposta_sip` | Sim |
| `autenticacao` | Alias | `no_autenticacao` | Não, no fluxo textual TIM Huawei |
| `prestadora` | Alias | `no_prestadora` | Sim |
| `rota_entrada` | Alias | `no_rota_entrada` | Não, no índice do Ericsson textual |
| `rota_saida` | Alias | `no_rota_saida` | Sim, mas Nokia Teleparser não é executável |
| `bilhetador` | Alias (somente base) | `no_bilhetador` | Sim |
| `celula_origem` | Alias (somente base) | `nu_cgi_origem` | Sim |
| `imei_origem` | Alias (somente base) | `nu_imei_origem` | Sim |
| `imsi_origem` | Alias (somente base) | `nu_imsi_origem` | Sim |
| `ip_origem` | Alias (somente base) | `nu_ip_origem` | Sim |
| `porta_ip_origem` | Alias (somente base) | `nu_porta_ip_origem` | Sim |
| `celula_destino` | Alias (somente base) | `nu_cgi_destino` | Sim |
| `imei_destino` | Alias (somente base) | `nu_imei_destino` | Sim |
| `imsi_destino` | Alias (somente base) | `nu_imsi_destino` | Sim |
| `ip_destino` | Alias (somente base) | `nu_ip_destino` | Sim |
| `porta_ip_destino` | Alias (somente base) | `nu_porta_ip_destino` | Sim |
| `agente_usuario` | Alias (somente base) | `no_agente_usuario` | Sim |
| `tipo_cdr` | Alias | `no_tipo_cdr` | Sim |
| `arquivo_origem` | Alias | `no_arquivo_origem` | Sim |
| `tipo_chamada` | `cast(string)` e alias | `no_tipo_chamada` | Sim |

As 16 colunas do override textual são: `nu_referencia`, `nu_origem_original`, `nu_destino_original`, `nu_origem`, `ic_origem_valido`, `nu_destino`, `ic_destino_valido`, `dh_chamada`, `qt_duracao_segundos`, `no_tipo_chamada`, `no_autenticacao`, `no_rota_entrada`, `no_rota_saida`, `no_prestadora`, `no_tipo_cdr` e `no_arquivo_origem`. As demais 21 pertencem apenas à seleção base/Teleparser.

## 6. Auditoria das ambiguidades

### `no_rota_saida` no Teleparser Nokia

O código torna o fluxo não executável, não apenas incerto: `transform_cdr_nokia` constrói `outgoing_route_rules` com `F.col("_rota")`, mas o schema Teleparser Nokia extrai `rota_entrada` e `rota_saida`, sem `_rota`. Como Spark resolve referências de coluna no plano, a transformação não chega a `_write_parquet` para esse fluxo. A documentação anterior marca a ausência de `_rota`, o que é correto, mas deve usar a formulação mais precisa: **linhagem final não materializável devido a coluna não resolvida**.

### Fluxos textuais

A documentação corretamente evita confirmar Parquet final materializado: `_format_date_time` sempre referencia `data_hora_referencia` e, quando `data_hora_fim` não existe, `_data` e `_hora_fim`; os schemas textuais não garantem esses campos. No Vivo FCDR há ainda chamada para `_preprocess_cdr_vivo_fcdr`, sem definição encontrada. As tabelas de origem desses fluxos devem ser apresentadas como linhagem estática pretendida, não como resultado final efetivamente produzido.

## 7. Problemas metodológicos da documentação

1. Há uma associação incorreta por equivalência aparente de transformação no IBCF: a documentação parou no primeiro regex e não rastreou o segundo `regexp_extract` aplicado sobre sua saída.
2. A extração textual por índice não foi confrontada integralmente com a ordem de `column_indices` e `column_names`, o que resultou no índice errado de `rota_entrada` Ericsson.
3. A seção textual TIM Huawei afirma que `_autenticacao` não é usada, apesar de o método comum chamado pelo próprio fluxo consumi-la. Isso indica rastreamento incompleto de chamada indireta.
4. A documentação deve separar melhor três situações: linhagem confirmada, intenção estática posterior a uma falha e fluxo que não materializa saída por referência a coluna/método inexistente.
5. Não foram encontradas colunas finais omitidas: a seleção final e seus dois contratos foram identificados corretamente.

## 8. Checklist final

## Cobertura

* [x] Todas as colunas finais foram verificadas.
* [x] Todas as funções principais foram verificadas.
* [x] As funções auxiliares relevantes foram verificadas.
* [x] A etapa final de geração do Parquet foi auditada.

## Linhagem

* [x] As origens foram verificadas contra o código.
* [x] As colunas intermediárias foram verificadas.
* [x] As transformações foram verificadas.
* [x] As renomeações foram verificadas.
* [x] Os casts relevantes foram verificados.

## Documentação

* [x] Não há colunas finais sem documentação.
* [ ] Não há linhagens assumidas apenas pela semelhança dos nomes.
* [x] As ambiguidades relevantes estão identificadas.
* [ ] As funções auxiliares comuns estão corretamente classificadas em todos os fluxos.