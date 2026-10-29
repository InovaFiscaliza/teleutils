# Relatório de Auditoria da Documentação de Linhagem de Dados

## 1. Resumo executivo

Esta auditoria reconstruiu a linhagem a partir do código ativo em
`src/teleutils`, excluindo `src/teleutils/robocalls`, antes de consultar
`docs/teleutils_linhagem_transformacoes_dados.md`.

O resultado final é o Parquet escrito por `CDRBaseTransformer._write_parquet`.
`CDRBaseTransformer._select_transformed_columns` projeta as 35 entradas de
`TARGET_SCHEMA`, aplica o cast configurado e usa o nome de destino como alias.
Há cinco transformadores ativos: Ericsson, LTE Huawei TIM, LTE Ericsson Vivo,
Nokia e Algar Huawei.

| Medida | Quantidade |
| :-- | --: |
| Colunas finais identificadas | 35 |
| Confirmadas | 35 |
| Parcialmente corretas | 0 |
| Incorretas | 0 |
| Não documentadas | 0 |
| Ambíguas na documentação | 0 |

**Classificação geral: Aprovada com ressalvas.** A documentação corresponde ao
comportamento estático implementado e registra os comportamentos que requerem
validação com dados reais. As ressalvas não são divergências documentais: são
limites verificáveis apenas com amostras de CDR, tipos físicos dos Parquets ou
convenções externas de layout.

## 2. Cobertura das colunas finais

| Coluna final | Status | Resultado da auditoria |
| :-- | :-- | :-- |
| `nu_referencia` | Confirmada | Alias de `referencia`, com cast string e sentinela de chave quando nula. |
| `nu_referencia_sip` | Confirmada | Alias de `referencia_sip`; origem ou ausência por fluxo estão documentadas. |
| `dh_referencia` | Confirmada | Alias timestamp de `data_hora_referencia`, com `try_to_timestamp`, limite e sentinela. |
| `dh_chamada` | Confirmada | Alias timestamp de `data_hora`; todos os cinco caminhos, inclusive nulo Ericsson/Vivo, estão corretos. |
| `dh_fim_chamada` | Confirmada | Alias timestamp de `data_hora_fim`; composição Algar e ausência Ericsson/Vivo estão corretas. |
| `qt_duracao_segundos` | Confirmada | Alias inteiro de `duracao`, incluindo parsing Ericsson, coalesce Nokia e fallback zero. |
| `nu_origem` | Confirmada | Saída formatada da UDF para todos os fluxos, com caminhos Huawei TIM explicitados. |
| `ic_origem_valido` | Confirmada | Flag booleana do struct retornado pela UDF de normalização. |
| `nu_origem_original` | Confirmada | Valor pós-pré-processamento; restauração só ocorre para colunas com prefixo `_`. |
| `nu_destino` | Confirmada | Saída formatada da UDF, inclusive regra FORW Nokia. |
| `ic_destino_valido` | Confirmada | Flag booleana do struct retornado pela UDF de normalização. |
| `nu_destino_original` | Confirmada | Valor pós-pré-processamento; ramos ATS/IBCF e FORW estão documentados. |
| `no_resultado_chamada` | Confirmada | Cópia ou classificação por layout; as faixas Huawei TIM, Nokia e mapeamentos estão descritos. |
| `co_resposta_sip` | Confirmada | Huawei TIM aplica limiar `>= 200`; demais fluxos criam nulo. |
| `no_autenticacao` | Confirmada | Classificação comum de `_autenticacao`; fontes Huawei TIM/Vivo e ausências estão corretas. |
| `no_prestadora` | Confirmada | Metadado do segmento `-3` de `input_file_name()`. |
| `no_rota_entrada` | Confirmada | Cópia por layout, seguida de preenchimento de chave quando nula. |
| `no_rota_saida` | Confirmada | Cópia por layout, seguida de preenchimento de chave quando nula. |
| `no_bilhetador` | Confirmada | Cópia por layout, seguida de preenchimento de chave quando nula. |
| `nu_cgi_origem` | Confirmada | Composição Ericsson/Nokia, decodificação hexadecimal Huawei/Vivo ou nulo nos demais. |
| `nu_cgi_origem_hex` | Confirmada | Cópia/extração Huawei e Vivo; nulo nos demais fluxos. |
| `nu_imei_origem` | Confirmada | Composição Ericsson, filtro e papel Huawei, remoção de hífen Vivo, cópia Nokia ou nulo Algar. |
| `nu_imsi_origem` | Confirmada | Composição Ericsson, JSON e papel Huawei, cópia Vivo/Nokia ou nulo Algar. |
| `nu_ip_origem` | Confirmada | Não há mapeamento ativo; é criada nula. |
| `nu_porta_ip_origem` | Confirmada | Não há mapeamento ativo; é criada nula e recebe cast inteiro. |
| `nu_cgi_destino` | Confirmada | Espelho condicionado da célula de origem nos fluxos que fornecem o dado. |
| `nu_cgi_destino_hex` | Confirmada | Cópia/extração Huawei e Vivo; nulo nos demais fluxos. |
| `nu_imei_destino` | Confirmada | Composição, papel, remoção de hífen, cópia ou nulo conforme o layout. |
| `nu_imsi_destino` | Confirmada | Composição, papel, cópia ou nulo conforme o layout. |
| `nu_ip_destino` | Confirmada | Não há mapeamento ativo; é criada nula. |
| `nu_porta_ip_destino` | Confirmada | Não há mapeamento ativo; é criada nula e recebe cast inteiro. |
| `no_agente_usuario` | Confirmada | `user-Agent-Value` somente no Huawei TIM; nulo nos demais. |
| `no_tipo_cdr` | Confirmada | Metadado do caminho, exceto Huawei TIM, que usa `recordType`. |
| `no_arquivo_origem` | Confirmada | Último segmento do caminho; o extrator texto aplica `url_decode`. |
| `no_tipo_chamada` | Confirmada | Cópia ou mapeamento de códigos por layout; também é coluna de partição. |

## 3. Divergências encontradas

Nenhuma divergência material foi encontrada entre a documentação auditada e o
código ativo.

Em especial, foram confirmadas afirmações que poderiam parecer inconsistentes,
mas refletem o comportamento real:

- Ericsson e LTE Ericsson Vivo extraem `_data`, `_hora` e `_hora_fim`, porém os
  respectivos transformadores não criam `data_hora` nem `data_hora_fim`; o
  pipeline comum cria essas colunas nulas e as normaliza para `MIN_SAFE_DATE`.
- O fluxo Algar Huawei monta `data_hora_fim` com `_data_fim` e `_hora`, e não
  com `_hora_fim`; a documentação não trata essa relação como hipótese.
- No Huawei TIM, `_numero_origem_ibcf` já contém o resultado numérico da
  primeira regex e é submetida novamente a uma regex que exige `sip:`. A
  documentação registra esse encadeamento literal e seu efeito estático.

## 4. Transformações omitidas

Não foram identificadas transformações omitidas na documentação auditada.

As seguintes operações foram conferidas e estão documentadas:

| Grupo | Operações verificadas | Resultado |
| :-- | :-- | :-- |
| Extração Parquet | `select`, `alias`, coluna ausente como literal nulo, metadados de caminho, `dropDuplicates` Huawei TIM | Documentadas. |
| Extração CSV | seleção posicional, aliases, `url_decode`, metadados de caminho | Documentadas. |
| Datas | `left`, `concat_ws`, `nullif`, `try_to_timestamp`, `greatest` | Documentadas. |
| Duração | `substring`, aritmética, `coalesce`, tratamento de `FFFFFF`, casts | Documentadas. |
| Números | UDF Pandas, struct temporário, restauração condicional de bruto | Documentadas. |
| Dispositivos | JSON, regex, `translate`, concatenação, padding, decodificação hexadecimal e atribuição por papel | Documentadas. |
| Status e autenticação | `when`, regex, faixas, códigos e fallbacks | Documentadas. |
| Contrato final | `select`, cast, alias, exclusão por projeção e particionamento | Documentadas. |

## 5. Funções auxiliares

| Função ou método | Localização | Status da documentação | Problema |
| :-- | :-- | :-- | :-- |
| `_null_if_blank` | `core/transformers/transformers.py` | Confirmada | Nenhum. |
| `_concat_or_null` | `core/transformers/transformers.py` | Confirmada | Nenhum. |
| `_build_composite_column` | `core/transformers/transformers.py` | Confirmada | Nenhum. |
| `_format_cell_id` | `core/transformers/transformers.py` | Confirmada | Nenhum. |
| `normalize_number` | `preprocessing/number_format.py` | Confirmada | Nenhum. |
| `spark_normalize_number` | `preprocessing/number_format.py` | Confirmada | Nenhum. |
| `CDRParquetExtractor.extract_cdr` | `core/extractors/parquet_extractors.py` | Confirmada | Nenhum. |
| `CDRTextExtractor.extract_cdr` | `core/extractors/text_extractors.py` | Confirmada | Nenhum. |
| `_fill_missing_columns` | `core/transformers/base_transformer.py` | Confirmada | Nenhum. |
| `_format_date_time` | `core/transformers/base_transformer.py` | Confirmada | Nenhum. |
| `_format_numbers` | `core/transformers/base_transformer.py` | Confirmada | Nenhum. |
| `_add_tn_validation_status` | `core/transformers/base_transformer.py` | Confirmada | Nenhum. |
| `_fill_primary_key_columns` | `core/transformers/base_transformer.py` | Confirmada | Nenhum. |
| `_apply_standard_pipeline` | `core/transformers/base_transformer.py` | Confirmada | Nenhum. |
| `_select_transformed_columns` | `core/transformers/base_transformer.py` | Confirmada | Nenhum. |
| `_write_parquet` | `core/transformers/base_transformer.py` | Confirmada | Nenhum. |

`normalize_number_pair` e as funções de CNPJ não foram classificadas como
auxiliares da linhagem: não há chamada delas nos cinco fluxos auditados.

## 6. Auditoria da etapa final

`_select_transformed_columns` percorre `TARGET_SCHEMA` e produz, para cada
entrada, `F.col(source_column).cast(data_type).alias(target_column)`. Portanto,
todas as 35 linhas abaixo são **cast + renomeação**, e não uma transformação de
conteúdo por si só. `_write_parquet` usa `mode("overwrite")` e
`partitionBy("no_tipo_chamada")`.

| Coluna anterior | Operação identificada | Coluna final | Documentação correta? |
| :-- | :-- | :-- | :-- |
| `referencia` | cast string + alias | `nu_referencia` | Sim |
| `referencia_sip` | cast string + alias | `nu_referencia_sip` | Sim |
| `data_hora_referencia` | cast timestamp_ntz + alias | `dh_referencia` | Sim |
| `data_hora` | cast timestamp_ntz + alias | `dh_chamada` | Sim |
| `data_hora_fim` | cast timestamp_ntz + alias | `dh_fim_chamada` | Sim |
| `duracao` | cast integer + alias | `qt_duracao_segundos` | Sim |
| `numero_origem_formatado` | cast string + alias | `nu_origem` | Sim |
| `numero_origem_valido` | cast boolean + alias | `ic_origem_valido` | Sim |
| `numero_origem` | cast string + alias | `nu_origem_original` | Sim |
| `numero_destino_formatado` | cast string + alias | `nu_destino` | Sim |
| `numero_destino_valido` | cast boolean + alias | `ic_destino_valido` | Sim |
| `numero_destino` | cast string + alias | `nu_destino_original` | Sim |
| `status_chamada` | cast string + alias | `no_resultado_chamada` | Sim |
| `codigo_resposta_sip` | cast string + alias | `co_resposta_sip` | Sim |
| `autenticacao` | cast string + alias | `no_autenticacao` | Sim |
| `prestadora` | cast string + alias | `no_prestadora` | Sim |
| `rota_entrada` | cast string + alias | `no_rota_entrada` | Sim |
| `rota_saida` | cast string + alias | `no_rota_saida` | Sim |
| `bilhetador` | cast string + alias | `no_bilhetador` | Sim |
| `celula_origem` | cast string + alias | `nu_cgi_origem` | Sim |
| `celula_origem_hex` | cast string + alias | `nu_cgi_origem_hex` | Sim |
| `imei_origem` | cast string + alias | `nu_imei_origem` | Sim |
| `imsi_origem` | cast string + alias | `nu_imsi_origem` | Sim |
| `ip_origem` | cast string + alias | `nu_ip_origem` | Sim |
| `porta_ip_origem` | cast integer + alias | `nu_porta_ip_origem` | Sim |
| `celula_destino` | cast string + alias | `nu_cgi_destino` | Sim |
| `celula_destino_hex` | cast string + alias | `nu_cgi_destino_hex` | Sim |
| `imei_destino` | cast string + alias | `nu_imei_destino` | Sim |
| `imsi_destino` | cast string + alias | `nu_imsi_destino` | Sim |
| `ip_destino` | cast string + alias | `nu_ip_destino` | Sim |
| `porta_ip_destino` | cast integer + alias | `nu_porta_ip_destino` | Sim |
| `agente_usuario` | cast string + alias | `no_agente_usuario` | Sim |
| `tipo_cdr` | cast string + alias | `no_tipo_cdr` | Sim |
| `arquivo_origem` | cast string + alias | `no_arquivo_origem` | Sim |
| `tipo_chamada` | cast string + alias + partição | `no_tipo_chamada` | Sim |

## 7. Múltiplos caminhos

| Elemento | Caminhos confirmados | Avaliação da documentação |
| :-- | :-- | :-- |
| `nu_origem` Huawei TIM | ATS com valor genérico; ATS sem valor genérico; demais tipos pelo caminho IBCF | Completa, incluindo a segunda regex incompatível com o valor já extraído. |
| `nu_destino` Huawei TIM | ATS por `substr(3, ...)`; demais tipos por regex SIP | Completa. |
| Dados de dispositivo Huawei TIM | Origem para `oRIGINATING-ROLE`; destino para `tERMINATING-ROLE`; nulo nos demais papéis | Completa. |
| `dh_chamada` Nokia | `data_hora_alocacao_canal`, com fallback em `data_hora_referencia` | Completa. |
| `dh_fim_chamada` Nokia | UCA usa `data_hora_desconexao` com fallback; demais usam `data_hora_fim` | Completa. |
| Duração Nokia | Todas as colunas iniciadas por `_duracao`, primeiro valor não nulo após excluir `FFFFFF` | Completa. |
| Números Nokia | Origem por `coalesce`; destino por `forwarding_number` em FORW | Completa. |
| Células Nokia | MNC 05 para Claro, 34 para Algar, nulo nos demais | Completa. |
| Células Huawei TIM/Vivo | Regras 3G, 4G, 5G ou preservação para outro comprimento | Completa. |

Não foram identificados caminhos condicionais ausentes ou simplificados de modo
incorreto no documento auditado.

## 8. Ambiguidades

| Elemento afetado | Motivo e ponto do rastreamento | Evidência disponível | Forma recomendada de documentar |
| :-- | :-- | :-- | :-- |
| Precedência Nokia | O código define a ordem de `coalesce`, mas não prova a semântica de negócio da prioridade. | `transform_cdr_nokia` contém as expressões determinísticas. | Manter a ordem implementada e declarar que a justificativa de negócio requer fonte externa. |
| CSV Algar Huawei | Os campos têm apenas posições: não há cabeçalho ou schema de origem. | `CDRTextSchema` usa `schema=None`, `has_header=False` e índices. | Referir-se às posições, sem inventar nomes anteriores à extração. |
| Status Nokia | O código compara `_status_chamada` a inteiros, mas não garante estaticamente o tipo físico do Parquet. | Não há cast explícito antes dos `when`. | Declarar o limite da análise estática e o tipo esperado pela expressão. |
| Metadados de caminho | A extração depende dos segmentos `-3`, `-2` e `-1` sem validar a estrutura. | `input_file_name`, `split` e `element_at` nos extratores. | Documentar os segmentos e a premissa de hierarquia. |
| Dados temporais Ericsson/Vivo | O código permite concluir o resultado nulo, mas não a intenção do layout. | Os campos são extraídos e não consumidos pelos transformadores específicos. | Distinguir fonte potencial de fonte efetiva; a documentação auditada já o faz. |

## 9. Problemas metodológicos

| Critério | Resultado |
| :-- | :-- |
| Associação apenas por semelhança de nomes | Não identificada. As relações foram declaradas a partir de schemas, aliases e expressões Spark. |
| Funções indiretas ignoradas | Não identificada. O documento alcança o pipeline base e a UDF Pandas. |
| Colunas intermediárias omitidas | Não identificada de forma material. Temporárias relevantes, como `_cell_id_hex`, `_imsi_parsed` e structs da UDF, foram consideradas. |
| Múltiplas origens simplificadas | Não identificada. Huawei TIM e Nokia possuem seções específicas de caminhos. |
| Condicionais ignoradas | Não identificada. Papéis Huawei, ATS/IBCF, UCA/FORW, prestadora Nokia e mapeamentos de status constam no documento. |
| Renomeação confundida com transformação | Não identificada. O documento separa pré-processamento de `cast + alias` final. |
| Colunas finais sem cobertura | Não identificada. Foram conferidas 35 de 35 por fluxo. |

## 10. Itens confirmados

- A documentação identifica corretamente os cinco fluxos ativos e exclui o
  módulo depreciado `robocalls`.
- A contagem e a ordem do contrato final coincidem com as 35 entradas de
  `TARGET_SCHEMA`.
- A documentação distingue extração, pré-processamento, pipeline comum e
  projeção final.
- `MIN_SAFE_DATE`, `NULL_SENTINEL_VALUE` e os preenchimentos de chave foram
  representados corretamente.
- A seleção final foi corretamente classificada como cast e renomeação, com
  exclusão implícita das demais colunas pela projeção.
- A escrita `overwrite` e o particionamento por `no_tipo_chamada` foram
  documentados corretamente.
- As fontes de metadados e a exceção Huawei TIM para `tipo_cdr` foram
  confirmadas.
- As funções de composição, decodificação, normalização e os métodos de base
  estão descritos com localização e impacto corretos.

## Checklist final

### Cobertura

- [x] Todas as colunas finais foram identificadas.
- [x] Todas foram comparadas com a documentação.
- [x] Todas as funções principais relevantes foram verificadas.
- [x] Todas as funções auxiliares relevantes foram verificadas.
- [x] A etapa final foi auditada.

### Linhagem

- [x] As origens foram verificadas.
- [x] As colunas extraídas foram verificadas.
- [x] As colunas intermediárias foram verificadas.
- [x] As transformações foram verificadas.
- [x] As renomeações foram verificadas.
- [x] Os casts relevantes foram verificados.
- [x] Os caminhos condicionais foram verificados.

### Documentação

- [x] Não há colunas finais sem classificação.
- [x] Não há relações assumidas apenas por nomes.
- [x] As ambiguidades foram identificadas.
- [x] Os múltiplos caminhos foram verificados.
- [x] As funções auxiliares comuns foram verificadas.
