# Relatório de Auditoria da Documentação de Linhagem de Dados

## 1. Resumo executivo

Esta auditoria partiu do código ativo dos extratores, catálogos, transformadores,
orquestrador e auxiliares, excluindo `robocalls`. Somente depois da reconstrução
do contrato e dos caminhos foi consultada a
[documentação auditada](teleutils_linhagem_transformacoes_dados.md). O resultado
analisado é o DataFrame projetado por `_select_transformed_columns` e gravado por
`_write_parquet`.

| Classificação por coluna final | Quantidade |
| :-- | --: |
| Total | 38 |
| Confirmadas | 28 |
| Parcialmente corretas | 8 |
| Incorretas | 2 |
| Não documentadas | 0 |
| Ambíguas como classificação documental | 0 |

**Resultado geral: Necessita correções.** Há contradições sobre a origem do
arquivo em dois layouts e uma afirmação incondicional incorreta sobre o acesso
a uma posição de array sob ANSI. Em Huawei VoLTE TIM, algumas tabelas omitem
que `_tipo_cdr` é copiada para `tipo_cdr` e removida antes da transformação.
Há ainda uma afirmação geral incorreta de que o cast final nunca altera conteúdo.
As classificações são por coluna e refletem o achado mais grave identificado;
não indicam que todos os ramos de uma coluna estejam errados.

A comparação estrutural automatizada confirmou 38 linhas, na ordem do contrato,
em cada uma das 17 tabelas, e conferiu a presença das posições textuais e dos
nomes de origem Parquet associados às colunas extraídas nomeadas. Essa checagem
não prova sozinha a semântica de todas as 646 linhas. As regras e caminhos abaixo
foram confrontados com o código; não foram executados todos os layouts com CDRs
reais. Os limites de dados, tipos e configurações constam na seção 8.

## 2. Cobertura das colunas finais

| Coluna final | Status | Resultado da auditoria |
| :-- | :-- | :-- |
| `nu_referencia` | Confirmada | `referencia`; SS8BF deriva de `record_id` por substring; chave. |
| `nu_referencia_sip` | Confirmada | `referencia_sip`, fornecida em Huawei, Vivo e PCL; chave. |
| `dh_referencia` | Confirmada | `data_hora_referencia`; máscara específica Italtel; em SS8BF deriva de `_referencia`. |
| `dh_chamada` | Confirmada | Início composto, selecionado ou recortado conforme layout, seguido de parsing e limite. |
| `dh_fim_chamada` | Confirmada | Término fornecido/composto ou calculado a partir do início e da duração. |
| `qt_duracao_segundos` | Confirmada | Ericsson converte HH:mm:ss; Nokia usa precedência; pipeline converte para inteiro. |
| `nu_origem` | Parcialmente correta | Regra Huawei correta em linhas gerais, mas tabela omite a cópia `_tipo_cdr` → `tipo_cdr`. |
| `ic_origem_valido` | Parcialmente correta | Indicador da UDF; mesma omissão da intermediária Huawei. |
| `nu_origem_original` | Parcialmente correta | Restauração Huawei/Vivo e precedência Nokia descritas; mesma omissão Huawei. |
| `nu_destino` | Parcialmente correta | Mesma omissão Huawei; tabela Nokia omite `record_type`/`tipo_chamada` como entrada da condição FORW. |
| `ic_destino_valido` | Parcialmente correta | Indicador da UDF; mesmas omissões Huawei e da condição Nokia. |
| `nu_destino_original` | Parcialmente correta | Restauração Huawei e valor pré-processado Nokia; mesmas omissões de entradas condicionais. |
| `no_resultado_chamada` | Parcialmente correta | Resultado por layout, inclusive descarte de `status_chamada` Ericsson; omissão Huawei de `tipo_cdr`. |
| `co_resposta_sip` | Parcialmente correta | Limiar Huawei e demais cópias identificados; omissão Huawei de `tipo_cdr` e ressalva sobre cast final. |
| `no_autenticacao` | Incorreta | Regra de prefixo correta; afirmação incondicional sobre ausência de `;` em Vivo falha com ANSI habilitado. |
| `no_prestadora` | Confirmada | Argumento `operator` ou segmento `-4`, decodificado. |
| `no_rota_entrada` | Confirmada | Fonte por layout e preenchimento de chave. |
| `no_rota_saida` | Confirmada | Fonte por layout e preenchimento de chave. |
| `no_bilhetador` | Confirmada | Fonte por layout e preenchimento de chave. |
| `nu_cgi_origem` | Confirmada | Composição Ericsson/Nokia ou decodificação Huawei/Vivo, por papel quando aplicável. |
| `nu_cgi_origem_hex` | Confirmada | Informação de rede Huawei ou campo hexadecimal Vivo. |
| `no_tecnologia_celula_origem` | Confirmada | Primeiro trecho da informação de rede Huawei/Vivo, condicionado pelo papel Huawei. |
| `nu_imei_origem` | Confirmada | Componentes Ericsson, cópia Nokia, filtro Huawei ou remoção de hífen Vivo. |
| `nu_imsi_origem` | Confirmada | Componentes Ericsson, cópia Nokia/Vivo ou JSON e papel Huawei. |
| `nu_ip_origem` | Confirmada | Sem fonte nos catálogos padrão; nulo no pipeline comum. |
| `nu_porta_ip_origem` | Confirmada | Sem fonte nos catálogos padrão; nulo e cast inteiro. |
| `nu_cgi_destino` | Confirmada | Composição ou decodificação por layout, inclusive papel Huawei. |
| `nu_cgi_destino_hex` | Confirmada | Informação de rede Huawei ou campo hexadecimal Vivo. |
| `no_tecnologia_celula_destino` | Confirmada | Primeiro trecho da informação de rede, condicionado pelo papel Huawei. |
| `nu_imei_destino` | Confirmada | Composição, cópia, filtro por papel ou remoção de hífen, conforme layout. |
| `nu_imsi_destino` | Confirmada | Composição, cópia ou JSON e papel, conforme layout. |
| `nu_ip_destino` | Confirmada | Sem fonte nos catálogos padrão; nulo no pipeline comum. |
| `nu_porta_ip_destino` | Confirmada | Sem fonte nos catálogos padrão; nulo e cast inteiro. |
| `no_agente_usuario` | Confirmada | `user-Agent-Value` no Huawei VoLTE TIM; sem fonte nos demais catálogos padrão. |
| `no_esquema` | Confirmada | Literal `cdr_schema` acrescentado pelo extrator. |
| `no_tipo_cdr` | Confirmada | `_tipo_cdr` prevalece quando existe; caso contrário argumento ou segmento `-2`. |
| `no_arquivo_origem` | Incorreta | Tabelas Vivo e Sigma apontam caminho/parâmetro, embora as fontes sejam `source_filename` e `source_file`. |
| `no_tipo_chamada` | Confirmada | Cópia, classificação ou constante por layout; sentinela quando ausente; partição. |

**Critério:** confirmação refere-se às relações examinadas no código e no
documento, não a uma garantia de que todos os possíveis dados e configurações
de execução tenham sido exercitados. A ressalva geral sobre casts da seção 3.4
é transversal e não torna automaticamente incorretas as 38 linhagens.

## 3. Divergências encontradas

### 3.1 `no_arquivo_origem`

**Classificação:** incorreta nas tabelas dos dois fluxos citados; a regra geral
e as observações detalhadas do documento registram a exceção corretamente.

**O que a documentação afirma:** as tabelas de `smp_ericsson_volte_vivo` e
`stfc_sigma_claro`, respectivamente nas linhas 477 e 609 da documentação
auditada, descrevem a coluna original como “metadado (caminho do arquivo ou
parâmetro do extrator)”.

**O que o código identifica:** os catálogos mapeiam `source_filename` e
`source_file`, respectivamente, para `arquivo_origem`. No Sigma, o extrator
também remove hífens finais. Caso o campo original não exista, o extrator
Parquet cria a coluna mapeada nula; como ela já existe, `add_source_metadata`
não infere o nome pelo caminho. Não há parâmetro do extrator para substituir
`arquivo_origem`. Evidências: [catálogo Parquet](../src/teleutils/core/extractors/schemas/parquet.py),
[seleção Parquet](../src/teleutils/core/extractors/parquet_extractors.py) e
[metadados](../src/teleutils/core/extractors/_extractors_utils.py).

```text
Vivo: source_filename → seleção/alias → arquivo_origem
Sigma: source_file → regexp_replace('-+$', '')/alias → arquivo_origem
→ preservação no transformador → cast string + alias → no_arquivo_origem
```

**Problema:** a coluna “originais” contradiz a regra efetiva e as próprias
notas do documento, atribuindo uma origem de caminho que não é usada nesses
dois layouts.

**Correção recomendada:** identificar os campos originais nas duas linhas,
registrar a limpeza Sigma e manter a ressalva de nulo quando o campo de origem
estiver ausente. Nos demais layouts padrão, manter a inferência do último
segmento apenas quando a coluna não existir após a seleção.

### 3.2 `no_autenticacao`

**Classificação:** incorreta na afirmação incondicional sobre o caso sem
marcador; a classificação de prefixos está correta.

**O que a documentação afirma:** na seção de Ericsson VoLTE Vivo, “quando
`_numero_origem_original` não contém `;`, `split(...)[1]` é nulo e
`no_autenticacao` fica `NULL`” (linha 483).

**O que o código identifica:** `getItem(1)` é usado sem teste de comprimento
em [transformers.py](../src/teleutils/core/transformers/transformers.py);
`_add_tn_validation_status` classifica a saída em
[base_transformer.py](../src/teleutils/core/transformers/base_transformer.py).
Em teste sintético com PySpark 3.5.5 e `split('5511', ';').getItem(1)`,
ANSI `false` retornou `None`; ANSI `true` falhou com
`INVALID_ARRAY_INDEX`. O teste avaliou a expressão, não um CDR completo.

```text
callingPartyNumber → _numero_origem_original
→ split(';').getItem(1) → _autenticacao
→ classificação por prefixo → autenticacao → cast/alias → no_autenticacao
```

**Problema:** sem qualificação por configuração, a documentação prevê um
resultado nulo mesmo em uma execução que pode falhar antes da escrita. A
limitação de ANSI sobre casts documentada em outra seção não explica este
acesso ao array. Há também a omissão intermediária Huawei da seção seguinte.

**Correção recomendada:** distinguir o resultado em ANSI desabilitado da
possibilidade de erro em ANSI habilitado, identificando versão e expressão
testadas; não generalizar o comportamento para todas as versões Spark.

### 3.3 Números, indicador, resultado e resposta SIP em Huawei VoLTE TIM

**Classificação:** parcialmente correta para `nu_origem`, `ic_origem_valido`,
`nu_origem_original`, `nu_destino`, `ic_destino_valido`,
`nu_destino_original`, `no_resultado_chamada` e `co_resposta_sip`.
`no_autenticacao` também é afetada, mas já recebe classificação incorreta pela
seção 3.2.

**O que a documentação afirma:** nas linhas 373–381 e em tabelas da seção 4,
`_tipo_cdr` aparece em “Colunas Extraídas”, embora a convenção do documento
defina essas colunas como presentes no DataFrame entregue após a extração.
As expressões de tratamento usam `tipo_cdr` e o resumo geral reconhece a
remoção, mas essas linhas não mostram a transição.

**O que o código identifica:** `recordType` é selecionada como `_tipo_cdr`;
`add_source_metadata` copia seu valor para `tipo_cdr`, inclusive quando nulo;
`extract_dataframe` remove `_tipo_cdr`. O transformador compara
`tipo_cdr` com `aTSRecord` e `iBCFRecord`, nunca `_tipo_cdr`.
Evidências: [catálogo](../src/teleutils/core/extractors/schemas/parquet.py),
[extrator](../src/teleutils/core/extractors/parquet_extractors.py),
[metadados](../src/teleutils/core/extractors/_extractors_utils.py) e
[transformador](../src/teleutils/core/transformers/transformers.py).

```text
recordType → _tipo_cdr (seleção do extrator)
→ tipo_cdr (cópia em add_source_metadata; drop de _tipo_cdr)
→ condições ATS/IBCF + campos de número, autenticação ou resultado
→ UDF/classificação/cast → respectivas colunas finais
```

**Problema:** a origem de controle está identificada, mas a tabela pode sugerir
que o transformador recebe ou consulta uma coluna que já foi descartada.

**Correção recomendada:** mostrar `_tipo_cdr → tipo_cdr` explicitamente nas
linhas afetadas e nas tabelas de múltiplos caminhos, distinguindo seleção
interna do extrator de saída efetiva. Não substituir o nome na coluna original.

### 3.4 `nu_destino`, `ic_destino_valido` e `nu_destino_original` em Nokia

**Classificação:** parcialmente correta; a condição FORW está corretamente
escrita na transformação.

**O que a documentação afirma:** as linhas Nokia de destino listam
`called_number` e `forwarding_number` como origens e os correspondentes
`numero_destino` e `numero_origem_encaminhamento` como colunas extraídas,
mas não listam a origem da condição.

**O que o código identifica:** `record_type → tipo_chamada` controla o ramo
`tipo_chamada == 'FORW'`. O resultado é usado pela UDF e, sem restauração
`_numero_destino_original`, também alimenta a coluna original final.

```text
record_type → tipo_chamada ─┐
called_number → numero_destino ├→ when(FORW, numero_origem_encaminhamento)
forwarding_number → numero_origem_encaminhamento ┘
→ numero_destino → UDF e/ou preenchimento de chave → três colunas finais
```

**Problema:** o tratamento textual informa a condição, mas as células de
origem omitem uma dependência necessária à escolha do ramo.

**Correção recomendada:** acrescentar `record_type` e `tipo_chamada` às
origens das três linhas, preservando as outras duas entradas.

### 3.5 Cast final e afirmação transversal

**Classificação:** afirmação geral incorreta; não é uma divergência dos
pares de nomes ou tipos da tabela final.

**O que a documentação afirma:** na linha 1647, todas as operações finais
seriam cast e alias, mas “o conteúdo não é alterado nesta etapa”.

**O que o código identifica:** o cast é uma conversão real, não apenas uma
renomeação. Por exemplo, no Huawei VoLTE TIM o resultado numérico
`codigo_resposta_sip` é convertido para string pelo contrato; o alias apenas
muda seu nome.

```text
serviceReasonReturnCode → _resultado_chamada → cast inteiro
→ valor >= 200 em codigo_resposta_sip → cast string → co_resposta_sip
```

**Problema:** o documento identifica o cast mas o descreve como incapaz de
modificar tipo ou representação; outros casts podem ainda produzir nulo ou
erro conforme dados e configuração.

**Correção recomendada:** separar conversão por `cast` de renomeação por
`alias` e evitar garantir preservação universal de valor ou representação.

## 4. Transformações omitidas ou incompletas

| Grupo | Resultado |
| :-- | :-- |
| Extração de metadados | As linhas Vivo/Sigma omitem as fontes nominais de `arquivo_origem`; a regra geral existe. |
| Cópia e descarte | As linhas Huawei afetadas não explicitam `_tipo_cdr → tipo_cdr` seguido de `drop`, embora o fluxo resumido o faça. |
| Condicional | As linhas Nokia de destino não listam a coluna que determina FORW. |
| Acesso a array | A descrição Vivo omite o erro possível de `getItem(1)` sob ANSI. |
| Cast | Os 38 casts estão listados, mas o texto geral nega que possam alterar o conteúdo/representação. |
| Parsing, concatenação, limpeza e nulos | As regras principais de `_concat_date_time`, `concat_ws` Sigma, datas, duração, UDF e sentinelas foram identificadas; limitações de execução na seção 8. |

## 5. Funções auxiliares

| Função ou grupo | Localização | Status da documentação | Problema |
| :-- | :-- | :-- | :-- |
| `resolve_cdr_schema` | `core/extractors/schemas/_schema_utils.py` | Confirmada | Nenhum identificado. |
| `extract_dataframe`/`extract` Parquet e texto | `core/extractors/parquet_extractors.py`, `text_extractors.py` | Parcial | Tabelas específicas contradizem ou omitem efeitos do catálogo e do `drop` descritos nos textos gerais. |
| `read_text_with_source` | `core/extractors/_extractors_utils.py` | Confirmada | Nenhum identificado. |
| `add_source_metadata`, `validate_metadata_arguments` | `core/extractors/_extractors_utils.py` | Confirmada na regra geral | Corrigir sua aplicação nas tabelas Vivo/Sigma e Huawei. |
| `_null_if_blank`, `_concat_or_null`, `_build_composite_column`, `_concat_date_time` | `core/transformers/transformers.py` | Confirmada | Nenhum identificado nas regras comuns verificadas. |
| `_extract_cell_info`, `_format_cell_id` | `core/transformers/transformers.py` | Confirmada | Nenhum identificado nas regras principais; dados reais não validados. |
| `_check_output_format` | `core/transformers/transformers.py` | Confirmada | `tim` é rejeitado antes do ramo Ericsson GSM; o orquestrador usa padrão. |
| `_extract_numbers`, `extract_number`, `_extract_number`, `_search_number`, `_is_rejected_number` | `core/transformers/base_transformer.py`, `preprocessing/phone_number_extractor.py` | Confirmada | Representação de nulos recebidos pela pandas UDF depende da execução. |
| `_fill_missing_columns`, `_format_date_time`, `_extract_phone_numbers`, `_add_tn_validation_status`, `_fill_primary_key_columns`, `_apply_standard_pipeline` | `core/transformers/base_transformer.py` | Confirmada na regra geral | A exceção ANSI de `split` ocorre antes da regra de autenticação. |
| `_select_transformed_columns`, `_write_parquet` | `core/transformers/base_transformer.py` | Parcial | Pares e tipos corretos; afirmação sobre cast não alterar conteúdo é excessiva. |
| `CDRTransformer.transform` | `core/transformers/transformers.py` | Confirmada | Procura funções globais; o orquestrador resolve métodos da instância por `getattr`. |

Não foram identificadas, entre as auxiliares acima, funções documentadas com
localização incorreta ou apresentadas como chamadas do fluxo apesar de não
utilizadas. Isso não constitui inventário exaustivo de funções fora do pipeline.

## 6. Auditoria da etapa final

Em [base_transformer.py](../src/teleutils/core/transformers/base_transformer.py),
`_select_transformed_columns` percorre
[`TARGET_SCHEMA`](../src/teleutils/_config.py), executa o cast indicado e aplica
o alias. A seleção exclui as demais colunas; `_write_parquet` valida a sessão,
usa `overwrite` e particiona por `no_tipo_chamada`. Os casts podem ser
transformações; apenas o alias é renomeação. “Sim” abaixo confirma par e tipo,
não a afirmação geral incorreta sobre preservação de conteúdo.

| Coluna anterior | Operação identificada | Coluna final | Documentação correta? |
| :-- | :-- | :-- | :-- |
| `referencia` | cast string + alias | `nu_referencia` | Sim |
| `referencia_sip` | cast string + alias | `nu_referencia_sip` | Sim |
| `data_hora_referencia` | cast timestamp_ntz + alias | `dh_referencia` | Sim |
| `data_hora` | cast timestamp_ntz + alias | `dh_chamada` | Sim |
| `data_hora_fim` | cast timestamp_ntz + alias | `dh_fim_chamada` | Sim |
| `duracao` | cast integer + alias | `qt_duracao_segundos` | Sim |
| `numero_origem_extraido` | cast string + alias | `nu_origem` | Sim |
| `sucesso_extracao_origem` | cast boolean + alias | `ic_origem_valido` | Sim |
| `numero_origem` | cast string + alias | `nu_origem_original` | Sim |
| `numero_destino_extraido` | cast string + alias | `nu_destino` | Sim |
| `sucesso_extracao_destino` | cast boolean + alias | `ic_destino_valido` | Sim |
| `numero_destino` | cast string + alias | `nu_destino_original` | Sim |
| `resultado_chamada` | cast string + alias | `no_resultado_chamada` | Sim |
| `codigo_resposta_sip` | cast string + alias | `co_resposta_sip` | Sim |
| `autenticacao` | cast string + alias | `no_autenticacao` | Sim |
| `prestadora` | cast string + alias | `no_prestadora` | Sim |
| `rota_entrada` | cast string + alias | `no_rota_entrada` | Sim |
| `rota_saida` | cast string + alias | `no_rota_saida` | Sim |
| `bilhetador` | cast string + alias | `no_bilhetador` | Sim |
| `celula_origem` | cast string + alias | `nu_cgi_origem` | Sim |
| `celula_origem_hex` | cast string + alias | `nu_cgi_origem_hex` | Sim |
| `tecnologia_celula_origem` | cast string + alias | `no_tecnologia_celula_origem` | Sim |
| `imei_origem` | cast string + alias | `nu_imei_origem` | Sim |
| `imsi_origem` | cast string + alias | `nu_imsi_origem` | Sim |
| `ip_origem` | cast string + alias | `nu_ip_origem` | Sim |
| `porta_ip_origem` | cast integer + alias | `nu_porta_ip_origem` | Sim |
| `celula_destino` | cast string + alias | `nu_cgi_destino` | Sim |
| `celula_destino_hex` | cast string + alias | `nu_cgi_destino_hex` | Sim |
| `tecnologia_celula_destino` | cast string + alias | `no_tecnologia_celula_destino` | Sim |
| `imei_destino` | cast string + alias | `nu_imei_destino` | Sim |
| `imsi_destino` | cast string + alias | `nu_imsi_destino` | Sim |
| `ip_destino` | cast string + alias | `nu_ip_destino` | Sim |
| `porta_ip_destino` | cast integer + alias | `nu_porta_ip_destino` | Sim |
| `agente_usuario` | cast string + alias | `no_agente_usuario` | Sim |
| `esquema` | cast string + alias | `no_esquema` | Sim |
| `tipo_cdr` | cast string + alias | `no_tipo_cdr` | Sim |
| `arquivo_origem` | cast string + alias | `no_arquivo_origem` | Sim |
| `tipo_chamada` | cast string + alias; particionamento | `no_tipo_chamada` | Sim |

## 7. Múltiplos caminhos

| Caminho ou condição | Resultado da comparação |
| :-- | :-- |
| Materializado | Confirmado: extração na sessão do chamador, escrita intermediária e leitura na nova sessão NTZ do transformador. |
| Direto | Confirmado: extrator e transformador usam `transformer.spark`; não há escrita intermediária; configurações SQL de runtime do chamador não são necessariamente copiadas. `add_source_metadata` pode executar `collect` antes da escrita final. |
| Huawei ATS/IBCF | Condições de número, autenticação e resultado descritas; a cópia e o descarte de `_tipo_cdr` faltam nas linhas afetadas. |
| Huawei papel de nó | Origem/destino de célula, tecnologia e dispositivo dependem de `oRIGINATING-ROLE`/`tERMINATING-ROLE`, como documentado. |
| Nokia | `coalesce` de duração e início, UCA para término, FORW para destino, MNC por prestadora; dependência de `record_type` omitida nas células de origem do destino. |
| Ericsson Vivo | Split de número/autenticação e restauração documentados, mas a consequência de índice ausente sob ANSI está incorreta. |
| Trópico Claro LEGA/LEGB | Seleções posicionais distintas, mesmos aliases de método transformador: confirmado. |
| Sigma | `fill_char` e `concat_ws` que ignora nulos documentados; a linha da origem de arquivo está incorreta. |

Não se identificou ausência de um dos dois caminhos arquiteturais na
documentação. A equivalência de resultados entre sessões não é presumida.

## 8. Ambiguidades e limites

| Elemento | Ponto de interrupção, evidência e recomendação |
| :-- | :-- |
| Tipo de `cause_for_termination` Nokia | O catálogo não define o tipo físico Parquet; comparações numéricas estão em `transform_smp_nokia_gsm`. Registrar a expressão e validar dados reais, sem inventar interpretação hexadecimal. |
| Semântica das precedências Nokia | A ordem de `coalesce` e os ramos FORW/UCA estão no código, mas não sua justificativa de negócio. Registrar a ordem sem presumir intenção. |
| Nomes semânticos dos layouts posicionais | Nos catálogos sem cabeçalho há apenas índices ou recortes; a interpretação de fornecedor requer fonte externa. Manter posição e largura em vez de inventar nomes. PIT/SS8BF possuem nomes de `StructType`. |
| Formato real dos campos Huawei | JSON, URI e valores de `recordType` são tratados por expressões e literais; sua distribuição real não está no código. Apresentar os ramos, não garantir conformidade dos dados. |
| Nulos Spark → Arrow → pandas | `extract_number` trata entrada falsy com sentinela, mas a representação de nulo na série depende da execução e dos tipos. Não garantir um valor por inferência estática. |
| Casts e sessões | Valores inválidos e coerções podem depender de ANSI e da versão Spark; o manifesto não fixa um teto de versão. Documentar configuração e testar amostras pertinentes. A diferença ANSI de `getItem` foi verificada apenas para a expressão e versão indicadas. |
| Caminho de metadados | A estrutura de diretórios é inferida por segmentos e validada apenas quanto a nulo/vazio; a semântica da hierarquia real não pode ser verificada estaticamente. |
| CSV de largura fixa AXE/PCL | `spark.read.csv` com `schema='value string'` precede `substring`; o efeito sobre linhas reais com delimitadores depende de leitura concreta. Não afirmar equivalência com leitura integral por RDD sem teste. |
| Arquivo Vivo/Sigma | O catálogo e o fallback nulo são conhecidos; não é possível determinar estaticamente se os Parquets reais contêm `source_filename`/`source_file`. |

## 9. Problemas metodológicos

| Critério | Avaliação |
| :-- | :-- |
| Associação apenas por nomes | Não identificada nos casos examinados; as origens incorretas de arquivo foram verificadas contra os mapeamentos, sem presumir equivalência. |
| Auxiliares indiretas | Pipeline comum e funções de número, composição e célula estão descritos; nenhuma omissão material identificada entre os helpers examinados. |
| Intermediárias | `_tipo_cdr → tipo_cdr` aparece no resumo, mas não nas linhas dependentes; isso obscurece o ponto de descarte. |
| Múltiplas origens e condicionais | Nokia FORW omite a origem do seletor nas células; acesso de array Vivo ignora configuração ANSI. |
| Transformação versus renomeação | O documento lista casts e aliases, mas afirma incorretamente que a operação completa nunca altera conteúdo. |
| Cobertura final | As 38 colunas e a ordem aparecem nas 17 tabelas; cobertura estrutural não equivale a prova de correção de cada regra. |

## 10. Itens confirmados

- Os catálogos padrão contêm seis chaves Parquet e onze de texto; os dois
  aliases Trópico Claro resolvem o mesmo método.
- `_fill_missing_columns` cria nulos, `_format_date_time` normaliza datas e
  duração, `_extract_phone_numbers` expande a UDF, a autenticação é derivada e
  `_fill_primary_key_columns` preenche as 15 chaves antes da projeção.
- Em Ericsson GSM, `callPosition → status_chamada` não alimenta
  `resultado_chamada`; a documentação registra a consequência, sem afirmar que
  isso seja a intenção de negócio.
- Na entrada Nokia, duração usa colunas `_duracao*` na ordem do DataFrame,
  convertendo `FFFFFF` em nulo antes de `coalesce`.
- O fim ausente é calculado com início normalizado e duração; um fim não nulo
  inválido segue a normalização, não o mesmo fallback.
- A projeção contém 38 pares e tipos corretos, elimina colunas fora do contrato
  e a escrita usa `overwrite` e partição por `no_tipo_chamada`.

## Checklist final

### Cobertura

- [x] Todas as colunas finais foram identificadas.
- [x] Todas foram comparadas com a documentação e classificadas.
- [x] As funções principais relevantes dos 17 layouts foram verificadas.
- [x] As auxiliares comuns relevantes foram verificadas.
- [x] A etapa final foi auditada.

### Linhagem

- [x] As origens nominais e posições de catálogo foram confrontadas com as tabelas.
- [x] Extração, intermediárias, transformações, renomeações e casts foram examinados.
- [x] Caminhos condicionais e os dois modos de execução foram examinados.
- [ ] Todos os ramos foram validados com dados reais e configurações possíveis.

### Documentação

- [x] Não há colunas finais sem classificação.
- [x] As divergências identificadas não foram inferidas apenas por nomes.
- [x] Ambiguidades e limites foram registrados.
- [x] Múltiplos caminhos e auxiliares comuns foram verificados.
- [ ] Todas as afirmações detalhadas das 646 linhas foram comprovadas em execução.
