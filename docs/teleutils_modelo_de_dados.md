# Modelo de dados dos CDRs

Este dicionário apresenta as colunas selecionadas pelos transformadores para o
Parquet final, na ordem do contrato definido em [`TARGET_SCHEMA`](../src/teleutils/_config.py).

Cada linha representa um registro CDR, não necessariamente uma chamada inteira. A disponibilidade dos valores depende do layout; campos ausentes podem receber valores nulos ou padrões do pipeline.

| Coluna final | Descrição |
| :-- | :-- |
| `nu_referencia` | Referência do registro CDR fornecida pela origem. |
| `nu_referencia_sip` | Referência adicional do CDR, incluindo o identificador de tarifação IMS em alguns layouts. |
| `dh_referencia` | Data e hora de referência do registro, quando disponíveis. |
| `dh_chamada` | Data e hora de início do segmento da chamada. |
| `dh_fim_chamada` | Data e hora de fim do segmento da chamada, informadas no CDR ou calculadas a partir do início e da duração. |
| `qt_duracao_segundos` | Duração do segmento da chamada em segundos. |
| `nu_origem` | Número de origem retornado pela extração de número telefônico. |
| `ic_origem_valido` | Indica se a extração do número de origem foi bem-sucedida; não confirma a existência do número. |
| `nu_origem_original` | Valor de origem antes da extração do número, ou valor original fornecido pelo pré-processamento. |
| `nu_destino` | Número de destino retornado pela extração de número telefônico. |
| `ic_destino_valido` | Indica se a extração do número de destino foi bem-sucedida; não confirma a existência do número. |
| `nu_destino_original` | Valor de destino antes da extração do número, ou valor original fornecido pelo pré-processamento. |
| `no_resultado_chamada` | Resultado ou condição de encerramento do segmento da chamada, conforme o layout. |
| `co_resposta_sip` | Código de resposta SIP registrado ou derivado pelo transformador. |
| `no_autenticacao` | Estado textual da validação de autenticação do número na sinalização. |
| `no_prestadora` | Prestadora associada ao processamento do CDR. |
| `no_rota_entrada` | Rota de entrada associada ao segmento da chamada. |
| `no_rota_saida` | Rota de saída associada ao segmento da chamada. |
| `no_bilhetador` | Identificação do bilhetador associada ao registro. |
| `nu_cgi_origem` | Identificador de célula associado à origem. |
| `nu_cgi_origem_hex` | Identificador de célula de origem em representação hexadecimal. |
| `no_tecnologia_celula_origem` | Tecnologia de acesso da célula associada à origem. |
| `nu_imei_origem` | Identificador IMEI associado à origem. |
| `nu_imsi_origem` | Identificador IMSI associado à origem. |
| `nu_ip_origem` | Não implementado: reservado para implementação futura. |
| `nu_porta_ip_origem` | Não implementado: reservado para implementação futura. |
| `nu_cgi_destino` | Identificador de célula associado ao destino. |
| `nu_cgi_destino_hex` | Identificador de célula de destino em representação hexadecimal. |
| `no_tecnologia_celula_destino` | Tecnologia de acesso da célula associada ao destino. |
| `nu_imei_destino` | Identificador IMEI associado ao destino. |
| `nu_imsi_destino` | Identificador IMSI associado ao destino. |
| `nu_ip_destino` | Não implementado: reservado para implementação futura. |
| `nu_porta_ip_destino` | Não implementado: reservado para implementação futura. |
| `no_agente_usuario` | Valor do agente de usuário informado no CDR. |
| `no_esquema` | Chave do esquema de extração usado para processar o CDR. |
| `no_tipo_cdr` | Tipo de CDR associado ao registro. |
| `no_arquivo_origem` | Nome do arquivo de origem associado ao registro. |
| `no_tipo_chamada` | Tipo do segmento da chamada associado ao registro; também é usado para particionar o Parquet final. |

> **Segmento de chamada**: uma parte individualizada da chamada, representada por um registro CDR, quando uma única chamada pode gerar múltiplos registros referentes a partes distintas da comunicação.

Para detalhes sobre origem, transformações, valores padrão e particularidades
dos layouts, consulte a [documentação de linhagem](teleutils_linhagem_transformacoes_dados.md).
