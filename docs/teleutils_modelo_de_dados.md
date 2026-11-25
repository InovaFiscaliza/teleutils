# Modelo de dados dos CDRs

Este dicionário descreve as colunas do Parquet final, na ordem do contrato definido em
[`TARGET_SCHEMA`](../src/teleutils/_config.py). Para saber como cada coluna é obtida
(origem, transformações, valores padrão e particularidades de cada layout), consulte a
[documentação de linhagem](teleutils_linhagem_transformacoes_dados.md).

## Convenções

- **Registro e segmento de chamada:** cada linha representa um registro CDR. Uma chamada
  pode gerar vários registros, cada um correspondendo a uma parte (segmento) da comunicação.
- **Origem e destino:** colunas com sufixo `_origem` referem-se a quem originou a chamada;
  colunas com sufixo `_destino`, a quem a recebeu.
- **Datas e horas:** as colunas de data e hora (`dh_referencia`, `dh_chamada` e
  `dh_fim_chamada`) não contêm informação de fuso horário.
- **Valores ausentes:** nem todos os layouts fornecem todos os campos. Quando o dado não
  existe na origem, a coluna pode ficar nula ou receber um valor padrão do pipeline.

## Colunas

| Coluna final | Descrição |
| :-- | :-- |
| `co_referencia` | Identificador do registro CDR atribuído pelo sistema de origem. |
| `co_referencia_sip` | Identificador complementar do registro, usado em layouts que o fornecem, como o identificador de tarifação dos CDRs IMS. |
| `dh_referencia` | Data e hora de referência do registro, quando informadas pela origem. |
| `dh_chamada` | Data e hora de início do segmento da chamada. |
| `dh_fim_chamada` | Data e hora de término do segmento da chamada, informadas pela origem ou calculadas a partir do início e da duração. |
| `qt_duracao_segundos` | Duração do segmento da chamada, em segundos. |
| `co_numero_origem` | Número de telefone de origem, em formato padronizado. |
| `ic_numero_origem_valido` | Indica se foi possível reconhecer e padronizar o número de origem. Não confirma que o número existe. |
| `co_numero_origem_original` | Número de origem como recebido da fonte, antes da padronização. Em alguns layouts, reflete o valor após ajustes preliminares. |
| `co_numero_destino` | Número de telefone de destino, em formato padronizado. |
| `ic_numero_destino_valido` | Indica se foi possível reconhecer e padronizar o número de destino. Não confirma que o número existe. |
| `co_numero_destino_original` | Número de destino como recebido da fonte, antes da padronização. Em alguns layouts, reflete o valor após ajustes preliminares. |
| `de_resultado_chamada` | Resultado ou motivo de encerramento do segmento da chamada, conforme o layout. |
| `co_resposta_sip` | Código de resposta SIP associado ao segmento da chamada, quando disponível. |
| `no_autenticacao` | Resultado da verificação de autenticação do número de origem na sinalização, quando disponível. |
| `no_prestadora_cdr` | Prestadora que forneceu o arquivo de CDR. |
| `co_rota_entrada` | Rota pela qual a chamada entrou na rede. |
| `co_rota_saida` | Rota pela qual a chamada saiu da rede. |
| `co_bilhetador` | Identificação do bilhetador que gerou o registro. |
| `co_cgi_origem` | Identificador da célula (CGI) em que a origem estava conectada. |
| `co_cgi_origem_hex` | Identificador da célula de origem em formato hexadecimal. |
| `no_tecnologia_celula_origem` | Tecnologia de acesso da célula de origem, conforme informada pelo layout. |
| `co_imei_origem` | IMEI do equipamento de origem. |
| `co_imsi_origem` | IMSI do assinante de origem. |
| `ed_ip_origem` | Não implementado: reservado para uso futuro. |
| `nu_porta_ip_origem` | Não implementado: reservado para uso futuro. |
| `co_cgi_destino` | Equivalente a `co_cgi_origem`, para o destino. |
| `co_cgi_destino_hex` | Equivalente a `co_cgi_origem_hex`, para o destino. |
| `no_tecnologia_celula_destino` | Equivalente a `no_tecnologia_celula_origem`, para o destino. |
| `co_imei_destino` | Equivalente a `co_imei_origem`, para o destino. |
| `co_imsi_destino` | Equivalente a `co_imsi_origem`, para o destino. |
| `ed_ip_destino` | Não implementado: reservado para uso futuro. |
| `nu_porta_ip_destino` | Não implementado: reservado para uso futuro. |
| `de_agente_usuario` | Identificação do software ou equipamento do usuário (*User-Agent*), quando registrada no CDR. |
| `co_esquema_extracao` | Identificador do layout de CDR usado na extração (fabricante e tecnologia). |
| `no_tipo_cdr` | Categoria do CDR definida pelo arquivo ou layout de origem. Difere de `no_tipo_chamada`, que classifica o segmento da chamada. |
| `no_arquivo_origem` | Nome do arquivo de CDR do qual o registro foi extraído. |
| `no_tipo_chamada` | Classificação do segmento da chamada, conforme o layout. Também define o particionamento do Parquet final. |
