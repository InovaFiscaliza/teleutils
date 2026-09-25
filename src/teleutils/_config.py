"""Módulo de configuração compartilhada do pacote teleutils.

Este módulo centraliza constantes utilizadas por diferentes componentes do
projeto para manter consistência entre etapas de extração, classificação e
persistência de dados de telecomunicações.

Responsabilidades:
        - Definir marcadores textuais compartilhados entre regras de negócio.
        - Consolidar limites operacionais usados em rotinas de leitura e escrita.
        - Evitar duplicação de valores sensíveis a manutenção em múltiplos módulos.

Principais funcionalidades:
        - Informar o indicador textual de chamadas autenticadas.
        - Definir o limite máximo de registros por arquivo parquet gerado.
        - Determinar o limiar padrão para classificação de chamadas curtas.

Dependências relevantes:
        - teleutils.robocalls.classifiers
        - teleutils.core.extractors.text_extractors

Example:
        >>> from teleutils._config import SHORT_CALL_THRESHOLD
        >>> SHORT_CALL_THRESHOLD
        6

Notes:
        Este módulo deve permanecer enxuto e conter apenas configurações estáticas
        reutilizáveis. Sempre que uma nova constante representar regra de negócio
        transversal, prefira defini-la aqui em vez de replicá-la em módulos de
        processamento.
"""

from pyspark.sql import functions as F
from pyspark.sql import types as T

# Marcador textual utilizado pelo classificador de robocalls para identificar
# chamadas que passaram pelo processo de autenticação da operadora.
AUTENTICATED_CALL_FLAG = "TN-Validation-Passed"

SPARK_DEFAULT_PARALLELISM = 20

# Limite operacional aplicado na escrita de parquet para reduzir a geração de
# arquivos excessivamente grandes, o que facilita particionamento e manuseio.
# -1 = sem limite, deixa o Spark decidir o tamanho do arquivo final
MAX_RECORDS_PER_FILE = 1000000

# Regra de negócio padrão para classificar chamadas muito curtas, utilizada nas
# rotinas analíticas de detecção de padrões potencialmente abusivos.
SHORT_CALL_THRESHOLD = 6

# Define a data limite como um literal do Spark para o Catalyst otimizar a comparação
MIN_SAFE_DATE = F.lit("1901-01-01 00:00:00").cast(T.TimestampNTZType())

# Código MCC/MNC para preenchimento em caso de ausência de informação de operadora, utilizado em transformações
# de CDRs para manter consistência de dados e evitar valores nulos em campos críticos
DEFAULT_MCC = F.lit("724")
ALGAR_MNC = F.lit("34")
CLARO_MNC = F.lit("05")

# Valor sentinela para preenchimentos de campos nulos necessários para desduplicação de registros,
# evitando que registros distintos sejam erroneamente considerados duplicados
NULL_SENTINEL_VALUE = F.lit("__NULL__").cast(T.StringType())

# Chave primária para desduplicação de registros, composta por campos críticos que identificam unicamente uma chamada
PRIMARY_KEY_COLUMNS = [
    "no_tipo_chamada",
    "nu_referencia",
    "nu_referencia_sip",
    "dh_referencia",
    "dh_chamada",
    "dh_fim_chamada",
    "qt_duracao_segundos",
    "nu_origem",
    "nu_origem_original",
    "nu_destino",
    "nu_destino_original",
    "no_resultado_chamada",
    "no_rota_entrada",
    "no_rota_saida",
    "no_bilhetador",
]

TARGET_SCHEMA = {
    # 1. Identificação Geral & Tempo (Quando e qual o contexto da carga)
    "referencia": ("nu_referencia", T.StringType()),
    "referencia_sip": ("nu_referencia_sip", T.StringType()),
    "data_hora_referencia": ("dh_referencia", T.TimestampNTZType()),
    "data_hora": ("dh_chamada", T.TimestampNTZType()),
    "data_hora_fim": ("dh_fim_chamada", T.TimestampNTZType()),
    "duracao": ("qt_duracao_segundos", T.IntegerType()),
    # 2. Partes Envolvidas (Quem ligou para quem)
    "numero_origem_formatado": ("nu_origem", T.StringType()),
    "numero_origem_valido": ("ic_origem_valido", T.BooleanType()),
    "numero_origem": ("nu_origem_original", T.StringType()),
    "numero_destino_formatado": ("nu_destino", T.StringType()),
    "numero_destino_valido": ("ic_destino_valido", T.BooleanType()),
    "numero_destino": ("nu_destino_original", T.StringType()),
    # 3. Status & Resultado da Chamada (O que aconteceu com a ligação)
    "resultado_chamada": ("no_resultado_chamada", T.StringType()),
    "codigo_resposta_sip": ("co_resposta_sip", T.StringType()),
    "autenticacao": ("no_autenticacao", T.StringType()),
    # 4. Roteamento & Rede Telecom (Por onde a chamada passou)
    "prestadora": ("no_prestadora", T.StringType()),
    "rota_entrada": ("no_rota_entrada", T.StringType()),
    "rota_saida": ("no_rota_saida", T.StringType()),
    "bilhetador": ("no_bilhetador", T.StringType()),
    # 5. Dados Técnicos de Dispositivo & IP (Células, aparelhos e IPs)
    "celula_origem": ("nu_cgi_origem", T.StringType()),
    "celula_origem_hex": ("nu_cgi_origem_hex", T.StringType()),
    "tecnologia_celula_origem": ("no_tecnologia_celula_origem", T.StringType()),
    "imei_origem": ("nu_imei_origem", T.StringType()),
    "imsi_origem": ("nu_imsi_origem", T.StringType()),
    "ip_origem": ("nu_ip_origem", T.StringType()),
    "porta_ip_origem": ("nu_porta_ip_origem", T.IntegerType()),
    "celula_destino": ("nu_cgi_destino", T.StringType()),
    "celula_destino_hex": ("nu_cgi_destino_hex", T.StringType()),
    "tecnologia_celula_destino": ("no_tecnologia_celula_destino", T.StringType()),
    "imei_destino": ("nu_imei_destino", T.StringType()),
    "imsi_destino": ("nu_imsi_destino", T.StringType()),
    "ip_destino": ("nu_ip_destino", T.StringType()),
    "porta_ip_destino": ("nu_porta_ip_destino", T.IntegerType()),
    "agente_usuario": ("no_agente_usuario", T.StringType()),
    # 6. Metadados do Arquivo & Regras de Negócio (Para auditoria e particionamento)
    "esquema": ("no_esquema", T.StringType()),
    "tipo_cdr": ("no_tipo_cdr", T.StringType()),
    "arquivo_origem": ("no_arquivo_origem", T.StringType()),
    "tipo_chamada": ("no_tipo_chamada", T.StringType()),
}
