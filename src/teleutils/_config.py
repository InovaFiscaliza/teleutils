"""Constantes e contratos de dados compartilhados do pacote teleutils.

Este módulo centraliza valores utilizados por diferentes etapas do pipeline de
CDRs (extração, transformação e carga) para evitar duplicação e manter a
consistência entre elas.

Responsabilidades:
    - Definir o contrato final de colunas (``TARGET_SCHEMA``) e a chave de
      desduplicação (``PRIMARY_KEY_COLUMNS``).
    - Definir os valores sentinela de preenchimento: ``MIN_SAFE_DATE``,
      ``NULL_SENTINEL_VALUE`` e ``SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE``.
    - Definir os códigos de rede usados na composição de células do layout
      Nokia: ``DEFAULT_MCC``, ``ALGAR_MNC`` e ``CLARO_MNC``.
    - Reunir constantes operacionais sem consumidor em ``src``:
      ``AUTENTICATED_CALL_FLAG``, ``SPARK_DEFAULT_PARALLELISM``,
      ``MAX_RECORDS_PER_FILE`` e ``SHORT_CALL_THRESHOLD`` (ver Notes).

Principais consumidores:
    - ``teleutils.core.transformers.base_transformer``: ``MIN_SAFE_DATE``,
      ``NULL_SENTINEL_VALUE``, ``PRIMARY_KEY_COLUMNS`` e ``TARGET_SCHEMA``.
    - ``teleutils.core.transformers.transformers``: ``DEFAULT_MCC``,
      ``ALGAR_MNC`` e ``CLARO_MNC``.
    - ``teleutils.core.loaders.trino_loader``: ``PRIMARY_KEY_COLUMNS`` e
      ``TARGET_SCHEMA``.
    - ``teleutils.preprocessing.phone_number_extractor``:
      ``SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE``.

Dependências:
    - ``pyspark.sql.types``: tipos Spark associados às colunas de
      ``TARGET_SCHEMA``.

Example:
    >>> from teleutils._config import NULL_SENTINEL_VALUE
    >>> NULL_SENTINEL_VALUE
    '__NULL__'

Notes:
    Nenhum módulo de ``src`` importa ``AUTENTICATED_CALL_FLAG``,
    ``SPARK_DEFAULT_PARALLELISM``, ``MAX_RECORDS_PER_FILE`` ou
    ``SHORT_CALL_THRESHOLD`` deste módulo. O pacote ``robocalls`` define seus
    próprios valores (por exemplo, ``MAX_RECORDS_PER_FILE`` em
    ``robocalls.extractors`` e ``_LIMIAR_CHAMADA_OFENSORA`` em
    ``robocalls.transformers``); alterar as constantes daqui não os modifica.

    Este módulo deve permanecer enxuto e conter apenas configurações estáticas
    reutilizáveis. Sempre que uma nova constante representar regra de negócio
    transversal, prefira defini-la aqui em vez de replicá-la em módulos de
    processamento.
"""

from pyspark.sql import types as T

# Valor textual 'TN-Validation-Passed'. Coincide com o rótulo produzido por
# CDRBaseTransformer._add_tn_validation_status, mas aquele método usa um
# literal próprio: esta constante não é importada por módulos de src.
AUTENTICATED_CALL_FLAG = "TN-Validation-Passed"

# Não é importada por módulos de src (não há referência a este valor no código).
SPARK_DEFAULT_PARALLELISM = 20

# Limite de registros por arquivo Parquet (opção maxRecordsPerFile do Spark).
# Não é importado por módulos de src: robocalls.extractors usa uma cópia própria.
# -1 = sem limite, deixa o Spark decidir o tamanho do arquivo final
MAX_RECORDS_PER_FILE = 1000000

# Limiar padrão, em segundos, para chamadas "curtas". Não é importado por
# módulos de src: robocalls.transformers define o seu próprio limiar.
SHORT_CALL_THRESHOLD = 6

# Limite inferior dos timestamps: CDRBaseTransformer._format_date_time usa
# greatest(valor, MIN_SAFE_DATE), de modo que nulos, valores inválidos e datas
# anteriores a este limite resultam nele; também é o padrão da chave primária.
MIN_SAFE_DATE = "1901-01-01 00:00:00"

# Códigos de rede usados por transform_smp_nokia_gsm para compor as células
# (MCC-MNC-LAC-CI), pois o layout Nokia não os fornece. O MNC depende da
# prestadora: "claro" usa CLARO_MNC, "algar" usa ALGAR_MNC e outros valores
# resultam em MNC nulo e, portanto, em célula nula.
DEFAULT_MCC = "724"
ALGAR_MNC = "34"
CLARO_MNC = "05"

# Valor sentinela para preenchimentos de campos nulos necessários para desduplicação de registros,
# evitando que registros distintos sejam erroneamente considerados duplicados
NULL_SENTINEL_VALUE = "__NULL__"

# Número retornado por extract_number (com success=False) quando a entrada é
# vazia ou nula; ver teleutils.preprocessing.phone_number_extractor.
SUBSCRIBER_NUMBER_NULL_SENTINEL_VALUE = "5599999999999"

# Chave primária para desduplicação de registros, composta por campos críticos que identificam unicamente uma chamada.
# Usa os nomes finais das colunas (primeiro elemento das tuplas de TARGET_SCHEMA),
# não as chaves do dicionário. Nulos desses campos são preenchidos por
# CDRBaseTransformer._fill_primary_key_columns antes da escrita.
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

# Contrato final de saída: {nome intermediário: (nome final, tipo Spark)}.
# A ordem das entradas define a ordem das colunas do Parquet final.
# CDRBaseTransformer._fill_missing_columns cria como nulas as chaves ausentes,
# _select_transformed_columns aplica cast e alias, e TrinoLoader exige os nomes
# finais (primeiro elemento de cada tupla) no Parquet a carregar.
TARGET_SCHEMA = {
    # 1. Identificação Geral & Tempo (Quando e qual o contexto da carga)
    "referencia": ("nu_referencia", T.StringType()),
    "referencia_sip": ("nu_referencia_sip", T.StringType()),
    "data_hora_referencia": ("dh_referencia", T.TimestampNTZType()),
    "data_hora": ("dh_chamada", T.TimestampNTZType()),
    "data_hora_fim": ("dh_fim_chamada", T.TimestampNTZType()),
    "duracao": ("qt_duracao_segundos", T.IntegerType()),
    # 2. Partes Envolvidas (Quem ligou para quem)
    "numero_origem_extraido": ("nu_origem", T.StringType()),
    "sucesso_extracao_origem": ("ic_origem_valido", T.BooleanType()),
    "numero_origem": ("nu_origem_original", T.StringType()),
    "numero_destino_extraido": ("nu_destino", T.StringType()),
    "sucesso_extracao_destino": ("ic_destino_valido", T.BooleanType()),
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
