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
