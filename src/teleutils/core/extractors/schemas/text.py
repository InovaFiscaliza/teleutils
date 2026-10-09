"""Contratos de leitura e mapeamento de CDRs delimitados ou de largura fixa.

Este módulo reúne a configuração declarativa usada por ``CDRTextExtractor``
para ler layouts de CDR delimitados ou de largura fixa. Cada contrato
especifica as opções de leitura CSV ou RDD, as posições de coluna ou os recortes
de caracteres, os nomes de saída e um filtro opcional aplicado pelo extrator.
O módulo não lê arquivos nem executa transformações Spark.

Os contratos disponíveis em ``TEXT_DEFAULT_SCHEMAS`` são indexados pelas
chaves consumidas por ``CDRTextExtractor.extract``. Os layouts PIT e SS8BF
reutilizam os schemas Spark definidos em ``schemas._claro``.

Notes:
    Sem ``column_sizes``, os índices representam colunas baseadas em zero (Python-style indexing).
    Com ``column_sizes``, representam posições de caracteres repassadas
    diretamente a ``substring``, cuja primeira posição é 1 (Spark-style indexing). Os índices devem
    manter correspondência posicional com os nomes de saída.

    A importação instancia e valida os contratos do catálogo, sem exigir uma
    sessão Spark ativa. ``TEXT_DEFAULT_SCHEMAS`` é um dicionário mutável;
    ``frozen=True`` se aplica às instâncias de ``CDRTextSchema``, não ao catálogo.

Example:
    >>> from teleutils.core.extractors.schemas.text import TEXT_DEFAULT_SCHEMAS
    >>> schema = TEXT_DEFAULT_SCHEMAS["stfc_huawei_ngn"]
    >>> schema.delimiter
    ','
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import types as T  # type: ignore

from teleutils.core.extractors.schemas._claro import (
    stfc_pit_claro_schema,
    stfc_ss8bf_claro_schema,
)


@dataclass(frozen=True)
class CDRTextSchema:
    """Configura a leitura e o mapeamento de um layout CDR.

    A construção converte índices e nomes de coluna em tuplas e valida a
    consistência do contrato. A ordem desses valores é preservada e define
    o pareamento aplicado pelo extrator durante a seleção ou o recorte e a
    atribuição dos nomes de saída. As operações Spark são executadas pelo
    extrator, não pela classe.

    Attributes:
        name: Nome amigável do fornecedor ou layout de origem.
        delimiter: Delimitador repassado à leitura CSV do Spark.
        schema: ``StructType``, string de definição de schema ou ``None``,
            repassado ao leitor CSV. Com ``None``, o extrator lê sem schema
            explícito e mantém a inferência de tipos desativada.
        has_header: Indica se a primeira linha do arquivo deve ser tratada como
            cabeçalho pela leitura CSV.
        lines_to_keep: Par ``(nome_da_coluna, valor)`` usado pelo extrator
            para manter registros por igualdade. Os valores especiais
            ``"is null"`` e ``"is not null"`` testam nulidade; ``None`` desativa
            o filtro. A coluna deve constar em ``column_names``. O filtro por
            igualdade não mantém valores nulos na coluna filtrada.
        column_indices: Índices de colunas baseados em zero, quando
            ``column_sizes`` está vazio, ou posições iniciais de caracteres
            para ``substring``, quando há comprimentos configurados.
        column_sizes: Comprimentos dos campos de largura fixa, pareados com
            ``column_indices`` e ``column_names``. Uma tupla vazia ativa a
            seleção de colunas em vez do recorte da coluna ``value``.
        column_names: Nomes atribuídos aos campos selecionados ou recortados,
            na mesma ordem de ``column_indices``.
        job_description: Descrição textual armazenada no contrato; não é
            utilizada por ``CDRTextExtractor.extract``.
        read_rdd: Se verdadeiro, o extrator lê linhas via RDD e produz a coluna
            ``value``, sem aplicar ``delimiter``, ``has_header`` ou ``schema``.
            Se falso, utiliza o leitor CSV. O padrão é ``False``.
            A leitura via RDD é utilizada para contornar falhas de interpretação
            de caminhos como URI pelo leitor CSV, relatadas no ambiente de origem
            para nomes de arquivos que contêm ``@``. A opção é configurada no
            contrato, sem detecção automática desses nomes.
        fill_char: Conteúdo usado sem escape no padrão regular
            ``fill_char + '+$'`` para remover preenchimento final de campos de
            largura fixa, antes de ``trim``. Uma string vazia desativa essa
            remoção, mas não a retirada de espaços das bordas.

    Notes:
        ``frozen=True`` impede a atribuição normal aos atributos, mas não torna
        objetos internos profundamente imutáveis. ``column_sizes`` não é
        convertido em tupla, e o ``StructType`` recebido não é copiado.

        O ramo de largura fixa exige uma coluna ``value`` na leitura. Ela é
        criada por ``read_rdd=True`` ou pode ser definida pelo schema CSV,
        como nos contratos que usam ``schema="value string"``.
    """

    name: str
    delimiter: str | None
    schema: T.StructType | str | None
    has_header: bool
    lines_to_keep: tuple[str, str] | None
    column_indices: tuple[int, ...]
    column_sizes: tuple[int, ...]
    column_names: tuple[str, ...]
    job_description: str
    read_rdd: bool = False
    fill_char: str = ""

    def __post_init__(self) -> None:
        """Normaliza e valida a consistência da configuração antes da extração.

        As coleções externas de índices e nomes recebidas são convertidas para
        tuplas antes das validações por meio de ``object.__setattr__``, apesar
        de a dataclass ser congelada. Os demais atributos não são normalizados.
        Quando há filtro, o nome da coluna filtrada deve estar entre os nomes de
        saída, pois a filtragem é realizada depois da seleção e renomeação.

        Raises:
            ValueError: Se ``schema`` não for ``None``, ``StructType`` ou ``str``;
                se ``lines_to_keep`` não for ``None`` nem uma tupla de duas
                strings; se sua coluna não estiver em ``column_names``; se os
                índices e nomes tiverem tamanhos distintos; se ``column_sizes``
                não vazio tiver tamanho diferente de ``column_indices``;
                se não houver índices; se houver índice negativo; ou se o maior
                índice for maior ou igual à quantidade de campos do
                ``StructType`` fornecido.

        Notes:
            A verificação de limites contra ``StructType`` também ocorre quando
            há ``column_sizes``; ela não distingue posições de caracteres de
            índices de colunas. Schemas em string não passam por essa verificação.

            Não há validação dos comprimentos individuais em ``column_sizes``,
            da sintaxe de schemas em string nem da existência da coluna
            ``value`` exigida pelo recorte de largura fixa.

            ``CDRTextExtractor`` verifica o maior índice contra as colunas
            efetivamente lidas apenas no ramo sem ``column_sizes``. O ramo de
            largura fixa não valida o comprimento das linhas recortadas.
        """
        object.__setattr__(self, "column_indices", tuple(self.column_indices))
        object.__setattr__(self, "column_names", tuple(self.column_names))
        if self.schema is not None and not isinstance(self.schema, (T.StructType, str)):
            raise ValueError(
                f"Schema '{self.name}': schema deve ser None ou um StructType. "
                f"Recebido: {type(self.schema).__name__}"
            )
        if self.lines_to_keep is not None:
            if (
                not isinstance(self.lines_to_keep, tuple)
                or len(self.lines_to_keep) != 2
                or not all(isinstance(value, str) for value in self.lines_to_keep)
            ):
                raise ValueError(
                    f"Schema '{self.name}': lines_to_keep deve ser None ou uma "
                    f"tupla de duas strings. Recebido: {self.lines_to_keep!r}"
                )
            column_name, _ = self.lines_to_keep
            if column_name not in self.column_names:
                raise ValueError(
                    f"Schema '{self.name}': coluna '{column_name}' em "
                    f"lines_to_keep não está presente em column_names: "
                    f"{self.column_names}"
                )
        if len(self.column_indices) != len(self.column_names):
            raise ValueError(
                f"Schema '{self.name}': column_indices tem "
                f"{len(self.column_indices)} elemento(s), mas column_names tem "
                f"{len(self.column_names)}. Devem ter o mesmo tamanho."
            )
        if self.column_sizes and len(self.column_sizes) != len(self.column_indices):
            raise ValueError(
                f"Schema '{self.name}': column_sizes tem "
                f"{len(self.column_indices)} elemento(s), mas column_names tem "
                f"{len(self.column_names)}. Devem ter o mesmo tamanho."
            )
        if not self.column_indices:
            raise ValueError(
                f"Schema '{self.name}': column_indices não pode ser vazio."
            )
        if any(index < 0 for index in self.column_indices):
            raise ValueError(
                f"Schema '{self.name}': índices negativos não são permitidos. "
                f"Recebido: {self.column_indices}"
            )
        if (
            self.schema is not None
            and isinstance(self.schema, T.StructType)
            and max(self.column_indices) >= len(self.schema)
        ):
            raise ValueError(
                f"Schema '{self.name}': schema possui {len(self.schema)} campo(s), "
                f"mas column_indices requer o índice {max(self.column_indices)}."
            )


TEXT_DEFAULT_SCHEMAS: dict[str, CDRTextSchema] = {
    "stfc_huawei_ngn": CDRTextSchema(
        name="Huawei NGN Algar/Claro",
        delimiter=",",
        schema=None,
        has_header=False,
        lines_to_keep=None,
        column_indices=(0, 1, 3, 4, 5, 6, 7, 8, 9, 17, 18, 19, 21, 22),
        column_sizes=(),
        column_names=(
            "referencia",
            "bilhetador",
            "_data",
            "_hora",
            "_data_fim",
            "_hora_fim",
            "duracao",
            "numero_origem",
            "numero_destino",
            "rota_entrada",
            "rota_saida",
            "_tipo_chamada",
            "codigo_resposta_sip",
            "_resultado_chamada",
        ),
        job_description="Extraindo CDR: STFC Huawei NGN Algar/Claro",
    ),
    "stfc_huawei_ngn_tim": CDRTextSchema(
        name="Huawei NGN Tim",
        delimiter=";",
        schema=None,
        has_header=False,
        lines_to_keep=None,
        column_indices=(5, 6, 8, 10, 11, 17, 18, 29, 30),
        column_sizes=(),
        column_names=(
            "duracao",
            "rota_entrada",
            "rota_saida",
            "numero_origem",
            "numero_destino",
            "bilhetador",
            "referencia",
            "data_hora",
            "data_hora_fim",
        ),
        job_description="Extraindo CDR: STFC Huawei NGN Tim",
    ),
    "stfc_fcdr_vivo": CDRTextSchema(
        name="FCDR Vivo",
        delimiter=";",
        schema=None,
        has_header=False,
        lines_to_keep=None,
        column_indices=(0, 1, 2, 22, 24, 25, 27, 28, 33, 34, 68, 78),
        column_sizes=(),
        column_names=(
            "bilhetador",
            "_tipo_cdr",
            "_tipo_chamada",
            "numero_destino",
            "_resultado_chamada",
            "_hora",
            "duracao",
            "_data",
            "rota_entrada",
            "rota_saida",
            "referencia",
            "numero_origem",
        ),
        job_description="Extraindo CDR: STFC FCDR Vivo",
    ),
    "stfc_7n_oi": CDRTextSchema(
        name="7N Oi",
        delimiter=None,
        schema=None,
        has_header=False,
        lines_to_keep=("_tipo_registro", "<"),
        column_indices=(1, 2, 34, 85, 194, 202, 208, 222, 235, 247, 255),
        column_sizes=(1, 32, 16, 30, 8, 6, 6, 8, 8, 2, 4),
        column_names=(
            "_tipo_registro",
            "referencia",
            "numero_origem",
            "numero_destino",
            "_data",
            "_hora",
            "duracao",
            "rota_entrada",
            "rota_saida",
            "_resultado_chamada",
            "bilhetador",
        ),
        job_description="Extraindo CDR: STFC 7N Oi",
        read_rdd=True,
        fill_char="-",
    ),
    "stfc_tropico_oi": CDRTextSchema(
        name="Tropico Oi",
        delimiter=None,
        schema=None,
        has_header=False,
        lines_to_keep=("_tipo_registro", "#"),
        column_indices=(1, 9, 45, 79, 142, 147, 153, 159, 173, 175, 191, 205),
        column_sizes=(1, 25, 16, 18, 3, 6, 6, 6, 1, 4, 7, 7),
        column_names=(
            "_tipo_registro",
            "referencia",
            "numero_origem",
            "numero_destino",
            "_resultado_chamada",
            "_data",
            "_hora",
            "duracao",
            "_tipo_chamada",
            "bilhetador",
            "rota_entrada",
            "rota_saida",
        ),
        job_description="Extraindo CDR: STFC Tropico Oi",
        read_rdd=True,
        fill_char="-",
    ),
    "stfc_axe_claro": CDRTextSchema(
        name="AXE Claro",
        delimiter=None,
        schema="value string",
        has_header=False,
        lines_to_keep=None,
        column_indices=(2, 13, 15, 36, 60, 62, 68, 74, 80, 92, 103, 142),
        column_sizes=(8, 2, 18, 24, 2, 6, 6, 6, 6, 4, 4, 8),
        column_names=(
            "bilhetador",
            "_tipo_chamada",
            "numero_origem",
            "numero_destino",
            "_resultado_chamada",
            "_hora",
            "_hora_fim",
            "duracao",
            "_data",
            "rota_entrada",
            "rota_saida",
            "referencia",
        ),
        job_description="Extraindo CDR: STFC AXE Claro",
    ),
    "stfc_pcl_claro": CDRTextSchema(
        name="PCL Claro",
        delimiter=None,
        schema="value string",
        has_header=False,
        lines_to_keep=None,
        column_indices=(5, 24, 32, 90, 98, 110, 116, 138, 297, 313, 499, 519, 550),
        column_sizes=(3, 8, 2, 8, 6, 6, 22, 28, 8, 8, 10, 10, 1),
        column_names=(
            "tecnologia_fornecedor",
            "bilhetador",
            "_tipo_chamada",
            "_data",
            "_hora",
            "duracao",
            "numero_origem",
            "numero_destino",
            "rota_entrada",
            "rota_saida",
            "referencia_sip",  # Nokia
            "referencia",  # Ericsson
            "_resultado_chamada",
        ),
        job_description="Extraindo CDR: STFC PCL Claro",
    ),
    "stfc_tropico_claro_lega": CDRTextSchema(
        name="Tropico Claro LEGA",
        delimiter=",",
        schema=None,
        has_header=False,
        lines_to_keep=None,
        column_indices=(1, 2, 3, 4, 5, 6, 7, 8, 9, 17, 18, 19, 25, 42),
        column_sizes=(),
        column_names=(
            "bilhetador",
            "_resultado_chamada",
            "_data",
            "_hora",
            "_data_fim",
            "_hora_fim",
            "duracao",
            "numero_origem",
            "numero_destino",
            "rota_entrada",
            "rota_saida",
            "_tipo_chamada",
            "codigo_resposta_sip",
            "referencia",
        ),
        job_description="Extraindo CDR: STFC Tropico Claro LEGA",
    ),
    "stfc_tropico_claro_legb": CDRTextSchema(
        name="Tropico Claro LEGB",
        delimiter=",",
        schema=None,
        has_header=False,
        lines_to_keep=None,
        column_indices=(1, 2, 3, 4, 5, 6, 7, 8, 9, 14, 15, 25, 45),
        column_sizes=(),
        column_names=(
            "bilhetador",
            "_resultado_chamada",
            "_data",
            "_hora",
            "_data_fim",
            "_hora_fim",
            "duracao",
            "numero_origem",
            "numero_destino",
            "rota_entrada",
            "rota_saida",
            "codigo_resposta_sip",
            "referencia",
        ),
        job_description="Extraindo CDR: STFC Tropico Claro LEGB",
    ),
    "stfc_pit_claro": CDRTextSchema(
        name="Pit Claro",
        delimiter=",",
        schema=stfc_pit_claro_schema,
        has_header=False,
        lines_to_keep=(("_tipo_registro", "5")),
        column_indices=(0, 1, 2, 3, 4, 5, 9, 10, 11, 42, 43, 108),
        column_sizes=(),
        column_names=(
            "_tipo_registro",
            "bilhetador",
            "rota_entrada",
            "rota_saida",
            "numero_origem",
            "numero_destino",
            "_data_hora_fim",
            "duracao",
            "resultado_chamada",
            "referencia",
            "_data_hora",
            "_prestadora_origem",
        ),
        job_description="Extraindo CDR: STFC Pit Claro",
    ),
    "stfc_ss8bf_claro": CDRTextSchema(
        name="SS8BF Claro",
        delimiter=",",
        schema=stfc_ss8bf_claro_schema,
        has_header=False,
        lines_to_keep=(("_data_hora", "is not null")),
        column_indices=(2, 3, 4, 5, 10, 11, 17, 39, 40, 64, 125, 126),
        column_sizes=(),
        column_names=(
            "_data_hora",
            "duracao",
            "bilhetador",
            "_referencia",
            "numero_destino",
            "numero_origem",
            "_resultado_chamada",
            "_identificador_origem",
            "_identificador_destino",
            "_numero_encaminhado",
            "rota_entrada",
            "rota_saida",
        ),
        job_description="Extraindo CDR: STFC SS8BF Claro",
    ),
}
