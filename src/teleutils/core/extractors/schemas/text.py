"""Contratos de leitura e mapeamento para CDRs em arquivos texto/CSV.

Este módulo reúne a configuração declarativa usada por ``CDRTextExtractor``
para ler layouts de CDR delimitados. Cada contrato especifica as opções de
leitura, as posições de coluna a selecionar, os nomes de saída e um filtro
opcional aplicado após a seleção. O módulo não lê arquivos nem executa
transformações Spark.

Os contratos disponíveis em ``TEXT_DEFAULT_SCHEMAS`` são indexados pelas
chaves consumidas pelos métodos específicos do extrator. Os índices são
baseados em zero e devem manter correspondência posicional com os nomes de
coluna.

Example:
    >>> from teleutils.core.extractors.schemas.text import TEXT_DEFAULT_SCHEMAS
    >>> schema = TEXT_DEFAULT_SCHEMAS["stfc_huawei_ngn"]
    >>> schema.delimiter
    ','
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import types as T  # type: ignore


@dataclass(frozen=True)
class CDRTextSchema:
    """Configura a leitura e o mapeamento de um layout CDR texto/CSV.

    Os índices e nomes de coluna são armazenados como tuplas para que a
    configuração não possa ser alterada após a instanciação. A ordem desses
    valores é preservada e define o pareamento aplicado pelo extrator durante a
    seleção e a renomeação das colunas.

    Attributes:
        name: Nome amigável do fornecedor ou layout de origem.
        delimiter: Delimitador repassado à leitura CSV do Spark.
        schema: Schema Spark aplicado à leitura ou ``None`` para deixar que o
            Spark produza as colunas sem um schema explícito.
        has_header: Indica se a primeira linha do arquivo deve ser tratada como
            cabeçalho pela leitura CSV.
        column_to_filter: Par ``(nome_da_coluna, valor)`` usado pelo extrator
            para remover registros cujo valor seja igual ao configurado, ou
            ``None`` quando nenhum filtro deve ser aplicado.
        column_indices: Posições, baseadas em zero, das colunas de origem a
            selecionar.
        column_names: Nomes atribuídos às colunas selecionadas, na mesma ordem
            de ``column_indices``.
        job_description: Descrição textual armazenada no contrato para uso do
            fluxo de extração e observabilidade.
    """

    name: str
    delimiter: str | None
    schema: T.StructType | None
    has_header: bool
    column_to_filter: tuple[str, str] | None
    column_indices: tuple[int, ...]
    column_sizes: tuple[int, ...]
    column_names: tuple[str, ...]
    job_description: str

    def __post_init__(self) -> None:
        """Normaliza e valida a consistência da configuração antes da extração.

        As coleções externas de índices e nomes recebidas são convertidas para
        tuplas antes das validações, preservando a imutabilidade do contrato.
        Quando há filtro, o nome da coluna filtrada deve estar entre os nomes de
        saída, pois a filtragem é realizada depois da seleção e renomeação.

        Raises:
            ValueError: Se ``schema`` não for ``None`` nem um ``StructType``;
                se ``column_to_filter`` não for ``None`` nem uma tupla de duas
                strings; se sua coluna não estiver em ``column_names``; se os
                índices e nomes tiverem tamanhos distintos; se não houver
                índices; se houver índice negativo; ou se o maior índice não
                existir no schema Spark fornecido.

        Notes:
            A compatibilidade dos índices com arquivos lidos sem schema é
            verificada posteriormente por ``CDRTextExtractor``, porque a
            quantidade de colunas só é conhecida após a leitura do arquivo.
        """
        object.__setattr__(self, "column_indices", tuple(self.column_indices))
        object.__setattr__(self, "column_names", tuple(self.column_names))
        if self.schema is not None and not isinstance(self.schema, T.StructType):
            raise ValueError(
                f"Schema '{self.name}': schema deve ser None ou um StructType. "
                f"Recebido: {type(self.schema).__name__}"
            )
        if self.column_to_filter is not None:
            if (
                not isinstance(self.column_to_filter, tuple)
                or len(self.column_to_filter) != 2
                or not all(isinstance(value, str) for value in self.column_to_filter)
            ):
                raise ValueError(
                    f"Schema '{self.name}': column_to_filter deve ser None ou uma "
                    f"tupla de duas strings. Recebido: {self.column_to_filter!r}"
                )
            column_name, _ = self.column_to_filter
            if column_name not in self.column_names:
                raise ValueError(
                    f"Schema '{self.name}': coluna '{column_name}' em "
                    f"column_to_filter não está presente em column_names: "
                    f"{self.column_names}"
                )
        if len(self.column_indices) != len(self.column_names):
            raise ValueError(
                f"Schema '{self.name}': column_indices tem "
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
        if self.schema is not None and max(self.column_indices) >= len(self.schema):
            raise ValueError(
                f"Schema '{self.name}': schema possui {len(self.schema)} campo(s), "
                f"mas column_indices requer o índice {max(self.column_indices)}."
            )


TEXT_DEFAULT_SCHEMAS: dict[str, CDRTextSchema] = {
    "stfc_huawei_ngn": CDRTextSchema(
        name="NGN Huawei",
        delimiter=",",
        schema=None,
        has_header=False,
        column_to_filter=None,
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
            "_status_chamada",
        ),
        job_description="Extraindo CDR: STFC Huawei NGN",
    ),
    "stfc_fcdr_vivo": CDRTextSchema(
        name="FCDR Vivo",
        delimiter=";",
        schema=None,
        has_header=False,
        column_to_filter=None,
        column_indices=(0, 1, 2, 22, 24, 25, 27, 28, 33, 34, 68, 78),
        column_sizes=(),
        column_names=(
            "bilhetador",
            "_tipo_cdr",
            "_tipo_chamada",
            "numero_destino",
            "_status_chamada",
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
    "stfc_tropico_oi": CDRTextSchema(
        name="Tropico Oi",
        delimiter=None,
        schema=None,
        has_header=False,
        column_to_filter=None,
        column_indices=(1, 2, 9, 45, 79, 97, 142, 147, 153, 159, 173, 175, 191, 205),
        column_sizes=(1, 7, 25, 16, 18, 24, 3, 6, 6, 6, 1, 4, 7, 7),
        column_names=(
            "_tipo_linha",
            "nu_referencia",
            "nu_referencia",
            "nu_origem",
            "nu_destino",
            "nu_destino_original",
            "no_resultado_chamada",
            "dh_chamada",
            "dh_chamada",
            "qt_duracao_segundos",
            "no_tipo_chamada",
            "no_bilhetador",
            "no_rota_entrada",
            "no_rota_saida",
        ),
        job_description="Extraindo CDR: STFC Tropico Oi",
    ),
}
