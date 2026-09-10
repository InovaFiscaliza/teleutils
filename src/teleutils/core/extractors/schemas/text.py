"""Contratos de leitura e mapeamento para CDRs em arquivos texto/CSV."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import types as T


@dataclass(frozen=True)
class CDRTextSchema:
    """Configura a leitura e o mapeamento de um layout CDR texto/CSV."""

    name: str
    delimiter: str
    schema: T.StructType | None
    has_header: bool
    column_to_filter: tuple[str, str] | None
    column_indices: list[int]
    column_names: list[str]
    job_description: str

    def __post_init__(self) -> None:
        """Valida a consistência da configuração antes da extração."""
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
    "ericsson": CDRTextSchema(
        name="Ericsson",
        delimiter=";",
        schema=None,
        has_header=True,
        column_to_filter=None,
        column_indices=[0, 1, 2, 3, 4, 8, 9, 11, 20],
        column_names=[
            "referencia",
            "numero_origem",
            "_data",
            "_hora",
            "_tipo_chamada",
            "rota_saida",
            "numero_destino",
            "duracao",
            "rota_entrada",
        ],
        job_description="Extraindo CDR: Ericsson",
    ),
    "tim_huawei": CDRTextSchema(
        name="Tim Huawei",
        delimiter=";",
        schema=T.StructType(
            [T.StructField(f"_c{i}", T.StringType(), True) for i in range(17)]
        ),
        has_header=False,
        column_to_filter=("_tipo_chamada", "TipodeCDR(role-of-Node)"),
        column_indices=[0, 1, 2, 3, 4, 7, 12, 16],
        column_names=[
            "numero_origem",
            "_data",
            "_hora",
            "_tipo_chamada",
            "numero_destino",
            "duracao",
            "referencia",
            "_autenticacao",
        ],
        job_description="Extraindo CDR: Tim Huawei",
    ),
    "vivo_fcdr": CDRTextSchema(
        name="Vivo FCDR",
        delimiter="|",
        schema=None,
        has_header=False,
        column_to_filter=None,
        column_indices=[0, 2, 5, 12, 13, 31, 45, 65, 66],
        column_names=[
            "_tipo_chamada",
            "_numero_origem",
            "numero_destino",
            "duracao",
            "_data",
            "_hora",
            "referencia",
            "rota_entrada",
            "rota_saida",
        ],
        job_description="Extraindo CDR: Vivo FCDR",
    ),
    "claro_nokia": CDRTextSchema(
        name="Claro Nokia",
        delimiter=";",
        schema=None,
        has_header=True,
        column_to_filter=None,
        column_indices=[0, 2, 3, 7, 8, 13, 15, 21],
        column_names=[
            "_tipo_chamada",
            "_referencia",
            "data_hora",
            "numero_origem",
            "numero_destino",
            "numero_conectado",
            "duracao",
            "_rota",
        ],
        job_description="Extraindo CDR: Claro Nokia",
    ),
}
