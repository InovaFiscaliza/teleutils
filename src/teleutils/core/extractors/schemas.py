"""Módulo de definição dos contratos de mapeamento de CDRs do Teleparser.

Este módulo centraliza a dataclass de configuração ``CDRTeleparserSchema`` e os
schemas padrão de cada fornecedor/layout suportado pelo Teleparser. A separação
entre configuração (este módulo) e execução (``teleparser_extractors.py``)
permite que novos schemas sejam adicionados ou atualizados sem necessidade de
alterar a lógica de extração, favorecendo o princípio de responsabilidade única
e a extensão do projeto sem modificação do código existente (OCP).

Responsabilidades principais:
    - Definir o contrato imutável ``CDRTeleparserSchema``.
    - Validar a consistência estrutural de cada schema configurado.
    - Consolidar os schemas padrão em ``TELEPARSER_DEFAULT_SCHEMAS``.

Example:
    >>> from teleutils.core.extractors.schemas import TELEPARSER_DEFAULT_SCHEMAS
    >>> schema = TELEPARSER_DEFAULT_SCHEMAS["ericsson"]
    >>> schema.name
    'Ericsson'
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CDRTeleparserSchema:
    """Representa o contrato de extração para um layout específico de CDR.

    A estrutura define quais colunas da origem devem ser selecionadas e como
    elas serão renomeadas no dataset intermediário. A ideia é separar configuração
    de execução: a classe ``CDRTeleparserExtractor`` apenas aplica esse contrato,
    enquanto cada instância de ``CDRTeleparserSchema`` define as regras.
    A configuração é imutável para evitar alteração acidental de regras em tempo de execução.

    Attributes:
        name:
            Nome amigável do schema (fornecedor/layout).
        column_mapping:
            Lista de pares ``(origem, destino)`` contendo o mapeamento de
            colunas da entrada para o nome padronizado intermediário.
        job_description:
            Descrição textual da operação, útil para observabilidade e logs.
    """

    name: str
    column_mapping: list[tuple[str, str]]
    job_description: str

    def __post_init__(self) -> None:
        """Valida a estrutura do mapeamento após a criação do dataclass.

        Objetivo da operação:
            Garantir que o schema contenha ao menos uma coluna e que cada item
            de ``column_mapping`` siga o formato ``(origem, destino)`` com
            valores textuais.

        Raises:
            ValueError:
                Quando ``column_mapping`` está vazio ou possui itens inválidos.

        Notes:
            - Regra de integridade: cada item deve ser uma tupla de 2 strings.
            - Anotação de manutenção: manter essa validação rígida evita falhas
              silenciosas durante o ``select`` em Spark.
        """
        if not self.column_mapping:
            raise ValueError(
                f"Schema '{self.name}': column_mapping nao pode ser vazio."
            )
        for item in self.column_mapping:
            if (
                not isinstance(item, tuple)
                or len(item) != 2
                or not isinstance(item[0], str)
                or not isinstance(item[1], str)
            ):
                raise ValueError(
                    f"Schema '{self.name}': cada item de column_mapping deve ser "
                    f"uma tupla (origem, destino) de strings. Recebido: {item!r}"
                )


TELEPARSER_DEFAULT_SCHEMAS: dict[str, CDRTeleparserSchema] = {
    "ericsson": CDRTeleparserSchema(
        name="Ericsson",
        column_mapping=[
            ("networkCallReference", "referencia"),
            ("callingPartyNumber.digits", "numero_origem"),
            ("dateForStartOfCharge", "_data"),
            ("timeForStartOfCharge", "_hora"),
            ("timeForStopOfCharge", "_hora_fim"),
            ("CallModule", "tipo_chamada"),
            ("calledPartyNumber.digits", "numero_destino"),
            ("chargeableDuration", "duracao"),
            ("incomingRoute", "rota_entrada"),
            ("outgoingRoute", "rota_saida"),
            ("exchangeIdentity", "bilhetador"),
            ("firstCallingLocationInformation.mcc", "celula_origem_mcc"),
            ("firstCallingLocationInformation.mnc", "celula_origem_mnc"),
            ("firstCallingLocationInformation.lac", "celula_origem_lac"),
            ("firstCallingLocationInformation.ci_sac", "celula_origem_ci_sac"),
            ("firstCalledLocationInformation.mcc", "celula_destino_mcc"),
            ("firstCalledLocationInformation.mnc", "celula_destino_mnc"),
            ("firstCalledLocationInformation.lac", "celula_destino_lac"),
            ("firstCalledLocationInformation.ci_sac", "celula_destino_ci_sac"),
            ("callingSubscriberIMSI.mcc", "imsi_origem_mcc"),
            ("callingSubscriberIMSI.mnc", "imsi_origem_mnc"),
            ("callingSubscriberIMSI.msin", "imsi_origem_msin"),
            ("calledSubscriberIMSI.mcc", "imsi_destino_mcc"),
            ("calledSubscriberIMSI.mnc", "imsi_destino_mnc"),
            ("calledSubscriberIMSI.msin", "imsi_destino_msin"),
            ("callingSubscriberIMEI.type_allocation_code", "imei_origem_tac"),
            ("callingSubscriberIMEI.serial_number", "imei_origem_sn"),
            ("calledSubscriberIMEI.type_allocation_code", "imei_destino_tac"),
            ("calledSubscriberIMEI.serial_number", "imei_destino_sn"),
            ("callPosition", "status_chamada"),
        ],
        job_description="Extraindo CDR Parquet: Ericsson",
    ),
    "tim_huawei": CDRTeleparserSchema(
        name="TIM Huawei",
        column_mapping=[
            ("network-Call-Reference", "referencia"),
            ("calling-Party-Address-Generic", "_numero_origem_generico"),
            ("list-Of-Calling-Party-Address_tEL-URI", "numero_origem"),
            ("recordOpeningTime", "data_hora"),
            ("role-of-Node", "tipo_chamada"),
            ("called-Party-Address_tEL-URI", "numero_destino"),
            ("duration", "duracao"),
            ("recordType", "_tipo_cdr"),
            ("specifiedTreatmentField_incoming-Route", "rota_entrada"),
            ("specifiedTreatmentField_outgoing-Route", "rota_saida"),
        ],
        job_description="Extraindo CDR Parquet: TIM Huawei",
    ),
    "vivo_fcdr": CDRTeleparserSchema(
        name="Vivo FCDR",
        column_mapping=[
            ("callModule", "_tipo_chamada"),
            ("callingPartyNumber", "_numero_origem"),
            ("calledPartyNumber", "numero_destino"),
            ("chargeableDurat", "duracao"),
            ("dateForStartOfCharge", "_data"),
            ("timeForStartOfCharge", "_hora"),
            ("networkCallReference", "referencia"),
            ("incomingRoute", "rota_entrada"),
            ("outgoingRoute", "rota_saida"),
        ],
        job_description="Extraindo CDR Parquet: Vivo FCDR",
    ),
    "nokia": CDRTeleparserSchema(
        name="Nokia",
        column_mapping=[
            ("record_type", "tipo_chamada"),
            ("call_reference", "referencia"),
            ("call_reference_time", "data_hora_referencia"),
            ("in_channel_allocated_time", "data_hora_alocacao_canal"),
            ("charging_end_time", "data_hora_fim_tarifacao"),
            ("release_time", "data_hora_desconexao"),
            ("calling_number", "numero_origem"),
            ("orig_calling_number", "numero_origem_original"),
            ("called_number", "numero_destino"),
            ("orig_called_number", "numero_destino_original"),
            ("connected_to_number", "numero_conectado"),
            ("forwarding_number", "numero_origem_encaminhamento"),
            ("forwarded_to_number", "numero_destino_encaminhamento"),
            ("orig_mcz_duration", "_duracao_orig_mcz"),
            ("term_mcz_duration", "_duracao_term_mcz"),
            ("forw_mcz_duration", "_duracao_forw_mcz"),
            ("roam_mcz_duration", "_duracao_roam_mcz"),
            ("iaz_duration", "_duracao_iaz"),
            ("oaz_duration", "_duracao_oaz"),
            ("chargeable_duration", "_duracao_tarifavel"),
            ("char_band_duration", "_duracao_banda_tarifavel"),
            ("in_circuit_group", "rota_entrada"),
            ("out_circuit_group", "rota_saida"),
            ("exchange_id", "bilhetador"),
            ("calling_subs_first_lac", "celula_origem_lac"),
            ("calling_subs_first_ci", "celula_origem_ci_sac"),
            ("called_subs_first_lac", "celula_destino_lac"),
            ("called_subs_first_ci", "celula_destino_ci_sac"),
            ("calling_imsi", "imsi_origem"),
            ("called_imsi", "imsi_destino"),
            ("calling_imei", "imei_origem"),
            ("called_imei", "imei_destino"),
            ("cause_for_termination", "status_chamada"),
        ],
        job_description="Extraindo CDR Parquet: Nokia",
    ),
}
