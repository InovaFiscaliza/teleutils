"""Contratos de mapeamento para CDRs Parquet do Teleparser.

Este módulo centraliza a configuração ``CDRParquetSchema`` e os contratos
padronizados dos fornecedores e layouts suportados pelo Teleparser. Cada
contrato informa o nome do layout, os pares de coluna de origem e destino e a
descrição usada pelo job de extração. O extrator aplica esses pares ao parquet
de entrada; este módulo não lê, transforma ou grava DataFrames.

A separação entre configuração (este módulo) e execução
(``teleparser_extractors.py``) permite adicionar ou atualizar layouts sem
alterar a lógica de extração. A validação realizada na criação do contrato é
estrutural: garante que o mapeamento não esteja vazio e que seus itens sejam
tuplas de duas strings, mas não verifica se as colunas existem na origem.

Responsabilidades principais:
    - Definir o contrato congelado ``CDRParquetSchema``.
    - Validar a consistência estrutural de cada schema configurado.
    - Consolidar os contratos padrão em ``PARQUET_DEFAULT_SCHEMAS``, indexados
      pelas chaves usadas pelos métodos do extrator.

Example:
    >>> from teleutils.core.extractors.schemas.parquet import (
    ...     PARQUET_DEFAULT_SCHEMAS,
    ... )
    >>> schema = PARQUET_DEFAULT_SCHEMAS["smp_ericsson_gsm"]
    >>> schema.name
    'SMP Ericsson GSM'
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CDRParquetSchema:
    """Representa o contrato de extração para um layout específico de CDR.

    A estrutura define quais colunas da origem devem ser selecionadas e como
    elas serão nomeadas no dataset intermediário. A classe
    ``CDRTeleparserExtractor`` consome esse contrato durante a extração, enquanto
    cada instância de ``CDRParquetSchema`` concentra as regras de um layout.

    O dataclass é congelado e o mapeamento é armazenado como uma tupla de
    tuplas, impedindo a reatribuição dos atributos e a alteração dos pares
    configurados após a criação do contrato. A ordem dos pares é preservada e
    define a ordem das colunas selecionadas pelo extrator.

    Attributes:
        name:
            Nome amigável do schema (fornecedor/layout).
        column_mapping:
            Tupla de pares ``(origem, destino)`` contendo o mapeamento de
            colunas da entrada para o nome padronizado intermediário. Deve
            conter pelo menos um par, e cada par deve ter duas strings.
        job_description:
            Descrição textual da operação, armazenada para uso do fluxo de
            extração e observabilidade.
    """

    name: str
    column_mapping: tuple[tuple[str, str], ...]
    job_description: str

    def __post_init__(self) -> None:
        """Valida a estrutura do mapeamento após a criação do dataclass.

        A validação garante que ``column_mapping`` contenha ao menos um item e
        que cada item siga o formato ``(origem, destino)`` com valores textuais.
        A coleção externa recebida é convertida para tupla a fim de preservar o
        contrato congelado do dataclass; seus itens devem ser tuplas e não são
        convertidos individualmente.
        Ela não consulta uma origem de dados nem valida a existência,
        duplicidade ou semântica das colunas informadas.

        Raises:
            ValueError:
                Quando ``column_mapping`` está vazio ou possui itens inválidos.

        Notes:
            - A validação ocorre no momento da instanciação, inclusive para os
              contratos definidos em ``PARQUET_DEFAULT_SCHEMAS``.
            - A checagem rígida do formato evita que um contrato malformado
              avance até a montagem da seleção de colunas no extrator.
        """
        if not self.column_mapping:
            raise ValueError(
                f"Schema '{self.name}': column_mapping nao pode ser vazio."
            )
        object.__setattr__(self, "column_mapping", tuple(self.column_mapping))
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


PARQUET_DEFAULT_SCHEMAS: dict[str, CDRParquetSchema] = {
    "smp_ericsson_gsm": CDRParquetSchema(
        name="Ericsson",
        column_mapping=(
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
        ),
        job_description="Extraindo CDR Parquet: SMP Ericsson GSM",
    ),
    "smp_gsm_nokia": CDRParquetSchema(
        name="Nokia",
        column_mapping=(
            ("record_type", "tipo_chamada"),
            ("call_reference", "referencia"),
            ("call_reference_time", "data_hora_referencia"),
            ("in_channel_allocated_time", "data_hora_alocacao_canal"),
            ("charging_end_time", "data_hora_fim"),
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
            ("calling_subs_first_ci", "celula_origem_ci"),
            ("called_subs_first_lac", "celula_destino_lac"),
            ("called_subs_first_ci", "celula_destino_ci"),
            ("calling_imsi", "imsi_origem"),
            ("called_imsi", "imsi_destino"),
            ("calling_imei", "imei_origem"),
            ("called_imei", "imei_destino"),
            ("cause_for_termination", "_resultado_chamada"),
            ("operator_profile", "perfil_prestadora"),
        ),
        job_description="Extraindo CDR Parquet: SMP Nokia GSM",
    ),
    "smp_huawei_volte_tim": CDRParquetSchema(
        name="LTE Huawei TIM",
        column_mapping=(
            ("network-Call-Reference", "referencia"),
            ("iMS-Charging-Identifier", "referencia_sip"),
            ("calling-Party-Address-Generic", "_numero_origem_ats_auth"),
            ("list-Of-Calling-Party-Address", "_numero_origem"),
            ("called-Party-Address_tEL-URI", "_numero_destino_ats"),
            ("called-Party-Address_sIP-URI", "_numero_destino_ibcf"),
            ("serviceRequestTimeStamp", "data_hora"),
            ("serviceDeliveryEndTimeStamp", "data_hora_fim"),
            ("role-of-Node", "tipo_chamada"),
            ("duration", "duracao"),
            ("recordType", "_tipo_cdr"),
            ("specifiedTreatmentField_incoming-Route", "rota_entrada"),
            ("specifiedTreatmentField_outgoing-Route", "rota_saida"),
            ("nodeAddress_domainName", "bilhetador"),
            ("accessNetworkInformation", "_informacao_rede"),
            (
                "private-User-Equipment-Info_private-User-Equipment-Info-Type",
                "_info_imei",
            ),
            ("private-User-Equipment-Info_private-User-Equipment-Info-Value", "_imei"),
            ("list-of-subscription-ID", "_info_imsi"),
            ("serviceReasonReturnCode", "_resultado_chamada"),
            ("user-Agent-Value", "agente_usuario"),
        ),
        job_description="Extraindo CDR Parquet: SMP Huawei VoLTE TIM",
    ),
    "smp_ericsson_volte_vivo": CDRParquetSchema(
        name="LTE Ericsson Vivo",
        column_mapping=(
            ("networkCallReference", "referencia"),
            ("imsChargingIdentifier", "referencia_sip"),
            ("callModule", "_tipo_chamada"),
            ("callingPartyNumber", "_numero_origem_original"),
            ("calledPartyNumber", "numero_destino"),
            ("chargeableDurat", "duracao"),
            ("dateForStartOfCharge", "_data"),
            ("timeForStartOfCharge", "_hora"),
            ("timeForStopOfCharge", "_hora_fim"),
            ("incomingRoute", "rota_entrada"),
            ("outgoingRoute", "rota_saida"),
            ("exchangeIdentity", "bilhetador"),
            ("firstCallingAccessNetInf", "_informacao_rede_origem"),
            ("firstCallingLocInf", "celula_origem_hex"),
            ("firstCalledAccessNetInf", "_informacao_rede_destino"),
            ("firstCalledLocInfo", "celula_destino_hex"),
            ("callingSubscriberIMEI", "imei_origem"),
            ("callingSubscriberIMSI", "imsi_origem"),
            ("calledSubscriberIMEI", "imei_destino"),
            ("calledSubscriberIMSI", "imsi_destino"),
            ("callPosition", "_resultado_chamada"),
        ),
        job_description="Extraindo CDR Parquet: SMP Ericsson VoLTE Vivo",
    ),
}
