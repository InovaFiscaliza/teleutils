"""Pacote core: pipeline de extração e transformação de CDRs.

Reúne os subpacotes responsáveis por converter arquivos brutos de CDR (Call
Detail Records), em diferentes formatos e layouts de fornecedores, para o
schema intermediário e final utilizado pelo projeto.

Subpacotes:
    - teleutils.core.extractors: leitura e padronização inicial de CDRs
      (texto/CSV e Teleparser) para um formato intermediário comum.
    - teleutils.core.transformers: normalização de dados intermediários
      (datas, números telefônicos, autenticação) para o contrato final.
"""
