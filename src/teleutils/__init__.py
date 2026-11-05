"""Pacote teleutils: utilitários para processamento de CDRs de telecomunicações brasileiras.

Este pacote reúne ferramentas para extração, transformação e análise de
registros de detalhes de chamadas (CDR) provenientes de diferentes prestadoras
e fornecedores de tecnologia de telecomunicações brasileiras.

Principais subpacotes:
    - teleutils.core: pipeline de extração e transformação de CDRs para o
      contrato de dados padronizado do projeto.
    - teleutils.preprocessing: funções utilitárias de normalização e validação
      (números telefônicos, CNPJ).
    - teleutils.robocalls: extração, transformação e análise de CDRs voltada à
      detecção de padrões de chamadas abusivas (robocalls).

Notes:
    Este pacote não reexporta classes ou funções e não registra ponto de
    entrada de linha de comando. As APIs devem ser importadas dos respectivos
    subpacotes.
"""
