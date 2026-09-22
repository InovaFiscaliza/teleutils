---
description: Analisa o pipeline atual e gera ou atualiza a documentação de linhagem e transformação dos dados.
---

Utilize a skill `skill-documentar-linhagem-dados`.

Analise o estado atual do código do projeto e gere ou atualize a documentação de linhagem e transformação dos dados.

Considere o código atual como fonte da verdade, exceto o módulo robocalls que está depreciado.

A análise deve:

1. identificar todas as colunas do resultado final;
2. rastrear cada coluna retroativamente;
3. identificar colunas de origem;
4. identificar colunas extraídas;
5. identificar colunas intermediárias;
6. identificar transformações;
7. identificar funções auxiliares;
8. diferenciar transformações de renomeações;
9. identificar múltiplos caminhos;
10. registrar ambiguidades.

Antes de concluir, valide que nenhuma coluna final ficou sem documentação.

Gere ou atualize a documentação exclusivamente no arquivo: docs/teleutils_linhagem_transformacoes_dados.md

Não altere o código.
