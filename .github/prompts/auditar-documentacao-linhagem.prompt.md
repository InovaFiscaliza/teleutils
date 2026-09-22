---
description: Audita a documentação de linhagem de dados comparando-a de forma independente com o código atual.
---

Utilize a skill `skill-auditar-linhagem-dados`.

Realize uma auditoria independente da documentação atual de linhagem e transformação dos dados.

Utilize o código atual como fonte da verdade, exceto o módulo robocalls que está depreciado.

A documentação existente não deve orientar a investigação inicial.

Execute obrigatoriamente:

```text
Código
↓
Análise independente
↓
Reconstrução da linhagem
↓
Identificação das colunas finais
↓
Comparação com a documentação
↓
Relatório de auditoria
```

Verifique especialmente:

* colunas finais omitidas;
* origens incorretas;
* colunas intermediárias omitidas;
* transformações não documentadas;
* casts omitidos;
* funções auxiliares omitidas;
* múltiplas origens;
* caminhos condicionais;
* renomeações confundidas com transformações;
* relações assumidas apenas pela semelhança dos nomes.

Não altere o código.

Não reescreva automaticamente a documentação.

Produza o relatório completo de auditoria definido pela skill.

Gere ou atualize a documentação exclusivamente no arquivo: docs/auditar_linhagem_transformacoes_dados.md
