---
name: documentar-modulo-aberto
description: Documenta o módulo Python atualmente aberto no editor, sem alterar sua lógica de negócio.
---

Utilize a skill `skill-documentar-modulo-python`.

Analise cuidadosamente o módulo Python atualmente aberto no editor e atualize sua documentação conforme as instruções da skill.

Considere o código atual e os componentes diretamente relacionados, quando necessários para compreensão, como fonte da verdade.

A documentação deve refletir exclusivamente o comportamento efetivamente implementado.

Não altere a lógica de negócio, o comportamento funcional, as assinaturas, os type hints ou qualquer instrução executável.

Limite as alterações a:

* docstrings;
* comentários explicativos;
* anotações de manutenção previstas pela skill.

Não modifique outros módulos. Eles podem ser analisados apenas quando necessário para compreender corretamente o módulo atualmente aberto.

Antes de concluir, valide que o módulo aberto foi documentado de acordo com todos os critérios definidos na skill `documentar-modulo-python`.
