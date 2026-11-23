---
name: documentar-modelo-de-dados
description: Gera ou atualiza o dicionário dos campos da tabela final de CDRs, identifica campos não implementados e referencia o documento no README.
user-invocable: true
---

# Documentar o modelo de dados

Crie ou atualize `docs/teleutils_modelo_de_dados.md` para quem consulta a tabela
final de CDRs. Documente **o que cada campo representa**, não o caminho percorrido
desde o CDR original. Produza um dicionário único, em português do Brasil, sem
seções por layout. Altere apenas o dicionário e sua referência no `README.md`;
não substitua `colunas.md` nem a documentação de linhagem.

Cada linha é um registro CDR e não necessariamente representa uma chamada
inteira. Descreva os campos relativos à comunicação como atributos do
**segmento da chamada**, sem presumir que um registro equivale a uma chamada.
Após a tabela, inclua literalmente esta definição como citação:

> **Segmento de chamada**: uma parte individualizada da chamada, representada por um registro CDR, quando uma única chamada pode gerar múltiplos registros referentes a partes distintas da comunicação.


## Identificar os campos finais

Parta da seleção efetivamente usada antes da gravação pelo módulo
`src/teleutils/core/transformers/`. Na implementação atual,
`CDRBaseTransformer._select_transformed_columns`, em `base_transformer.py`,
percorre `TARGET_SCHEMA`: para cada coluna de origem, seleciona o **alias de
destino**. Extraia esses nomes na ordem da seleção; se a implementação mudar,
siga o contrato vigente, não uma lista copiada de documentação anterior.

Inclua também campos nulos em determinados layouts e campos usados para
particionar a saída, desde que façam parte da seleção final. Não inclua campos
brutos, intermediários ou acrescentados posteriormente pelo carregador.

## Escrever e atualizar o dicionário

Use o título `# Modelo de dados dos CDRs`, uma introdução breve e uma tabela
com exatamente estas duas colunas:

```markdown
| Coluna final | Descrição |
| --- | --- |
| `alias_final` | |
```

Substitua a linha ilustrativa por uma linha para cada nome final real.

- Escreva uma frase curta sobre o significado do campo. Procure evidência na
  implementação, nos schemas, nos testes ou na documentação compatível com o
  código atual. Não generalize para todos os CDRs uma regra de um único layout.
- Para campos sem implementação explícita nos layouts e transformadores ativos
  do projeto, use exatamente `Não implementado: reservado para implementação futura.`
  como descrição. Verifique os schemas, os transformadores e o pipeline comum:
  a inclusão no contrato, o cast final e o preenchimento genérico de coluna
  ausente não são, por si só, implementação do conteúdo do campo. Um campo
  implementado em algum layout não é não implementado apenas por estar ausente
  ou nulo em outro.
- Para um campo implementado cujo significado não tenha evidência suficiente,
  **deixe a célula de descrição vazia** para preenchimento pelo usuário, como
  no exemplo. Não deduza o significado ou o estado de implementação pelo
  nome, prefixo ou tipo. Se não for possível verificar o estado de
  implementação, não o classifique como não implementado: registre a incerteza
  no relatório final.
- Ao atualizar, associe as descrições existentes pelo nome final, não pela
  posição da linha. Preserve o texto preenchido pelo usuário para campos
  implementados que continuam no contrato, mesmo que não encontre outra fonte.
  Para campos comprovadamente não implementados, use a frase definida acima.
  Se houver conflito com o código ou o estado for incerto, sinalize-o ao
  usuário em vez de substituir o texto silenciosamente.
- Acrescente novos campos e remova da tabela os que já não pertencem ao
  contrato. Não transfira descrições entre aliases por semelhança de nomes.

## Validar e referenciar

1. Compare a sequência de nomes da tabela com a seleção final: mesma grafia,
   ordem e quantidade, sem campos ausentes, extras ou duplicados. Revise as
   descrições adicionadas, as classificações de implementação, as lacunas
   editáveis, a terminologia de segmento e a definição final.
2. Na tabela **Documentação técnica** do `README.md`, inclua ou atualize uma
   única entrada `[Modelo de dados dos CDRs](docs/teleutils_modelo_de_dados.md)`.
   Verifique se o caminho relativo resolve para o documento gerado e preserve
   as demais referências, inclusive a de linhagem.
3. Execute `git diff --check`. Informe quantos campos foram documentados,
   quais não estão implementados, quais aguardam descrição, eventuais conflitos
   ou estados incertos e as validações realizadas.
