---
name: gerar-descricao-pr
description: Gera título e descrição de Pull Request a partir do diff da branch atual, dos commits e da issue relacionada. Use quando o usuário pedir para preparar, escrever ou atualizar uma mensagem de PR em qualquer projeto.
user-invocable: true
---

# Gerar descrição de Pull Request

Produza um título e uma descrição de Pull Request claros, objetivos e
fundamentados nas alterações reais do projeto.

Adapte o conteúdo à linguagem, à arquitetura e às convenções do repositório.
Não presuma uma tecnologia ou estrutura específica.

## 1. Identificar o contexto

Antes de escrever:

- Leia as instruções aplicáveis do repositório, incluindo AGENTS.md,
  CONTRIBUTING.md e instruções do GitHub Copilot, quando existirem.
- Procure templates de Pull Request e respeite seus campos obrigatórios.
- Identifique a branch atual e a branch de destino.
- Leia a issue relacionada quando seu conteúdo estiver disponível.
- Identifique convenções de idioma, título, escopo e referências a issues.

Use português do Brasil quando não houver outra preferência definida.

Não invente requisitos, números de issues, decisões ou critérios de aceite.
Se uma informação não estiver acessível, registre a limitação quando ela
afetar a descrição.

## 2. Delimitar as alterações do PR

Determine a branch de destino nesta ordem:

1. Branch informada pelo usuário.
2. Branch de destino de um PR existente para a branch atual.
3. Branch padrão identificada nos metadados do repositório ou do remoto.

Não presuma que a branch de destino seja main ou master.

Se não for possível identificar a base com segurança, solicite somente
essa informação antes de produzir a descrição definitiva.

Compare as alterações versionadas da branch atual com seu ancestral
comum com a branch de destino.

Exemplos de comandos de leitura, após resolver a referência de destino:

- `git status --short`
- `git merge-base <base> HEAD`
- `git diff --stat <base>...HEAD`
- `git diff --name-status <base>...HEAD`
- `git diff <base>...HEAD`
- `git log --oneline <base>..HEAD`

Substitua `<base>` pela referência identificada.
Não use o último PR aceito ou o último commit de merge como marco padrão.

Considere:

- O diff como fonte principal do comportamento final.
- Os commits como contexto complementar.
- Os arquivos e seus consumidores para entender impactos relevantes.
- Renomeações, exclusões, configurações, dependências e documentação.

Alterações locais não commitadas não integram o PR por padrão.
Informe sua existência separadamente e só as inclua na proposta se o
usuário solicitar.

Se houver histórico insuficiente, referências ausentes ou diff incompleto,
explique a limitação. Não apresente uma análise parcial como completa.

Se não houver alterações no intervalo analisado, informe isso e não
invente uma descrição.

## 3. Analisar e classificar

Identifique:

- O problema resolvido ou a melhoria entregue.
- O comportamento anterior e o comportamento resultante.
- As mudanças relevantes para usuários, integradores e mantenedores.
- A relação com a issue e os critérios de aceite disponíveis.
- Limitações ou requisitos ainda não atendidos.

Classifique as mudanças conforme o conteúdo real:

- feat: nova funcionalidade.
- fix: correção de comportamento incorreto.
- refactor: reorganização interna sem mudança de comportamento.
- perf: melhoria de desempenho.
- docs: documentação.
- test: testes.
- build: dependências, empacotamento ou construção.
- ci: integração ou entrega contínua.
- chore: manutenção que não se enquadre nas categorias anteriores.
- style: formatação sem alteração de comportamento.

Use somente as categorias presentes.
Não replique a lista de commits nem descreva cada linha alterada.

Escolha o tipo do título pelo objetivo principal do PR.
Use um escopo reconhecível no projeto quando ele acrescentar precisão.

## 4. Verificar evidências

Diferencie explicitamente:

- Testes adicionados ou modificados.
- Testes efetivamente executados.
- Resultados observados.
- Verificações pendentes.

Não afirme que testes passaram, que o código foi revisado ou que não há
regressões sem evidências disponíveis.

A existência de testes ou de uma configuração de CI não comprova execução.
Se não houver evidência de execução, escreva:
“Testes não executados nesta análise.”

Não execute testes apenas para preencher a descrição, salvo quando
solicitado pelo usuário ou exigido pelas instruções aplicáveis.

Identifique mudanças incompatíveis quando houver evidências, incluindo
alterações em APIs, contratos de dados, configurações e comportamento
público. Explique o impacto e a adaptação necessária.

Descreva riscos concretos relacionados às mudanças. Não acrescente
alertas genéricos ou benefícios de desempenho não demonstrados.

## 5. Redigir o título e a descrição

Respeite primeiro o template do repositório.
Na ausência de template, use a estrutura abaixo.

### Título

Use Conventional Commits:

`<type>[escopo opcional][! se incompatível]: <descrição resumida>`

Escreva um título específico e coerente com o objetivo principal.
Não adicione emojis, salvo quando a convenção do projeto exigir.

### Corpo

#### Resumo

Explique o problema e o resultado da mudança em um ou dois parágrafos.
Quando útil, apresente um exemplo breve de antes e depois.

#### Alterações

Agrupe as mudanças relevantes por tipo de Conventional Commits.
Inclua apenas categorias presentes e evite repetir o resumo.

#### Arquivos ou módulos relevantes

Inclua esta seção somente quando ajudar a revisão.
Liste os componentes principais e a responsabilidade de cada mudança.
Não reproduza o inventário completo do diff.

#### Validação

Informe as verificações realizadas e seus resultados.
Inclua comandos quando disponíveis e úteis para reprodução.
Registre testes não executados, falhas e limitações conhecidas.

#### Compatibilidade e migração

Inclua somente quando aplicável.

Para mudanças incompatíveis, use:

`BREAKING CHANGE: <impacto e adaptação necessária>`

Descreva migrações, novas configurações ou ações manuais necessárias.

#### Riscos e pontos de atenção

Inclua somente riscos concretos, limitações ou aspectos que mereçam
atenção especial dos revisores.

#### Issue relacionada

Inclua somente referências confirmadas.

Use `Closes #<número>` apenas quando o PR resolver integralmente a issue
e o fechamento automático for apropriado. Para relação parcial, use
`Refs #<número>`.

#### Checklist

Inclua quando exigido pelo template ou quando houver pendências úteis.
Marque `[x]` somente com evidência; use `[ ]` para pendências.
Não inclua afirmações vagas como “sem regressões aparentes”.

## 6. Regras de saída e atuação

- Entregue o título separadamente do corpo, para copiar nos campos do GitHub.
- Apresente o corpo em Markdown pronto para colar.
- Não inclua comentários sobre o processo de geração dentro da descrição.
- Dimensione o texto à complexidade do PR.
- Omita seções opcionais vazias e textos de preenchimento.
- Descreva o resultado final, sem narrar tentativas abandonadas.
- Não exponha credenciais, tokens ou dados sensíveis encontrados no diff.
- Gere somente a proposta: não crie, publique ou edite o PR remoto.
- Não altere arquivos, faça commits ou modifique o histórico Git.
