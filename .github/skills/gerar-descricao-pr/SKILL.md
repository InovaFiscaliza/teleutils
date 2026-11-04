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

## 5. Redigir uma descrição técnica detalhada

Produza uma descrição abrangente, adequada à revisão técnica.
Não reduza o resultado a um resumo executivo.

Use o diff para explicar as mudanças efetivamente entregues e os commits
para complementar o contexto e calcular as estatísticas do intervalo.

Respeite o template obrigatório do repositório, quando existir, incorporando
nele as informações abaixo. Na ausência de template, siga exatamente esta
estrutura e ordem de seções.

### Título do PR

Use Conventional Commits:

`<type>[escopo opcional][! se incompatível]: <descrição resumida>`

Escolha o tipo pelo objetivo principal do PR, sem tentar representar
todos os tipos de alteração no título.

### Estrutura obrigatória do corpo

# Descrição Geral

Explique:

- O problema ou a necessidade que motivou o PR.
- O objetivo e o resultado das alterações.
- Os principais eixos de implementação, em lista numerada.
- A branch de origem, a branch de destino e o intervalo analisado.

Apresente o seguinte resumo quantitativo, calculado a partir do Git:

- Commits no intervalo.
- Arquivos alterados.
- Linhas adicionadas e removidas.
- Distribuição dos commits por tipo de Conventional Commits.

Para calcular:

- Conte os commits alcançáveis em HEAD e ausentes na branch de destino.
- Obtenha arquivos e linhas do diff entre o ancestral comum e HEAD.
- Classifique os commits pelos prefixos de suas mensagens.
- Conte commits de merge separadamente.
- Agrupe mensagens sem prefixo reconhecível como “não classificados”.

Diferencie a classificação das mensagens de commit da classificação
semântica das mudanças identificadas no diff.

Não invente números nem use estimativas sem explicar a limitação.
Se uma métrica não puder ser calculada, informe “não disponível”
e explique brevemente o motivo.

# Alterações Organizadas por Tipo

Crie subseções para os tipos identificados:

## feat
## fix
## refactor
## perf
## docs
## test
## chore / build / ci
## style

Inclua somente subseções com alterações relevantes.

Em cada subseção:

- Detalhe as mudanças em itens separados por responsabilidade.
- Identifique funcionalidades, módulos, contratos ou fluxos afetados.
- Explique o comportamento resultante e sua finalidade.
- Apresente diferenças entre antes e depois quando ajudarem a revisão.
- Cubra todos os grupos relevantes de mudanças encontrados no diff.

Não limite cada categoria a uma frase genérica.
Não transcreva o histórico de commits.
Não repita a mesma mudança em diferentes categorias.

Não classifique simplificações de código como melhorias de desempenho
sem evidências. Diferencie otimização pretendida de ganho medido.

# Principais Arquivos ou Módulos Afetados

Liste os arquivos ou módulos relevantes, usando caminhos relativos
ao repositório.

Para cada item, explique concretamente:

- Sua responsabilidade.
- O que foi alterado.
- O impacto relevante para o restante do projeto.

Agrupe arquivos com a mesma responsabilidade quando isso melhorar
a leitura. Evite listar individualmente arquivos gerados ou alterações
puramente mecânicas.

# Validação

Separe:

- Testes adicionados ou modificados e os cenários cobertos.
- Verificações efetivamente executadas e seus resultados.
- Verificações pendentes e limitações de cobertura.

Inclua comandos e evidências quando disponíveis.

Se não houver evidência de execução, declare:
“Testes não executados nesta análise.”

Não confunda existência de testes com execução bem-sucedida.
Não presuma resultados de CI ou homologação.

# Breaking Changes

Para cada incompatibilidade confirmada, use:

BREAKING CHANGE: <descrição da incompatibilidade>

Explique:

- O contrato ou comportamento anterior.
- O novo contrato ou comportamento.
- Os consumidores afetados.
- A adaptação ou migração necessária.

Não trate uma reorganização interna como incompatibilidade sem verificar
se ela afeta interfaces ou comportamentos consumidos externamente.

Se nenhuma incompatibilidade for identificada, escreva:
“Não foram identificadas mudanças incompatíveis na análise realizada.”

# Riscos e Pontos de Atenção

Descreva riscos concretos associados ao diff, incluindo, quando aplicável:

- Compatibilidade de APIs, imports, configurações e contratos de dados.
- Mudanças de comportamento em fluxos existentes.
- Migrações, dependências e requisitos operacionais.
- Cenários relevantes ainda não validados.
- Divergências entre a implementação e os critérios da issue.

Diferencie problemas confirmados de riscos potenciais.
Explique o possível impacto e a verificação recomendada.
Evite alertas genéricos que poderiam se aplicar a qualquer PR.

Se não houver pontos específicos, informe isso sem garantir ausência
de riscos ou regressões.

# Issue Relacionada

Inclua somente referências confirmadas.

Use `Closes #<número>` quando a implementação resolver integralmente
a issue e o fechamento automático for apropriado.

Use `Refs #<número>` quando a implementação for parcial ou quando
não for possível confirmar o atendimento integral.

Se nenhuma issue estiver identificada, escreva:
“Issue não informada.”

# Checklist

Construa um checklist adequado às mudanças, considerando:

- Revisão do código.
- Execução dos testes relevantes.
- Atualização da documentação, quando necessária.
- Avaliação de compatibilidade, quando aplicável.
- Atendimento aos critérios da issue, quando disponíveis.
- Conformidade do título proposto com Conventional Commits.

Marque `[x]` somente quando houver evidência de conclusão.
Mantenha `[ ]` para itens pendentes ou não verificados.
Omita itens que não se aplicam.

A leitura do diff para escrever o PR não equivale a uma revisão
completa do código nem comprova ausência de regressões.

# Observações para Revisão Final

Liste ações concretas ainda necessárias antes do merge,
como validação funcional, execução de testes ou alinhamento
de contratos com a issue.

Se não houver ações adicionais identificadas, informe isso
brevemente.

## 6. Regras de saída e atuação

- Entregue uma descrição técnica detalhada, seguindo a estrutura
  da seção 5. Priorize informações concretas e evite redundância.
- Apresente o título e o corpo em dois blocos de código separados,
  ambos identificados como `markdown`.
- O primeiro bloco deve conter somente o título, em uma única linha,
  sem o prefixo “Título” e sem marcação de cabeçalho.
- O segundo bloco deve conter o corpo completo em Markdown,
  pronto para ser copiado e colado diretamente no campo de descrição
  de um Pull Request no GitHub.
- Fora dos blocos, use apenas os rótulos “Título do PR” e
  “Descrição do PR”. Informe antes dos blocos qualquer limitação
  que impeça uma descrição definitiva.
- Não inclua placeholders, instruções de preenchimento ou comentários
  sobre o processo de geração dentro do conteúdo copiável.
- Descreva o estado final das mudanças, sem narrar tentativas abandonadas.
- Não exponha credenciais, tokens ou dados sensíveis encontrados no diff.
- Gere somente a proposta: não crie, publique ou edite o PR remoto.
- Não altere arquivos, faça commits ou modifique o histórico Git.
