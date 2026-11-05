---
name: auditar-apis-publicas
description: Auditoria de consistência dos arquivos __init__.py e das APIs públicas do projeto Python.
user-invocable: true
---
# Auditoria de consistência dos arquivos `__init__.py` e das APIs públicas

Você é um engenheiro de software sênior especializado em Python, arquitetura de pacotes, organização de bibliotecas, gerenciamento de dependências e boas práticas de manutenção de APIs públicas.

## 1. Objetivo

Realize uma auditoria completa de todos os arquivos `__init__.py` do projeto, verificando a consistência, a integridade e a organização das exportações de módulos, classes, funções, constantes e demais símbolos.

O objetivo é garantir que os pacotes Python apresentem uma **API pública coerente, previsível, bem organizada e compatível com a arquitetura efetivamente implementada**.

A análise deverá considerar o código real do projeto, suas dependências internas, os padrões de importação existentes e a documentação disponível.

**Nesta etapa, realize exclusivamente análise, diagnóstico e planejamento. Não modifique nenhum arquivo.**

## 2. Escopo da auditoria

Localize e analise recursivamente todos os arquivos `__init__.py` pertencentes ao código-fonte do projeto.

Desconsidere ambientes virtuais, diretórios de dependências externas, caches, artefatos de build e outros diretórios que não integrem o código-fonte mantido pelo projeto.

Para cada arquivo identificado:

1. Determine a responsabilidade do pacote.
2. Identifique os módulos e subpacotes existentes.
3. Identifique os símbolos definidos e potencialmente exportáveis.
4. Analise as importações realizadas pelo `__init__.py`.
5. Identifique a API pública efetivamente disponibilizada.
6. Verifique a utilização desses símbolos nos demais arquivos do projeto.
7. Compare a implementação com as convenções adotadas nos demais pacotes.

Não presuma que todo símbolo definido em um módulo deva ser exportado pelo respectivo `__init__.py`.

## 3. Critérios de análise

### 3.1. Consistência das exportações

Verifique:

- Símbolos públicos relevantes que deveriam ser exportados, mas não são.
- Símbolos exportados desnecessariamente.
- Exportações duplicadas ou redundantes.
- Referências a símbolos inexistentes ou removidos.
- Importações que apontam para módulos incorretos.
- Reexportações desnecessárias entre diferentes níveis da hierarquia.
- Inconsistências na disponibilização de classes, funções e constantes equivalentes.
- Diferenças de organização entre pacotes com responsabilidades semelhantes.

Considere sempre a finalidade e o uso efetivo de cada pacote.

### 3.2. Utilização de `__all__`

Avalie:

- Presença ou ausência de `__all__`.
- Coerência entre `__all__` e os símbolos efetivamente importados ou definidos.
- Entradas inexistentes, duplicadas ou desatualizadas.
- Símbolos públicos omitidos de `__all__`.
- Ordem e organização dos nomes exportados.
- Consistência do padrão entre os pacotes.

Observe que `__all__` controla principalmente as exportações de `from pacote import *` e não impede, por si só, o acesso direto a outros atributos.

Não recomende adicionar `__all__` indiscriminadamente. Avalie sua utilidade considerando a arquitetura e as convenções do projeto.

### 3.3. Hierarquia e organização da API pública

Analise se as exportações estão disponíveis nos níveis apropriados da hierarquia de pacotes.

Por exemplo, considere estruturas como:

```python
from pacote.subpacote import MinhaClasse
```

e:

```python
from pacote import MinhaClasse
```

Determine quando a reexportação no pacote superior é realmente desejável.

Priorize:

- Clareza da API pública.
- Encapsulamento das implementações internas.
- Previsibilidade dos caminhos de importação.
- Baixo acoplamento.
- Facilidade de manutenção.
- Compatibilidade com os consumidores existentes.

Evite recomendar que todos os símbolos sejam automaticamente reexportados no pacote raiz.

### 3.4. Dependências e importações circulares

Investigue possíveis problemas relacionados a:

- Importações circulares diretas ou indiretas.
- Dependências desnecessárias entre pacotes.
- Importações executadas antecipadamente sem necessidade.
- Efeitos colaterais provocados pela inicialização dos pacotes.
- Dependências opcionais carregadas obrigatoriamente.
- Importações que aumentem excessivamente o acoplamento entre componentes.

Identifique as cadeias de importação envolvidas e diferencie riscos teóricos de problemas efetivamente demonstrados.

Considere importações tardias (*lazy imports*) apenas quando houver justificativa técnica concreta.

### 3.5. Compatibilidade com o código existente

Pesquise os usos das APIs públicas em todo o repositório, incluindo:

- Código de produção.
- Scripts.
- Testes automatizados.
- Exemplos.
- Notebooks, quando disponíveis.
- Documentação técnica.
- README.

Identifique caminhos de importação que possam ser afetados por alterações sugeridas.

Classifique como potencialmente incompatíveis as mudanças que removam ou alterem caminhos públicos existentes.

Não considere um símbolo desnecessário apenas porque não foi encontrada uma referência interna: a biblioteca pode possuir consumidores externos.

### 3.6. Convenções e documentação

Avalie:

- Padronização da organização dos arquivos `__init__.py`.
- Legibilidade e agrupamento das importações.
- Consistência das nomenclaturas.
- Documentação do propósito dos pacotes.
- Docstrings de módulo, quando apropriadas.
- Compatibilidade entre API pública e documentação existente.
- Aplicação coerente das convenções Python e das recomendações relevantes da PEP 8.

Toda documentação proposta deverá utilizar **português do Brasil (pt-BR)**.

Evite comentários redundantes e docstrings que apenas repitam o nome do pacote.

## 4. Metodologia

Execute a auditoria na seguinte sequência:

**Etapa 1 — Inventário**

Localize todos os arquivos `__init__.py` e identifique a estrutura hierárquica dos pacotes.

**Etapa 2 — Mapeamento das exportações**

Identifique os símbolos atualmente exportados, seus módulos de origem e os respectivos níveis de exposição.

**Etapa 3 — Análise de utilização**

Pesquise as importações realizadas pelos consumidores internos, distinguindo acessos diretos aos módulos de importações feitas pelas APIs dos pacotes.

**Etapa 4 — Análise de dependências**

Investigue a estrutura das importações e possíveis dependências circulares ou efeitos colaterais.

**Etapa 5 — Avaliação de consistência**

Compare os padrões adotados entre os diferentes pacotes e identifique divergências que mereçam correção.

**Etapa 6 — Validação**

Quando o ambiente permitir, proponha ou execute verificações não destrutivas para confirmar a resolução dos imports.

Não instale dependências nem altere a configuração do ambiente sem autorização.

**Etapa 7 — Plano de correção**

Consolide os problemas identificados e proponha as modificações necessárias, priorizando segurança e compatibilidade.

## 5. Restrições obrigatórias

- Não altere a lógica de negócio.
- Não refatore os módulos internos.
- Não renomeie classes, funções ou constantes.
- Não mova arquivos ou diretórios.
- Não altere assinaturas de funções e métodos.
- Não modifique arquivos fora do escopo sem justificativa explícita.
- Não remova exportações públicas existentes sem analisar seu impacto.
- Não introduza importações circulares.
- Não adicione dependências externas.
- Não faça mudanças exclusivamente estéticas sem benefício identificável.
- Não considere ausência de `__all__` um erro automaticamente.
- Não invente símbolos, módulos ou contratos de API.
- Não execute alterações nesta fase.

**Princípio fundamental:** preservar o comportamento atual e a compatibilidade da biblioteca, recomendando apenas correções justificadas por evidências encontradas no código.

## 6. Formato do relatório

Apresente os resultados organizados nas seguintes seções.

### 6.1. Resumo executivo

Descreva a situação geral das APIs públicas, os principais problemas encontrados e o nível de consistência da organização atual.

### 6.2. Inventário dos pacotes

Apresente uma tabela contendo:

| Pacote | Arquivo `__init__.py` | Símbolos exportados | `__all__` | Situação |
|---|---|---|---|---|
| ... | ... | ... | ... | ... |

### 6.3. Problemas identificados

Para cada problema, informe:

- **Identificador:** referência única.
- **Arquivo:** caminho completo relativo ao repositório.
- **Localização:** linha ou trecho relevante.
- **Categoria:** exportação, organização, dependência, compatibilidade ou documentação.
- **Severidade:** crítica, alta, média ou baixa.
- **Descrição:** comportamento ou inconsistência identificada.
- **Evidência:** referências concretas ao código.
- **Impacto:** consequências reais ou potenciais.
- **Recomendação:** correção sugerida.
- **Compatibilidade:** risco de afetar consumidores existentes.

Distinga claramente problemas confirmados, riscos potenciais e oportunidades opcionais de melhoria.

### 6.4. Proposta de padronização

Apresente recomendações para estabelecer uma convenção consistente de organização dos arquivos `__init__.py`.

A convenção deve refletir a estrutura existente, sem impor padronizações artificiais.

### 6.5. Plano de alterações

Organize as correções propostas por prioridade:

1. Correções necessárias para resolver erros reais.
2. Correções de inconsistências que afetam a API pública.
3. Melhorias de organização e manutenção.
4. Ajustes opcionais de documentação.

Identifique eventuais dependências entre as alterações.

### 6.6. Estratégia de validação

Proponha verificações para garantir que:

- Os caminhos públicos de importação existentes continuam funcionando.
- Os símbolos declarados em `__all__` podem ser resolvidos.
- As exportações recomendadas apontam para os objetos corretos.
- Não surgiram importações circulares.
- A importação dos pacotes não produz efeitos colaterais inesperados.
- Os testes existentes permanecem válidos.

### 6.7. Conclusão

Informe se os arquivos `__init__.py` estão adequados ou se necessitam de ajustes.

Diferencie correções obrigatórias de melhorias opcionais.

## 7. Resultado esperado

Ao final, entregue:

1. Um diagnóstico completo da organização atual.
2. Um inventário das APIs públicas.
3. Uma relação de inconsistências comprovadas.
4. Uma proposta de padronização compatível com a arquitetura do projeto.
5. Um plano objetivo de correção e validação.

**Não implemente as alterações antes da revisão e aprovação do plano.**

Evite sugerir mudanças por mera preferência estilística. Toda recomendação deve possuir uma justificativa técnica verificável.
