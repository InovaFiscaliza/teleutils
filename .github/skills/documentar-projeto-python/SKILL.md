---
name: documentar-projeto-python
description: Analisa, documenta, reorganiza e valida a documentação técnica de projetos Python. Infere a arquitetura, as responsabilidades, as dependências e a ordem lógica dos componentes a partir do código. Use ao criar, revisar, atualizar ou reorganizar documentação técnica, referências de API, docstrings e README.
argument-hint: "[escopo, módulo ou documentação a revisar]"
user-invocable: true
---

# Documentar Projeto Python

## Objetivo

Analisar, criar, reorganizar, atualizar e validar a documentação técnica de projetos Python.

A skill deve ser reutilizável em projetos de diferentes domínios, portes, arquiteturas, estruturas de diretórios, frameworks e ferramentas.

Seu objetivo é produzir documentação tecnicamente correta, organizada, consistente e alinhada à implementação efetiva.

**Princípio fundamental: descobrir a arquitetura do projeto antes de decidir como documentá-lo.**

Não impor uma estrutura arquitetural ou uma sequência documental predefinida. A organização deve refletir as responsabilidades, dependências e fluxos efetivamente identificados no projeto.

## 1. Escopo

Esta skill destina-se exclusivamente a projetos desenvolvidos em Python.

Pode ser aplicada, entre outros, a:

- Bibliotecas e pacotes reutilizáveis.
- Aplicações e serviços.
- APIs e sistemas distribuídos.
- Pipelines ETL e processamento de dados.
- Projetos de ciência de dados e machine learning.
- Ferramentas de linha de comando.
- Scripts e automações.
- Aplicações web e desktop.
- Sistemas de integração e processamento distribuído.

Não pressupor a utilização de linguagens, frameworks, bibliotecas ou ferramentas específicas além do ecossistema Python.

Quando uma tecnologia estiver presente, considerar suas particularidades sem transformar suas convenções em requisitos universais.

## 2. Princípios obrigatórios

### 2.1. Código como fonte de verdade

A implementação efetiva é a principal referência para documentar o comportamento do projeto.

Consultar, conforme necessário:

1. Código-fonte e implementação.
2. Testes automatizados e exemplos executáveis.
3. Configurações e contratos explicitamente definidos.
4. Documentação técnica existente.
5. Comentários, docstrings e materiais complementares.

Aplicar essa hierarquia com discernimento. Um contrato formal ou requisito de negócio pode representar o comportamento esperado, mesmo quando a implementação diverge dele.

Quando houver divergências:

- Identificar e registrar a inconsistência.
- Não alterar silenciosamente a documentação para ocultar o problema.
- Não presumir que a implementação representa necessariamente o comportamento desejado.
- Solicitar esclarecimentos quando a divergência envolver decisões de negócio ou contratos não verificáveis.

Não inventar funcionalidades, responsabilidades, dependências ou comportamentos.

### 2.2. Arquitetura descoberta, não presumida

Não impor:

- Estrutura de diretórios.
- Quantidade ou nomes de camadas.
- Sequência fixa de módulos.
- Padrões arquiteturais específicos.
- Organização obrigatória de documentos.
- Nomes padronizados para arquivos de documentação.

Estruturas como `src/`, `tests/`, `docs/`, `core/`, `services/`, `models/` e `utils/` são apenas exemplos.

A organização deve ser inferida a partir das evidências encontradas no repositório.

### 2.3. Preservação do escopo

A finalidade desta skill é documentar, não modificar o software.

Não alterar código-fonte, testes, configurações, dependências, contratos ou comportamento da aplicação durante uma tarefa exclusivamente documental.

Se uma correção no código for necessária para validar ou documentar corretamente algum comportamento, registrar a necessidade e solicitar autorização antes de realizá-la.

### 2.4. Fidelidade e rastreabilidade

Toda afirmação técnica relevante deve ser sustentada por evidências identificáveis no projeto.

Distinguir claramente:

- Comportamento implementado.
- Comportamento especificado.
- Comportamento observado nos testes.
- Comportamento esperado, mas ainda não implementado.
- Hipóteses ou pontos que dependem de confirmação.

## 3. Processo de trabalho

Seguir as etapas abaixo. A profundidade da análise pode variar conforme o tamanho e a complexidade do projeto, mas nenhuma etapa relevante deve ser ignorada.

### Etapa 1 — Reconhecimento do projeto

Identificar a raiz do repositório e compreender sua organização.

Examinar, quando existentes:

- Arquivos e diretórios Python (`.py`).
- Pacotes e subpacotes.
- Módulos e pontos de entrada.
- Classes, funções e métodos.
- Arquivos de configuração.
- Gerenciamento de dependências e ambientes.
- Testes automatizados.
- Scripts de execução e manutenção.
- Configurações de build e distribuição.
- Documentação existente.
- Arquivos de automação e integração contínua.

Considerar, conforme encontrados:

- `pyproject.toml`.
- `setup.py` e `setup.cfg`.
- Arquivos `requirements.txt`.
- Arquivos de lock.
- `__init__.py`.
- `__main__.py`.
- Configurações de ferramentas de testes, lint e formatação.
- Configurações de documentação.
- Arquivos de instruções para agentes, como `AGENTS.md`.

Não presumir que todos esses arquivos existam ou sejam relevantes.

Identificar também instruções locais, convenções do repositório e eventuais restrições sobre arquivos que não podem ser modificados.

### Etapa 2 — Análise arquitetural

Antes de organizar ou escrever a documentação, investigar como o projeto funciona.

Identificar:

1. Componentes principais e suas responsabilidades.
2. Módulos, pacotes e subpacotes.
3. Relações de dependência entre os componentes.
4. Fluxos de chamadas e execução.
5. Fluxos de dados, quando existentes.
6. Pontos de entrada e mecanismos de inicialização.
7. Interfaces públicas e integrações externas.
8. Componentes auxiliares e ferramentas independentes.
9. Testes que evidenciem contratos e comportamentos.
10. Dependências externas relevantes.

Utilizar imports, chamadas, assinaturas, configurações, testes e demais evidências disponíveis.

Não inferir responsabilidades exclusivamente pelos nomes de arquivos, diretórios ou classes.

#### 2.1. Inferência da ordem lógica

Construir uma representação conceitual da arquitetura e, a partir dela, inferir a ordem mais adequada para apresentar os componentes na documentação.

A organização deve favorecer uma compreensão progressiva do projeto.

Considerar, conforme aplicável:

- Conceitos fundamentais e requisitos.
- Componentes que fornecem funcionalidades básicas.
- Componentes que dependem de outros módulos internos.
- Transformações e processamento.
- Coordenação e orquestração.
- Integrações e persistência.
- Interfaces externas e pontos de utilização.
- Recursos auxiliares, utilitários e ferramentas complementares.

Esses elementos são critérios de análise, não uma sequência obrigatória.

A ordem final deve ser específica para o projeto analisado.

**Não confundir:**

- Ordem de apresentação com ordem de execução.
- Dependência de importação com sequência temporal de processamento.
- Dependência direta com dependência indireta.
- Existência de um componente com sua efetiva participação no fluxo principal.
- Organização física dos arquivos com arquitetura lógica.

Quando houver fluxos independentes ou subsistemas distintos, documentá-los em seções próprias, sem forçar uma sequência linear artificial.

Quando uma relação arquitetural não puder ser determinada com segurança, registrar a ambiguidade e solicitar esclarecimento antes de documentá-la como fato.

#### 2.2. Classificação dos componentes

Classificar os componentes de acordo com suas responsabilidades reais.

A classificação pode incluir, quando pertinente:

- Componentes fundamentais.
- Componentes de domínio.
- Componentes de processamento.
- Componentes de integração.
- Componentes de infraestrutura.
- Interfaces públicas.
- Mecanismos de configuração.
- Ferramentas auxiliares.
- Scripts administrativos.
- Recursos de desenvolvimento e testes.

Não é necessário utilizar todas essas categorias.

Módulos auxiliares não devem ser tratados como componentes centrais apenas por estarem próximos deles na estrutura de diretórios.

### Etapa 3 — Diagnóstico da documentação existente

Localizar e analisar a documentação existente.

Avaliar:

- Cobertura funcional e técnica.
- Correspondência com o código atual.
- Organização e sequência de apresentação.
- Clareza das responsabilidades.
- Consistência terminológica.
- Duplicações e contradições.
- Exemplos e instruções de utilização.
- Referências e links internos.
- Informações desatualizadas.
- Documentos especializados e protegidos.

Classificar os documentos conforme sua finalidade:

| Categoria | Tratamento |
|---|---|
| Documentação geral | Avaliar e atualizar quando necessário |
| Referência técnica/API | Avaliar e atualizar quando aplicável |
| Guias de instalação e utilização | Avaliar e atualizar quando necessário |
| Documentação de arquitetura | Preservar ou atualizar conforme o escopo |
| Documentação de testes | Consultar e atualizar somente se estiver no escopo |
| Documentação de linhagem | Consultar, mas não modificar |
| Documentos explicitamente protegidos | Não modificar sem autorização |
| Materiais externos ou gerados automaticamente | Respeitar as instruções e o processo de geração |

A classificação não deve depender de nomes específicos de arquivos. Considerar conteúdo, finalidade e instruções existentes.

#### 3.1. Proteção de documentos

Antes de editar qualquer documento:

- Verificar se existem instruções explícitas de proteção.
- Identificar documentos com manutenção independente.
- Verificar se o documento é gerado automaticamente.
- Respeitar regras específicas do repositório.
- Não substituir documentos especializados por conteúdo genérico.

Se houver dúvida sobre a possibilidade de alterar um documento, não modificá-lo até obter esclarecimento.

### Etapa 4 — Planejamento das alterações

Com base na análise arquitetural e no diagnóstico documental, elaborar um plano.

O planejamento deve identificar:

- Documentos que precisam ser criados.
- Documentos que precisam ser atualizados.
- Documentos que precisam ser reorganizados.
- Documentos que devem permanecer intactos.
- Estrutura documental proposta.
- Ordem lógica de apresentação dos componentes.
- Informações que precisam de confirmação.
- Validações necessárias.

A estrutura proposta deve aproveitar a organização existente sempre que ela for adequada.

Não criar documentos apenas para preencher uma estrutura predefinida.

#### 4.1. Apresentação prévia

Antes de realizar alterações estruturais significativas, apresentar uma síntese contendo:

1. Arquitetura identificada.
2. Componentes principais e suas responsabilidades.
3. Relações e fluxos relevantes.
4. Ordem documental proposta.
5. Documentos que serão criados ou modificados.
6. Documentos protegidos ou fora do escopo.
7. Ambiguidades que dependem de confirmação.

Solicitar esclarecimentos quando houver decisões relevantes que não possam ser tomadas com segurança a partir das evidências.

Não solicitar confirmação para decisões meramente editoriais ou para informações já demonstradas pelo código.

### Etapa 5 — Atualização da documentação técnica

Executar as alterações previstas no planejamento.

Priorizar:

- Precisão técnica.
- Organização arquitetural coerente.
- Clareza das responsabilidades.
- Consistência terminológica.
- Ausência de redundâncias.
- Exemplos corretos.
- Referências válidas.
- Facilidade de navegação.

#### 5.1. Organização dos documentos

A documentação deve apresentar o projeto em uma sequência que facilite sua compreensão.

Quando houver uma referência técnica geral, organizar seus capítulos e seções conforme a arquitetura inferida.

Quando houver múltiplos documentos especializados, utilizar referências cruzadas para evitar duplicação de conteúdo.

A documentação deve distinguir, quando aplicável:

- Visão geral do projeto.
- Arquitetura e componentes.
- Interfaces e contratos.
- Instalação e configuração.
- Utilização e exemplos.
- Operação e manutenção.
- Testes e validação.

Essas seções podem estar em um único documento ou distribuídas em vários arquivos, conforme o porte e a organização do projeto.

#### 5.2. Referência de API

Avaliar se o projeto possui uma API pública ou interfaces técnicas que justifiquem uma referência específica.

Quando aplicável, documentar:

- Pacotes e módulos públicos.
- Classes, funções e métodos.
- Responsabilidades.
- Assinaturas.
- Parâmetros e tipos.
- Valores de retorno.
- Exceções.
- Efeitos colaterais.
- Dependências e requisitos.
- Exemplos de utilização.

Considerar `__all__`, exports e mecanismos de exposição pública quando existentes.

Distinguir elementos públicos de implementações internas.

Não presumir que todos os módulos devam ser expostos ou documentados como API pública.

Se o projeto não possuir uma API pública, documentar suas interfaces e componentes relevantes de acordo com sua natureza, sem criar artificialmente uma referência de API.

#### 5.3. Docstrings e convenções Python

Ao documentar código Python, respeitar as convenções já adotadas pelo projeto.

Considerar:

- Google Style.
- NumPy Style.
- Sphinx Style.
- Outros padrões identificados.

Não impor um padrão novo sem necessidade ou autorização.

Quando o escopo incluir docstrings:

- Documentar módulos, classes, funções e métodos relevantes.
- Explicar responsabilidades e comportamentos.
- Documentar parâmetros, tipos, retornos e exceções.
- Registrar efeitos colaterais e pré-condições relevantes.
- Preservar assinaturas, lógica, type hints e imports.
- Não inventar informações ausentes.
- Evitar docstrings redundantes que apenas repitam o nome do objeto.

A atualização de docstrings não autoriza refatorações ou mudanças no comportamento do código.

#### 5.4. Exemplos de utilização

Os exemplos devem ser coerentes com a implementação atual.

Quando possível, validar os exemplos utilizando os mecanismos de teste ou execução disponíveis.

Considerar:

- Imports corretos.
- Assinaturas reais.
- Configurações necessárias.
- Dependências.
- Entradas e saídas.
- Tratamento de erros.
- Restrições de execução.

Não apresentar pseudocódigo como se fosse um exemplo executável.

Quando um exemplo for ilustrativo e não puder ser executado diretamente, identificá-lo como tal.

### Etapa 6 — Atualização do README

O README é o ponto de entrada da documentação do projeto.

Atualizá-lo somente depois de consolidar a documentação técnica.

O README deve apresentar, conforme aplicável:

- Nome e objetivo do projeto.
- Descrição resumida.
- Funcionalidades principais.
- Requisitos.
- Instalação.
- Configuração inicial.
- Instruções essenciais de execução.
- Exemplo básico de utilização.
- Links para a documentação complementar.

Evitar reproduzir explicações técnicas extensas que já estejam disponíveis em documentos especializados.

O README deve refletir a organização e as instruções efetivamente documentadas.

Não criar instruções de instalação ou execução que não possam ser sustentadas pela configuração do projeto.

### Etapa 7 — Validação final

Após atualizar a documentação técnica e o README, realizar uma revisão completa.

#### 7.1. Validação técnica

Verificar:

- Correspondência entre documentação e implementação.
- Correção de nomes de módulos, classes e funções.
- Assinaturas e parâmetros.
- Tipos e valores de retorno.
- Exceções e efeitos colaterais.
- Dependências e requisitos.
- Exemplos de utilização.
- Fluxos e responsabilidades documentados.
- Ausência de relações arquiteturais inventadas.

#### 7.2. Validação estrutural

Verificar:

- Coerência da ordem lógica adotada.
- Organização dos documentos.
- Consistência dos títulos.
- Links e referências internas.
- Ausência de seções duplicadas.
- Formatação Markdown.
- Navegação entre documentos.
- Coerência entre README e documentação técnica.

#### 7.3. Validação de escopo

Confirmar que:

- Apenas os documentos previstos foram alterados.
- Documentos protegidos permaneceram intactos.
- A documentação de linhagem não foi modificada.
- Nenhum código, teste ou configuração foi alterado sem autorização.
- Não foram criados documentos desnecessários.

Quando houver ferramentas de validação documental disponíveis, utilizá-las conforme as convenções do projeto.

Não executar testes ou comandos potencialmente destrutivos sem avaliar seus efeitos.

## 4. Documentação de linhagem de dados

A documentação de linhagem de dados possui tratamento especial e está fora do escopo de manutenção desta skill.

Quando existir documentação de linhagem:

- Consultá-la quando for relevante para compreender o projeto.
- Utilizá-la como fonte complementar para compreender origens, transformações e destinos dos dados.
- Considerar as informações documentadas ao avaliar os fluxos do projeto.
- Comparar suas informações com a implementação quando isso fizer parte da análise.
- Registrar divergências ou indícios de desatualização no relatório final.

**Não criar, editar, reorganizar ou atualizar automaticamente documentos de linhagem de dados.**

Caso a documentação de linhagem pareça desatualizada ou apresente inconsistências com o código, informar o problema e solicitar uma decisão específica.

Não incorporar silenciosamente informações de linhagem a outros documentos como forma de contornar essa restrição.

## 5. README e documentação complementar

O README deve ser tratado como ponto de entrada, não como substituto da documentação técnica.

A documentação complementar deve ser organizada conforme as necessidades reais do projeto.

Ferramentas como DeepWiki podem ser utilizadas como recursos adicionais de navegação e compreensão, quando disponíveis.

Entretanto:

- Não considerar conteúdo gerado automaticamente como fonte de verdade.
- Não substituir a análise do código por informações externas.
- Não assumir que a documentação externa esteja atualizada.
- Utilizar referências externas apenas como apoio.
- Priorizar sempre as evidências do repositório.

## 6. Convenções de documentação

### 6.1. Idioma

Utilizar português brasileiro para a documentação, salvo quando o projeto possuir uma convenção explícita diferente ou houver solicitação específica.

Preservar nomes técnicos, identificadores, APIs e termos consagrados em inglês quando apropriado.

### 6.2. Estilo

Adotar linguagem técnica, objetiva e consistente.

Priorizar:

- Clareza.
- Precisão.
- Concisão.
- Organização hierárquica.
- Terminologia uniforme.
- Exemplos contextualizados.

Evitar explicações vagas, redundâncias e afirmações sem evidências.

### 6.3. Markdown

Respeitar a formatação e as convenções existentes.

Utilizar adequadamente:

- Títulos hierárquicos.
- Listas.
- Tabelas.
- Blocos de código.
- Links internos e externos.
- Avisos e observações quando necessários.

Não utilizar níveis de títulos de forma arbitrária. Manter hierarquia consistente e facilitar a navegação.

## 7. Restrições operacionais

Durante toda a execução:

1. Trabalhar dentro do escopo solicitado.
2. Respeitar as instruções do repositório e dos arquivos de orientação.
3. Não presumir que todos os documentos devam ser atualizados.
4. Não substituir conteúdo válido apenas para padronizar a redação.
5. Não remover informações técnicas sem justificativa.
6. Não alterar contratos ou comportamento do software.
7. Não modificar documentos protegidos.
8. Não criar documentação baseada em hipóteses não confirmadas.
9. Não afirmar que testes ou validações foram executados sem realmente executá-los.
10. Não declarar uma alteração como concluída sem verificar o resultado.

Quando o usuário solicitar apenas análise, diagnóstico ou planejamento, não realizar alterações nos arquivos.

Quando solicitar atualização documental, executar as alterações dentro do escopo autorizado, respeitando as etapas de análise e planejamento.

## 8. Resultado esperado

Ao concluir uma tarefa de documentação, apresentar um resumo objetivo contendo:

### Análise realizada
- Arquitetura identificada.
- Principais componentes e responsabilidades.
- Critério utilizado para definir a ordem lógica.

### Alterações realizadas
- Documentos criados.
- Documentos atualizados.
- Documentos reorganizados.
- Principais correções e melhorias.

### Validação
- Verificações realizadas.
- Exemplos ou referências validados.
- Resultado das validações.

### Pendências
- Divergências encontradas.
- Ambiguidades que dependem de confirmação.
- Documentos protegidos ou fora do escopo.
- Recomendações para trabalhos futuros.

Não apresentar como concluídas atividades que não foram executadas.

## 9. Critérios de aceitação

Uma tarefa de documentação será considerada concluída quando:

- A documentação refletir o comportamento efetivamente implementado.
- A arquitetura tiver sido analisada antes da organização documental.
- A ordem de apresentação dos componentes for coerente com as responsabilidades, dependências e fluxos identificados.
- Componentes principais e auxiliares estiverem organizados de forma compreensível.
- Interfaces e contratos relevantes estiverem documentados quando aplicável.
- O README estiver consistente com a documentação técnica.
- Documentos protegidos e especializados tiverem sido respeitados.
- Referências, exemplos e links tiverem sido verificados dentro das possibilidades do ambiente.
- Nenhuma alteração fora do escopo autorizado tiver sido realizada.
- Pendências e limitações estiverem explicitamente registradas.

**A qualidade da documentação deve ser avaliada pela fidelidade à implementação e pela facilidade de compreensão do projeto, não pela quantidade de arquivos ou pela adoção de uma estrutura documental padronizada.**
