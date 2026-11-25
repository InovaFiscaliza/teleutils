---
name: documentar-projeto
description: Revisa e atualiza a documentação técnica do projeto, incluindo a API pública e o README, após confirmar a atualização da linhagem e classificar os documentos com o usuário.
user-invocable: true
---

# Documentar o projeto

Analise cuidadosamente o estado atual do repositório e mantenha atualizada a documentação técnica e principal do projeto.

## Escopo de execução

Antes de qualquer alteração, confirme com o usuário a atualização prévia da documentação de linhagem, conforme a Etapa 0. Sem confirmação positiva, interrompa a execução sem alterar arquivos.

Analise o estado atual do repositório, exceto módulos depreciados, e execute o fluxo definido por esta skill. Documentos com prefixo `deprecated_` seguem integralmente a regra geral dessa categoria.

Atualize primeiro a documentação técnica aplicável, incluindo obrigatoriamente a documentação da API pública.

Após validar a documentação técnica, atualize o `README.md`, mantendo-o como porta de entrada do projeto e evitando duplicação desnecessária da documentação especializada.

A documentação de linhagem e transformação de dados deve ser apenas consultada, sem criação, renomeação ou revisão. A única alteração permitida é a remoção pontual de referências a documentos `deprecated_`.

Não altere o código-fonte, testes ou arquivos de configuração.

Esta skill é responsável por:

1. analisar o estado atual do projeto;
2. identificar a documentação técnica necessária;
3. criar ou atualizar a documentação técnica em `docs/`;
4. criar ou atualizar a documentação da API pública;
5. validar a documentação técnica;
6. atualizar o `README.md` somente após a documentação técnica estar atualizada;
7. manter referências consistentes entre o `README.md` e a documentação técnica;
8. preservar documentação existente que continue correta e relevante.

A documentação deve refletir fielmente o estado atual do projeto.

---

# Objetivo

Produzir uma estrutura de documentação sustentável que permita:

* a novos usuários compreenderem o propósito da solução;
* novos usuários instalarem e executarem o projeto;
* desenvolvedores compreenderem a arquitetura e os principais componentes;
* usuários localizarem rapidamente a documentação técnica necessária;
* desenvolvedores consultarem a API pública sem precisar inspecionar inicialmente toda a implementação;
* a documentação permanecer consistente com a evolução do código.

A documentação deve ser organizada em camadas.

```text
Código, testes e configurações
              ↓
     Documentação técnica
       docs/teleutils_*.md
              ↓
         README.md
   Porta de entrada do projeto
```

O código atual é a fonte prioritária da verdade.

A documentação técnica deve ser produzida ou atualizada antes do `README.md`.

O README deve ser atualizado após a documentação técnica estar consolidada.

---

# Escopo

Esta skill gerencia:

* `README.md`;
* documentação técnica em `docs/`;
* documentação da API pública;
* referências entre o README e a documentação técnica.

A skill não deve:

* alterar código-fonte;
* alterar comportamento funcional;
* refatorar módulos;
* alterar testes;
* alterar arquivos de configuração;
* inventar funcionalidades;
* inventar interfaces;
* inventar comportamentos;
* inventar links ou referências.

---

# Fontes da verdade

Utilize as informações disponíveis seguindo, em caso de conflito, a seguinte ordem de prioridade:

1. código-fonte atual;
2. comportamento efetivamente implementado;
3. testes automatizados atuais;
4. arquivos de configuração atuais;
5. interfaces e contratos atuais;
6. type hints;
7. docstrings;
8. histórico Git disponível;
9. commits e merges disponíveis;
10. Pull Requests acessíveis;
11. documentação técnica existente;
12. `README.md` existente.

Em caso de conflito entre o código atual e qualquer documentação ou informação histórica, o código atual prevalece.

Essa prioridade se aplica à descrição do comportamento atual. Registros históricos, resultados de testes e decisões de compatibilidade devem ser avaliados conforme sua data, contexto e evidências próprias; uma implementação diferente hoje não torna falso um relato sobre o passado.

A documentação existente pode ser utilizada para:

* identificar conteúdo que deve ser preservado;
* identificar documentos existentes;
* manter contexto;
* corrigir referências;
* identificar possíveis inconsistências.

Ela não deve prevalecer sobre o comportamento atual implementado.

---

# Idioma

Toda explicação produzida ou substancialmente atualizada por esta skill deve ser escrita em português brasileiro (`pt-BR`), sem exigir tradução de conteúdo histórico ou protegido que deva ser preservado.

Utilize inglês apenas quando necessário para:

* comandos de terminal;
* nomes de módulos;
* nomes de classes;
* nomes de funções;
* nomes de parâmetros;
* nomes de bibliotecas;
* mensagens produzidas pelo próprio código;
* termos técnicos cuja tradução prejudique a precisão.

Toda explicação, descrição e orientação deve ser escrita em português.

## Idioma dos exemplos de código

Em todos os exemplos de código (README, API e demais documentos), os identificadores criados pelo exemplo devem ser escritos em inglês, em `snake_case` para variáveis e funções:

* nomes de variáveis (`intermediate`, `parquet_extractor`, `result`);
* nomes de funções, métodos e classes definidos no exemplo;
* nomes de parâmetros e atributos definidos no exemplo;
* nomes de alias de importação.

Não traduza nem altere identificadores que pertencem ao projeto ou a bibliotecas (módulos, classes, funções, métodos, parâmetros, campos e valores aceitos pela interface), mesmo que estejam em português: o exemplo deve refletir a interface real.

Valores literais (caminhos, nomes de arquivos, mensagens) e comentários explicativos acompanham o idioma da documentação, a menos que o usuário peça outro idioma. Não use nomes que misturem idiomas (por exemplo, `parquet_extrator`).

Ao atualizar um exemplo existente, renomeie as variáveis em português para inglês e atualize, na mesma edição, as menções a elas no texto ao redor.

---

# Público-alvo

Assuma que o leitor pode:

* nunca ter tido contato com o projeto;
* não possuir acesso aos desenvolvedores;
* não conhecer o domínio de negócio;
* não conhecer a arquitetura;
* não conhecer a estrutura do repositório;
* não ter participado do desenvolvimento.

Não assuma conhecimento implícito sobre:

* dependências;
* variáveis de ambiente;
* comandos internos;
* estrutura de diretórios;
* formatos de entrada;
* formatos de saída;
* etapas de processamento;
* convenções específicas do projeto.

As instruções necessárias para instalação e utilização devem ser explícitas.

---

# Fluxo obrigatório de execução

Execute as etapas abaixo na ordem indicada.

## Etapa 0 — Confirmar a atualização da linhagem

Antes de renomear, criar, editar ou remover qualquer arquivo, pergunte ao usuário se `docs/teleutils_linhagem_transformacoes_dados.md` foi atualizada para o estado atual do projeto.

* Se a resposta for positiva, prossiga; a confirmação do usuário não constitui auditoria independente nem comprova automaticamente a atualidade.
* Se a resposta for negativa, incerta, indisponível ou não houver confirmação, interrompa sem alterar arquivos e oriente a executar `documentar-linhagem-dados` antes de uma nova tentativa.

## Etapa 1 — Analisar o estado atual do projeto

Analise, conforme aplicável:

* código-fonte;
* estrutura do repositório;
* pacotes;
* subpacotes;
* módulos;
* interfaces públicas;
* classes;
* funções;
* type hints;
* docstrings;
* testes;
* arquivos de configuração;
* pontos de entrada;
* comandos CLI;
* pipelines;
* formatos de entrada e saída;
* histórico Git disponível.

Identifique:

### Funcionalidades

* novas funcionalidades;
* módulos relevantes;
* classes públicas;
* funções públicas;
* comandos CLI;
* fluxos principais.

### Alterações de comportamento

* mudanças de parâmetros;
* mudanças de retorno;
* alterações nos formatos de entrada;
* alterações nos formatos de saída;
* alterações de compatibilidade;
* alterações relevantes de configuração.

### Alterações arquiteturais

* reorganização de diretórios;
* extração ou criação de componentes;
* criação ou alteração de camadas;
* criação ou alteração de pipelines;
* mudanças de responsabilidades;
* refatorações relevantes.

### Melhorias relevantes

* performance;
* escalabilidade;
* extensibilidade;
* padronização;
* manutenção;
* usabilidade.

Quando não for possível determinar com segurança o histórico de alterações, não interrompa a tarefa.

Utilize o estado atual do projeto como referência.

---

## Etapa 2 — Identificar a documentação técnica necessária

Antes de atualizar qualquer conteúdo do `README.md`, determine:

* quais documentos existem em `docs/`, por nome e categoria, inclusive os protegidos e os não classificados;
* quais documentos estão desatualizados;
* quais documentos precisam ser criados;
* quais interfaces públicas precisam ser documentadas;
* quais fluxos justificam documentação especializada;
* quais informações são extensas demais para o README;
* quais documentos técnicos devem ser referenciados posteriormente pelo README.

Não crie documentos apenas para aumentar a quantidade de documentação.

Crie documentos especializados apenas quando houver conteúdo técnico real e relevante que justifique sua existência.

Não crie documentos:

* vazios;
* genéricos;
* especulativos;
* redundantes.

---

# Regras para a pasta `docs/`

Inventarie os documentos por nome antes de decidir o tratamento. O prefixo identifica a categoria, mas não substitui as exceções abaixo:

* `teleutils_`: documentação do projeto; pode ser revisada e atualizada e, quando relevante, referenciada no README.
* `internal_`: documentação interna; pode ser consultada e atualizada, mas não deve ser listada automaticamente no README.
* `deprecated_`: documento obsoleto; preserve seu conteúdo e não o consulte nem revise na documentação geral. Não o liste ou referencie em outros documentos. Uma execução posterior deve reconhecê-lo como já classificado.

### Documentos não classificados

Para cada documento sem um desses três prefixos, pergunte ao usuário qual opção aplicar:

1. `teleutils_`: incorporar ao projeto, renomear, revisar e atualizar;
2. `internal_`: renomear como interno, consultar e atualizar quando aplicável;
3. `deprecated_`: renomear como obsoleto, preservar o conteúdo e remover suas referências dos demais documentos, conforme as regras abaixo;
4. preservar sem classificar.

Sem escolha afirmativa, preserve o nome e o conteúdo e registre a pendência. Não infira uma categoria. A única exceção de conteúdo é a remoção pontual de referências a documentos `deprecated_`.

Não peça classificação da documentação de linhagem ou do relatório de auditoria: eles seguem as regras especiais desta skill.

### Renomeações e referências

Para uma escolha de categoria, acrescente o prefixo escolhido ao nome existente, preservando o restante do nome e a extensão, e converta-o em snake_case. Antes de renomear, verifique se o destino existe e identifique as referências ao nome e ao caminho anteriores; não sobrescreva arquivos nem apague ou altere conteúdo para realizar a renomeação.

Atualize referências de documentos `teleutils_` e `internal_` renomeados apenas nos arquivos autorizados a serem editados. Reporte referências em arquivos fora do escopo. Se uma colisão ou referência impedir uma migração segura, interrompa a operação afetada e peça orientação ao usuário.

Para cada documento `deprecated_`, procure referências aos caminhos e nomes anteriores e atuais e remova-as dos demais documentos, incluindo README, documentos não classificados, linhagem e relatório de auditoria. Nesses arquivos protegidos, a autorização se limita à remoção pontual dessas referências; não autoriza revisão, outras alterações de conteúdo ou renomeação.

Preserve também o conteúdo de outros documentos `deprecated_`, sem inspecioná-los para procurar referências. Reporte referências entre eles que sejam conhecidas e registre que as demais não foram verificadas. Não altere código-fonte, testes ou configurações para remover referências; identifique e reporte essas ocorrências. Quando uma referência não puder ser identificada com segurança ou removida sem perda indevida de contexto, interrompa sua remoção, peça orientação e registre a pendência. Não declare a remoção completa enquanto houver pendências ou referências não verificadas.

### Documentos especializados protegidos

A documentação de linhagem é tratada pela skill `documentar-linhagem-dados`, com a mesma exceção pontual. O relatório no caminho de saída definido pela skill `auditar-linhagem-dados` pertence a essa skill: não o revise, renomeie ou liste em outros documentos, inclusive no README. Apenas remova dele referências a `deprecated_`. Não crie novas referências ao relatório; remova referências existentes apenas em documentos autorizados para essa finalidade e reporte as demais. A referência operacional ao caminho de saída na própria skill de auditoria é necessária e não constitui listagem documental.

Não crie links para arquivos inexistentes.

---

# Etapa 3 — Criar ou atualizar a documentação técnica

Antes de alterar o `README.md`, crie ou atualize toda a documentação técnica necessária.

A documentação técnica deve refletir:

* o estado atual do código;
* as interfaces atuais;
* os fluxos atuais;
* os formatos atuais;
* as responsabilidades atuais dos componentes.

Preserve conteúdo existente que continue:

* correto;
* atual;
* relevante;
* útil.

Corrija ou remova apenas informações:

* comprovadamente obsoletas;
* incorretas;
* incompatíveis com o código atual;
* redundantemente prejudiciais.

Não reescreva documentação adequada apenas para alterar sua redação. Antes de alterar o significado ou remover uma afirmação, identifique o conteúdo anterior, o motivo e a evidência que sustenta a mudança. Ausência de evidência não comprova obsolescência: preserve o conteúdo não confirmado e registre a dúvida.

Preserve condições e ressalvas relevantes ao resumir ou eliminar duplicações, especialmente limitações, erros, efeitos colaterais, segurança, concorrência, migração, exemplos e limites de testes. Mantenha registros históricos com data, ambiente, comandos efetivamente executados, resultados e limitações; acrescente informações atuais separadamente sem apresentar testes antigos como certificação atual. Registre alterações substantivas e informações não confirmadas no resultado final.

---

# Documentação da API pública

A documentação da API é obrigatória.

O arquivo principal deve ser:

```text
docs/teleutils_api.md
```

Se o arquivo não existir, ele deve ser criado.

Se existir, deve ser revisado e atualizado.

## Escopo da API

Documente as interfaces públicas relevantes do projeto.

Identifique, quando aplicável:

* pacotes públicos;
* subpacotes relevantes;
* módulos públicos;
* classes públicas;
* funções públicas;
* métodos públicos relevantes;
* pontos de entrada;
* comandos CLI;
* contratos de entrada;
* contratos de saída.

Não documente como API pública componentes exclusivamente internos.

Na ausência de outra convenção explícita:

* elementos iniciados por `_` devem ser considerados internos;
* elementos destinados exclusivamente à implementação interna não devem receber destaque como API pública.

## Pacotes e subpacotes

Para cada pacote ou subpacote público relevante, documente:

* responsabilidade;
* finalidade;
* contexto de utilização;
* principais componentes;
* relação com outros componentes.

## Módulos

Para cada módulo público relevante, documente:

* responsabilidade;
* finalidade;
* contexto de utilização;
* principais interfaces disponibilizadas;
* dependências relevantes para sua utilização, quando necessário.

## Classes

Para cada classe pública relevante, documente, quando aplicável:

* finalidade;
* responsabilidade;
* contexto de utilização;
* parâmetros de inicialização;
* atributos relevantes;
* principais métodos;
* estruturas manipuladas;
* exemplos de utilização.

## Funções

Para cada função pública relevante, documente:

* assinatura;
* finalidade;
* parâmetros;
* retorno;
* exceções relevantes;
* efeitos colaterais relevantes;
* exemplos de utilização, quando úteis.

## Métodos

Para cada método público relevante, documente:

* finalidade;
* parâmetros;
* retorno;
* efeitos colaterais relevantes;
* exceções relevantes;
* comportamento esperado.

## Precisão

As informações documentadas devem refletir:

1. o código atual;
2. os type hints;
3. os contratos efetivamente utilizados;
4. os testes atuais, quando relevantes.

Não invente:

* parâmetros;
* retornos;
* exceções;
* efeitos colaterais;
* comportamentos;
* exemplos.

Quando uma informação não puder ser determinada com segurança, não invente detalhes.

Registre uma limitação apenas quando ela for relevante para a compreensão ou utilização da interface.

## Nível de detalhe

A documentação da API deve permitir que um desenvolvedor compreenda e utilize as interfaces públicas sem precisar inicialmente inspecionar toda a implementação.

Não reproduza integralmente o código-fonte.

Priorize a documentação de:

* responsabilidades;
* contratos;
* entradas;
* saídas;
* comportamento;
* formas de utilização.

---

# Documentação técnica especializada

Além da API, crie ou atualize documentos especializados quando houver conteúdo técnico relevante que justifique sua existência.

## Arquitetura

Quando a complexidade do projeto justificar, crie ou atualize documentação de arquitetura.

Documente, quando aplicável:

* componentes;
* responsabilidades;
* camadas;
* dependências;
* relações entre módulos;
* fluxo principal;
* decisões arquiteturais confirmáveis.

Não documente decisões arquiteturais que não possam ser confirmadas.

# Linhagem e transformação de dados

A documentação de linhagem e transformação de dados é especializada e não faz parte do escopo de atualização desta skill.

Esta skill não deve criar, renomear ou revisar a documentação de linhagem de dados. A única alteração permitida é a remoção pontual de referências a documentos `deprecated_`.

Após a confirmação exigida na Etapa 0, esta skill pode consultá-la para:

* compreender o fluxo de processamento dos dados;
* manter consistência com a documentação técnica geral;
* incluir referências para essa documentação no `README.md`, quando aplicável.

Caso seja identificada alguma possível inconsistência entre o código e a documentação de linhagem, registre-a no resultado final para revisão pela skill especializada. Não afirme que a linhagem foi validada independentemente apenas com base na confirmação do usuário.


## Configuração

Crie ou atualize documentação específica de configuração quando a complexidade ou extensão das configurações justificar sua separação do README.

## Guias especializados

Crie documentação especializada para fluxos complexos quando eles não puderem ser adequadamente explicados no README sem torná-lo excessivamente extenso.

---

# Etapa 4 — Validar a documentação técnica

Após criar ou atualizar a documentação técnica e antes de modificar o `README.md`, valide:

* os documentos refletem o código atual;
* as interfaces públicas relevantes estão documentadas;
* a API está atualizada;
* os documentos especializados possuem conteúdo real e relevante;
* não existem funcionalidades inventadas;
* não existem comportamentos inventados;
* os links internos adicionados ou alterados apontam para destinos existentes;
* as referências a documentos `deprecated_` foram removidas conforme as regras, ou as pendências foram registradas;
* as categorias, decisões do usuário e exceções para documentos protegidos foram respeitadas;
* a documentação técnica não possui duplicações desnecessárias.

Confirme obrigatoriamente que:

```text
docs/teleutils_api.md
```

existe e está atualizado.

Somente após validar a documentação técnica editável prossiga para o README. Divergências em documentos protegidos ou outras pendências que não possam ser resolvidas sem ultrapassar o escopo devem ser relatadas, sem declarar validada toda a documentação.

---

# Etapa 5 — Criar ou atualizar o README.md

Atualize o `README.md` somente após concluir a atualização e validação da documentação técnica.

O README é a porta de entrada principal do projeto.

Ele deve permitir que uma pessoa sem conhecimento prévio consiga:

1. compreender o propósito da solução;
2. entender o problema que ela resolve;
3. identificar os principais componentes;
4. conhecer os requisitos;
5. instalar o projeto;
6. configurar o ambiente;
7. executar os principais fluxos;
8. verificar a instalação;
9. localizar a documentação técnica detalhada.

O README deve utilizar:

* o código atual como fonte prioritária da verdade;
* a documentação técnica recém-validada para garantir consistência e fornecer navegação.

O README não deve duplicar desnecessariamente o conteúdo técnico detalhado disponível em `docs/`.

---

# Princípios de redação do README

Estes princípios valem para qualquer projeto Python e prevalecem sobre a tendência de documentar tudo no README.

## Essencial, claro e visual

O README é uma **visão geral** que apresenta o projeto a quem nunca teve contato com ele. Deve conter apenas o estritamente essencial para compreender, instalar e usar o projeto, em linguagem clara e bem formatada, com o mínimo de jargão técnico.

Todo o restante pertence a `docs/` e deve ser acessado por link:

* contratos completos de funções, classes e parâmetros;
* regras internas, detalhes de implementação e comportamentos de borda;
* efeitos colaterais raros, políticas de consistência e tratamento detalhado de erros;
* resultados de testes e registros históricos ou datados.

Teste de inclusão: se o leitor consegue instalar e executar o fluxo sem a informação, ela não pertence ao README.

## Tabelas em vez de listas e parágrafos

Use tabelas sempre que houver três ou mais itens com atributos comparáveis. Em especial para:

* componentes e módulos (tipo, nome, função);
* pré-requisitos (requisito, detalhe);
* parâmetros, campos de configuração e opções (nome, obrigatório, descrição);
* comandos, scripts e classes equivalentes (nome, uso);
* variantes suportadas, como formatos, layouts, provedores ou backends (categoria, itens);
* documentação técnica (documento, descrição).

Use listas apenas para passos sequenciais e para ressalvas curtas (uma linha cada). Parágrafos devem ter no máximo 3 ou 4 linhas; explicações maiores devem ser quebradas em tabela, lista ou movidas para `docs/`.

## Principal antes do auxiliar

Apresente sempre primeiro os componentes **principais**, os que implementam o propósito central do projeto (identificados pelo código, pela descrição em `pyproject.toml` e pela documentação técnica), na ordem do fluxo de dados. Só depois apresente os componentes **auxiliares**: utilitários independentes, scripts de apoio e módulos acessórios.

Nunca intercale componentes auxiliares entre os principais.

## Conteúdo mínimo por exemplo

Cada exemplo deve ser o menor trecho que demonstra o uso real da interface. Declare uma vez as premissas comuns (por exemplo, uma sessão, cliente ou variável já criada) em vez de repeti-las em cada exemplo. Não inclua exemplos de variações raras, que ficam em `docs/`. Variáveis e nomes de funções definidos nos exemplos devem estar em inglês, conforme "Idioma dos exemplos de código".

---

# Conteúdo do README

Adapte o conteúdo à realidade do projeto.

Inclua apenas seções que possuam conteúdo real e relevante.

Use, nesta ordem, as seções abaixo que se aplicarem:

```text
Badge DeepWiki (primeira linha)
Título e introdução
Sumário
Visão geral (com a tabela "Componentes")
Início rápido (Pré-requisitos, Instalação, Verificação da instalação)
Como funciona (diagrama e passos)
Como usar (um item por componente principal e "Módulos auxiliares")
Catálogo de variantes suportadas (quando houver)
Estrutura do projeto
Documentação técnica
Desenvolvimento
Limitações conhecidas
```

Configuração, solução de problemas e compatibilidade entram como seção própria apenas quando tiverem conteúdo essencial que não caiba em "Início rápido" ou "Como usar".

Títulos de seção devem ser únicos, curtos e em português, para que as âncoras do sumário não colidam.

---

# Introdução e visão geral

A introdução (logo abaixo do título) tem no máximo três linhas: o que o projeto é, o problema que resolve e o resultado que entrega.

A seção "Visão geral" apresenta, em no máximo dois parágrafos curtos:

* o problema resolvido;
* o que o projeto faz, em uma frase por etapa;
* os usuários esperados e os cenários de utilização.

Em seguida, inclua uma subseção "Componentes" com uma tabela de colunas `Tipo`, `Módulo` e `Função`:

* `Tipo` recebe `Principal` ou `Auxiliar`;
* as linhas principais vêm primeiro, na ordem do fluxo de dados; as auxiliares, depois;
* `Módulo` usa o nome real do pacote, módulo ou diretório, em código;
* `Função` descreve a responsabilidade em uma frase.

Antes da tabela, uma frase deve explicar a distinção entre principais e auxiliares.

A explicação deve ser compreensível para uma pessoa sem conhecimento prévio do projeto.

---

# Sumário

Inclua um sumário navegável logo após a introdução.

O sumário deve apontar para todas as seções relevantes de nível 2 (`##`).

## Seções sem subseções

Devem aparecer como links simples, sem bullets.

Exemplo:

```markdown
[Visão Geral](#visão-geral)
```

## Seções com subseções

Podem utilizar blocos `<details>`.

Exemplo:

```html
<details>
<summary><a href="#início-rápido">Início Rápido</a></summary>

- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Verificação da Instalação](#verificação-da-instalação)

</details>
```

Exemplo com subseções de nível 4 (`####`) aninhadas em uma subseção de nível 3:

```html
<details>
<summary><a href="#como-usar">Como Usar</a></summary>

- [Processar dados](#processar-dados)
- [Módulos auxiliares](#módulos-auxiliares)
  - [Normalizar números](#normalizar-números)
  - [Transferir arquivos](#transferir-arquivos)

</details>
```

Regras:

* não utilizar bullets para itens de nível superior;
* utilizar bullets dentro dos blocos `<details>`;
* quando houver subseções de nível 4, listá-las como bullets indentados com dois espaços sob a subseção de nível 3 correspondente;
* manter uma linha em branco após `<summary>`;
* utilizar âncoras compatíveis com o GitHub;
* refletir os títulos efetivamente presentes no README;
* ao alterar a hierarquia ou os títulos, atualizar o sumário na mesma edição.

---

# Navegação

Insira:

```markdown
[⬆ Voltar ao topo](#sumário)
```

imediatamente antes de cada seção de nível 2 (`##`), exceto antes da própria seção `Sumário`.

Não insira automaticamente links de retorno antes de todas as subseções de nível 3.

Utilize links adicionais apenas quando agregarem valor à navegação.

Evite poluição visual.

---

# Início rápido

A seção deve permitir que um novo usuário instale e execute o projeto.

## Pré-requisitos

Documente apenas requisitos confirmáveis e apresente-os em uma tabela com as colunas `Requisito` e `Detalhe`, como:

* versões suportadas do Python;
* dependências do sistema operacional;
* softwares externos;
* ferramentas necessárias.

Indique em uma frase que as bibliotecas Python declaradas no projeto são instaladas automaticamente, sem listá-las uma a uma. Indique na tabela, quando aplicável, os requisitos exigidos apenas por componentes específicos.

Não invente versões mínimas.

## Instalação

Forneça um passo a passo baseado na estrutura real do projeto: um único bloco de comandos para o método principal e, no máximo, uma linha com a alternativa suportada.

Os comandos devem ser compatíveis com:

* o gerenciador de dependências utilizado;
* os arquivos de configuração existentes;
* o método de instalação suportado.

Não utilize comandos genéricos que não correspondam ao projeto.

## Verificação da instalação

Forneça um método realista para verificar a instalação, preferindo um comando leve que não dependa de serviços externos nem de componentes pesados.

Baseie-se em:

* comandos existentes;
* testes disponíveis;
* importações válidas;
* funcionalidades confirmadas.

Execute o comando e transcreva a saída real. Se não for possível executá-lo, não apresente uma saída esperada: descreva apenas o critério de sucesso confirmável pelo código.

---

# Como funciona

Apresente no README apenas uma visão de alto nível, composta por:

1. um diagrama Mermaid do fluxo principal;
2. uma lista numerada de 3 a 5 passos curtos, um por componente principal, na ordem do fluxo;
3. uma frase sobre qualquer comportamento global que afete o usuário (por exemplo, sobrescrita de saídas).

Regras do diagrama:

* use `flowchart LR` simples, sem blocos `config` ou opções de layout específicas de renderizadores;
* agrupe com `subgraph` a origem dos dados, os componentes principais e, quando útil, as saídas;
* represente dados e artefatos persistidos como nós de cilindro (`[( )]`) e componentes como retângulos;
* represente etapas, artefatos ou coordenações opcionais com setas tracejadas;
* use rótulos curtos, com quebra de linha `<br/>`;
* inclua somente componentes e relações confirmados pelo código;
* não crie diagramas artificiais nem inclua componentes auxiliares que não participam do fluxo principal;
* valide a sintaxe com uma ferramenta disponível (por exemplo, `mmdc`) ou, na ausência dela, revise-a manualmente e informe que a renderização não foi verificada.

Quando houver documentação detalhada em `docs/`, apresente uma visão resumida e inclua um link para o documento correspondente.

---

# Estrutura do projeto

Apresente a árvore principal de diretórios e arquivos relevantes em um bloco `text`, com um comentário curto (`#`) alinhado à direita em cada elemento.

Explique brevemente a finalidade de cada elemento apresentado.

A estrutura deve refletir o estado atual do repositório.

Não inclua:

* caches;
* arquivos temporários;
* artefatos gerados;
* diretórios sem relevância para usuários ou desenvolvedores.

Módulos depreciados só podem aparecer na árvore, marcados como depreciados, e nunca são documentados no README.

---

# Configuração

Documente as configurações necessárias para utilização inicial.

Considere, quando aplicável:

* variáveis de ambiente;
* arquivos de configuração;
* parâmetros obrigatórios;
* parâmetros opcionais;
* formatos aceitos.

Apresente-as em tabela (nome, obrigatório, descrição). Em geral, a configuração essencial fica junto do componente que a utiliza, em "Como usar", e não em seção própria.

Não invente configurações.

Quando a documentação for extensa, mantenha os detalhes em documento técnico específico e referencie-o.

---

# Como usar

A seção "Como usar" apresenta os principais fluxos de utilização identificados. Estruture-a assim:

1. **Introdução:** uma frase informando que os exemplos são ilustrativos (caminhos, nomes e ambiente devem ser ajustados), um link para a referência da API e a declaração única das premissas comuns dos exemplos (por exemplo, uma sessão ou cliente já criado em uma variável).
2. **Um item `###` por componente principal**, na ordem do fluxo de dados e com títulos orientados à ação (por exemplo, "Extrair…", "Transformar…", "Processar lotes…", "Carregar…"). Se o projeto tiver um componente que coordena os demais, apresente-o depois dos componentes que ele coordena.
3. **Um item `###` "Módulos auxiliares"**, depois de todos os principais, com uma frase de introdução e uma tabela (`Utilitário`, `Para que serve`). Cada auxiliar detalhado recebe uma subseção `####` abaixo dele. Omita o item se não houver auxiliares relevantes.

Cada item de componente deve conter, nesta ordem:

* uma ou duas frases sobre o que o componente faz;
* uma tabela, quando houver classes, funções, opções ou campos de configuração a distinguir;
* o exemplo mínimo;
* de 3 a 5 ressalvas essenciais em lista de uma linha cada (valor de retorno que exige verificação, metadados inferidos, sobrescrita de saídas, comportamento em caso de falha e similares), com link para a API quando houver mais detalhes.

Variações do uso principal (por exemplo, um modo sem etapa intermediária) ficam no mesmo item, depois do exemplo principal, com um segundo exemplo curto. Não use blocos `<details>` para conteúdo de uso: separe em itens ou mova para `docs/`.

Os exemplos devem refletir funcionalidades reais.

Quando possível, valide exemplos.

Não apresente exemplos como executados quando não tiverem sido executados.

Quando a validação não for possível, construa os exemplos exclusivamente a partir de interfaces confirmadas (assinaturas, nomes de métodos e parâmetros conferidos no código) e não afirme que foram executados.

Não inclua no README detalhes que pertençam à documentação técnica, como isolamento interno de sessões, consistência entre commits, validações lexicais e comportamentos de borda.

---

# Catálogo de variantes suportadas

Quando o projeto aceitar um conjunto relevante de variantes (formatos, layouts, provedores, backends, versões), inclua uma seção com uma tabela de colunas `Categoria` e `Itens`, com os nomes reais em código e agrupados por categoria, e indique onde o usuário informa a variante. Apresente a contagem total. Não repita atributos que já constem da documentação técnica.

---

# Documentação técnica complementar

Título da seção no README: "Documentação técnica".

Após concluir a documentação técnica, liste automaticamente no README apenas documentos relevantes existentes em `docs/` cujo nome comece com:

```text
teleutils_
```

Para cada documento relevante, apresente:

* título;
* breve descrição;
* link relativo.

Não duplique integralmente o conteúdo dos documentos.

Não liste automaticamente arquivos sem o prefixo `teleutils_`. Preserve links existentes relevantes para documentos internos ou não classificados, salvo quando houver motivo comprovado ou uma regra especial exigir sua remoção. Nunca liste o relatório de auditoria ou documentos `deprecated_`; remova suas referências conforme as regras de `docs/`.

Exemplo:

```markdown
| Documento | Descrição |
|---|---|
| [Referência da API](docs/teleutils_api.md) | Documentação das interfaces públicas do projeto. |
| [Arquitetura](docs/teleutils_arquitetura.md) | Descrição da arquitetura e dos principais componentes. |
| [Linhagem e Transformações dos Dados](docs/teleutils_linhagem_transformacoes_dados.md) | Origem, transformação e destino dos dados processados pelo projeto. |
```

A tabela deve conter apenas documentos realmente existentes.

---

# Desenvolvimento

Quando relevante, apresente de forma resumida, em um bloco de comandos curto seguido de uma ou duas frases:

* instalação das dependências de desenvolvimento;
* execução dos testes;
* ferramentas de qualidade, linters e formatadores configurados (confirmados nos arquivos de configuração).

Detalhes extensos podem permanecer em documentação técnica especializada.

---

# Solução de problemas

Inclua esta seção apenas quando houver problemas e soluções confirmáveis.

Organize, quando aplicável, em tabela com as colunas `Problema`, `Diagnóstico` e `Resolução`.

Não invente problemas ou soluções.

---

# Compatibilidade e limitações conhecidas

Documente apenas informações confirmáveis sobre:

* versões suportadas do Python;
* sistemas operacionais;
* dependências obrigatórias;
* ferramentas externas.

Não presuma compatibilidade.

Reúna as limitações conhecidas na seção final "Limitações conhecidas", com no máximo cinco itens de uma linha, cada um relevante para quem usa o projeto (por exemplo, ausência de transações, de retentativas ou de uma interface de linha de comando, e armadilhas de uso). Não inclua nela resultados de testes, relatos datados ou detalhes internos; mantenha-os em `docs/` com data e contexto.

---

# DeepWiki

Não consulte o DeepWiki nem utilize suas informações como fonte para análise,
documentação ou validação. Não acesse o site, a página do projeto ou o badge,
inclusive para verificar a existência ou disponibilidade do link.

No `README.md`, a única referência ao DeepWiki deve ser um badge na primeira
linha, no seguinte formato:

```markdown
[![Ask DeepWiki](./docs/deepwiki-badge.svg)](https://deepwiki.com/InovaFiscaliza/teleutils)
```

Substitua `<proprietário>` e `<repositório>` pelos identificadores reais do
repositório GitHub, identificados localmente pelo remoto Git ou por metadados
existentes do projeto, sem consultar o DeepWiki. Se não for possível identificar
o repositório com segurança, peça os identificadores ao usuário; não invente
valores nem grave os marcadores literalmente.

Preserve ou insira o badge correspondente ao atualizar o README. Não crie seção, explicação,
entrada no sumário ou outra referência ao DeepWiki no README. Corrija ou remova
referências adicionais ao DeepWiki no README sem alterar documentos protegidos.
Valide a posição e o texto do badge localmente, sem acessar seus URLs.

---

# Etapa 6 — Validação final

Após atualizar a documentação técnica e o README, valide a consistência completa:

```text
Código atual
      ↓
Documentação técnica
      ↓
README.md
```

Verifique:

* a documentação técnica corresponde ao código atual;
* a API está documentada;
* o README corresponde ao estado atual;
* os links adicionados ou alterados apontam para arquivos existentes;
* documentos internos ou não classificados não foram listados automaticamente;
* as escolhas do usuário, as categorias e as exceções de documentos protegidos foram respeitadas;
* o relatório de auditoria não foi listado em outros documentos;
* as referências a documentos `deprecated_` foram removidas quando autorizado e possível, e as pendências foram registradas;
* não existem funcionalidades inventadas;
* informações comprovadamente obsoletas foram tratadas dentro do escopo autorizado ou registradas como pendências;
* não existem duplicações desnecessárias;
* a documentação produzida ou atualizada está escrita em português brasileiro, preservando registros históricos e conteúdo protegido;
* o Markdown é válido;
* a navegação do README funciona corretamente, e cada âncora do sumário (níveis 2, 3 e 4) corresponde a um título existente, verificadas por script ou revisão;
* o README segue os "Princípios de redação": contém só o essencial, usa tabelas para componentes, pré-requisitos, parâmetros, variantes e documentos, e não contém detalhes técnicos que pertençam a `docs/`;
* os componentes principais aparecem antes dos auxiliares na tabela "Componentes" e em "Como usar";
* os comandos e exemplos foram executados ou, quando não foi possível, foram construídos a partir de interfaces conferidas no código e não foram apresentados como executados;
* as variáveis e os nomes de funções definidos nos exemplos estão em inglês e as menções a eles no texto foram atualizadas;
* o diagrama Mermaid segue as regras de "Como funciona" e teve a sintaxe validada ou a ausência de validação registrada;
* o badge DeepWiki está na primeira linha do README, sem outras referências ao DeepWiki nesse arquivo e sem consulta aos URLs.

Corrija inconsistências apenas dentro do escopo autorizado. Registre as demais e não apresente a revisão como validação completa da documentação.

---

# Checklist final

## Análise

* [ ] A atualização prévia da linhagem foi confirmada positivamente pelo usuário antes de qualquer alteração.
* [ ] O estado atual do código foi analisado.
* [ ] A estrutura do projeto foi analisada.
* [ ] As interfaces públicas relevantes foram identificadas.
* [ ] As alterações relevantes disponíveis foram consideradas.

## Documentação técnica

* [ ] A documentação técnica necessária foi identificada.
* [ ] Documentos necessários foram criados ou atualizados.
* [ ] `docs/teleutils_api.md` existe.
* [ ] A API pública relevante está documentada.
* [ ] Documentos especializados foram criados apenas quando justificados.
* [ ] A linhagem não foi criada, renomeada ou revisada; qualquer alteração limitou-se à remoção autorizada de referências a `deprecated_`.
* [ ] Não existem documentos técnicos vazios ou genéricos.

## Pasta `docs/`

* [ ] Os documentos não classificados foram apresentados ao usuário com as opções de categoria e preservação.
* [ ] Os documentos sem decisão afirmativa foram preservados, ressalvada a remoção pontual autorizada de referências a `deprecated_`.
* [ ] Documentos `internal_` não foram listados automaticamente.
* [ ] Documentos `deprecated_` foram preservados e ignorados na revisão; as referências removidas e pendentes foram informadas.
* [ ] Documentos protegidos foram preservados, exceto a remoção pontual autorizada de referências a `deprecated_`.
* [ ] O relatório de auditoria não foi listado em outros documentos; referências existentes fora do escopo autorizado foram reportadas.
* [ ] Todos os links adicionados apontam para arquivos existentes.

## README

* [ ] O README foi atualizado somente após a documentação técnica.
* [ ] O README funciona como porta de entrada.
* [ ] Um usuário externo consegue instalar o projeto seguindo o README.
* [ ] Um usuário externo consegue identificar como executar os principais fluxos.
* [ ] Existe uma forma de verificar a instalação.
* [ ] O README não duplica desnecessariamente a documentação técnica.
* [ ] Os documentos técnicos relevantes são facilmente localizáveis.
* [ ] O sumário corresponde à estrutura atual, inclusive subseções aninhadas.
* [ ] Os links de retorno ao topo foram aplicados corretamente.
* [ ] Visão geral com tabela "Componentes" (principais antes dos auxiliares).
* [ ] Pré-requisitos, parâmetros, variantes e documentação técnica estão em tabelas.
* [ ] "Como usar" tem um item por componente principal e "Módulos auxiliares" ao final.
* [ ] O diagrama Mermaid de "Como funciona" é simples, validado e acompanhado de passos numerados.
* [ ] Detalhes técnicos extensos e resultados de testes foram deixados em `docs/`.
* [ ] "Limitações conhecidas" tem no máximo cinco itens relevantes ao usuário.

## DeepWiki

* [ ] O DeepWiki não foi consultado nem utilizado como fonte.
* [ ] O badge especificado está na primeira linha do README e é a única referência ao DeepWiki nesse arquivo.
* [ ] A verificação do badge foi local, sem acesso aos URLs.

## Qualidade

* [ ] A documentação reflete o código atual.
* [ ] Nenhuma funcionalidade foi inventada.
* [ ] Nenhuma interface foi inventada.
* [ ] Nenhuma configuração foi inventada.
* [ ] Os exemplos refletem interfaces reais.
* [ ] Variáveis e nomes de funções definidos nos exemplos estão em inglês, sem alterar os identificadores reais da interface.
* [ ] A documentação produzida ou atualizada está em português brasileiro, respeitados os conteúdos históricos e protegidos.
* [ ] Não existem duplicações desnecessárias.
* [ ] O Markdown produzido é válido.

---

# Regras obrigatórias

1. Confirme com o usuário a atualização prévia da linhagem; sem confirmação positiva, interrompa sem alterar arquivos.
2. Analise o código atual antes de documentar e use-o como referência para o comportamento atual, preservando o contexto de registros históricos.
3. Atualize a documentação técnica e valide-a antes de atualizar o README; mantenha `docs/teleutils_api.md`.
4. Classifique documentos sem prefixo somente após a escolha explícita do usuário; respeite as categorias e exceções para documentos protegidos.
5. Preserve documentos `deprecated_` e remova suas referências conforme as regras de `docs/`, registrando pendências.
6. Não revise ou renomeie a documentação de linhagem nem o relatório de auditoria; apenas remova referências a `deprecated_` nesses arquivos.
7. Não liste o relatório de auditoria em outros documentos; não liste automaticamente documentos `internal_` ou não classificados.
8. Não altere código-fonte, comportamento funcional, testes ou arquivos de configuração.
9. Não invente funcionalidades, interfaces, parâmetros, comportamentos ou links.
10. Preserve conteúdo correto, relevante e não confirmado; fundamente alterações substantivas e mantenha ressalvas e evidências históricas.
11. Não consulte o DeepWiki; mantenha apenas o badge especificado na primeira linha do README, sem verificar os URLs.
12. Valide o trabalho dentro do escopo autorizado e relate inconsistências, referências e informações pendentes.
13. Escreva o README como visão geral essencial: tabelas para dados comparáveis, componentes principais antes dos auxiliares, exemplos mínimos e detalhes técnicos apenas em `docs/`.

---

# Resultado esperado

Ao concluir, o projeto deve possuir uma estrutura de documentação em que:

```text
Código atual
    ↓
Documentação técnica atualizada
    ↓
README atualizado e consistente
```

A documentação deve:

* refletir fielmente o estado atual do projeto;
* possuir documentação técnica detalhada quando necessária;
* possuir documentação atualizada da API pública;
* manter `docs/teleutils_api.md`;
* criar ou atualizar documentos técnicos especializados quando justificados;
* preservar documentos protegidos, respeitada a remoção pontual autorizada de referências a `deprecated_`;
* utilizar o README como porta de entrada;
* permitir que novos usuários compreendam, instalem e executem o projeto;
* permitir que desenvolvedores localizem e consultem a API pública;
* listar automaticamente no README apenas documentos técnicos relevantes `teleutils_*.md`, preservando outras referências permitidas;
* manter apenas o badge DeepWiki especificado na primeira linha do README, sem consultar o serviço;
* evitar duplicação desnecessária;
* permanecer tecnicamente precisa, clara, sustentável e consistente com o código atual.

Ao concluir a execução, informe resumidamente:

* quais arquivos de documentação foram criados;
* quais arquivos de documentação foram atualizados;
* quais documentos técnicos foram referenciados no README;
* quais classificações e renomeações foram realizadas ou ficaram pendentes;
* quais referências a documentos `deprecated_` foram removidas ou permaneceram pendentes;
* quais alterações substantivas foram realizadas e em quais evidências se basearam;
* quais inconsistências e documentos protegidos ficaram fora da validação;
* se o badge DeepWiki foi mantido ou inserido na primeira linha do README;
* quais informações relevantes não puderam ser confirmadas com segurança.
