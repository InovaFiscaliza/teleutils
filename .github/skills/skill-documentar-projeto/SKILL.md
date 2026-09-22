---

name: documentar-projeto
description: Cria e atualiza a documentação técnica do projeto, incluindo a documentação da API pública, e atualiza o README.md com base no estado atual do código.

---

# Documentar o projeto

Analise cuidadosamente o estado atual do repositório e mantenha atualizada a documentação técnica e principal do projeto.

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

A documentação existente pode ser utilizada para:

* identificar conteúdo que deve ser preservado;
* identificar documentos existentes;
* manter contexto;
* corrigir referências;
* identificar possíveis inconsistências.

Ela não deve prevalecer sobre o comportamento atual implementado.

---

# Idioma

Toda documentação produzida ou atualizada por esta skill deve ser escrita em português brasileiro (`pt-BR`).

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

* quais documentos técnicos já existem;
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

A documentação técnica gerenciada automaticamente por esta skill deve utilizar o prefixo:

```text
teleutils_
```

Exemplos:

```text
docs/teleutils_api.md
docs/teleutils_arquitetura.md
docs/teleutils_linhagem_transformacoes_dados.md
docs/teleutils_configuracao.md
docs/teleutils_guias_uso.md
```

Os nomes acima são exemplos.

Não crie automaticamente todos esses documentos.

A criação de cada documento deve depender da existência de conteúdo correspondente no projeto.

## Arquivos pertencentes ao escopo da skill

Considere automaticamente para:

* criação;
* atualização;
* validação;
* referência no README;

apenas arquivos em `docs/` cujo nome comece com:

```text
teleutils_
```

## Arquivos fora do escopo automático

Arquivos em `docs/` cujo nome não comece com `teleutils_`:

* não devem ser alterados automaticamente;
* não devem ser removidos;
* não devem ser renomeados;
* não devem ser listados automaticamente no README;
* não devem ser utilizados como fonte para determinar o comportamento atual do projeto.

Esses arquivos podem existir para outras finalidades.

Não renomeie arquivos existentes apenas para adequá-los ao prefixo.

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

Não reescreva documentação adequada apenas para alterar sua redação.

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

Esta skill não deve criar, alterar ou atualizar a documentação de linhagem de dados.

Esta skill pressupõe que a documentação de linhagem e transformação de dados já esteja atualizada antes de sua execução, utilizando a skill específica destinada à auditoria e documentação de linhagem de dados.

Quando existir documentação de linhagem relevante, esta skill pode apenas consultá-la para:

* compreender o fluxo de processamento dos dados;
* manter consistência com a documentação técnica geral;
* incluir referências para essa documentação no `README.md`, quando aplicável.

Caso seja identificada alguma possível inconsistência entre o código e a documentação de linhagem, não altere o documento automaticamente. Registre a inconsistência no resultado final para revisão.


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
* não existem links internos quebrados;
* não existem referências comprovadamente obsoletas;
* documentos sem o prefixo `teleutils_` não foram alterados;
* a documentação técnica não possui duplicações desnecessárias.

Confirme obrigatoriamente que:

```text
docs/teleutils_api.md
```

existe e está atualizado.

Somente após essa validação prossiga para o README.

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

# Conteúdo do README

Adapte o conteúdo à realidade do projeto.

Inclua apenas seções que possuam conteúdo real e relevante.

A estrutura pode incluir:

```text
Título e introdução
Sumário
Visão geral
DeepWiki
Início rápido
Arquitetura em alto nível
Principais componentes
Principais fluxos
Estrutura do projeto
Configuração
Guias de uso
Documentação técnica complementar
Desenvolvimento
Solução de problemas
Compatibilidade
Referências
```

---

# Introdução e visão geral

Apresente:

* nome do projeto;
* propósito;
* problema resolvido;
* cenários de utilização;
* benefícios principais;
* usuários esperados.

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

Regras:

* não utilizar bullets para itens de nível superior;
* utilizar bullets dentro dos blocos `<details>`;
* manter uma linha em branco após `<summary>`;
* utilizar âncoras compatíveis com o GitHub;
* refletir os títulos efetivamente presentes no README.

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

Documente apenas requisitos confirmáveis, como:

* versões suportadas do Python;
* dependências do sistema operacional;
* softwares externos;
* ferramentas necessárias.

Não invente versões mínimas.

## Instalação

Forneça um passo a passo baseado na estrutura real do projeto.

Os comandos devem ser compatíveis com:

* o gerenciador de dependências utilizado;
* os arquivos de configuração existentes;
* o método de instalação suportado.

Não utilize comandos genéricos que não correspondam ao projeto.

## Verificação

Forneça um método realista para verificar a instalação.

Baseie-se em:

* comandos existentes;
* testes disponíveis;
* importações válidas;
* funcionalidades confirmadas.

---

# Arquitetura e principais fluxos

Apresente no README apenas uma visão de alto nível.

Explique, quando aplicável:

* principais componentes;
* responsabilidades;
* fluxo geral;
* relações relevantes entre componentes.

Utilize diagramas Mermaid apenas quando agregarem valor.

Não crie diagramas artificiais.

Quando houver documentação detalhada em `docs/`, apresente uma visão resumida e inclua um link para o documento correspondente.

---

# Estrutura do projeto

Apresente a árvore principal de diretórios e arquivos relevantes.

Explique brevemente a finalidade de cada elemento apresentado.

A estrutura deve refletir o estado atual do repositório.

Não inclua:

* caches;
* arquivos temporários;
* artefatos gerados;
* diretórios sem relevância para usuários ou desenvolvedores.

---

# Configuração

Documente as configurações necessárias para utilização inicial.

Considere, quando aplicável:

* variáveis de ambiente;
* arquivos de configuração;
* parâmetros obrigatórios;
* parâmetros opcionais;
* formatos aceitos.

Não invente configurações.

Quando a documentação for extensa, mantenha os detalhes em documento técnico específico e referencie-o.

---

# Guias de uso

Apresente os principais fluxos de utilização identificados.

Podem incluir, quando aplicável:

* uso básico;
* uso avançado;
* processamento em lote;
* processamento de arquivos;
* integração com outros componentes;
* execução de pipelines.

Os exemplos devem refletir funcionalidades reais.

Quando possível, valide exemplos.

Não apresente exemplos como executados quando não tiverem sido executados.

Quando a validação não for possível, construa os exemplos exclusivamente a partir de interfaces confirmadas.

---

# Documentação técnica complementar

Após concluir a documentação técnica, liste no README apenas documentos existentes em `docs/` cujo nome comece com:

```text
teleutils_
```

Para cada documento relevante, apresente:

* título;
* breve descrição;
* link relativo.

Não duplique integralmente o conteúdo dos documentos.

Não liste automaticamente arquivos sem o prefixo `teleutils_`.

Exemplo:

```markdown
| Documento | Descrição |
|---|---|
| [Referência da API](docs/teleutils_api.md) | Documentação das interfaces públicas do projeto. |
| [Arquitetura](docs/teleutils_arquitetura.md) | Descrição da arquitetura e dos principais componentes. |
| [Linhagem e Transformações dos Dados](docs/teleutils_linhagem-transformacoes-dados.md) | Origem, transformação e destino dos dados processados pelo projeto. |
```

A tabela deve conter apenas documentos realmente existentes.

---

# Desenvolvimento

Quando relevante, apresente informações resumidas sobre:

* ambiente de desenvolvimento;
* execução de testes;
* ferramentas de qualidade;
* linters;
* formatadores.

Detalhes extensos podem permanecer em documentação técnica especializada.

---

# Solução de problemas

Inclua esta seção apenas quando houver problemas e soluções confirmáveis.

Organize, quando aplicável, em:

* erros comuns;
* diagnóstico;
* resolução.

Não invente problemas ou soluções.

---

# Compatibilidade

Documente apenas informações confirmáveis sobre:

* versões suportadas do Python;
* sistemas operacionais;
* dependências obrigatórias;
* ferramentas externas;
* limitações conhecidas.

Não presuma compatibilidade.

---

# DeepWiki

O DeepWiki é um recurso adicional de exploração e documentação do repositório.

Quando houver uma página confirmável do projeto no DeepWiki:

* inclua uma referência no README;
* inclua o link oficial;
* explique brevemente sua finalidade;
* apresente-o como recurso adicional.

O DeepWiki não é fonte prioritária para geração ou atualização da documentação.

A documentação deve continuar sendo produzida a partir do código e dos demais elementos do repositório.

Não invente links.

Quando não houver um link confirmável, não mencione o DeepWiki.

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
* os links do README apontam para arquivos existentes;
* os documentos referenciados possuem o prefixo `teleutils_`;
* arquivos sem o prefixo `teleutils_` não foram alterados;
* arquivos sem o prefixo `teleutils_` não foram listados automaticamente;
* não existem funcionalidades inventadas;
* não existem informações comprovadamente obsoletas;
* não existem duplicações desnecessárias;
* a documentação está escrita em português brasileiro;
* o Markdown é válido;
* a navegação do README funciona corretamente.

Em caso de inconsistência, corrija a documentação antes de concluir.

---

# Checklist final

## Análise

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
* [ ] A documentação de linhagem, quando existente, foi apenas consultada e não foi alterada por esta skill.
* [ ] Não existem documentos técnicos vazios ou genéricos.

## Pasta `docs/`

* [ ] Apenas documentos `teleutils_*.md` foram alterados automaticamente.
* [ ] Arquivos sem o prefixo `teleutils_` foram preservados.
* [ ] Arquivos sem o prefixo `teleutils_` não foram listados automaticamente no README.
* [ ] Todos os links adicionados apontam para arquivos existentes.

## README

* [ ] O README foi atualizado somente após a documentação técnica.
* [ ] O README funciona como porta de entrada.
* [ ] Um usuário externo consegue instalar o projeto seguindo o README.
* [ ] Um usuário externo consegue identificar como executar os principais fluxos.
* [ ] Existe uma forma de verificar a instalação.
* [ ] O README não duplica desnecessariamente a documentação técnica.
* [ ] Os documentos técnicos relevantes são facilmente localizáveis.
* [ ] O sumário corresponde à estrutura atual.
* [ ] Os links de retorno ao topo foram aplicados corretamente.

## DeepWiki

* [ ] O DeepWiki foi mencionado apenas quando existe um link confirmável.
* [ ] Foi apresentado como recurso adicional.
* [ ] Não foi utilizado como fonte prioritária da verdade.

## Qualidade

* [ ] A documentação reflete o código atual.
* [ ] Nenhuma funcionalidade foi inventada.
* [ ] Nenhuma interface foi inventada.
* [ ] Nenhuma configuração foi inventada.
* [ ] Os exemplos refletem interfaces reais.
* [ ] A documentação está em português brasileiro.
* [ ] Não existem duplicações desnecessárias.
* [ ] O Markdown produzido é válido.

---

# Regras obrigatórias

1. Analise o estado atual do código antes de documentar.
2. Considere o código atual como fonte prioritária da verdade.
3. Atualize primeiro a documentação técnica.
4. Atualize o README somente após validar a documentação técnica.
5. Mantenha obrigatoriamente `docs/teleutils_api.md`.
6. Crie documentos técnicos especializados apenas quando houver conteúdo real e relevante.
7. Todo documento gerenciado automaticamente por esta skill deve utilizar o prefixo `teleutils_`.
8. Não altere documentos em `docs/` cujo nome não comece com `teleutils_`.
9. Não liste automaticamente no README documentos cujo nome não comece com `teleutils_`.
10. Não altere código-fonte.
11. Não altere comportamento funcional.
12. Não altere testes.
13. Não altere arquivos de configuração.
14. Não invente funcionalidades.
15. Não invente interfaces, parâmetros ou comportamentos.
16. Não invente links.
17. Preserve documentação existente que continue correta e relevante.
18. Corrija ou remova apenas informações comprovadamente obsoletas ou incorretas.
19. Não utilize o DeepWiki como fonte prioritária da verdade.
20. Apresente o DeepWiki apenas como recurso adicional quando houver um link confirmável.
21. Mantenha consistência entre código, documentação técnica e README.
22. Mantenha o README como porta de entrada, sem transformá-lo em duplicação integral da documentação técnica.
23. Antes de concluir, execute a validação final de consistência.
24. Não criar, alterar ou atualizar a documentação de linhagem e transformação de dados.

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
* preservar documentos fora do escopo da skill;
* utilizar o README como porta de entrada;
* permitir que novos usuários compreendam, instalem e executem o projeto;
* permitir que desenvolvedores localizem e consultem a API pública;
* referenciar no README apenas documentos técnicos `teleutils_*.md`;
* apresentar o DeepWiki como recurso adicional quando disponível;
* evitar duplicação desnecessária;
* permanecer tecnicamente precisa, clara, sustentável e consistente com o código atual.

Ao concluir a execução, informe resumidamente:

* quais arquivos de documentação foram criados;
* quais arquivos de documentação foram atualizados;
* quais documentos técnicos foram referenciados no README;
* se o DeepWiki foi identificado e referenciado;
* quais informações relevantes não puderam ser confirmadas com segurança.
