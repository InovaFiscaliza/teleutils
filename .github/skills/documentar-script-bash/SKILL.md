---
name: documentar-script-bash
description: Documenta scripts Bash utilizando comentários estruturados, cabeçalhos técnicos, documentação de funções e anotações de manutenção, sem alterar comandos executáveis, lógica ou comportamento funcional.
user-invocable: true
---

# Documentar script Bash

Analise cuidadosamente o script Bash em contexto e produza uma versão tecnicamente bem documentada, adicionando ou aperfeiçoando **comentários estruturados**, **cabeçalhos de documentação**, **documentação de funções** e **anotações de manutenção**, sem alterar o comportamento, a lógica de negócio ou a estrutura funcional do script.

A documentação deve seguir convenções adequadas à linguagem Bash e facilitar a compreensão, utilização e manutenção do script por desenvolvedores e administradores de sistemas.

## Objetivo

Produzir documentação técnica de alta qualidade que permita que qualquer desenvolvedor, mesmo sem conhecimento prévio do projeto, compreenda:

- O propósito geral do script.
- Sua responsabilidade dentro do projeto ou processo operacional.
- Os requisitos e dependências para execução.
- Os argumentos e opções de linha de comando.
- As variáveis de ambiente e configurações relevantes.
- A responsabilidade de cada função.
- O fluxo principal de execução.
- As regras de negócio implementadas.
- As validações realizadas.
- Os arquivos, diretórios e recursos manipulados.
- As transformações e movimentações de dados.
- As integrações com comandos e sistemas externos.
- Os códigos de saída relevantes.
- Os efeitos colaterais importantes.
- Os pontos sensíveis para manutenção futura.

A documentação deve explicar o comportamento efetivamente implementado no código, sem inventar funcionalidades, regras ou intenções.

---

# Idioma

Toda a documentação adicionada ou alterada deve ser escrita em **português brasileiro (pt-BR)**.

Isso inclui:

- Cabeçalhos de documentação.
- Comentários de funções.
- Comentários internos.
- Descrições de parâmetros.
- Explicações sobre variáveis.
- Anotações de manutenção.
- Exemplos explicativos.

Não traduza ou altere nomes existentes de:

- Funções.
- Variáveis.
- Parâmetros.
- Argumentos de linha de comando.
- Variáveis de ambiente.
- Comandos.
- Ferramentas.
- Arquivos.
- Diretórios.
- APIs.
- Estruturas técnicas que devam permanecer em inglês.

Não modifique mensagens exibidas pelo script, inclusive mensagens de ajuda, erro, aviso ou log, pois elas fazem parte de seu comportamento funcional.

---

# Preservação rigorosa do código

A lógica e o comportamento funcional existentes devem ser integralmente preservados.

As únicas alterações permitidas são:

- Criação ou atualização de comentários.
- Criação ou atualização de blocos de documentação.
- Criação de anotações de manutenção estritamente relacionadas à documentação.

Não altere:

- Comandos executáveis.
- Shebang.
- Diretivas `set`, `shopt` ou `trap`.
- Declarações e atribuições de variáveis.
- Expansões de parâmetros.
- Substituições de comandos.
- Substituições de processos.
- Expansões aritméticas.
- Condições.
- Estruturas `if`, `case`, `for`, `while`, `until` ou `select`.
- Definições e chamadas de funções.
- Parâmetros posicionais.
- Argumentos e opções.
- Redirecionamentos.
- Here-documents e here-strings.
- Pipelines.
- Subshells.
- Comandos externos.
- Ordem das operações.
- Códigos de saída.
- Tratamento de erros.
- Comportamento de logs.
- Operações sobre arquivos e diretórios.
- Fluxos de execução.

Não:

- Refatore o script.
- Reorganize funções.
- Renomeie variáveis.
- Otimize comandos.
- Corrija problemas.
- Adicione validações.
- Adicione funcionalidades.
- Remova funcionalidades.
- Altere regras de negócio.
- Substitua comandos por alternativas consideradas melhores.
- Altere expressões de quoting ou escaping.
- Modifique permissões ou operações sobre arquivos.
- Altere chamadas a sistemas externos.

A documentação deve se adaptar ao script existente. O script não deve ser alterado para se adequar à documentação.

## Cuidados específicos com Bash

Em Bash, alterações aparentemente pequenas podem modificar o comportamento da execução.

Portanto:

- Preserve exatamente as expansões como `$var`, `${var}`, `"$var"`, `$@`, `"$@"`, `$*` e `$?`.
- Não altere diferenças entre variáveis com e sem aspas.
- Não substitua `[` por `[[` ou vice-versa.
- Não substitua `test`, `(( ))` ou outras construções condicionais.
- Não modifique operadores como `&&`, `||`, `|`, `&` ou `;`.
- Não altere a ordem de comandos em pipelines.
- Não modifique redirecionamentos como `>`, `>>`, `2>`, `2>&1` ou `&>`.
- Não mova comandos para dentro ou fora de subshells.
- Não modifique `IFS`, `PATH`, `umask` ou outras configurações de ambiente.
- Não altere `export`, `local`, `readonly`, `declare` ou `unset`.
- Não altere chamadas a `exit`, `return` ou `exec`.

Os comentários devem ser inseridos somente em posições sintaticamente seguras.

**A inserção de documentação não pode modificar a interpretação do script pelo Bash.**

---

# Documentação e comentários existentes

Preserve comentários existentes quando estiverem:

- Corretos.
- Atualizados.
- Tecnicamente adequados.
- Úteis para compreensão ou manutenção.

Não reescreva documentação adequada apenas para alterar sua redação ou estilo.

Atualize um comentário ou bloco de documentação existente apenas quando ele estiver:

- Incorreto.
- Desatualizado.
- Incompleto de forma relevante.
- Inconsistente com o comportamento atual.
- Incompatível com o padrão documental adotado.
- Redundante e prejudicial à clareza.

Ao atualizar documentação existente, preserve as informações corretas.

Preserve também comentários que expliquem decisões históricas ou restrições operacionais ainda relevantes.

Comentários que sejam utilizados por ferramentas externas, geradores, linters ou mecanismos de configuração não devem ser modificados sem autorização explícita.

---

# Processo de análise

Antes de adicionar ou alterar documentação, analise cuidadosamente o script para compreender:

1. Seu propósito geral.
2. Sua responsabilidade dentro do projeto.
3. O interpretador esperado.
4. Os requisitos de execução.
5. Os argumentos e opções aceitos.
6. As variáveis de ambiente utilizadas.
7. Os arquivos de configuração consumidos.
8. As dependências externas.
9. As integrações com outros sistemas.
10. O fluxo principal de execução.
11. As funções existentes.
12. As funções auxiliares.
13. As regras de negócio.
14. As validações implementadas.
15. As operações sobre arquivos e diretórios.
16. As transformações de dados.
17. O tratamento de erros.
18. Os códigos de saída.
19. Os efeitos colaterais.
20. Os pontos de entrada e saída relevantes.

Não escreva documentação baseada apenas:

- No nome de uma função.
- No nome de uma variável.
- No nome de um arquivo.
- Em convenções presumidas.
- Em suposições sobre o domínio.

A documentação deve refletir o comportamento efetivamente identificado no código.

---

# Análise de contexto externo

Quando o comportamento, a finalidade ou o significado de um elemento não puder ser determinado com segurança apenas pelo script em contexto, analise os componentes diretamente relacionados quando necessário.

Podem ser analisados, por exemplo:

- Scripts chamados por meio de `source` ou `.`.
- Scripts executados como subprocessos.
- Funções definidas em arquivos auxiliares.
- Arquivos de configuração.
- Arquivos JSON, YAML, INI ou `.env`.
- Comandos externos utilizados.
- Variáveis exportadas por scripts relacionados.
- Arquivos de entrada e saída.
- Contratos de integração.
- Scripts chamadores.
- Documentação técnica existente.

O objetivo dessa análise externa é exclusivamente compreender corretamente o script que está sendo documentado.

Não invente contexto.

Não trate como fato informações que não possam ser confirmadas pelo código ou pelos componentes diretamente relacionados.

Componentes externos podem ser analisados para compreensão, mas não devem ser modificados, salvo se a tarefa solicitar explicitamente sua documentação.

---

# Documentação principal do script

Adicione ou atualize um bloco de documentação próximo ao início do arquivo.

**O shebang, quando existente, deve permanecer na primeira linha.**

Posicione o cabeçalho documental imediatamente após o shebang, respeitando diretivas ou comentários especiais que precisem conservar posições específicas.

O cabeçalho deve explicar, quando aplicável:

- Nome ou finalidade do script.
- Descrição geral.
- Responsabilidade dentro do projeto.
- Principais funcionalidades.
- Fluxo resumido de execução.
- Sintaxe de utilização.
- Argumentos e opções relevantes.
- Variáveis de ambiente necessárias.
- Arquivos de configuração.
- Entradas e saídas.
- Dependências externas.
- Efeitos colaterais importantes.
- Códigos de saída relevantes.
- Exemplos de utilização.

Utilize blocos de comentários Bash convencionais.

## Exemplo de cabeçalho

```bash
#!/usr/bin/env bash

# =============================================================================
# Descrição:
#   Processa arquivos de registros telefônicos e transfere os resultados
#   para um sistema de armazenamento remoto.
#
# Uso:
#   ./transferir_cdr.sh --config ARQUIVO --semana SEMANA
#
# Argumentos:
#   --config ARQUIVO  Caminho do arquivo de configuração.
#   --semana SEMANA   Identificador da semana a processar.
#
# Dependências:
#   jq  - Leitura das configurações JSON.
#   mc  - Transferência de arquivos para armazenamento compatível com S3.
#
# Entradas:
#   Arquivos de registros telefônicos localizados nos diretórios configurados.
#
# Saídas:
#   Objetos gravados no destino remoto configurado.
#
# Observações:
#   O processamento utiliza os caminhos definidos no arquivo de configuração.
# =============================================================================
```

O exemplo é apenas uma referência de organização. Não reproduza argumentos, dependências ou comportamentos que não estejam implementados no script analisado.

Não inclua seções artificiais ou informações irrelevantes apenas para preencher uma estrutura padrão.

Não adicione automaticamente informações como autor, versão, licença ou data de criação quando não estiverem disponíveis.

---

# Documentação de funções

Documente todas as funções que representem operações relevantes e não possuam documentação adequada.

Para funções auxiliares simples, adicione ou aperfeiçoe a documentação apenas quando sua:

- Finalidade.
- Regra de negócio.
- Comportamento.
- Efeito colateral.
- Papel no fluxo.
- Relação com outras operações.

não for imediatamente evidente pelo código.

Não adicione blocos extensos a funções triviais apenas para aumentar a cobertura documental.

Cada bloco deve descrever o comportamento real da função.

Inclua, quando aplicável:

- Descrição.
- Responsabilidade.
- Argumentos posicionais.
- Variáveis globais utilizadas.
- Variáveis globais modificadas.
- Dados produzidos em `stdout`.
- Mensagens produzidas em `stderr`.
- Código de retorno.
- Efeitos colaterais.
- Dependências externas.
- Regras de negócio.
- Observações importantes para manutenção.

## Padrão de documentação de funções

Utilize preferencialmente um bloco de comentários imediatamente anterior à declaração da função.

Exemplo:

```bash
# -----------------------------------------------------------------------------
# Função: validar_arquivo
#
# Descrição:
#   Verifica se o caminho informado corresponde a um arquivo regular
#   que pode ser lido pelo processo atual.
#
# Argumentos:
#   $1 - Caminho do arquivo a validar.
#
# Retorno:
#   0 - Arquivo encontrado e legível.
#   1 - Arquivo inexistente ou sem permissão de leitura.
#
# Efeitos colaterais:
#   Nenhum.
# -----------------------------------------------------------------------------
validar_arquivo() {
    [[ -f "$1" && -r "$1" ]]
}
```

Não inclua seções vazias.

Não documente retornos, validações ou efeitos colaterais que não existam.

Não acrescente comentários como `Efeitos colaterais: Nenhum` a todas as funções indiscriminadamente.

---

# Argumentos e parâmetros

Documente os argumentos de linha de comando e os parâmetros de funções quando forem relevantes.

Para cada argumento, explique:

- Nome ou posição.
- Finalidade.
- Significado no processamento.
- Formato esperado.
- Obrigatoriedade, quando verificável.
- Valor padrão, quando existente.
- Restrições importantes.
- Comportamento associado.

Considere as diferentes formas utilizadas pelo Bash:

- Parâmetros posicionais `$1`, `$2`, `$3` etc.
- Argumentos completos por meio de `"$@"`.
- Opções curtas.
- Opções longas.
- Processamento com `getopts`.
- Processamento manual com `case` e `shift`.

Diferencie corretamente:

- Parâmetros obrigatórios.
- Parâmetros opcionais.
- Opções com valores.
- Flags booleanas.
- Argumentos posicionais.

Não presuma que um argumento é obrigatório apenas porque é utilizado.

Confirme se a obrigatoriedade é efetivamente validada pelo código.

Quando houver uma interface de linha de comando, documente a sintaxe real de utilização.

---

# Variáveis de ambiente e configurações

Documente variáveis de ambiente e configurações relevantes para a execução.

Considere:

- Variáveis recebidas do ambiente.
- Variáveis exportadas pelo script.
- Variáveis globais.
- Variáveis definidas em arquivos de configuração.
- Valores padrão.
- Diretórios de trabalho.
- Caminhos de entrada e saída.
- Configurações de conexão.
- Configurações de ferramentas externas.

Diferencie variáveis de ambiente de variáveis locais ou globais do script.

Documente, quando relevante:

- Finalidade da variável.
- Origem do valor.
- Valor padrão efetivamente implementado.
- Regras de precedência.
- Restrições.
- Etapas que dependem da variável.

Quando um argumento de linha de comando substituir um valor obtido de configuração, explique a regra de precedência somente se ela estiver confirmada no código.

Não documente como obrigatória uma variável que possui tratamento alternativo.

## Informações sensíveis

Não reproduza valores de:

- Senhas.
- Tokens.
- Chaves de acesso.
- Credenciais.
- Segredos.
- Dados privados desnecessários.

Quando necessário, documente apenas a finalidade e a forma de fornecimento da informação.

Não introduza valores sensíveis nos exemplos.

---

# Retornos e códigos de saída

Em Bash, diferencie corretamente:

1. O código de retorno de uma função.
2. O código de saída do script.
3. A saída textual produzida em `stdout`.
4. As mensagens produzidas em `stderr`.

Documente, quando aplicável:

- Códigos retornados por `return`.
- Códigos definidos por `exit`.
- Condições associadas aos códigos.
- Valores escritos em `stdout`.
- Mensagens relevantes escritas em `stderr`.
- Resultados produzidos por comandos externos.

Não confunda saída textual com código de retorno.

Não descreva `echo` ou `printf` como mecanismos de retorno de uma função sem esclarecer que produzem saída textual.

Considere que o status de uma função pode depender do último comando executado.

Não invente códigos de saída específicos quando eles não forem explicitamente definidos ou determináveis pela implementação.

Quando um código de erro for propagado de um comando externo, documente essa dependência somente quando relevante.

---

# Tratamento de erros

Documente mecanismos relevantes de tratamento e propagação de erros.

Considere:

- Verificações condicionais.
- Testes de status com `$?`.
- Operadores `&&` e `||`.
- Comandos `exit` e `return`.
- Funções de tratamento de erro.
- Uso de `trap`.
- Mensagens de erro.
- Comportamento de recuperação.
- Encerramento antecipado.
- Continuidade após falhas.

Ao identificar o uso de:

```bash
set -e
set -u
set -o pipefail
```

ou:

```bash
set -euo pipefail
```

documente seus efeitos relevantes no fluxo apenas quando contribuírem para compreender o comportamento do script.

Não afirme que `set -e` garante encerramento em qualquer erro, pois seu comportamento depende do contexto de execução.

Não presuma que uma falha em comando externo será necessariamente tratada.

Descreva a lógica de tratamento efetivamente implementada.

---

# Comentários internos

Adicione comentários somente quando agregarem valor real à compreensão ou manutenção.

Antes de adicionar um comentário, avalie se a informação já está suficientemente clara:

- Pelo nome da variável.
- Pelo nome da função.
- Pela estrutura do código.
- Pela operação realizada.
- Pelo cabeçalho existente.

Não adicione comentários que apenas descrevam literalmente a instrução seguinte.

Priorize comentários que expliquem:

- O motivo da operação.
- A regra de negócio.
- Uma restrição operacional.
- Uma decisão arquitetural.
- Uma dependência importante.
- Uma transformação não trivial.
- Uma particularidade do Bash.
- Um comportamento não evidente.
- Uma limitação relevante.

## Regras de negócio

```bash
# Arquivos que não pertencem à semana solicitada são ignorados
# para evitar mistura de períodos na carga.
```

## Transformações complexas

```bash
# Remove apenas o prefixo correspondente ao diretório de origem,
# preservando a estrutura relativa dos arquivos no destino.
```

## Decisões arquiteturais

```bash
# Mantém as transferências sequenciais porque a etapa seguinte
# depende da conclusão da transferência anterior.
```

## Restrições operacionais

```bash
# Executa a leitura antes da movimentação para evitar que
# arquivos ainda em processamento sejam considerados concluídos.
```

Os exemplos são ilustrativos e somente devem ser utilizados quando o comportamento descrito existir no código.

Evite comentários redundantes como:

```bash
# Cria diretório
mkdir -p "$diretorio"
```

quando a operação já for autoexplicativa.

---

# Regras de negócio

Sempre que uma regra de negócio estiver implementada no script e não for imediatamente evidente, documente:

- A regra.
- Sua finalidade.
- Sua condição de aplicação.
- Seu impacto no processamento.
- Sua relação com outras etapas, quando relevante.

Não altere, corrija ou otimize a regra.

A documentação deve representar a implementação atual, inclusive quando houver decisões de negócio aparentemente incomuns.

Diferencie regras de negócio de restrições técnicas do Bash ou das ferramentas externas.

---

# Fluxo de execução

Quando o script implementar um fluxo relevante, a documentação deve torná-lo compreensível.

Explique, quando aplicável:

```text
Leitura de argumentos
        |
        v
Carregamento de configurações
        |
        v
Validação
        |
        v
Identificação dos arquivos
        |
        v
Processamento
        |
        v
Transferência ou gravação
        |
        v
Finalização
```

O fluxo acima é apenas ilustrativo.

Não é necessário criar diagramas artificiais para todos os scripts.

Descreva somente as etapas efetivamente existentes.

Quando necessário, explique:

- Condições que interrompem o processamento.
- Etapas opcionais.
- Caminhos alternativos de execução.
- Dependências entre etapas.
- Operações repetidas em loops.
- Condições para continuidade após falhas.

---

# Manipulação de arquivos e diretórios

Quando o script manipular arquivos ou diretórios, documente as operações relevantes.

Considere:

- Origem dos arquivos.
- Diretórios de entrada.
- Diretórios de destino.
- Padrões de seleção.
- Globs e curingas.
- Criação de diretórios.
- Cópia de arquivos.
- Movimentação de arquivos.
- Renomeação.
- Exclusão.
- Compactação.
- Descompactação.
- Alteração de permissões.
- Sobrescrita.
- Arquivos temporários.
- Limpeza de recursos.
- Preservação de estrutura de diretórios.

Documente especialmente operações potencialmente destrutivas ou não reversíveis.

Quando houver uso de comandos como `rm`, `mv`, `cp`, `find`, `tar` ou `rsync`, explique restrições e efeitos relevantes que não sejam imediatamente evidentes.

Não presuma que operações sejam atômicas, seguras ou recuperáveis sem evidência no código.

---

# Processamento e transformação de dados

Quando o script realizar transformações, documente, quando relevante:

- Origem dos dados.
- Formato de entrada.
- Estrutura esperada.
- Filtros aplicados.
- Extração de campos.
- Normalizações.
- Conversões.
- Ordenações.
- Agregações.
- Substituições.
- Alterações de formato.
- Critérios de descarte.
- Estrutura produzida.
- Destino dos resultados.

Considere ferramentas como:

- `awk`.
- `sed`.
- `grep`.
- `cut`.
- `sort`.
- `uniq`.
- `tr`.
- `jq`.
- `xargs`.

Explique o propósito das transformações complexas dentro do fluxo.

Não transforme os comentários em tutoriais genéricos sobre essas ferramentas.

Não documente transformações inexistentes ou inferidas apenas pelo nome de uma variável.

---

# Particularidades da linguagem Bash

Ao documentar operações que dependem de características específicas do Bash, considere os aspectos seguintes.

## Expansões de variáveis

Documente comportamentos não evidentes relacionados a:

- Expansão de parâmetros.
- Valores padrão.
- Substituição de padrões.
- Remoção de prefixos e sufixos.
- Expansão de arrays.
- Expansão aritmética.
- Word splitting.
- Pathname expansion.

Não modifique a expressão original.

## Quoting e escaping

Quando uma operação depender da interpretação de aspas ou caracteres especiais, documente a razão quando ela não for evidente.

Não altere:

- Aspas simples.
- Aspas duplas.
- Barras invertidas.
- Caracteres de escape.
- Separadores.
- Expansões.

## Pipelines

Quando houver pipelines relevantes, explique:

- A finalidade do encadeamento.
- O papel das etapas importantes.
- O destino dos dados.
- O tratamento de falhas, quando relevante.

Não descreva um pipeline apenas repetindo todos os comandos.

## Subshells e substituições de comandos

Quando o uso de subshells ou substituições de comandos afetar:

- Escopo de variáveis.
- Diretório de trabalho.
- Código de retorno.
- Ambiente.
- Disponibilidade de resultados.

documente a particularidade quando ela for relevante para manutenção.

## Traps e sinais

Quando houver uso de `trap`, documente:

- Sinais ou condições associados.
- Finalidade do tratamento.
- Recursos liberados.
- Ações executadas.
- Efeitos relevantes sobre o encerramento.

Não presuma que o tratamento seja executado em situações não cobertas pela implementação.

---

# Dependências e integrações

Documente dependências relevantes para compreender e manter o script.

Considere:

- Interpretador Bash.
- Comandos do sistema operacional.
- Ferramentas externas.
- Scripts auxiliares.
- Arquivos de configuração.
- Sistemas de armazenamento.
- Serviços de rede.
- Bancos de dados.
- APIs.
- Sistemas de arquivos locais ou remotos.
- Sistemas de autenticação.
- Variáveis de ambiente necessárias.

Não transforme a documentação em uma reprodução indiscriminada de todos os comandos utilizados.

Documente especialmente dependências que:

- Precisam estar instaladas.
- Exigem configuração.
- Exigem autenticação.
- Utilizam arquivos externos.
- Dependem de recursos remotos.
- Impõem restrições operacionais.

Não declare versões mínimas de ferramentas quando elas não puderem ser confirmadas.

Não presuma compatibilidade com `sh`, `dash`, `zsh` ou outros interpretadores apenas porque o script utiliza sintaxe semelhante.

---

# Integrações com sistemas remotos

Quando o script interagir com sistemas remotos, documente, quando relevante:

- Tipo de serviço.
- Finalidade da integração.
- Origem e destino dos dados.
- Ferramenta utilizada.
- Forma de configuração.
- Requisitos de autenticação identificáveis.
- Tratamento de falhas.
- Efeitos colaterais.
- Condições de sobrescrita.
- Limitações identificáveis.

Considere integrações com:

- SFTP.
- SSH.
- HTTP e HTTPS.
- APIs REST.
- S3 e armazenamentos compatíveis.
- MinIO.
- Bancos de dados.
- Sistemas de arquivos de rede.

Não invente garantias de entrega, integridade, repetição, idempotência ou recuperação.

Documente essas propriedades apenas quando sustentadas pela implementação.

---

# Logs e mensagens

Quando houver mecanismos de logging, documente seu papel no processamento.

Considere:

- Mensagens informativas.
- Avisos.
- Erros.
- Logs de execução.
- Logs de auditoria.
- Redirecionamentos para arquivos.
- Uso de `stdout` e `stderr`.
- Níveis de verbosidade.
- Arquivos de log.
- Identificadores de execução.

Não altere o conteúdo ou formato das mensagens existentes.

Não acrescente novas mensagens.

Não altere canais de saída.

Não presuma que mensagens sejam registros de auditoria quando isso não estiver confirmado.

---

# Anotações de manutenção

Adicione anotações de manutenção apenas quando houver evidência concreta no código.

Podem ser utilizadas para registrar:

- Dependências implícitas.
- Acoplamentos importantes.
- Comportamentos não óbvios.
- Limitações identificáveis.
- Pontos sensíveis para alterações futuras.
- Ordem obrigatória de operações.
- Efeitos colaterais relevantes.
- Dependências entre etapas.
- Restrições de ambiente.
- Particularidades de ferramentas externas.
- Dependências entre variáveis globais e funções.

Não crie observações especulativas.

Não introduza:

- `TODO`.
- `FIXME`.
- `HACK`.

ou anotações semelhantes, salvo quando já existirem no código ou quando uma limitação concreta puder ser confirmada diretamente pela implementação.

Não utilize anotações de manutenção para sugerir refatorações.

Quando identificar uma situação potencialmente problemática, mas fora do escopo documental, não altere o código.

Caso seja relevante e exista evidência suficiente, registre a observação de maneira neutra ou mencione-a no resumo final.

---

# Cuidados ao inserir comentários

Comentários em Bash precisam respeitar o contexto sintático em que são inseridos.

Antes de inserir documentação, verifique se a posição escolhida pode modificar a interpretação do script.

Tenha atenção especial a:

- Comandos continuados com `\`.
- Expressões distribuídas em múltiplas linhas.
- Here-documents.
- Here-strings.
- Strings entre aspas.
- Substituições de comandos.
- Subshells.
- Comandos compostos.
- Arrays.
- Expressões condicionais.
- Comandos com redirecionamento.
- Blocos passados a comandos externos.
- Conteúdo gerado dinamicamente.

Não insira comentários dentro de construções em que `#` possa ser interpretado como dado ou modificar a sintaxe.

Não modifique delimitadores de here-documents.

Não altere separadores, continuações de linha ou espaços sintaticamente significativos.

Quando não houver uma posição comprovadamente segura para adicionar um comentário, prefira documentar o comportamento em um bloco externo à construção.

**A integridade sintática e funcional do script tem prioridade sobre a cobertura documental.**

---

# Ambiguidades e informações insuficientes

Não apresente como fato comportamentos que não possam ser confirmados pelo código disponível.

Quando uma informação importante não puder ser determinada com segurança:

- Não invente a explicação.
- Não presuma a intenção do desenvolvedor.
- Não associe componentes apenas pela semelhança dos nomes.
- Não presuma garantias de comandos externos.
- Não presuma condições específicas do ambiente.
- Não invente valores padrão ou regras de precedência.

Quando necessário, utilize documentação neutra e tecnicamente precisa, descrevendo apenas o comportamento confirmado.

Registre a ambiguidade como anotação de manutenção apenas quando ela for relevante para compreensão ou manutenção futura.

---

# Qualidade da documentação

A documentação deve:

- Ser objetiva.
- Ser tecnicamente precisa.
- Ser escrita em português brasileiro.
- Utilizar blocos de comentários apropriados para Bash.
- Refletir o comportamento efetivo do código.
- Evitar redundância.
- Evitar comentários óbvios.
- Explicar o "porquê" quando necessário.
- Explicar o "o quê" quando isso não for evidente.
- Distinguir entradas, saídas e códigos de retorno.
- Identificar dependências e efeitos colaterais relevantes.
- Facilitar manutenção futura.
- Manter consistência dentro do script.

Evite documentação excessivamente longa quando uma explicação curta for suficiente.

Não utilize separadores visuais extensos em excesso.

Não crie blocos documentais grandes para operações triviais.

O objetivo é aumentar a compreensão do script, e não simplesmente aumentar a quantidade de comentários.

---

# Verificação de integridade

Após adicionar ou atualizar a documentação, verifique se as alterações estão restritas a comentários.

## Integridade sintática

Quando houver ambiente de execução disponível, utilize uma verificação de sintaxe não executável:

```bash
bash -n caminho/do/script.sh
```

Essa verificação não executa as operações do script.

Se houver acesso ao `shellcheck`, ele poderá ser utilizado para identificar possíveis problemas introduzidos pela documentação.

Entretanto:

- Não corrija automaticamente os problemas apontados.
- Não altere instruções executáveis.
- Não adicione diretivas para silenciar avisos.
- Não trate avisos preexistentes como falhas introduzidas pela documentação.

## Comparação das alterações

Sempre que possível, examine o diff das alterações realizadas.

Confirme que somente comentários foram adicionados ou alterados.

Preste atenção especial a:

- Continuação de linhas.
- Delimitadores.
- Aspas.
- Redirecionamentos.
- Espaçamento sintaticamente significativo.
- Shebang.
- Blocos de comandos.
- Here-documents.

Não execute o script apenas para validar sua documentação, especialmente quando ele manipular arquivos, realizar transferências ou interagir com serviços externos.

Não afirme que o comportamento funcional foi validado por execução quando apenas a sintaxe tiver sido verificada.

---

# Revisão final

Antes de concluir, realize as verificações seguintes.

## Cobertura

- [ ] O script possui um cabeçalho documental adequado.
- [ ] O propósito geral está documentado.
- [ ] Os argumentos relevantes estão documentados.
- [ ] As opções de linha de comando estão documentadas.
- [ ] As variáveis de ambiente relevantes estão documentadas.
- [ ] As configurações relevantes estão documentadas.
- [ ] Todas as funções relevantes possuem documentação adequada.
- [ ] As funções auxiliares não triviais possuem documentação adequada.

## Conteúdo

- [ ] Os argumentos e parâmetros relevantes estão descritos.
- [ ] Os códigos de retorno relevantes estão documentados.
- [ ] As saídas relevantes estão documentadas.
- [ ] Os efeitos colaterais relevantes estão documentados.
- [ ] As regras de negócio não óbvias estão explicadas.
- [ ] As operações complexas estão explicadas.
- [ ] As dependências externas relevantes estão identificadas.
- [ ] Os arquivos e diretórios relevantes estão descritos.
- [ ] O tratamento de erros está compreensível.
- [ ] O fluxo principal de execução está compreensível.

## Qualidade

- [ ] Comentários redundantes foram evitados.
- [ ] Comentários adequados existentes foram preservados.
- [ ] A documentação está integralmente em português brasileiro.
- [ ] Os blocos de comentários seguem uma estrutura consistente.
- [ ] A documentação diferencia retorno, saída textual e efeitos colaterais.
- [ ] Nenhum comportamento foi documentado por suposição.
- [ ] Nenhuma dependência foi inventada.
- [ ] Nenhuma garantia operacional foi presumida.
- [ ] Nenhum valor sensível foi introduzido na documentação.

## Preservação do código

- [ ] O shebang foi preservado.
- [ ] Nenhum comando executável foi alterado.
- [ ] Nenhuma variável foi alterada.
- [ ] Nenhum argumento foi modificado.
- [ ] Nenhuma função foi renomeada ou reestruturada.
- [ ] Nenhuma condição foi alterada.
- [ ] Nenhum redirecionamento foi modificado.
- [ ] Nenhuma expansão foi alterada.
- [ ] Nenhum pipeline foi alterado.
- [ ] Nenhuma diretiva de execução foi modificada.
- [ ] Nenhuma lógica de negócio foi alterada.
- [ ] Nenhuma regra de negócio foi modificada.
- [ ] Nenhuma funcionalidade foi adicionada ou removida.
- [ ] Nenhuma mensagem de execução foi modificada.
- [ ] Nenhum comportamento de erro foi modificado.

## Verificação técnica

- [ ] Os comentários foram inseridos em posições sintaticamente seguras.
- [ ] Nenhum here-document foi modificado.
- [ ] Nenhuma continuação de linha foi comprometida.
- [ ] Nenhum espaço sintaticamente significativo foi alterado.
- [ ] O diff contém exclusivamente alterações documentais.
- [ ] A sintaxe foi verificada com `bash -n`, quando disponível.
- [ ] O script não foi executado desnecessariamente.

---

# Resultado esperado

Ao concluir, o script Bash deve estar melhor documentado e mais fácil de compreender, utilizar e manter, sem qualquer alteração em seu comportamento funcional.

Informe resumidamente:

- Quais elementos foram documentados ou atualizados.
- Quais funções receberam documentação.
- Quais comentários relevantes foram adicionados.
- Se alguma documentação existente precisou ser corrigida.
- Se alguma ambiguidade ou informação insuficiente foi identificada.
- Se a verificação sintática foi realizada e qual foi seu resultado.
- Se foi confirmado que o diff contém apenas alterações documentais.

Caso alguma verificação não tenha sido possível, informe essa limitação sem presumir sucesso.

Não inclua como fato comportamentos que não possam ser confirmados pelo código.
