---
name: documentar-modulo-python
description: Documenta módulos Python utilizando docstrings Google Style, comentários explicativos e anotações de manutenção, sem alterar o código ou a lógica de negócio.
---

# Documentar módulo Python

Analise cuidadosamente o módulo Python em contexto e produza uma versão tecnicamente bem documentada, adicionando ou aperfeiçoando **docstrings**, **comentários explicativos** e **anotações de manutenção**, sem alterar o comportamento, a lógica de negócio ou a estrutura funcional do código.

## Objetivo

Produzir documentação técnica de alta qualidade que permita que qualquer desenvolvedor, mesmo sem conhecimento prévio do projeto, compreenda:

* o propósito geral do módulo;
* sua responsabilidade dentro do projeto;
* a responsabilidade de cada classe;
* a finalidade de cada função e método;
* o fluxo de processamento dos dados;
* as regras de negócio implementadas;
* as transformações realizadas;
* as dependências e integrações relevantes;
* os efeitos colaterais importantes;
* os pontos relevantes para manutenção futura.

A documentação deve explicar o comportamento efetivamente implementado no código, sem inventar funcionalidades, regras ou intenções.

---

# Idioma

Toda a documentação adicionada ou alterada deve ser escrita em **português brasileiro (pt-BR)**.

Isso inclui:

* docstrings de módulos;
* docstrings de classes;
* docstrings de funções;
* docstrings de métodos;
* comentários internos;
* anotações de manutenção.

Não traduza ou altere nomes existentes de:

* classes;
* funções;
* métodos;
* variáveis;
* parâmetros;
* APIs;
* bibliotecas;
* estruturas técnicas que devam permanecer em inglês.

---

# Preservação rigorosa do código

A lógica e o comportamento funcional existentes devem ser integralmente preservados.

As únicas alterações permitidas são:

* criação ou atualização de docstrings;
* criação ou atualização de comentários;
* criação de anotações de manutenção estritamente relacionadas à documentação.

Não altere:

* instruções executáveis;
* lógica de negócio;
* comportamento funcional;
* assinaturas públicas ou privadas;
* parâmetros;
* valores padrão;
* retornos;
* imports;
* decorators;
* type hints;
* estruturas de classes;
* estruturas de funções;
* ordem das operações;
* condições;
* estruturas de dados;
* nomes de variáveis;
* nomes de funções;
* nomes de classes;
* fluxos de execução.

Não:

* refatore o código;
* reorganize o módulo;
* otimize o código;
* corrija problemas;
* adicione funcionalidades;
* remova funcionalidades;
* altere regras de negócio.

A documentação deve se adaptar ao código existente. O código não deve ser alterado para se adequar à documentação.

---

# Documentação e comentários existentes

Preserve docstrings e comentários existentes quando estiverem:

* corretos;
* atualizados;
* tecnicamente adequados;
* úteis para compreensão ou manutenção.

Não reescreva documentação adequada apenas para alterar sua redação ou estilo.

Atualize uma docstring ou comentário existente apenas quando ele estiver:

* incorreto;
* desatualizado;
* incompleto de forma relevante;
* inconsistente com o comportamento atual;
* incompatível com o padrão de documentação definido para o módulo;
* redundante e prejudicial à clareza.

Ao atualizar documentação existente, preserve as informações corretas.

---

# Processo de análise

Antes de adicionar ou alterar documentação, analise cuidadosamente o módulo para compreender:

1. seu propósito geral;
2. sua responsabilidade dentro do projeto;
3. suas dependências relevantes;
4. suas integrações;
5. o fluxo principal de processamento;
6. as classes existentes;
7. as funções existentes;
8. os métodos existentes;
9. as funções auxiliares;
10. as regras de negócio;
11. as transformações de dados;
12. os efeitos colaterais relevantes;
13. os pontos de entrada e saída relevantes.

Não escreva documentação baseada apenas:

* no nome de uma função;
* no nome de uma variável;
* no nome de uma classe;
* em convenções presumidas;
* em suposições sobre o domínio.

A documentação deve refletir o comportamento efetivamente identificado no código.

---

# Análise de contexto externo

Quando o comportamento, a finalidade ou o significado de um elemento não puder ser determinado com segurança apenas pelo módulo em contexto, analise os componentes diretamente relacionados quando necessário.

Podem ser analisados, por exemplo:

* módulos chamadores;
* módulos chamados;
* classes relacionadas;
* funções auxiliares;
* configurações diretamente utilizadas;
* schemas;
* estruturas compartilhadas;
* contratos de entrada e saída.

O objetivo dessa análise externa é exclusivamente compreender corretamente o módulo que está sendo documentado.

Não invente contexto.

Não trate como fato informações que não possam ser confirmadas pelo código ou pelos componentes diretamente relacionados.

Módulos externos podem ser analisados para compreensão, mas não devem ser modificados, salvo se a tarefa solicitar explicitamente sua documentação.

---

# Documentação do módulo

Adicione ou atualize uma docstring no início do arquivo.

A docstring do módulo deve explicar, quando aplicável:

* objetivo principal;
* responsabilidade dentro do projeto;
* principais funcionalidades;
* fluxo geral de processamento;
* tipos ou estruturas de dados relevantes;
* dependências relevantes;
* integrações relevantes;
* exemplo de uso.

Utilize uma estrutura compatível com o padrão Google Style.

Exemplo:

```python
"""
Módulo responsável pela transformação de registros telefônicos.

Este módulo implementa funções para padronização,
normalização e enriquecimento dos dados extraídos
de diferentes fontes de telecomunicações.

Principais funcionalidades:
    - Conversão de formatos de data.
    - Padronização de números telefônicos.
    - Normalização de campos.
    - Validação de registros.

Dependências:
    - pyspark

Exemplo:
    >>> transformar_dataframe(df)
"""
```

Não inclua seções artificiais ou informações irrelevantes apenas para preencher uma estrutura padrão.

---

# Documentação de classes

Todas as classes devem possuir docstrings adequadas.

A docstring da classe deve explicar, quando aplicável:

* responsabilidade;
* contexto de uso;
* papel dentro do módulo;
* principais atributos;
* dependências relevantes;
* relacionamento com outros componentes;
* comportamento geral.

Utilize o padrão Google Style.

Exemplo:

```python
class TransformadorChamadas:
    """
    Responsável por transformar registros brutos de chamadas.

    A classe centraliza regras de normalização e enriquecimento
    dos dados antes das etapas analíticas.

    Attributes:
        configuracao: Configurações utilizadas durante o processamento.
    """
```

Não documente atributos inexistentes ou inferidos apenas para preencher a seção `Attributes`.

---

# Documentação de funções e métodos

Documente todas as funções e métodos públicos que não possuam documentação adequada.

Para funções e métodos privados, adicione ou aperfeiçoe a documentação quando sua:

* finalidade;
* regra de negócio;
* comportamento;
* efeito colateral;
* papel no fluxo;
* relação com outras operações;

não for imediatamente evidente pelo código.

Não adicione docstrings extensas a funções privadas triviais apenas para aumentar a cobertura documental.

Cada docstring deve descrever o comportamento real da função ou método.

Inclua, quando aplicável:

* descrição;
* objetivo da operação;
* parâmetros;
* valor retornado;
* exceções relevantes;
* efeitos colaterais;
* regras de negócio;
* observações importantes;
* detalhes relevantes para manutenção.

Utilize o padrão Google Style.

Utilize as seções:

```text
Args:
Returns:
Raises:
Notes:
```

somente quando aplicáveis.

Não inclua seções vazias.

---

# Parâmetros

Para cada parâmetro relevante, documente:

* finalidade;
* significado no processamento;
* formato ou estrutura esperada, quando relevante;
* restrições importantes;
* comportamento esperado.

Mantenha consistência entre:

* nome do parâmetro;
* type hints existentes;
* descrição na docstring.

Não altere a tipagem existente.

---

# Retornos

Documente claramente, quando aplicável:

* o que é retornado;
* o significado do resultado;
* a estrutura retornada;
* como o resultado é utilizado no fluxo seguinte.

Quando o retorno possuir uma estrutura complexa, explique sua composição.

Exemplo:

```python
Returns:
    dict[str, list[int]]:
        Mapeamento entre identificadores e suas respectivas ocorrências.
```

Quando uma função modificar um objeto recebido e retornar `None`, documente o efeito colateral quando isso for relevante para compreensão ou manutenção.

---

# Exceções

Documente `Raises:` apenas quando a exceção for relevante e puder ser confirmada pelo código.

Considere especialmente exceções que sejam:

* explicitamente levantadas;
* tratadas de maneira específica;
* parte relevante do contrato da função.

Não documente uma exceção apenas porque uma biblioteca ou operação poderia, em teoria, lançá-la.

Não utilize descrições genéricas como:

```text
Raises:
    Exception: Caso ocorra algum erro.
```

Não invente exceções.

---

# Comentários internos

Adicione comentários apenas quando eles agregarem valor real à compreensão ou manutenção.

Antes de adicionar um comentário, avalie se a informação já está suficientemente clara:

* pelo nome da variável;
* pelo nome da função;
* pela estrutura do código;
* pela operação realizada.

Não adicione comentários que apenas descrevam literalmente a instrução imediatamente abaixo.

Priorize comentários que expliquem:

* o motivo da operação;
* a regra de negócio;
* a decisão arquitetural;
* uma restrição;
* uma transformação não trivial;
* um comportamento não evidente;
* uma dependência importante.

Comentários são especialmente apropriados para os casos seguintes.

## Regras de negócio

```python
# Telefones com menos de 10 dígitos são considerados inválidos
# para fins de processamento estatístico.
```

## Transformações complexas

```python
# Converte diferentes representações de data para um
# formato único utilizado em todo o pipeline.
```

## Algoritmos não triviais

```python
# Utiliza busca binária para reduzir o tempo de localização
# dos registros em grandes volumes de dados.
```

## Decisões arquiteturais

```python
# Mantido como processamento sequencial para preservar
# a ordem temporal dos eventos.
```

Evite comentários redundantes como:

```python
# Incrementa contador
contador += 1
```

quando a operação já for autoexplicativa.

---

# Regras de negócio

Sempre que uma regra de negócio estiver implementada no código e não for imediatamente evidente, documente:

* a regra;
* sua finalidade;
* sua condição de aplicação;
* seu impacto no processamento.

Não altere, corrija ou otimize a regra.

A documentação deve representar a implementação atual, inclusive quando houver decisões de negócio aparentemente incomuns.

---

# Fluxo de processamento

Quando o módulo implementar um fluxo relevante, a documentação deve tornar esse fluxo compreensível.

Explique, quando aplicável:

```text
Entrada
  ↓
Validação
  ↓
Transformação
  ↓
Normalização
  ↓
Processamento
  ↓
Resultado
```

Não é necessário criar diagramas ou representações artificiais para todos os módulos.

Utilize a forma de explicação que melhor contribua para a compreensão do código.

---

# Transformações de dados

Quando o módulo realizar transformações de dados, documente, quando relevante:

* origem dos dados;
* formato esperado;
* estrutura de entrada;
* colunas ou campos relevantes;
* normalizações;
* conversões;
* enriquecimentos;
* filtros;
* agregações;
* colunas criadas;
* colunas alteradas;
* colunas removidas;
* estrutura produzida.

Explique especialmente o propósito das transformações que não forem imediatamente evidentes pelo código.

Não documente transformações inexistentes ou inferidas apenas pelo nome de uma coluna.

---

# Código PySpark

Ao documentar código que utiliza PySpark, considere explicitamente as características do processamento distribuído.

Documente, quando relevante:

* DataFrame de entrada;
* DataFrame produzido;
* colunas utilizadas;
* colunas criadas;
* colunas alteradas;
* colunas removidas;
* transformações de schema;
* filtros;
* joins;
* agregações;
* operações de leitura;
* operações de gravação;
* dependências entre transformações;
* transformações e ações;
* efeitos relevantes da lazy evaluation.

Ao descrever operações com DataFrames, explique a finalidade da transformação dentro do pipeline.

Por exemplo, quando uma operação cria uma nova coluna, documente:

* qual é a finalidade da coluna;
* quais colunas participam de sua construção, quando relevante;
* qual transformação é aplicada;
* como o resultado é utilizado posteriormente.

Não explique conceitos genéricos do PySpark quando isso não for necessário para compreender o código.

Evite transformar as docstrings em tutoriais sobre PySpark.

---

# Dependências e integrações

Documente dependências relevantes para compreensão e manutenção do módulo.

Considere, quando aplicável:

* bibliotecas externas;
* módulos internos;
* APIs;
* sistemas de armazenamento;
* arquivos de entrada ou saída;
* estruturas compartilhadas;
* schemas;
* componentes externos.

Não transforme a documentação em uma simples reprodução de todos os imports.

Documente apenas dependências relevantes para compreender o comportamento e a manutenção do módulo.

---

# Tipagem e documentação

Quando houver type hints:

* mantenha a documentação consistente com a tipagem;
* explique tipos complexos quando necessário;
* explique estruturas retornadas quando necessário;
* não altere os type hints existentes.

Quando houver uma possível inconsistência entre:

* a tipagem existente;
* o comportamento aparente do código;

não altere automaticamente a tipagem.

Documente o comportamento com cautela e, quando necessário, registre a situação como uma anotação de manutenção.

---

# Anotações de manutenção

Adicione anotações de manutenção apenas quando houver evidência concreta no código.

Podem ser utilizadas para registrar:

* dependências implícitas;
* acoplamentos importantes;
* comportamentos não óbvios;
* limitações identificáveis;
* pontos sensíveis para alterações futuras;
* ordem obrigatória de operações;
* efeitos colaterais relevantes;
* dependências entre etapas do processamento.

Não crie observações especulativas.

Não introduza:

* `TODO`;
* `FIXME`;
* `HACK`;

ou anotações semelhantes, salvo quando já existirem no código ou quando uma limitação concreta puder ser confirmada diretamente pela implementação.

Não utilize anotações de manutenção para sugerir refatorações ou modificar o comportamento.

---

# Ambiguidades e informações insuficientes

Não apresente como fato comportamentos que não possam ser confirmados pelo código disponível.

Quando uma informação importante não puder ser determinada com segurança:

* não invente a explicação;
* não presuma a intenção do desenvolvedor;
* não associe componentes apenas pela semelhança dos nomes.

Quando necessário, utilize documentação neutra e tecnicamente precisa, descrevendo apenas o comportamento confirmado.

Registre a ambiguidade como anotação de manutenção apenas quando ela for relevante para compreensão ou manutenção futura.

---

# Qualidade da documentação

A documentação deve:

* ser objetiva;
* ser tecnicamente precisa;
* ser escrita em português brasileiro;
* seguir o padrão Google Style;
* refletir o comportamento efetivo do código;
* evitar redundância;
* evitar comentários óbvios;
* explicar o "porquê" quando necessário;
* explicar o "o quê" quando isso não for evidente;
* facilitar manutenção futura;
* manter consistência dentro do módulo.

Evite documentação excessivamente longa quando uma explicação curta for suficiente.

O objetivo é aumentar a compreensão do código, e não simplesmente aumentar a quantidade de texto.

---

# Revisão final

Antes de concluir, verifique:

## Cobertura

* [ ] O módulo possui uma docstring principal adequada.
* [ ] Todas as classes possuem docstrings adequadas.
* [ ] Todas as funções públicas possuem documentação adequada.
* [ ] As funções privadas relevantes possuem documentação adequada.
* [ ] Os métodos relevantes possuem documentação adequada.

## Conteúdo

* [ ] Os parâmetros relevantes estão documentados.
* [ ] Os retornos relevantes estão documentados.
* [ ] As exceções relevantes estão documentadas.
* [ ] Os efeitos colaterais relevantes estão documentados.
* [ ] Regras de negócio não óbvias estão explicadas.
* [ ] Transformações complexas estão explicadas.
* [ ] Dependências relevantes estão identificadas quando necessário.
* [ ] O fluxo de processamento está compreensível.

## Qualidade

* [ ] Comentários redundantes foram evitados.
* [ ] Docstrings adequadas existentes foram preservadas.
* [ ] A documentação está integralmente em português brasileiro.
* [ ] As docstrings seguem o padrão Google Style.
* [ ] A documentação é compatível com os type hints existentes.
* [ ] Nenhum comportamento foi documentado por suposição.

## Preservação do código

* [ ] Nenhuma instrução executável foi alterada.
* [ ] Nenhum import foi alterado.
* [ ] Nenhum decorator foi alterado.
* [ ] Nenhum type hint foi alterado.
* [ ] Nenhuma assinatura foi modificada.
* [ ] Nenhuma lógica de negócio foi alterada.
* [ ] Nenhuma regra de negócio foi modificada.
* [ ] Nenhuma funcionalidade foi adicionada ou removida.

---

# Resultado esperado

Ao concluir, o módulo deve estar melhor documentado e mais fácil de compreender e manter, sem qualquer alteração em seu comportamento funcional.

Informe resumidamente:

* quais elementos foram documentados ou atualizados;
* quais comentários relevantes foram adicionados;
* se alguma documentação existente precisou ser corrigida;
* se alguma ambiguidade ou informação insuficiente foi identificada.

Não inclua como fato comportamentos que não possam ser confirmados pelo código.
