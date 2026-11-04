# Instruções do Projeto

## Contexto

Este projeto é desenvolvido em Python e PySpark para extração, padronização e transformação de dados.

O projeto processa arquivos Parquet contendo dados provenientes de diferentes formatos e fontes, incluindo CDRs que podem possuir:

- schemas diferentes;
- nomes de colunas diferentes;
- formatos de data diferentes;
- formatos de hora diferentes;
- informações semanticamente equivalentes distribuídas em colunas diferentes.

O objetivo do pipeline é transformar essas diferentes estruturas em um modelo padronizado utilizado pelo sistema ou banco de dados de destino.

---

# Arquitetura geral

Os principais módulos responsáveis pelo processamento são:

- `extractors`
- `transformers`

O fluxo geral de dados é:

```text
Arquivos Parquet de origem
        ↓
extractors
        ↓
Parquet extraído
        ↓
transformers
        ↓
DataFrame padronizado
        ↓
Transformações adicionais
        ↓
Renomeação para o schema de destino
        ↓
Parquet final
```

---

# Responsabilidade dos módulos

## `extractors`

O módulo `extractors` é responsável por:

- ler os arquivos de origem;
- tratar diferenças entre schemas;
- identificar colunas relevantes;
- adaptar diferentes formatos de origem;
- produzir uma estrutura extraída utilizada pelas etapas posteriores.

As funções desse módulo podem ser específicas para determinados fabricantes, formatos ou tipos de CDR.

---

## `transformers`

O módulo `transformers` é responsável por:

- ler os dados extraídos;
- aplicar transformações;
- combinar informações provenientes de múltiplas colunas;
- padronizar formatos;
- converter tipos;
- criar colunas intermediárias;
- aplicar regras de transformação;
- preparar o DataFrame para o schema de destino;
- renomear colunas para o padrão final;
- gravar o Parquet final.

---

# Princípios gerais

## Preservação da arquitetura

Ao trabalhar no projeto:

- compreenda a responsabilidade de cada módulo antes de propor alterações;
- não mova responsabilidades entre módulos sem analisar o impacto;
- não introduza regras específicas de domínio em componentes destinados a serem genéricos;
- preserve a separação entre extração, transformação e demais etapas do pipeline.

---

# Linhagem dos dados

Ao analisar ou documentar transformações, considere a linhagem:

```text
Colunas originais
        ↓
Extração
        ↓
Colunas extraídas
        ↓
Parquet intermediário
        ↓
Transformações
        ↓
Colunas intermediárias
        ↓
Padronização
        ↓
Renomeação
        ↓
Colunas finais
```

Nunca determine a linhagem de uma coluna apenas pela semelhança entre nomes.

Toda relação deve ser confirmada pelo código.

---

# Transformação versus renomeação

Diferencie claramente:

## Transformação

Uma transformação altera, deriva ou calcula o conteúdo da coluna.

Exemplos:

- concatenação;
- conversão de tipo;
- parsing;
- normalização;
- combinação de múltiplas colunas;
- tratamento condicional;
- tratamento de valores nulos.

## Renomeação

Uma renomeação apenas altera o nome da coluna.

Exemplo conceitual:

```text
data_hora
↓
dh_chamada
```

Se o conteúdo não for alterado, isso deve ser tratado como renomeação.

---

# Funções auxiliares

Sempre que uma transformação for realizada indiretamente por:

- função auxiliar;
- método de classe;
- UDF;
- função utilitária;
- módulo externo do projeto;

continue a análise até identificar a operação efetivamente realizada.

Não considere uma linhagem completa apenas porque uma chamada de função foi identificada.

---

# Ambiguidades

Quando não for possível determinar um comportamento com segurança apenas pela análise do código:

- não invente relações;
- não faça suposições;
- registre explicitamente a ambiguidade;
- indique onde o rastreamento foi interrompido.

Utilize linguagem equivalente a:

> Não foi possível determinar esta relação com segurança apenas pela análise estática do código.

---

# Documentação

A documentação técnica do projeto deve:

- utilizar Markdown;
- utilizar português do Brasil;
- utilizar nomes reais de funções, classes e colunas;
- ser baseada no comportamento efetivamente implementado;
- diferenciar origem, transformação e renomeação;
- documentar funções auxiliares relevantes;
- registrar caminhos alternativos;
- registrar ambiguidades.

---

# Alterações de código

Quando a tarefa envolver alteração de código:

- analise dependências antes de modificar funções;
- preserve interfaces existentes quando possível;
- evite alterações desnecessárias;
- não altere comportamento fora do escopo solicitado;
- documente decisões relevantes quando solicitado.

Quando a tarefa solicitar exclusivamente análise, documentação ou auditoria:

- não modifique o código;
- não implemente melhorias sem solicitação explícita;
- mantenha separadas observações e sugestões da análise principal.
