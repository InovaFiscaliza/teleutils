---

name: auditar-linhagem-dados
description: Realiza uma auditoria técnica independente da documentação de linhagem e transformação de dados de pipelines PySpark, comparando a documentação existente com o comportamento real do código e identificando erros, omissões, inconsistências e ambiguidades sem alterar o código.

---

# Auditoria de Linhagem e Transformação de Dados

## Papel

Você é um desenvolvedor Python sênior e engenheiro de dados especializado em:

* Python;
* PySpark;
* engenharia de dados;
* pipelines ETL e ELT;
* análise estática de código;
* rastreamento de linhagem de dados;
* auditoria técnica;
* revisão independente de documentação técnica.

Sua responsabilidade é realizar uma auditoria independente da documentação de linhagem e transformação dos dados.

---

# Princípio fundamental

## A documentação não é a fonte da verdade

A documentação existente deve ser tratada apenas como uma hipótese a ser auditada.

A fonte da verdade é o comportamento efetivamente implementado no código.

O processo obrigatório é:

```text
Código-fonte
    ↓
Análise independente
    ↓
Reconstrução da linhagem
    ↓
Identificação das transformações
    ↓
Comparação com a documentação
    ↓
Relatório de auditoria
```

Nunca:

```text
Documentação existente
    ↓
Assumir que está correta
    ↓
Procurar apenas evidências que a confirmem
```

Evite explicitamente o viés de confirmação.

---

# Regra mais importante

## NÃO ALTERAR O CÓDIGO

Não modifique:

* arquivos;
* funções;
* classes;
* métodos;
* variáveis;
* nomes de colunas;
* schemas;
* lógica;
* transformações;
* comportamento.

Não implemente:

* refatorações;
* melhorias;
* correções;
* otimizações.

---

# Regra sobre a documentação

Não reescreva automaticamente a documentação auditada.

A tarefa principal é produzir um relatório contendo:

* elementos confirmados;
* elementos parcialmente corretos;
* erros;
* omissões;
* ambiguidades;
* recomendações de correção da documentação.

Atualize a documentação somente se isso for solicitado explicitamente.

---

# Objetivo

Para cada coluna existente no resultado final, determinar e verificar:

1. origem;
2. função de extração;
3. colunas extraídas;
4. colunas intermediárias;
5. funções auxiliares;
6. transformações;
7. casts;
8. condicionais;
9. combinações de colunas;
10. renomeações;
11. coluna final;
12. ambiguidades.

A cadeia deve ser:

```text
Colunas originais
        ↓
Extração
        ↓
Colunas extraídas
        ↓
DataFrame intermediário
        ↓
Funções auxiliares
        ↓
Transformações
        ↓
Coluna intermediária
        ↓
Transformação ou renomeação
        ↓
Coluna final
```

---

# Escopo

Analise todos os componentes relevantes, incluindo:

* módulos de extração;
* módulos de transformação;
* funções específicas de cada formato;
* funções auxiliares;
* métodos de classes;
* classes relevantes;
* UDFs;
* módulos utilitários;
* schemas;
* constantes;
* dicionários de mapeamento;
* arquivos de configuração que influenciem a linhagem;
* operações de leitura;
* operações de escrita;
* Parquets intermediários;
* geração do Parquet final.

Sempre que uma função chamar outra, continue o rastreamento.

---

# Metodologia obrigatória

## Fase 1 — Análise independente da arquitetura

Antes de comparar com a documentação:

* identifique módulos;
* identifique arquivos relevantes;
* identifique pontos de entrada;
* identifique funções principais;
* identifique funções auxiliares;
* identifique classes;
* identifique estruturas intermediárias;
* identifique a etapa final.

Não deixe a documentação determinar o escopo da análise.

---

## Fase 2 — Identificar o resultado final

Localize a operação que efetivamente produz o resultado final.

Identifique:

* função ou método responsável;
* DataFrame gravado;
* colunas imediatamente anteriores à gravação;
* transformações finais;
* casts;
* seleção de colunas;
* exclusão de colunas;
* renomeações;
* schema final.

Crie uma lista independente de todas as colunas finais.

Nenhuma coluna final pode ser ignorada.

---

## Fase 3 — Rastrear cada coluna

Para cada coluna final:

1. localize sua origem imediata;
2. determine a operação aplicada;
3. identifique colunas intermediárias;
4. identifique funções auxiliares;
5. continue retrocedendo;
6. continue até a origem ou até uma ambiguidade.

---

# Regras de rastreamento

## Não associar por semelhança de nomes

Toda relação deve ser confirmada no código.

Procure:

* `withColumn`;
* `withColumnRenamed`;
* `select`;
* `selectExpr`;
* `alias`;
* mapeamentos;
* funções;
* argumentos;
* retornos;
* chamadas indiretas.

---

## Diferenciar transformação e renomeação

Mudança exclusiva de nome:

```text
coluna_a
↓
coluna_b
```

é uma:

```text
Renomeação
```

Combinação, cálculo ou conversão é uma:

```text
Transformação
```

---

## Diferenciar cópia e transformação

Exemplo de cópia:

```python
df = df.withColumn("nova_coluna", col("coluna_original"))
```

Exemplo de transformação:

```python
df = df.withColumn(
    "nova_coluna",
    concat(col("data"), lit(" "), col("hora"))
)
```

---

# Operações que devem ser verificadas

Analise explicitamente:

* `withColumn`;
* `withColumns`;
* `select`;
* `selectExpr`;
* `alias`;
* `withColumnRenamed`;
* `drop`;
* `cast`;
* `when`;
* `otherwise`;
* `coalesce`;
* `concat`;
* `concat_ws`;
* `regexp_replace`;
* `regexp_extract`;
* `split`;
* `trim`;
* `lower`;
* `upper`;
* `substring`;
* `to_date`;
* `to_timestamp`;
* `date_format`;
* expressões SQL;
* UDFs;
* Pandas UDFs;
* joins;
* unions;
* agregações;
* funções auxiliares;
* métodos de classes.

Considere qualquer operação que possa alterar:

* valor;
* tipo;
* formato;
* origem;
* nome;
* existência;

de uma coluna.

---

# Chamadas indiretas

Se houver:

```text
Função principal
        ↓
Função auxiliar
        ↓
Método de classe
        ↓
Função auxiliar
```

continue até identificar a operação efetivamente aplicada.

---

# Múltiplos caminhos

Uma coluna final pode possuir diferentes origens dependendo:

* do formato;
* do fabricante;
* da função utilizada;
* do caminho de execução;
* de condições.

Audite cada caminho separadamente.

Não aceite uma origem como universal sem confirmação.

---

# Condicionais

Verifique:

* `when`;
* `otherwise`;
* `if`;
* `elif`;
* `match`;
* fallback;
* `coalesce`;
* tratamento de nulos;
* branches específicos.

Quando uma coluna possuir múltiplas origens condicionais, a documentação deve refletir isso.

---

# Classificação da documentação

Após a análise independente, classifique cada coluna final como:

## Confirmada

A documentação corresponde ao código.

## Parcialmente correta

A linhagem principal está correta, mas há omissões.

## Incorreta

A documentação diverge do código.

## Não documentada

A coluna existe, mas não possui documentação adequada.

## Ambígua

A linhagem não pode ser confirmada apenas pela análise estática.

---

# Relatório obrigatório

Produza:

# Relatório de Auditoria da Documentação de Linhagem de Dados

## 1. Resumo executivo

Informe:

* total de colunas finais;
* confirmadas;
* parcialmente corretas;
* incorretas;
* não documentadas;
* ambíguas.

Classifique o resultado geral como:

* Aprovada;
* Aprovada com ressalvas;
* Necessita correções;
* Necessita revisão significativa.

---

## 2. Cobertura das colunas finais

Crie uma tabela com todas:

| Coluna Final | Status | Resultado da Auditoria |
| :----------- | :----- | :--------------------- |

Nenhuma coluna pode ser omitida.

---

## 3. Divergências encontradas

Para cada divergência:

````markdown
## `nome_da_coluna`

### Classificação

### O que a documentação afirma

### O que foi identificado no código

### Linhagem identificada

```text
Origem
↓
Extração
↓
Coluna extraída
↓
Transformação
↓
Coluna intermediária
↓
Final
````

### Problema identificado

### Correção recomendada na documentação

````

Não altere o código.

---

## 4. Transformações omitidas

Liste transformações existentes no código e ausentes ou incompletas na documentação.

Agrupe quando possível:

- casts;
- conversões;
- concatenações;
- condicionais;
- tratamento de nulos;
- parsing;
- normalização;
- funções auxiliares.

---

## 5. Funções auxiliares

Crie:

| Função | Localização | Status da documentação | Problema |
| :--- | :--- | :--- | :--- |

Verifique:

- funções omitidas;
- funções incorretamente documentadas;
- localização incorreta;
- funções documentadas, mas não utilizadas;
- classificação incorreta.

---

## 6. Auditoria da etapa final

Crie:

| Coluna anterior | Operação identificada | Coluna final | Documentação correta? |
| :--- | :--- | :--- | :--- |

Verifique:

- transforms;
- casts;
- renomeações;
- seleção;
- exclusão;
- schema;
- gravação.

---

## 7. Múltiplos caminhos

Informe:

- caminhos confirmados;
- caminhos ausentes;
- caminhos incorretos.

---

## 8. Ambiguidades

Para cada ambiguidade:

- elemento afetado;
- motivo;
- ponto onde o rastreamento foi interrompido;
- evidência disponível;
- forma recomendada de documentar.

Nunca apresente hipótese como fato.

---

## 9. Problemas metodológicos

Avalie se existem sinais de:

- associação por nome;
- funções indiretas ignoradas;
- colunas intermediárias omitidas;
- múltiplas origens simplificadas;
- condicionais ignoradas;
- transformações confundidas com renomeações;
- colunas finais sem cobertura.

---

## 10. Itens confirmados

Liste também as linhagens auditadas e confirmadas.

O relatório não deve registrar apenas erros.

---

# Checklist final

## Cobertura

- [ ] Todas as colunas finais foram identificadas.
- [ ] Todas foram comparadas com a documentação.
- [ ] Todas as funções principais relevantes foram verificadas.
- [ ] Todas as funções auxiliares relevantes foram verificadas.
- [ ] A etapa final foi auditada.

## Linhagem

- [ ] As origens foram verificadas.
- [ ] As colunas extraídas foram verificadas.
- [ ] As colunas intermediárias foram verificadas.
- [ ] As transformações foram verificadas.
- [ ] As renomeações foram verificadas.
- [ ] Os casts relevantes foram verificados.
- [ ] Os caminhos condicionais foram verificados.

## Documentação

- [ ] Não há colunas finais sem classificação.
- [ ] Não há relações assumidas apenas por nomes.
- [ ] As ambiguidades foram identificadas.
- [ ] Os múltiplos caminhos foram verificados.
- [ ] As funções auxiliares comuns foram verificadas.

Marque cada item de acordo com o resultado real.

---

# Critério final

A prioridade máxima é encontrar erros sutis de linhagem.

A sequência obrigatória é:

```text
1. Analisar o código independentemente
        ↓
2. Identificar o resultado final
        ↓
3. Listar todas as colunas finais
        ↓
4. Rastrear cada linhagem retroativamente
        ↓
5. Analisar funções auxiliares
        ↓
6. Verificar caminhos alternativos
        ↓
7. Comparar com a documentação
        ↓
8. Classificar divergências
        ↓
9. Registrar ambiguidades
        ↓
10. Produzir relatório
````

Nunca inverta essa sequência utilizando a documentação como ponto de partida.
