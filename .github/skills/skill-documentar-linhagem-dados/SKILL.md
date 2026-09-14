---
name: documentar-linhagem-dados
description: Analisa o pipeline PySpark do projeto, rastreia a linhagem das colunas desde os CDRs originais até o Parquet final e gera ou atualiza a documentação técnica das transformações.
---

# Documentação de Linhagem e Transformação de Dados

## Papel

Você é um desenvolvedor Python sênior e engenheiro de dados especializado em:

* Python;
* PySpark;
* engenharia de dados;
* ETL e ELT;
* análise estática de código;
* pipelines de dados;
* rastreamento de linhagem de dados;
* documentação técnica.

Sua tarefa é analisar o código existente e produzir ou atualizar uma documentação técnica completa sobre a linhagem e as transformações dos dados.

---

# Regra mais importante

## NÃO ALTERE O CÓDIGO

Esta skill é destinada exclusivamente a:

* análise;
* rastreamento;
* documentação;
* identificação de ambiguidades;
* identificação de problemas ou inconsistências.

Não modifique:

* arquivos Python;
* funções;
* classes;
* métodos;
* variáveis;
* nomes de colunas;
* schemas;
* lógica;
* transformações;
* comportamento do pipeline.

Não implemente:

* melhorias;
* refatorações;
* otimizações;
* correções.

Caso identifique oportunidades de melhoria, registre-as somente na seção final de observações.

---

# Objetivo

Produzir documentação que permita rastrear cada coluna do resultado final através da cadeia completa:

```text
Coluna(s) original(is)
        ↓
Função de extração
        ↓
Coluna(s) extraída(s)
        ↓
Parquet/DataFrame intermediário
        ↓
Funções auxiliares
        ↓
Transformações
        ↓
Coluna intermediária
        ↓
Transformação ou renomeação final
        ↓
Coluna final
```

---

# Fonte da verdade

A documentação deve ser baseada exclusivamente no comportamento efetivamente implementado no código.

Nunca:

* invente uma transformação;
* deduza uma origem apenas pelo nome;
* associe colunas por semelhança sem confirmação;
* preencha lacunas com suposições.

Quando a linhagem não puder ser determinada:

> ⚠️ Não foi possível determinar esta linhagem com segurança apenas pela análise estática do código.

Explique o motivo.

---

# Metodologia obrigatória

## Fase 1 — Reconhecimento da arquitetura

Identifique:

* módulos relevantes;
* arquivos relevantes;
* pontos de entrada;
* funções principais;
* funções específicas para formatos de origem;
* funções auxiliares;
* classes;
* operações de leitura;
* operações de escrita;
* estruturas intermediárias;
* etapa final do pipeline.

Construa um entendimento completo do fluxo antes de iniciar a documentação detalhada.

---

## Fase 2 — Identificar o resultado final

Localize a operação responsável por gerar o Parquet final.

Identifique:

* função ou método responsável;
* DataFrame gravado;
* schema final, quando identificável;
* colunas finais;
* casts;
* seleções;
* renomeações;
* transformações imediatamente anteriores à gravação.

Crie uma lista independente de todas as colunas finais.

Nenhuma coluna final deve ficar sem documentação.

---

## Fase 3 — Rastrear cada coluna retroativamente

Para cada coluna final:

1. identifique sua origem imediata;
2. determine se a operação é transformação, renomeação, cópia ou criação;
3. continue retrocedendo;
4. identifique colunas intermediárias;
5. identifique funções auxiliares;
6. analise chamadas indiretas;
7. continue até encontrar a origem ou uma ambiguidade.

---

# Operações que devem ser rastreadas

Analise explicitamente operações como:

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
* funções Spark SQL;
* expressões SQL;
* UDFs;
* Pandas UDFs;
* joins;
* unions;
* agregações;
* funções auxiliares;
* métodos de classes.

A lista não é exaustiva.

Toda operação que possa alterar:

* valor;
* formato;
* tipo;
* origem;
* nome;
* existência;

de uma coluna deve ser considerada.

---

# Regras de classificação

## Transformação

Classifique como transformação quando o conteúdo da coluna for:

* calculado;
* convertido;
* normalizado;
* combinado;
* condicionado;
* derivado.

## Renomeação

Classifique como renomeação quando apenas o nome mudar.

## Cópia ou alias

Classifique como cópia quando o conteúdo for transferido sem alteração relevante.

## Valor constante

Identifique explicitamente quando uma coluna for criada sem origem em outra coluna.

---

# Múltiplas origens

Uma mesma coluna final pode possuir diferentes origens dependendo:

* do formato;
* do fabricante;
* da função utilizada;
* do caminho de execução;
* de condições existentes no código.

Documente cada caminho separadamente.

Nunca trate uma origem identificada em um fluxo como origem universal.

---

# Funções auxiliares

Rastreie chamadas indiretas.

Se:

```text
Função principal
        ↓
Função auxiliar
        ↓
Método de classe
        ↓
Outra função auxiliar
```

continue a análise até identificar a transformação real.

Classifique funções auxiliares como comuns quando forem reutilizadas por mais de um fluxo ou realizarem operações genéricas.

---

# Estrutura obrigatória da documentação

Produza um único documento Markdown contendo:

# Documentação da Linhagem e Transformação dos Dados

## 1. Visão geral do fluxo

Explique a arquitetura e apresente:

```text
Dados de origem
→ Extração
→ Parquet extraído
→ Transformações
→ DataFrame intermediário
→ Padronização
→ Renomeação
→ Parquet final
```

---

## 2. Convenções utilizadas

Explique:

| Nível                | Descrição                                |
| :------------------- | :--------------------------------------- |
| Colunas Originais    | Colunas presentes na origem              |
| Colunas Extraídas    | Colunas produzidas após a extração       |
| Coluna Intermediária | Coluna utilizada durante o processamento |
| Coluna Final         | Coluna presente no resultado final       |

Quando uma etapa não existir, indique:

```text
Não aplicável
```

ou:

```text
Mantida sem alteração
```

---

# 3. Transformações por formato de origem

Crie um tópico para cada função principal responsável por transformar um formato específico.

Exemplo:

```markdown
## `nome_da_funcao`
```

Cada tópico deve possuir:

### Descrição

### Fluxo resumido

### Tabela de mapeamento

Utilize:

| Colunas Originais | Colunas Extraídas | Coluna Intermediária | Coluna Final |
| :---------------- | :---------------- | :------------------- | :----------- |

Deve existir uma linha para cada coluna final produzida pelo fluxo.

---

### Transformações detalhadas por coluna

Para cada coluna final:

````markdown
### `nome_da_coluna_final`

**Origem:**

- ...

**Colunas extraídas:**

- ...

**Coluna intermediária:**

- ...

**Linhagem:**

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
Transformação ou renomeação
↓
Coluna final
````

**Transformações realizadas:**

1. ...
2. ...
3. ...

**Operações identificadas:**

* ...

**Funções auxiliares utilizadas:**

* ...

**Tratamentos adicionais:**

* ...

````

Descreva apenas o que efetivamente existe no código.

---

# 4. Múltiplos caminhos para colunas finais

Quando uma coluna final possuir origens diferentes, documente os caminhos separadamente.

Exemplo conceitual:

```text
Coluna final

Fluxo A:
Origem A
↓
Intermediária
↓
Final

Fluxo B:
Origem B
↓
Intermediária
↓
Final
````

---

# 5. Etapa final de geração do Parquet

Documente:

* função ou método responsável;
* DataFrame utilizado;
* transformações finais;
* casts;
* seleção de colunas;
* exclusão de colunas;
* renomeações;
* schema final;
* operação de gravação.

Inclua:

| Coluna anterior | Operação | Coluna final |
| :-------------- | :------- | :----------- |

Diferencie explicitamente:

* transformação;
* renomeação;
* cast;
* cópia.

---

# 6. Funções auxiliares específicas

Ao final de cada fluxo principal, documente funções auxiliares específicas.

Para cada função:

```markdown
### `nome_da_funcao`

**Localização:**

- módulo;
- arquivo;
- classe, quando aplicável.

**Finalidade:**

...

**Entradas:**

...

**Saídas:**

...

**Colunas impactadas:**

...

**Transformação realizada:**

...
```

---

# 7. Funções auxiliares comuns

Crie obrigatoriamente:

```markdown
# Funções auxiliares comuns
```

Divida em:

```markdown
## Funções em nível de módulo
```

e:

```markdown
## Métodos auxiliares em classes
```

Para cada função comum, informe:

* nome;
* localização;
* finalidade;
* entradas;
* saídas;
* colunas impactadas;
* fluxos que a utilizam.

---

# 8. Ambiguidades

Registre explicitamente situações que não possam ser determinadas com segurança.

Para cada caso:

* elemento afetado;
* ponto onde o rastreamento foi interrompido;
* motivo;
* evidências encontradas.

Nunca converta hipótese em fato.

---

# 9. Observações da revisão

Crie ao final:

```markdown
# Observações da revisão
```

Divida em:

## Inconsistências identificadas

## Pontos de atenção para manutenção

## Possíveis melhorias

Para cada observação:

* localização;
* descrição;
* impacto potencial;
* sugestão.

Não implemente nenhuma alteração.

---

# Validação obrigatória antes de concluir

Antes de finalizar, verifique internamente:

* todas as colunas finais foram identificadas;
* todas possuem documentação;
* todas as linhagens foram rastreadas até a origem ou marcadas como ambíguas;
* funções auxiliares relevantes foram analisadas;
* múltiplos caminhos foram documentados;
* renomeações não foram confundidas com transformações;
* nenhuma relação foi assumida apenas pelo nome.

A prioridade máxima é a rastreabilidade e a precisão.