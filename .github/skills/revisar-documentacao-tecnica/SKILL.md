---
name: revisar-documentacao-tecnica
description: Revisa e reorganiza a documentação técnica do projeto `teleutils` pela ordem lógica dos módulos e do fluxo de processamento, e garante que cada API pública tenha exemplos de uso verificados contra o código.
user-invocable: true
---

# Reorganizar a documentação técnica e enriquecê-la com exemplos de uso

Você é um engenheiro de software Python sênior, especialista em arquitetura de pacotes, documentação técnica e engenharia de dados com PySpark.

Sua tarefa tem duas partes complementares sobre a documentação técnica do projeto `teleutils`:

1. reorganizar os documentos para que a apresentação dos módulos acompanhe a ordem lógica do fluxo de processamento e das responsabilidades arquiteturais;
2. garantir que cada API pública relevante tenha exemplos de uso corretos, executáveis ou verificáveis, posicionados junto ao contrato que ilustram.

## 1. Objetivo

A documentação técnica deve apresentar primeiro os componentes que implementam o fluxo principal do projeto, na ordem em que os dados são processados e disponibilizados.

Para o `teleutils`, a sequência arquitetural principal é:

```text
teleutils.core.extractors
          |
          v
teleutils.core.transformers
          |
          v
teleutils.core.orchestrators
          |
          v
teleutils.core.loaders
```

Essa sequência representa a organização desejada para a documentação, não necessariamente uma cadeia rígida de chamadas entre os módulos.

Os módulos e scripts auxiliares, que oferecem funcionalidades complementares ou independentes do fluxo principal, devem ser documentados somente depois de todos os componentes principais.

O objetivo é permitir que um desenvolvedor que nunca teve contato com o projeto compreenda sua arquitetura acompanhando a documentação na mesma ordem em que conheceria seu funcionamento e, ao chegar a cada API, veja como utilizá-la na prática.

## 2. Ordem obrigatória dos componentes

Organize a documentação da API pública conforme a seguinte hierarquia:

### 2.1. Componentes principais

A ordem deve ser preservada:

1. **`teleutils.core.extractors`**
   - Responsabilidade: leitura das fontes de CDR e extração dos campos conforme os contratos de cada layout.
   - Inclua a documentação de `teleutils.core.extractors.schemas` junto desta camada, respeitando a relação entre os contratos declarativos e os extratores.
   - Apresente os schemas antes dos extratores quando isso facilitar a compreensão de suas dependências.

2. **`teleutils.core.transformers`**
   - Responsabilidade: transformação dos dados extraídos e padronização no contrato analítico final.
   - Apresente as classes e os métodos de transformação depois dos extratores.

3. **`teleutils.core.orchestrators`**
   - Responsabilidade: coordenação das etapas de extração e transformação.
   - Apresente as funções de configuração, planejamento, execução e acompanhamento dos lotes.
   - A documentação deve deixar clara sua relação com os extratores e transformadores.

4. **`teleutils.core.loaders`**
   - Responsabilidade: disponibilização dos dados processados nos destinos de armazenamento.
   - Apresente os mecanismos de carga depois da documentação do processamento e da orquestração.
   - Preserve as distinções entre a execução do pipeline e a persistência dos resultados.

### 2.2. Módulos e scripts auxiliares

Depois de todos os componentes principais, documente os módulos auxiliares relevantes.

No estado atual do projeto, considere especialmente:

- `teleutils.preprocessing`;
- utilitários independentes;
- funções de apoio;
- scripts de transferência ou administração;
- outros módulos públicos que não façam parte diretamente do fluxo principal.

A lista acima é orientativa. Analise a estrutura atual do repositório para identificar os componentes efetivamente existentes.

Não classifique automaticamente um módulo como auxiliar apenas por seu nome ou localização. Considere sua responsabilidade, suas dependências e sua participação no fluxo principal.

## 3. Análise prévia obrigatória

Antes de editar qualquer documento:

1. Examine a estrutura atual do repositório.
2. Identifique os módulos, subpacotes, classes e funções públicas.
3. Identifique todos os documentos técnicos existentes em `docs/`.
4. Localize a documentação da API pública, especialmente `docs/teleutils_api.md`.
5. Identifique a posição atual de cada componente na documentação.
6. Verifique se existem documentos especializados que também apresentam os componentes em uma ordem diferente.
7. Identifique referências cruzadas que possam ser afetadas pela reorganização.
8. **Inventarie os exemplos de uso existentes**: para cada API pública, registre se já há exemplo, onde ele está, se ainda é válido (assinatura, imports, nomes de parâmetros) e se está sob o módulo correto.
9. **Levante as fontes de verdade para os exemplos**: assinaturas e docstrings no código, testes em `tests/`, o `README.md` e eventuais notebooks. Esses materiais indicam usos reais e válidos da API.

Utilize o código atual como referência para identificar as responsabilidades dos módulos.

Não presuma que a ordem atual dos documentos representa a arquitetura correta.

Antes de editar, apresente um resumo da estrutura encontrada, da reorganização proposta e **da matriz de exemplos** (API × exemplo existente / exemplo a criar / exemplo a corrigir), destacando eventuais ambiguidades que precisem de decisão do usuário.

## 4. Regras de reorganização

A reorganização deve seguir estas regras:

### Preservação do conteúdo

- Preserve integralmente as informações técnicas que continuam corretas.
- Não elimine contratos, parâmetros, retornos, exceções ou efeitos colaterais.
- Não simplifique explicações técnicas apenas para reduzir o tamanho do documento.
- Não remova exemplos de utilização que continuem válidos.
- Não elimine ressalvas importantes sobre comportamento, desempenho, consistência ou limitações.
- Não transforme informações específicas em afirmações genéricas.
- Não reescreva trechos adequados apenas para alterar sua redação.

### Organização estrutural

- Reorganize os títulos e seções para refletir a sequência arquitetural definida.
- Mantenha classes e funções agrupadas sob seus respectivos módulos.
- Mantenha subpacotes junto da camada à qual pertencem.
- Evite separar contratos e componentes que dependam diretamente uns dos outros.
- Documente os módulos auxiliares somente depois de todos os componentes principais.
- Preserve seções gerais de referência, como requisitos, erros, efeitos colaterais e orientações de utilização, em posições coerentes com sua abrangência.

### Consistência arquitetural

A ordem documental não deve sugerir relações de dependência inexistentes.

Em particular:

- Não afirme que o orquestrador é uma dependência obrigatória para utilizar extratores ou transformadores.
- Não afirme que o loader é invocado automaticamente pelo orquestrador quando isso não estiver implementado.
- Não confunda a ordem de apresentação com a ordem obrigatória de execução.
- Preserve a possibilidade de utilização independente dos componentes quando suportada pela API.

## 5. Escopo de alterações

A tarefa é de reorganização e enriquecimento documental.

É permitido:

- mover seções;
- reorganizar subseções;
- ajustar títulos para melhorar a hierarquia;
- corrigir referências internas afetadas pela movimentação;
- ajustar sumários e índices;
- corrigir pequenas transições textuais necessárias para manter a coerência;
- **criar exemplos de uso novos e corrigir exemplos existentes que estejam desatualizados**, conforme a seção 6.

Não é permitido:

- alterar código-fonte;
- alterar testes;
- alterar configurações;
- modificar assinaturas ou contratos;
- inventar funcionalidades;
- modificar regras de negócio;
- criar novas responsabilidades arquiteturais;
- alterar o comportamento descrito sem evidências concretas;
- **criar arquivos de exemplo no repositório** (scripts, notebooks, fixtures): os exemplos vivem apenas nos blocos de código da documentação.

Não crie novos documentos apenas para distribuir o conteúdo existente. A separação de documentos somente deve ocorrer quando houver justificativa arquitetural ou documental clara.

Respeite integralmente as regras de classificação e proteção de documentos já estabelecidas pela skill `documentar-projeto`.

Em particular, não revise nem modifique o conteúdo da documentação de linhagem, exceto pelas alterações pontuais expressamente autorizadas pelas regras existentes.

## 6. Exemplos de uso das APIs

Cada API pública documentada deve ter ao menos um exemplo de uso, a menos que seja trivialmente autoexplicativa ou que não exista uso direto (por exemplo, constantes internas). Um exemplo ausente deve ser criado; um exemplo inválido deve ser corrigido.

### 6.1. Princípios

Todo exemplo deve ser:

- **Fiel ao código**: usa somente nomes, parâmetros, tipos de retorno e comportamentos que existem na versão atual do repositório. Confirme cada assinatura lendo o código ou com `inspect.signature`. Não deduza a API a partir da documentação existente.
- **Mínimo e focado**: demonstra um conceito por exemplo. Prefira um exemplo básico e, quando houver variação relevante e distinta, um segundo exemplo curto. Não crie exemplos redundantes para preencher espaço.
- **Autocontido**: inclui os imports necessários, importados dos subpacotes públicos (o pacote raiz não reexporta as interfaces) e declara toda variável usada. Um leitor deve conseguir copiá-lo e adaptá-lo sem consultar outro trecho.
- **Realista, sem segredos**: usa caminhos e nomes plausíveis (`s3a://meu-bucket/cdr/...`, `/dados/cdr/...`, chaves de schema reais do catálogo) e nunca inclui credenciais, endpoints reais ou dados sensíveis. Credenciais vêm do ambiente, e o exemplo diz isso quando relevante.
- **Honesto quanto a resultados**: só escreva valores de retorno ou saídas em comentários (`# ->`) quando forem determinísticos e verificados. Não invente saídas de Spark, contagens, tempos ou relatórios. Quando a saída depende de dados, descreva o formato em vez do valor.
- **Coerente com a arquitetura**: respeita a seção 4. Em especial, não sugira que extratores e transformadores dependem do orquestrador, nem que o orquestrador chama o loader.

### 6.2. Posição e formato

- Posicione cada exemplo **imediatamente após o contrato que ele ilustra**, dentro do mesmo módulo e da mesma seção, sob um título de nível inferior como `#### Exemplo de uso` (ou `Exemplo: <descrição curta>` quando houver mais de um). Não agrupe os exemplos em uma seção genérica distante.
- Exemplos que combinam componentes de camadas diferentes ficam na seção da camada **posterior** entre as envolvidas, para não antecipar conceitos ainda não apresentados. O exemplo de fluxo combinado de ponta a ponta fica ao final de `teleutils.core.loaders`, antes dos módulos auxiliares, com cada etapa claramente separada e comentada como independente.
- Use blocos de código com a linguagem indicada (` ```python `, ` ```bash `, ` ```json `). Comentários dentro do código devem explicar o motivo do passo, não repetir o que a linha faz.
- Texto de apoio fica em uma ou duas frases: o que o exemplo demonstra e qualquer pré-requisito (por exemplo, sessão Spark configurada, JARs do Iceberg, variáveis de ambiente do MinIO).
- Preserve exemplos existentes que continuam válidos; corrija somente o que estiver desatualizado, mantendo o objetivo original.

### 6.3. Cobertura mínima por componente

Use esta matriz como ponto de partida e adapte às interfaces que efetivamente existirem no código:

| Componente | Exemplos esperados |
| :-- | :-- |
| `extractors.schemas` | Definir um `CDRTextSchema` e/ou `CDRParquetSchema` customizado; `resolve_cdr_schema` com um catálogo; passar um catálogo customizado ou estendido ao extrator (copiando o catálogo padrão em vez de alterá-lo). |
| `CDRParquetExtractor` | `extract` (grava o Parquet intermediário e retorna o caminho); `extract_dataframe` (retorna o DataFrame sem escrita); verificação do retorno `None` para entrada ausente; sobrescrita de `operator`, `service` e `cdr_type`. |
| `CDRTextExtractor` | `extract` e `extract_dataframe` com um layout de texto; lista de caminhos de origem; tratamento de retorno `None`. |
| `CDRTransformer` | Instanciar com a sessão Spark; chamar um método `transform_<layout>` a partir de um caminho Parquet; chamar a partir de um DataFrame extraído com `transformer.spark` (a mesma sessão), quando a API suportar essa entrada. |
| `load_config` | Leitura de um JSON local e de uma URI `s3a://`, com o formato do arquivo de configuração (bloco `json`) e a ressalva sobre credenciais via ambiente. |
| `list_files` | Listagem com `pattern` e `recursive`, incluindo a diferença entre datasets `.parquet` e arquivos comuns, quando relevante. |
| `get_config` | Planejamento de caminhos para `storage_type` `s3a` e `local`; uso de `materialize_intermediate=False`; inspeção dos campos de `ConfigPaths`. |
| `run_etl` / `run_etl_item` | Execução de um lote com `spark` e uma lista de configurações; `base_folder` + `week_dir`; modo direto (`materialize_intermediate=False`); execução de um único item. |
| `ETLReport` / `ETLItemResult` / `print_etl_report` | Percorrer `report.items` e tratar `status`, `stage` e `error_message`; imprimir o relatório. |
| `TrinoLoader` | `upsert_iceberg` com a sessão configurada para Iceberg; recarga de uma origem já presente; pré-requisitos de catálogo explicitados em texto. |
| Fluxo combinado | Extração → transformação → (opcional) carga, com cada etapa independente, e o mesmo fluxo via `run_etl` seguido de `upsert_iceberg` como etapa explícita e separada. |
| `teleutils.preprocessing` | `extract_number` (com e sem `remove_non_numeric`) e `is_valid_cnpj`, preservando os exemplos válidos existentes. |
| Scripts auxiliares | Exemplo de invocação do script Bash somente se o script existir e seu uso puder ser confirmado pelo código ou pela documentação própria. |

### 6.4. Verificação dos exemplos

Antes de incluir ou manter um exemplo, verifique-o. Aplique o maior nível de verificação possível no ambiente e registre o nível efetivamente alcançado:

1. **Sintaxe**: o bloco compila (`ast.parse` ou `python -m py_compile` sobre uma cópia em `/tmp`).
2. **Imports e assinaturas**: todos os imports resolvem a partir dos subpacotes públicos, e as chamadas correspondem à assinatura real (`inspect.signature`), incluindo parâmetros somente nomeados.
3. **Execução**: exemplos que não exigem infraestrutura externa (como `extract_number` e `is_valid_cnpj`) são executados, e os `assert` ou valores em comentários são confirmados. Exemplos Spark são executados com Spark local e dados sintéticos mínimos quando `pyspark` estiver disponível.
4. **Infraestrutura externa**: exemplos que dependem de MinIO/S3A, Iceberg ou Trino não são executados contra ambientes reais. Verifique-os até o nível 2, apoie-os nos testes existentes (que usam diretórios temporários, Spark local e catálogo JDBC/H2) e declare no texto os pré-requisitos.

Regras da verificação:

- Execute scripts de verificação a partir de `/tmp` ou de um diretório temporário. Não deixe arquivos, saídas Spark nem artefatos no repositório; remova o que criar.
- Um exemplo que falha na verificação é corrigido ou removido, nunca mantido com a esperança de que esteja certo.
- Se o código contradisser a documentação existente, não escolha silenciosamente: o código prevalece para o exemplo, e a divergência é registrada nas pendências.
- Nunca modifique código ou testes para fazer um exemplo funcionar.

## 7. Estrutura documental desejada

Como referência, a documentação da API pública deverá seguir uma estrutura semelhante à seguinte (os itens `Exemplo` indicam onde os exemplos se posicionam):

```markdown
# Referência da API Pública

## Visão geral

## Requisitos de uso

## `teleutils.core.extractors`

### `teleutils.core.extractors.schemas`

#### `CDRParquetSchema`
##### Exemplo de uso

#### `CDRTextSchema`
##### Exemplo de uso

#### `resolve_cdr_schema`
##### Exemplo de uso

#### `PARQUET_DEFAULT_SCHEMAS` e `TEXT_DEFAULT_SCHEMAS`
##### Exemplo: catálogo customizado

### `CDRParquetExtractor`
#### Exemplo de uso

### `CDRTextExtractor`
#### Exemplo de uso

## `teleutils.core.transformers`

### `CDRTransformer`
#### Exemplo de uso

## `teleutils.core.orchestrators`

### `load_config`
#### Exemplo de uso

### `list_files`
#### Exemplo de uso

### `get_config`
#### Exemplo de uso

### `run_etl`
#### Exemplo de uso

### `run_etl_item`
#### Exemplo de uso

### Resultados imutáveis
#### Exemplo: inspeção do relatório

### `print_etl_report`

### Limites e validação local

## `teleutils.core.loaders`

### `TrinoLoader`
#### Exemplo de uso

### Substituição por origem

### Consistência e desempenho

### Testes de integração do loader

### Exemplo de fluxo combinado

## Módulos auxiliares

### `teleutils.preprocessing`

#### `extract_number`
##### Exemplo de uso

#### Integração com o transformador base

#### `is_valid_cnpj`
##### Exemplo de uso

### Outros utilitários e scripts auxiliares

## Erros e efeitos colaterais
```

Essa estrutura é uma referência organizacional, não uma exigência de reprodução literal.

Adapte os títulos à estrutura e às interfaces efetivamente existentes. Ajuste o nível dos títulos de exemplo para ficar um nível abaixo da API que ilustram. Quando a API já tiver exemplos em blocos de código integrados ao texto, não duplique: acrescente apenas o que faltar.

Se uma seção geral pertencer mais naturalmente a outra posição, preserve sua coerência sem comprometer a sequência dos componentes principais.

## 8. Validação

Após a reorganização e a inclusão dos exemplos, verifique:

- todos os componentes principais estão presentes;
- a ordem é exatamente extractors → transformers → orchestrators → loaders;
- os schemas estão associados à camada de extração;
- os módulos auxiliares aparecem somente depois dos componentes principais;
- nenhuma classe, função ou interface pública foi omitida;
- nenhum conteúdo técnico relevante foi perdido;
- não existem duplicações introduzidas pela movimentação;
- os títulos e níveis hierárquicos estão consistentes;
- todos os links internos continuam apontando para destinos existentes;
- as referências cruzadas permanecem válidas;
- o Markdown está estruturalmente correto, com todos os blocos de código fechados e com linguagem indicada;
- nenhum código, teste ou arquivo de configuração foi modificado, e nenhum arquivo de exemplo ou artefato temporário foi deixado no repositório;
- **toda API pública relevante possui exemplo de uso ou justificativa explícita para sua ausência**;
- **cada exemplo está sob o módulo correto, imediatamente após o contrato que ilustra**;
- **cada exemplo passou pelo nível de verificação aplicável (seção 6.4) e esse nível foi registrado**;
- **nenhum exemplo contém credenciais, endpoints reais, saídas inventadas ou sugere dependências arquiteturais inexistentes**;
- **os exemplos preexistentes válidos foram preservados**.

Quando existirem outros documentos técnicos que apresentem os módulos em uma sequência incompatível, avalie se a ordem precisa ser corrigida para manter a consistência. Não altere documentos protegidos ou fora do escopo autorizado.

## 9. Resultado esperado

Ao concluir, apresente:

1. **Estrutura anterior:** resumo da organização encontrada.
2. **Estrutura nova:** ordem final das seções e módulos.
3. **Arquivos modificados:** relação dos documentos reorganizados.
4. **Conteúdo preservado:** confirmação de que contratos e informações técnicas relevantes foram mantidos.
5. **Referências corrigidas:** links e sumários ajustados.
6. **Exemplos de uso:** tabela com cada API e a situação do exemplo (`criado`, `corrigido`, `preservado`, `sem exemplo` + justificativa) e o nível de verificação alcançado (sintaxe, assinatura, execução local, dependente de infraestrutura).
7. **Validação:** verificações realizadas e respectivos resultados.
8. **Pendências:** inconsistências entre código e documentação, exemplos que não puderam ser executados e decisões que dependam do usuário.

A reorganização deve melhorar a navegabilidade e a compreensão arquitetural sem provocar perda de informação ou alteração indevida do conteúdo técnico, e os exemplos devem permitir que o leitor passe do contrato ao uso sem consultar o código-fonte.

**Critério principal de sucesso:** um desenvolvedor deve conseguir acompanhar a documentação do `teleutils` na mesma sequência conceitual em que compreenderia seu fluxo principal (extração, transformação, orquestração e carga) e, em cada etapa, copiar e adaptar um exemplo correto para usar a API. Somente depois deve encontrar os utilitários e scripts auxiliares.
