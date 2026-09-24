# Devcontainer: Persistência de Alterações e Quando Reconstruir

Este documento descreve o modelo de persistência de dados do ambiente de
desenvolvimento em container (Dev Container) utilizado neste projeto, e
estabelece critérios objetivos para decidir quando uma reconstrução
(*rebuild*) da imagem ou do container é necessária.

## 1. Contexto

O ambiente de desenvolvimento é definido por dois artefatos versionados em
`.devcontainer/`:

- `Dockerfile` — define a **imagem**: sistema operacional base, pacotes de
  sistema (`apt`), runtime Java, binário do `uv` e o usuário não-root
  (`devuser`).
- `devcontainer.json` — define como o **container** é instanciado a partir
  dessa imagem: usuário remoto, volumes (`runArgs`), variáveis de ambiente
  (`containerEnv`), extensões do VS Code e o `postCreateCommand`.

Entender a diferença entre esses dois níveis é a base para saber onde uma
alteração feita durante o desenvolvimento realmente é persistida.

## 2. Onde as alterações "moram"

A raiz do projeto (`/workspaces/<nome-do-projeto>` dentro do container) é um
**bind mount** do diretório do repositório no host RHEL9. Isso significa que
qualquer arquivo criado, modificado ou instalado *dentro dessa pasta* existe
fisicamente no host e é independente do ciclo de vida do container.

Consequência direta: tudo que fica dentro do bind mount **sobrevive** à
destruição e recriação do container.

| Artefato | Onde vive | Sobrevive a rebuild do container? |
|---|---|---|
| Dependências instaladas via `uv add` / `uv sync` | `.venv/` (dentro do repo) | Sim |
| Hooks do `pre-commit install` | `.git/hooks/` (dentro do repo) | Sim |
| Qualquer arquivo de código, dados de teste, notebooks | Repositório (bind mount) | Sim |
| Pacotes instalados manualmente com `apt-get` dentro do container | Camada gravável do container | **Não** |
| Variáveis de ambiente exportadas manualmente no shell | Camada gravável do container | **Não** |
| Extensões do VS Code declaradas em `customizations.vscode.extensions` | `devcontainer.json` (repositório) | Sim — reinstaladas automaticamente pelo Dev Containers a cada rebuild |
| Extensões do VS Code instaladas manualmente pela aba Extensions | Servidor VS Code dentro do container (camada gravável) | **Não** |

> **Nota — extensões e Remote Tunnels:** ao acessar via Remote Tunnels, existe
> um VS Code Server rodando no host RHEL9 e, quando o devcontainer está
> ativo, um segundo VS Code Server provisionado *dentro do container*. As
> extensões de workspace (Python, Pylance, Jupyter, Ruff etc.) são instaladas
> nesse segundo servidor — portanto no filesystem do container, não no
> RHEL9 e não na imagem Docker. Isso as torna sujeitas às mesmas regras de
> persistência de qualquer outra alteração feita na camada gravável do
> container.

## 3. Quando o rebuild é necessário

| Alteração | Comando necessário |
|---|---|
| Dependências Python (`pyproject.toml`, `uv add`/`uv sync`) | Nenhum — refletido imediatamente |
| Hooks do git (`pre-commit install`) | Nenhum — refletido imediatamente |
| Qualquer arquivo dentro do workspace | Nenhum — é o próprio host |
| Alteração no `Dockerfile` (pacotes `apt`, versão do Python/Java, etc.) | **Dev Containers: Rebuild Container** |
| Alteração em `devcontainer.json` → `runArgs`, `mounts`, `containerEnv`, `features` | **Dev Containers: Rebuild Container** |
| Alteração em `devcontainer.json` → `customizations.vscode.extensions/settings` | Normalmente reload da janela; se não surtir efeito, rebuild |
| Extensão instalada manualmente pela aba Extensions, sem declarar em `devcontainer.json` | Nenhum comando resolve — a extensão é perdida no próximo rebuild; a correção é declará-la em `customizations.vscode.extensions` |

Reabrir a janela (*Reload Window*) ou reconectar o túnel **não** reprocessa
`Dockerfile` nem `runArgs`. Apenas o comando explícito de rebuild reconstrói
o container a partir da definição atual.

## 4. Alerta: instalações "na unha" dentro do container

Instalar um pacote de sistema diretamente no terminal do container (por
exemplo, `sudo apt-get install htop`, sem adicionar a linha correspondente
ao `Dockerfile`) — ou instalar uma extensão do VS Code apenas pela aba
Extensions, sem declará-la em `devcontainer.json` — grava a alteração
apenas na camada gravável do container/servidor VS Code em execução. Essa
alteração:

- é perdida no próximo *Rebuild Container*;
- não existe para qualquer outra pessoa que clone o repositório e suba o
  devcontainer em outro servidor;
- não é reprodutível nem rastreável via controle de versão.

**Regra prática:** qualquer alteração de nível de sistema operacional
(pacotes `apt`, variáveis de ambiente globais, runtime Java, etc.) só deve
ser considerada definitiva quando refletida no `Dockerfile` e validada com
um rebuild. Testes exploratórios rápidos no terminal são aceitáveis, desde
que a alteração seja posteriormente portada para o `Dockerfile`.

## 5. Checklist — preciso reconstruir o container?

Use este checklist antes de perguntar "cadê minha alteração?":

- [ ] Editei o `Dockerfile` (pacotes `apt`, versão de imagem base, usuário,
      variáveis `ENV`)? → **Rebuild Container**
- [ ] Editei `runArgs` em `devcontainer.json` (volumes, `--group-add`,
      outras flags de `docker run`)? → **Rebuild Container**
- [ ] Editei `mounts`, `containerEnv` ou `features` em `devcontainer.json`?
      → **Rebuild Container**
- [ ] Instalei algo com `apt-get`/`yum` manualmente no terminal do
      container, sem colocar no `Dockerfile`? → Portar a alteração para o
      `Dockerfile` e rodar **Rebuild Container**
- [ ] Editei apenas `dependency-groups`/`dependencies` do `pyproject.toml`
      e rodei `uv sync`? → Nenhuma ação adicional necessária
- [ ] Rodei `pre-commit install` ou criei/editei arquivos dentro do
      workspace? → Nenhuma ação adicional necessária
- [ ] Editei apenas extensões/configurações do VS Code em
      `customizations.vscode`? → Tentar *Reload Window* primeiro; se a
      mudança não aparecer, **Rebuild Container**
- [ ] Instalei uma extensão manualmente pela aba Extensions e quero que ela
      persista? → Adicionar o identificador em
      `customizations.vscode.extensions` no `devcontainer.json` (não requer
      rebuild imediato, mas garante que sobreviva ao próximo)

## 6. Referência rápida de comandos

Na paleta de comandos do VS Code (`Ctrl+Shift+P` / `Cmd+Shift+P`):

- **Dev Containers: Rebuild Container** — reconstrói o container a partir da
  imagem já existente (mais rápido; usa cache de camadas do Docker).
- **Dev Containers: Rebuild Without Cache** — reconstrói ignorando o cache
  de camadas; usar quando há suspeita de que uma alteração no `Dockerfile`
  não foi corretamente refletida mesmo após um rebuild normal.
