FROM python:3.9-slim-bookworm

# Java é obrigatório para o PySpark rodar (a JVM por baixo do capô)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    git \
    openssh-client \
    curl \
    procps \
    ca-certificates \
    locales \
    && sed -i '/^# *en_US.UTF-8 UTF-8/s/^# *//' /etc/locale.gen \
    && locale-gen en_US.UTF-8 \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

ARG USERNAME=devuser
ARG USER_UID=1000
ARG USER_GID=$USER_UID
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m -s /bin/bash $USERNAME

USER $USERNAME
WORKDIR /workspaces
