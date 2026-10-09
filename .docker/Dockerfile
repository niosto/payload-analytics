FROM python:3.13-slim-bookworm AS python-base
FROM node:24-bookworm-slim AS img-base

# Arguments
ARG USER=payload-analytics
ARG USER_HOME=/home/$USER
ARG VOLUME=/app
ARG SHELL=/bin/bash

COPY --from=python-base /usr/local/ /usr/local/
RUN ldconfig
RUN pip3 install --upgrade pip

RUN apt-get update
RUN npm install -g npm

# Install OS utilities
RUN apt-get install -y locales \
    && sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen \
    && locale-gen
RUN apt-get install -y sudo lsb-release git curl zip unzip jq libsqlite3-0

# Install VSCode extensions dependencies
RUN pip3 install pylint yapf isort cfn-lint cfn-lint-serverless
RUN npm install -g @anthropic-ai/claude-code

# Install CLI utilities
RUN case "$(dpkg --print-architecture)" in \
        "amd64") AWSCLI_ARCH="x86_64" ;; \
        "arm64"|"aarch64") AWSCLI_ARCH="aarch64" ;; \
        *) echo "Unsupported architecture" && exit 1 ;; \
    esac; \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-${AWSCLI_ARCH}.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && apt-get install -y less

# Install Serverless framework + plugins
RUN npm install -g serverless

# Set up non root user
RUN echo "$USER ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN useradd -ms $SHELL $USER
RUN mkdir -p $VOLUME \
    && chown -R $USER:$USER $VOLUME

# Let non-root user self-update global npm packages (serverless, claude-code) at runtime
RUN chown -R $USER:$USER /usr/local/lib/node_modules /usr/local/bin

# cfn-lint: ignore W3005 (redundant DependsOn already implied by GetAtt)
RUN echo "ignore_checks:" > "$USER_HOME/.cfnlintrc" \
    && echo "  - W3005" >> "$USER_HOME/.cfnlintrc" \
    && chown $USER:$USER "$USER_HOME/.cfnlintrc"

# Preserve bash history
RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" \
    && echo $SNIPPET >> "/root/.bashrc" \
    && mkdir /commandhistory \
    && touch /commandhistory/.bash_history \
    && chown -R $USER /commandhistory \
    && echo $SNIPPET >> "$USER_HOME/.bashrc"

USER $USER

######## INFRASTRUCTURE IMAGE ########
FROM img-base AS img-infrastructure
ARG USER=payload-analytics
USER root
USER $USER

######## BACKEND IMAGE ########
FROM img-base AS img-backend
ARG USER=payload-analytics
USER root
RUN pip3 install boto3
RUN pip3 install yoyo-migrations
RUN pip3 install psycopg2-binary
RUN pip3 install dbt-core==1.12.3 dbt-postgres==1.11.0

# Docker CLI only (no daemon). Builds and pushes the dbt image from this container.
RUN apt-get install -y ca-certificates gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian bookworm stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y docker-ce-cli

USER $USER
