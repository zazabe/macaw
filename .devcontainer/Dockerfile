FROM docker.io/library/rust:1.97.1-trixie

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    build-essential clang libclang-dev ca-certificates locales man-db \
    htop procps psmisc telnet dnsutils net-tools iputils-ping \
    wget curl zip unzip git less jq ripgrep vim-nox rsync openssh-client netcat-traditional \
    python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*