FROM docker.io/library/rust:1.90.0

ENV PROJECT_NAME=macaw

### Setup
# - Create user with same UID and GID as host user
#    - UID and GID can be changed in .devcontainer/.env
#    - Allow mounting host folders with correct permissions (e.g. .ssh host folder)
# - Create folder structure with correct permissions
# - Setup for bash history persistence

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=1000
RUN groupadd --gid $USER_GID $USERNAME && \
    useradd --uid $USER_UID --gid $USER_GID -s /bin/bash -m $USERNAME && \
    mkdir -p /workspace/$PROJECT_NAME \
             /home/$USERNAME/.cache/.storage \
             /home/$USERNAME/.cursor-server/data/User/History && \
    chown -R $USERNAME:$USERNAME \
             /workspace/$PROJECT_NAME \
             /home/$USERNAME
RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/home/$USERNAME/.cache/.storage/.bash_history" \
    && echo "$SNIPPET" >> "/home/$USERNAME/.bashrc" \
    && chown $USERNAME:$USERNAME "/home/$USERNAME/.bashrc"
WORKDIR /workspace/$PROJECT_NAME

# Keep cargo cache in persistent volume
ENV CARGO_HOME=/home/$USERNAME/.cache/.storage/cargo

RUN rustup component add rustfmt clippy
RUN cargo install cargo-insta

ENTRYPOINT ["sleep", "infinity"]