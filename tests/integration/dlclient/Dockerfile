# dlclient/Dockerfile
FROM debian:bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive

# accept build-args from docker-compose
ARG IMAGE_TAG
ARG SLURM_TAG

# System deps: Slurm client, munge, gosu, git-annex, Python venv
RUN apt-get update && apt-get install -y --no-install-recommends \
    git git-annex python3 python3-venv ca-certificates \
    slurm-client munge procps gosu \
    && rm -rf /var/lib/apt/lists/* 

COPY slurm.conf /etc/slurm/slurm.conf
COPY slurmdbd.conf /etc/slurm/slurmdbd.conf
RUN set -x \
    && chown slurm:slurm /etc/slurm/slurmdbd.conf \
    && chmod 600 /etc/slurm/slurmdbd.conf

# venv to avoid Debian PEP 668 restriction
RUN python3 -m venv /opt/dl-venv
ENV PATH="/opt/dl-venv/bin:${PATH}"

# Base DataLad
RUN pip install --upgrade pip && pip install datalad

# Clone and install the extension in editable mode

# Entrypoint
COPY run_tests.sh /usr/local/bin/run_tests.sh
RUN chmod +x /usr/local/bin/run_tests.sh
ENTRYPOINT ["/usr/local/bin/run_tests.sh"]