FROM python:3.10-slim-bookworm@sha256:5be5aaecb962a41567bc8041f1e9e3432cb085b037fe057f9a562630b2e4b370

ENV DEBIAN_FRONTEND=noninteractive \
    E4_TERMINUS_IPC=/run/e4bank \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends asciinema bash ca-certificates tmux \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --no-cache-dir numpy==2.2.6 pandas==2.2.3 scipy==1.15.3 scikit-learn==1.7.2

COPY terminus_task/submit_diagnosis.py /usr/local/bin/submit_diagnosis
RUN chmod 0755 /usr/local/bin/submit_diagnosis \
    && mkdir -p /workspace /data /run/e4bank \
    && chmod 1777 /run/e4bank

WORKDIR /workspace
CMD ["bash"]
