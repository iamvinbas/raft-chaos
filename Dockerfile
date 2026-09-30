FROM python:3.12-slim

# iproute2 gives `tc` (latency and loss), iptables gives partitions. Used by scripts/chaos-demo.sh.
RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 iptables \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

ENTRYPOINT ["raftchaos"]
