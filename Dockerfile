FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java-home

ENV JAVA_HOME=/opt/java-home

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv \
    && uv export --frozen --no-dev --no-hashes -o /tmp/requirements.txt \
    && uv pip install --system --break-system-packages -r /tmp/requirements.txt

CMD ["sleep", "infinity"]
