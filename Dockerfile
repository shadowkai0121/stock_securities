FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt pyproject.toml README.md ./
COPY src ./src
COPY data ./data
COPY research ./research
COPY universe ./universe
COPY features ./features
COPY experiments ./experiments
COPY agents ./agents
COPY memory-bank ./memory-bank
COPY docs ./docs
COPY strategies ./strategies
COPY tests ./tests
COPY scripts ./scripts

RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt \
    && python -m pip install -e .

ENV PYTHONPATH=/workspace:/workspace/src

CMD ["bash"]
