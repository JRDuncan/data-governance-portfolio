# Ubuntu 22.04 base (matches your other Dockerfiles)
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install system deps for pymssql (FreeTDS), python and utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      python3 python3-venv python3-pip python3-dev build-essential \
      freetds-dev freetds-bin unixodbc unixodbc-dev curl gnupg ca-certificates \
      && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# copy requirements and source
COPY requirements.txt .
RUN pip3 --no-cache-dir install -r requirements.txt

COPY generator.py .
COPY entrypoint.sh .
RUN chmod +x /app/entrypoint.sh

# run entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

