# Ubuntu 22.04 base
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies: Python3 + pip + Microsoft ODBC Driver 18
# Note: All backslashes are at the end of lines with NO trailing spaces or comments on the same line
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    ca-certificates \
    unixodbc-dev \
    python3 \
    python3-pip \
    python3-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc \
    && curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && echo "/opt/microsoft/msodbcsql18/lib64" > /etc/ld.so.conf.d/msodbcsql18.conf \
    && ldconfig \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and source
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY generator.py .
COPY entrypoint.sh .
RUN chmod +x /app/entrypoint.sh

# Run entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
