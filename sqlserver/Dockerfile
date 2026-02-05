FROM mcr.microsoft.com/mssql/server:2022-latest

USER root

# Clean duplicate repos
RUN rm -f /etc/apt/sources.list.d/microsoft-prod.list || true

# Install tools
RUN apt-get update && \
    apt-get install -y curl apt-transport-https gnupg software-properties-common && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y \
        msodbcsql18 \
        mssql-tools18 \
        unixodbc-dev && \
    echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> /root/.bashrc && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PATH="$PATH:/opt/mssql-tools18/bin"

COPY init-db.sh /init-db.sh
RUN chmod +x /init-db.sh

EXPOSE 1433

# Start server + agent + init
CMD /bin/bash -c "\
    /opt/mssql/bin/sqlservr & \
    echo 'Waiting for SQL Server...' && \
    until /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P \"${SA_PASSWORD}\" -C -Q \"SELECT 1\" >/dev/null 2>&1; do sleep 5; done && \
    echo 'Running init script...' && \
    /init-db.sh && \
    echo 'Container ready' && \
    wait \
"
