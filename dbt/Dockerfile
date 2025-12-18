# ----------------------------------------------------
# 🐳 DBT/Runner Service Dockerfile using Ubuntu 22.04
# ----------------------------------------------------

# Use the official Ubuntu 22.04 LTS (Jammy Jellyfish) as the base
FROM ubuntu:22.04

# Set environment variables for non-interactive installs and to accept the EULA
ENV DEBIAN_FRONTEND=noninteractive
ENV ACCEPT_EULA=Y

# Set the working directory for your application
WORKDIR /app

# The RUN command installs dependencies, the Microsoft ODBC driver, and Python packages
RUN apt-get update && \
    # Install core Linux dependencies
    apt-get install -y --no-install-recommends \
    git curl gnupg unixodbc-dev libgssapi-krb5-2 \
    python3 python3-pip python3-venv python3-dev \
    && \
    \
    # 1. Download the Microsoft GPG key, dearmor it, and save it securely
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg && \
    \
    # 2. Add the Microsoft repository configuration for Ubuntu 22.04 (Jammy)
    # This uses the correct package repository URL, resolving the 'Release file not found' error.
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" > /etc/apt/sources.list.d/mssql-release.list && \
    \
    # 3. Update repos and install the ODBC driver (msodbcsql18) and tools
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 && \
    \
    # 4. Install Python packages for DBT
    pip3 install --no-cache-dir dbt-core dbt-sqlserver && \
    \
    # Clean up APT lists to keep the image small
    rm -rf /var/lib/apt/lists/*

# Add the mssql-tools directory to the PATH environment variable
ENV PATH="/opt/mssql-tools18/bin:${PATH}"

# Copy your application files (uncomment and adjust as needed)
# COPY runner_requirements.txt .
# COPY setup_runner.sh .
# COPY src/run_ge_checks.py .

# Define the command to run when the container starts (uncomment and adjust as needed)
# CMD ["bash", "setup_runner.sh"]
