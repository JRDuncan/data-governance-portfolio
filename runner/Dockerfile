# Use Ubuntu 22.04 as base image
FROM ubuntu:22.04

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install Python, pip, and supporting tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-venv python3-pip python3-dev unixodbc unixodbc-dev git curl docker.io  && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency and setup scripts
COPY runner_requirements.txt .
#COPY setup_runner.sh .
#COPY scripts ./scripts

# Install Python dependencies
RUN pip install --no-cache-dir -r runner_requirements.txt

# Mount src and reports folders at runtime, so no need to COPY them here
# Just run setup_runner.sh when container starts
#CMD ["bash", "setup_runner.sh"]

