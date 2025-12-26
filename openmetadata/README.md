# OpenMetadata Configuration

This directory contains all OpenMetadata configuration managed as code.

## Structure
- services/  : Metadata services (Kafka, SQL Server, etc.)
- ingestion/ : Metadata ingestion workflows
- lineage/   : Cross-system lineage definitions
- env/       : Environment configuration
- scripts/   : Automation scripts for registration and ingestion

## Execution Order
1. Register services
2. Run ingestion workflows
3. Apply lineage workflows
