# Ethereum Log Ingestion Pipeline (RPC → BigQuery)
## Overview

This project implements an Ethereum log ingestion pipeline that retrieves logs from the Ethereum blockchain via JSON-RPC and ingests them into Google BigQuery for scalable storage and analysis.

The pipeline maintains ingestion state by tracking the last successfully ingested block or log, and resumes ingestion from that point to fetch logs from subsequent blocks. This enables incremental, idempotent log ingestion without reprocessing previously stored data.

## Pipeline Flow

Fetch Ethereum logs using JSON-RPC

Ingest raw logs into BigQuery

Track the last ingested block / log

Resume ingestion from the next block range

## Key Features

Incremental log ingestion based on last processed block

BigQuery as the analytical data warehouse
