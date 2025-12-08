# 🐦 Bird Analysis Worker (Orchestrator)

The **Bird Analysis Worker** is a Python-based orchestration service that listens to an AWS SQS queue and automatically triggers large-scale audio analysis tasks on AWS Fargate whenever a `manifest.json` is uploaded.

It acts as the **Commander** of the entire Bird Species Detection & Analysis Pipeline.

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Prerequisites](#-prerequisites)
- [Environment Variables](#%EF%B8%8F-environment-variables)
- [Local Development](#-local-development--testing)
- [Production Deployment](#-deployment-to-aws-production)
- [License](#-license)

---

## 📌 Overview

This service performs the following functions:

- **Long-polls** an SQS queue for notifications.
- **Detects** uploaded `manifest.json` files in S3.
- **Parses** the manifest to extract audio file lists.
- **Batches** thousands of files into chunks (default 50).
- **Dispatches** BirdNET and Perch analysis tasks to AWS Fargate.
- **Triggers** a final aggregator task to combine results.

---

## 🏗 Architecture

### System Flow (Mermaid Diagram)

```mermaid
flowchart TD
    %% Styles
    classDef storage fill:#3F8624,stroke:#232F3E,color:white;
    classDef aws fill:#FF9900,stroke:#232F3E,color:white;
    classDef compute fill:#326CE5,stroke:#232F3E,color:white;

    %% Nodes
    S3[("S3 Bucket<br/>(Audio & Results)")]
    SQS["SQS Queue<br/>(BirdAnalysisQueue)"]
    
    subgraph Orchestrator ["Orchestration Layer (Worker Service)"]
        Worker["Worker<br/>(worker.py)"]
    end
    
    subgraph Compute ["Compute Layer (AWS ECS Fargate)"]
        direction TB
        BirdNET["BirdNET Task<br/>(Batch Analysis)"]
        Perch["Perch Task<br/>(Batch Analysis)"]
        Aggregator["Aggregator Task<br/>(Final Summary)"]
    end

    %% Flows
    %% 1. Trigger: S3 event sends message to SQS
    S3 -.->|1. S3 Event: PUT manifest.json| SQS
    
    %% 2. Worker Logic
    Worker -->|2. Long Poll| SQS
    Worker -->|3. Download and Parse Manifest| S3
    
    Worker == "4. Batch Dispatch" ==> BirdNET
    Worker == "4. Batch Dispatch" ==> Perch
    Worker -.->|5. Launch Aggregator After Dispatch| Aggregator
    
    %% 3. Compute Logic
    BirdNET -->|6. Download Audio and Analyze| S3
    Perch -->|6. Download Audio and Analyze| S3
    
    BirdNET -->|7. Upload Result JSON| S3
    Perch -->|7. Upload Result JSON| S3
    
    %% 4. Aggregation Logic
    Aggregator -.->|8. Poll Until All Results Ready| S3
    Aggregator -->|9. Merge and Upload final_report.json| S3

    %% Apply Styles
    class S3 storage;
    class SQS,Worker aws;
    class BirdNET,Perch,Aggregator compute;
```
