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
    classDef user fill:#666,stroke:#333,color:white;

    %% Nodes
    User([Frontend / User])
    S3[("S3 Bucket\n(Audio & Results)")]
    SQS["SQS Queue\n(BirdAnalysisQueue)"]
    
    subgraph Orchestrator ["Orchestration Layer (Worker Service)"]
        Worker["Worker\n(worker.py)"]
    end
    
    subgraph Compute ["Compute Layer (AWS ECS Fargate)"]
        direction TB
        BirdNET["BirdNET Task\n(Batch Analysis)"]
        Perch["Perch Task\n(Batch Analysis)"]
        Aggregator["Aggregator Task\n(Final Summary)"]
    end

    %% Flows
    User -->|1. Upload Audio & manifest.json| S3
    S3 -.->|2. S3 Event Notification (Put)| SQS
    
    Worker -->|3. Long Polling| SQS
    Worker -->|4. Download & Parse Manifest| S3
    
    Worker == "5. Batch Dispatch" ==> BirdNET
    Worker == "5. Batch Dispatch" ==> Perch
    Worker -.->|6. Launch After All Batches Dispatched| Aggregator
    
    BirdNET -->|7. Download Audio & Analyze| S3
    Perch -->|7. Download Audio & Analyze| S3
    
    BirdNET -->|8. Upload Result JSON| S3
    Perch -->|8. Upload Result JSON| S3
    
    Aggregator -.->|9. Poll Until All JSON Ready| S3
    Aggregator -->|10. Merge & Upload final_report.json| S3

    %% Apply Styles
    class S3 storage;
    class SQS,Worker aws;
    class BirdNET,Perch,Aggregator compute;
    class User user;
```
