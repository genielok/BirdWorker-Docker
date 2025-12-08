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
    %% 定义样式
    classDef storage fill:#3F8624,stroke:#232F3E,color:white;
    classDef aws fill:#FF9900,stroke:#232F3E,color:white;
    classDef compute fill:#326CE5,stroke:#232F3E,color:white;
    classDef user fill:#666,stroke:#333,color:white;

    %% 节点定义
    User([前端 / 用户])
    S3[("S3 Bucket\n(Audio & Results)")]
    SQS[SQS Queue\n(BirdAnalysisQueue)]
    
    subgraph Orchestrator ["调度层 (Worker Service)"]
        Worker[Worker\n(worker.py)]
    end
    
    subgraph Compute ["计算层 (AWS ECS Fargate)"]
        direction TB
        BirdNET[BirdNET Task\n(Batch Analysis)]
        Perch[Perch Task\n(Batch Analysis)]
        Aggregator[Aggregator Task\n(Final Summary)]
    end

    %% 流程连线
    User -->|1. 上传音频 & manifest.json| S3
    S3 -.->|2. S3 Event Notification (Put)| SQS
    
    Worker -->|3. 长轮询 (Long Poll)| SQS
    Worker -->|4. 下载 & 解析 Manifest| S3
    
    Worker == "5. 批量调度 (Batch Dispatch)" ==> BirdNET
    Worker == "5. 批量调度 (Batch Dispatch)" ==> Perch
    Worker -.->|6. 所有批次派发后启动| Aggregator
    
    BirdNET -->|7. 下载音频 & 分析| S3
    Perch -->|7. 下载音频 & 分析| S3
    
    BirdNET -->|8. 上传结果 JSON| S3
    Perch -->|8. 上传结果 JSON| S3
    
    Aggregator -.->|9. 轮询检查所有 JSON 是否就绪| S3
    Aggregator -->|10. 合并 & 上传 final_report.json| S3

    %% 应用样式
    class S3 storage;
    class SQS,Worker aws;
    class BirdNET,Perch,Aggregator compute;
    class User user;
```
