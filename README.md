# End-to-End Data Engineering Project: Real-Time Data Streaming Pipeline  

This project demonstrates the development of an **end-to-end real-time data streaming pipeline**, covering all phases of the data engineering lifecycle—from ingestion to processing and storage. By leveraging a robust stack of modern tools and technologies, this pipeline is designed to handle high-throughput data streams efficiently, ensuring scalability, fault tolerance, and maintainability.  

The stack includes **Apache Airflow**, **Python**, **Apache Kafka**, **Apache Zookeeper**, **Apache Spark**, **MySQL**, and **Docker** for containerization.  

## Key Features  

1. **Data Ingestion**: Capture real-time data streams using **Apache Kafka**, with **Apache Zookeeper** ensuring reliability in message brokering.  
2. **Data Processing**: Perform complex transformations and aggregations in real time using **Apache Spark**.  
3. **Data Storage**: Store processed data into a **MySQL** database for querying and analytics.  
4. **Workflow Orchestration**: Manage and automate data workflows using **Apache Airflow**.  
5. **Containerized Deployment**: Use **Docker** to simplify deployment and ensure a consistent, reproducible environment.  

## Technologies  

| Technology           | Purpose                                              |  
|-----------------------|------------------------------------------------------|  
| **Apache Kafka**      | Real-time messaging and data ingestion               |  
| **Apache Zookeeper**  | Coordination and management for Kafka brokers        |  
| **Apache Spark**      | Real-time data processing and computation            |  
| **MySQL**             | Relational database for storing processed data       |  
| **Apache Airflow**    | Workflow orchestration and task scheduling           |  
| **Docker**            | Containerization and service orchestration           |  
| **Python**            | Programming language for custom workflows and logic  |  

## Setup and Deployment  

### Prerequisites  
- Docker and Docker Compose installed.  
- Basic familiarity with Kafka, Spark, Airflow, and MySQL.  
