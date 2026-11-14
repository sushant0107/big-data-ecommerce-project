# 🛒 Big Data E-Commerce Analytics Platform

[![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.2.1-red.svg?logo=apachehadoop&logoColor=white)](https://hadoop.apache.org/)
[![Hive](https://img.shields.io/badge/Apache%20Hive-2.3.2-yellow.svg?logo=apachehive&logoColor=white)](https://hive.apache.org/)
[![Docker](https://img.shields.io/badge/Docker%20Compose-v2+-blue.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![Java](https://img.shields.io/badge/Java-1.8-orange.svg?logo=openjdk&logoColor=white)](https://openjdk.org/)
[![License](https://img.shields.io/badge/Dataset%20License-CC0%20Public%20Domain-green.svg)](https://creativecommons.org/publicdomain/zero/1.0/)

An end-to-end distributed Big Data analytics pipeline for an e-commerce platform. This project orchestrates a complete Hadoop & Hive cluster via **Docker Compose**, ingests transactional data into **HDFS**, executes custom **Java MapReduce** batch jobs, and performs data warehouse querying via **Apache Hive** and **Beeline**.

---

## 📋 Table of Contents
- [Architecture Overview](#-architecture-overview)
- [Repository Structure](#-repository-structure)
- [Dataset Overview](#-dataset-overview)
- [Cluster Infrastructure & Setup](#-cluster-infrastructure--setup)
- [HDFS Data Ingestion](#-hdfs-data-ingestion)
- [MapReduce Analytics](#-mapreduce-analytics)
  - [1. Order Count by Category](#1-order-count-by-category-categorycount)
  - [2. Total Sales by Category](#2-total-sales-by-category-categorysales)
  - [3. Payment Method Frequency](#3-payment-method-frequency-paymentmethodcount)
- [Apache Hive Data Warehousing](#-apache-hive-data-warehousing)
- [Results Summary & Cross-Validation](#-results-summary--cross-validation)
- [Web UIs and Monitoring](#-web-uis-and-monitoring)

---

## 🏗 Architecture Overview

```mermaid
graph TD
    subgraph Client / Host
        CLI["Docker CLI / PowerShell"]
        Code["Java MapReduce (Maven)"]
        SQL["Hive Queries (Beeline)"]
    end

    subgraph "Hadoop Cluster (Docker Network)"
        NN["NameNode (:9870, :9000)"]
        DN["DataNode (:9864)"]
        RM["ResourceManager (:8088)"]
        NM["NodeManager (:8042)"]
        NN <--> DN
        RM <--> NM
    end

    subgraph "Hive Data Warehouse (Docker Network)"
        HS["HiveServer2 (:10000)"]
        HM["Hive Metastore (:9083)"]
        PG[("PostgreSQL DB (:5432)")]
        HS <--> HM
        HM <--> PG
        HS <--> NN
    end

    CLI -->|docker compose up| NN
    Code -->|hadoop jar| RM
    SQL -->|jdbc:hive2| HS
```

---

## 📁 Repository Structure

```plaintext
big-data-ecommerce-project/
├── docker-compose.yml              # Cluster definition (HDFS, YARN, Hive, PostgreSQL)
├── README.md                       # Comprehensive project documentation
├── .gitignore                      # Git ignore rules for build and temp artifacts
├── data/
│   └── cleaned_orders.csv          # Preprocessed e-commerce dataset (5,000 records)
├── hdfs/                           # HDFS execution logs and verification screenshots
│   ├── Screenshot 2026-11-14 200759.png
│   ├── Screenshot 2026-11-14 200803.png
│   ├── Screenshot 2026-11-14 202230.png
│   ├── Screenshot 2026-11-14 202244.png
│   ├── Screenshot 2026-11-14 202305.png
│   ├── Screenshot 2026-11-14 204600.png
│   ├── Screenshot 2026-11-14 204625.png
│   └── Screenshot 2026-11-14 204635.png
├── hive/                           # Hive DDL, queries, and execution screenshots
│   ├── hive_queries.sql            # Hive table schema & analytical queries
│   ├── Screenshot 2026-11-14 235715.png
│   ├── Screenshot 2026-11-14 235729.png
│   ├── Screenshot 2026-11-14 235741.png
│   ├── Screenshot 2026-11-14 235753.png
│   └── Screenshot 2026-11-14 235801.png
├── mapreduce/                      # MapReduce JAR and execution screenshots
│   ├── ecommerce-mapreduce-1.0-SNAPSHOT.jar
│   ├── Screenshot 2026-11-14 235229.png
│   ├── Screenshot 2026-11-14 235249.png
│   ├── Screenshot 2026-11-14 235316.png
│   ├── Screenshot 2026-11-14 235347.png
│   ├── Screenshot 2026-11-14 235358.png
│   ├── Screenshot 2026-11-14 235412.png
│   ├── Screenshot 2026-11-14 235441.png
│   ├── Screenshot 2026-11-14 235452.png
│   └── Screenshot 2026-11-14 235510.png
├── ecommerce-mapreduce/            # Maven Java project source code
│   ├── pom.xml                     # Maven project descriptor & Hadoop dependencies
│   └── src/main/java/
│       ├── CategoryCount.java      # Order count by category
│       ├── CategorySales.java      # Total revenue by category
│       └── PaymentMethodCount.java # Orders count by payment method
└── report/                         # Detailed project report
    └── PROJECT_REPORT.md
```

---

## 📊 Dataset Overview

The dataset is derived from the **E-Commerce Customer Behavior & Sales Analysis (TR)** dataset (CC0 Public Domain). Unused demographic and web-session attributes were pruned to retain 10 core commercial columns across 5,000 transaction rows:

| Column Name | Data Type | Description | Sample Value |
|---|---|---|---|
| `Order_ID` | `STRING` | Unique order identifier | `ORD_001337` |
| `Customer_ID` | `STRING` | Customer identification key | `CUST_01337` |
| `Order_Date` | `STRING` | Date when the order was placed | `1/1/2023` |
| `City` | `STRING` | Delivery destination city (10 cities) | `Bursa` |
| `Product_Category` | `STRING` | Merchandise category (8 categories) | `Toys` |
| `Quantity` | `INT` | Quantity of items purchased | `1` |
| `Unit_Price` | `DOUBLE` | Price per item unit | `54.28` |
| `Discount_Amount` | `DOUBLE` | Discount deducted from the transaction | `0.00` |
| `Total_Amount` | `DOUBLE` | Final transaction amount paid | `54.28` |
| `Payment_Method` | `STRING` | Payment mechanism utilized | `Debit Card` |

---

## 🚀 Cluster Infrastructure & Setup

The distributed environment is orchestrated via `docker-compose.yml` with 7 coordinated services:

### 1. Start Cluster Services
```bash
docker compose up -d
```
![Docker Compose Startup](hdfs/Screenshot%202026-11-14%20200759.png)

### 2. Verify Container Health
```bash
docker ps
```
![Docker PS Check](hdfs/Screenshot%202026-11-14%20200803.png)

---

## 🗄 HDFS Data Ingestion

### 1. Ingest Data into HDFS
Access the NameNode container and create the designated HDFS directory structure:
```bash
docker exec -it namenode bash

# Create directory and put dataset into HDFS
hdfs dfs -mkdir -p /ecommerce
hdfs dfs -put /data/cleaned_orders.csv /ecommerce/
hdfs dfs -ls -h /ecommerce
```
![HDFS Ingestion](hdfs/Screenshot%202026-11-14%20202230.png)

### 2. Inspect Ingested Records
```bash
hdfs dfs -cat /ecommerce/cleaned_orders.csv | head -n 5
```
![HDFS Cat](hdfs/Screenshot%202026-11-14%20202244.png)

### 3. Check HDFS Directory via NameNode Web UI
Navigate to `http://localhost:9870/explorer.html#/` to browse `/ecommerce`:
![HDFS Web UI Directory](hdfs/Screenshot%202026-11-14%20202305.png)

### 4. Organize HDFS Input Folder
Move the dataset into `/ecommerce/input` for structured MapReduce and Hive external table integration:
```bash
hdfs dfs -mkdir -p /ecommerce/input
hdfs dfs -mv /ecommerce/cleaned_orders.csv /ecommerce/input/
```
![HDFS Organize Input](hdfs/Screenshot%202026-11-14%20204600.png)

HDFS Web UI showing `/ecommerce` with `input/` and MapReduce output directories:
![HDFS Web UI Structure](hdfs/Screenshot%202026-11-14%20204625.png)

HDFS Web UI showing `/ecommerce/input/cleaned_orders.csv` (Replication: 3, Block size: 128 MB):
![HDFS Web UI Input File](hdfs/Screenshot%202026-11-14%20204635.png)

---

## ⚡ MapReduce Analytics

The MapReduce programs are written in Java using the Hadoop MapReduce API (`org.apache.hadoop.mapreduce.*`) and compiled using Maven into `ecommerce-mapreduce-1.0-SNAPSHOT.jar`.

### 1. Order Count by Category (`CategoryCount`)
- **Mapper**: Reads CSV records, extracts `Product_Category` (column 4), and emits `(Category, 1)`.
- **Combiner & Reducer**: Aggregates the total order counts for each category.

```bash
hadoop jar /mapreduce/ecommerce-mapreduce-1.0-SNAPSHOT.jar CategoryCount /ecommerce/input/cleaned_orders.csv /ecommerce/out_cat_count
```
![CategoryCount Job Start](mapreduce/Screenshot%202026-11-14%20235229.png)
![CategoryCount Job Completion](mapreduce/Screenshot%202026-11-14%20235249.png)

**View Output:**
```bash
hdfs dfs -cat /ecommerce/out_cat_count/part-r-00000
```
![CategoryCount Output](mapreduce/Screenshot%202026-11-14%20235316.png)

---

### 2. Total Sales by Category (`CategorySales`)
- **Mapper**: Extracts `Product_Category` (column 4) and calculates gross sales as `Quantity` (column 5) $\times$ `Price` (column 6), emitting `(Category, Quantity * Price)`.
- **Combiner & Reducer**: Aggregates total gross revenue per product category rounded to two decimal places.

```bash
hadoop jar /mapreduce/ecommerce-mapreduce-1.0-SNAPSHOT.jar CategorySales /ecommerce/input/cleaned_orders.csv /ecommerce/out_cat_sales
```
![CategorySales Job Start](mapreduce/Screenshot%202026-11-14%20235347.png)
![CategorySales Job Completion](mapreduce/Screenshot%202026-11-14%20235358.png)

**View Output:**
```bash
hdfs dfs -cat /ecommerce/out_cat_sales/part-r-00000
```
![CategorySales Output](mapreduce/Screenshot%202026-11-14%20235412.png)

---

### 3. Payment Method Frequency (`PaymentMethodCount`)
- **Mapper**: Extracts `Payment_Method` (column 9) and emits `(PaymentMethod, 1)`.
- **Combiner & Reducer**: Computes the distribution of orders by payment gateway.

```bash
hadoop jar /mapreduce/ecommerce-mapreduce-1.0-SNAPSHOT.jar PaymentMethodCount /ecommerce/input/cleaned_orders.csv /ecommerce/out_payment
```
![PaymentMethodCount Job Start](mapreduce/Screenshot%202026-11-14%20235441.png)
![PaymentMethodCount Job Completion](mapreduce/Screenshot%202026-11-14%20235452.png)

**View Output:**
```bash
hdfs dfs -cat /ecommerce/out_payment/part-r-00000
```
![PaymentMethodCount Output](mapreduce/Screenshot%202026-11-14%20235510.png)

---

## 🐝 Apache Hive Data Warehousing

Using Beeline JDBC to connect to HiveServer2:
```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -n root
```

### 1. Create External Table
An external table is created pointing directly to `/ecommerce/input` (the CSV has no header row):
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_orders (
    order_id STRING,
    customer_id STRING,
    order_date STRING,
    city STRING,
    category STRING,
    quantity INT,
    price DOUBLE,
    discount DOUBLE,
    total_amount DOUBLE,
    payment_method STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/ecommerce/input';
```
![Hive Table Creation](hive/Screenshot%202026-11-14%20235715.png)

### 2. Verify Table Record Count
```sql
SELECT COUNT(*) AS total_orders FROM ecommerce_orders;
```
![Hive Total Orders Count](hive/Screenshot%202026-11-14%20235729.png)

### 3. Query 1: Total Sales Revenue by Category
```sql
SELECT category, ROUND(SUM(quantity * price), 2) AS total_sales
FROM ecommerce_orders
GROUP BY category
ORDER BY total_sales DESC;
```
![Hive Query 1 Total Sales by Category](hive/Screenshot%202026-11-14%20235741.png)

### 4. Query 2: Order Distribution by City
```sql
SELECT city, COUNT(*) AS total_orders
FROM ecommerce_orders
GROUP BY city
ORDER BY total_orders DESC;
```
![Hive Query 2 Orders by City](hive/Screenshot%202026-11-14%20235753.png)

### 5. Query 3: Payment Method Distribution
```sql
SELECT payment_method, COUNT(*) AS total_orders
FROM ecommerce_orders
GROUP BY payment_method
ORDER BY total_orders DESC;
```
![Hive Query 3 Payment Methods](hive/Screenshot%202026-11-14%20235801.png)

---

## 📈 Results Summary & Cross-Validation

The results from custom Java MapReduce jobs and declarative Hive queries match with 100% precision:

### 1. Product Category Analysis
| Category | Order Volume (MapReduce) | Order Volume (Hive) | Total Sales ($) (MapReduce) | Total Sales ($) (Hive) |
|---|---|---|---|---|
| **Electronics** | 624 | 624 | $2,391,002.99 | $2,391,002.99 |
| **Home & Garden** | 621 | 621 | $930,368.69 | $930,368.69 |
| **Sports** | 667 | 667 | $772,840.62 | $772,840.62 |
| **Fashion** | 622 | 622 | $383,755.13 | $383,755.13 |
| **Toys** | 610 | 610 | $228,410.33 | $228,410.33 |
| **Beauty** | 621 | 621 | $160,345.92 | $160,345.92 |
| **Food** | 619 | 619 | $98,351.13 | $98,351.13 |
| **Books** | 616 | 616 | $74,733.78 | $74,733.78 |

### 2. Payment Method Breakdown
| Payment Method | Transaction Count | Share (%) |
|---|---|---|
| **Credit Card** | 2,012 | 40.24% |
| **Debit Card** | 1,265 | 25.30% |
| **Digital Wallet** | 965 | 19.30% |
| **Bank Transfer** | 510 | 10.20% |
| **Cash On Delivery** | 248 | 4.96% |

### 3. Top Cities by Order Volume
| Rank | City | Total Orders | Share (%) |
|---|---|---|---|
| 1 | **Istanbul** | 1,284 | 25.68% |
| 2 | **Ankara** | 735 | 14.70% |
| 3 | **Izmir** | 600 | 12.00% |
| 4 | **Bursa** | 496 | 9.92% |
| 5 | **Adana** | 378 | 7.56% |

---

## 🌐 Web UIs and Monitoring

When the Docker Compose stack is running, access the following web dashboards:

| Dashboard | URL | Purpose |
|---|---|---|
| **HDFS NameNode UI** | [http://localhost:9870](http://localhost:9870) | Browse HDFS filesystem, datanode health, block reports |
| **YARN ResourceManager UI** | [http://localhost:8088](http://localhost:8088) | Monitor MapReduce applications, job metrics, and cluster resources |
| **HiveServer2 Endpoint** | `jdbc:hive2://localhost:10000` | Beeline JDBC connection endpoint for interactive SQL |

---

## 🛠 Compilation & Build Instructions

If modifying the Java MapReduce jobs:
```bash
cd ecommerce-mapreduce
mvn clean package
# Copy compiled jar to mapreduce folder
cp target/ecommerce-mapreduce-1.0-SNAPSHOT.jar ../mapreduce/
```

---

## 📜 License
- **Dataset**: Creative Commons Zero (CC0) Public Domain
- **Code & Configuration**: Educational & Open Source
