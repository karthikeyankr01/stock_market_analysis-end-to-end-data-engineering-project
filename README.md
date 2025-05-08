# Stock-Market-Analysis: End-to-End Data Engineering Pipeline

### Introduction
Analyzing stock prices, trading volume, and volatility for various companies on a specific trading day using daily market data from the Alpha Vantage API

### Architecture
![Architecture Diagram](https://github.com/karthikeyankr01/stock_market_analysis-end-to-end-data-engineering-project/blob/main/Architecture.png)

### About Dataset/API
This API contains information about a stock’s daily trading activity - [Alphavantage API](https://www.alphavantage.co/).

### Services used
1. **AWS S3 (Simple Storage Service):** It is a scalable object storage service that allows you to store and retrieve any amount of data from anywhere on the web, offering durability, availability, security, and performance for files of virtually any size.
2. **AWS Lambda:** It is a serverless compute service that runs your code in response to events and automatically manages the underlying compute resources, allowing you to build and deploy applications without provisioning or managing servers.
3.  **Cloud Watch:** AWS CloudWatch is a monitoring service that collects and tracks metrics, logs, and events from your AWS resources and applications, providing visibility into your system performance.
4.   **Glue Crawler:** AWS Glue Crawler automatically scans your data sources, identifies data formats, and creates metadata tables in the AWS Glue Data Catalog for use in analytics and data processing workflows.
5.   **Data Catalog:** AWS Glue Data Catalog is a centralized metadata repository that stores information about all your data assets, making them discoverable and available for data processing.
6.   **Amazon Athena:** Amazon Athena is an interactive query service that lets you analyze data directly in Amazon S3 using standard SQL.

### Install Pckages
```
pip install pandas
pip install numpy
pip install urlib
```

### Workflow

* Extract stock data from API using Lambda and store as JSON in S3 (raw data).
* Manually trigger extraction via CloudWatch.
* Automatically trigger another Lambda to transform raw data into stock price, volume, and volatility, and store in S3 (processed data).
* Use AWS Glue crawler to catalog processed data.
* Query and analyze data using Amazon Athena.

