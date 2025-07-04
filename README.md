# data_pipelines_dagster
Data pipelines project which uses Dagster as a scheduling tool. In this project, I get fake API data (from JSONPlaceholder), transform it, and insert it into prepared SQL tables. In the next steps, I create simulated new and edited data. I perform Slowly Changing Dimension (type 2) processing to keep data consistency.

I created two Dagster jobs. The first one is an 'initial run' that creates new tables in the local database. The second job simulates new and edited fake data. I schedule the second job to run every 1 minute, which simulates fetching online data from the API. The tables keep updating continuously as long as the Dagster server is running. 


The diagram below illustrates the overall workflow:

![diagram](dagster_jobs.png)

---

## 📦 Dataset

Project based on the Fake API data from from https://jsonplaceholder.typicode.com/, which contains comments (https://jsonplaceholder.typicode.com/comments) and posts (https://jsonplaceholder.typicode.com/comments) data.

---

## 🔧 Project Overview

(`data_loader_dagster.py`) :
Main python module which contains DataLoadManager class and SQL schema queries.

(`dagster_ops.py`): 
This file conatins dagster simple operation like fetch data or apply Slowly Changing Dimenson model. It is based on the DataLoadManager class functions. 

(`dagster_jobs.py`):
This file contains dagster jobs which are based on dagster ops.
 - etl_initial_job() - contains tables creation and fetching comments and posts data
 - mock_comment_job() - contains simulation of mock data and insertion of this data with the SCD2 model

(`dagster_jobs.py`):
This file schedule mock_comment_job() every one minut

Other files (resources, config, repository) are used to configurate dagster pipelines.

Slowly Changing DImension 2 (SCD2) Data Model:


![Star Schema Overview](dagster_runs.png)

## 📊 Data Analysis (SQL & Power BI)

Based on the created Data Warehouse, key statistics are calculated using SQL queries (`query_wh.ipynb`).

Additionally, a Power BI report has been created to visualize the data:  

