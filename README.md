# NYC-End-to-End-Azure-Data-Engineering-Project

## Description
Hey, This project demonstrates a data engineering pipeline for handling NYC datasets. The pipeline leverages Azure services like Azure Data Factory (ADF), Azure Data Lake Storage Gen2 (ADLS Gen2), and Databricks for seamless ingestion, transformation, and storage of data in bronze, silver, and Gold layers (Using Medallion architecture) and creating delta tables on top of gold layer using parquet file format. The processed data can then be used for advanced analytics and reporting.

---

## Features
- Automated data ingestion from NYC's public datasets via Azure Data Factory (ADF).
- Data storage in **bronze**, **silver**, and **Gold layers** using Azure Data Lake Storage Gen2.
- Dynamic data processing and transformation using Databricks notebooks.
- Efficient data storage and querying with Delta Lake.
- Scalable architecture for handling dynamic datasets.

---

## Architecture
Below is the high-level architecture of the pipeline:

1. **Data Ingestion**:
   - Azure Data Factory dynamically fetches datasets from the NYC public data portal using HTTP requests.
   - The raw data is stored in the **bronze layer** of ADLS Gen2.

2. **Data Transformation**:
   - Databricks processes and cleanses the data from the bronze layer, storing the refined data in the **silver layer**.

3. **Final Transformation**:
   - Databricks performs advanced transformations and aggregations on the silver data.
   - The processed data is stored in Delta Lake for downstream analytics and reporting
   - Delta Lake is used for optimized storage and version control in the gold layer and creating delta tables on top of our data in order to track the changes commited to our final data.

---

## Technologies Used
- **Azure Data Factory (ADF)**: For dynamic data ingestion from external URLs.
- **Azure Data Lake Storage Gen2 (ADLS Gen2)**: For raw and transformed data storage in bronze and silver layers.
- **Databricks**: For dynamic data transformation and creation of Delta Lake tables.
- **Delta Lake**: For optimized data storage and query performance.
- **Python**: For scripting in Databricks notebooks specifically pyspark.

---

## Getting Started

### Prerequisites
- Azure Subscription with access to:
  - Azure Data Factory
  - Azure Data Lake Storage Gen2
  - Azure Databricks Workspace
- Git installed locally

### Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/vinaysinghverma07/-NYC-End-to-End-Azure-Data-Engineering-Project.git

---

### **Why This Structure Works for Your Case**:
1. **Description**: Tailored to your actual architecture using ADF, ADLS Gen2, and Databricks.
2. **Features**: Focuses on the NYC dataset ingestion and transformation workflow.
3. **Architecture**: Highlights the layers (Medallion architecture (bronze, silver, Gold)) and the tools used.
4. **Usage**: Clear steps for setting up and running the pipeline.


Let me know if you need further modifications!
