## AWS Glue Studio OLAP ETL Pipeline 🛠️
### Overview 📊

This project demonstrates building an OLAP-ready dataset using AWS Glue Studio. It showcases visual ETL workflow design, PySpark-based transformations, and proper handling of DynamicFrameCollections to prepare raw freelancer data for analytics and reporting.

### Key Features ✨
Visual ETL design in Glue Studio for simplified workflow creation.
Conversion between DynamicFrameCollection and Spark DataFrames.
Custom transformations, including unique identifier generation.
Modular pipeline structure for scalable, production-ready ETL.
Prepares data for OLAP analysis, BI dashboards, and reporting.


### Architecture 🏗️
Source Node: Reads raw freelancer data (CSV, JSON, Parquet).
Custom Transform Node: Applies PySpark transformations and adds IDs.
SelectFromCollection Node: Extracts single DynamicFrame for downstream processing.
ApplyMapping / ResolveChoice: Ensures schema consistency.
Target Node: Writes structured, OLAP-ready dataset to S3 or data warehouse.


### Usage 🚀
Create a new ETL job in AWS Glue Studio.
Add the raw data source node.
Add the Custom Transform Node and attach the PySpark script.
Use SelectFromCollection if the next node expects a single frame.
Add schema-resolving nodes and set your target location.
Run the ETL job to generate OLAP-ready datasets.


### Technologies 💻
AWS Glue Studio – visual ETL workflow design
PySpark – data transformations
S3 – storage
DynamicFrame / DynamicFrameCollection – flexible schema management


### Benefits 🎯
Simplifies creation of analytics-ready OLAP datasets.
Reduces coding via visual workflows.
Scalable, production-ready ETL pipeline suitable for real-world datasets.
