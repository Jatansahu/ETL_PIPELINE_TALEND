# ETL_PIPELINE_TALEND
Abstract:


This project aims to streamline the data management process through the
implementation of various stages encompassing data modeling, acquisition,
integration, migration, and validation. Initially, a data model is constructed
utilizing the Start/Snowflake Schema, providing a foundation for structured
data organization. Subsequently, cricket data is obtained from external sources
via RAPID API and stored in Amazon S3 utilizing Python, with scheduled data
acquisition ensuring timely updates. The acquired data is then transferred to
Oracle utilizing Talend for further processing and analysis. Following this, a
migration process is employed to transition data from Oracle to Snowflake,
leveraging Talend's capabilities for seamless data migration. Finally, to ensure
data integrity and reliability, a comprehensive data validation process is
established, complemented by the generation of sample reports using Power BI,
facilitating insightful data analysis and decision-making. Through the
integration of these components, this project endeavors to optimize data
management practices, enabling organizations to harness the full potential of
their data assets.

Table of Contents

Abstract………………………………………………………………………………………………………………………………………………………….. 3

Project architecture …………………………………………………………………………………………………………………………………. 4

Methodology ……………………………………………………………………………………………………………………………………………… 5

Task 1: Data modeling and schemas ……………………..………………………………………………………………………. 5

Task 2: Data fetching and S3 transfer ……………………………………………………………………………………………. 6

Task 3: S3 to oracle data transfer ……………………………………………………………………………………………………..15

Task 4: Oracle Database to Snowflake database …………………………………………………………………… 22

Task 5: Data Validation and BI Notebook. ………………………………………………………………………………… 30

i) Data Validation …………………………………………………………………………………………………………………… 30

ii) SnowFlake to PowerBI …………………………………………………………………………………………………… 35

Conclusion …………………………………………………………………………………………………………………………………………………. 41

![semi_final_inputs](semi_final_inputs.jpg)
