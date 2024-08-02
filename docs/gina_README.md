# Tech Catalyst Data Engineering Capstone
### Group 3
Ginamarie Mastrorilli, Joseph Cocozza, Andy Amoah Mensah & Jason Juncker


## Project Overview
NYC Taxi Corporation engaged a team of coluntants from the Tech Catalyst Data Engineering Program to assist with identifying areas where revenue can be increased by reducing risk or modifying the amount of drivers in the area. The scope of the project involves creating a data pipeline to extract the raw data from S3, perform necessary transformations, load the data into a Conformed S3 bucket, and finally into a Transformed S3 bucket before importing it into Snowflake for final analysis using Tableau. Our team must create a implementation plan for NYC Taxi Corporation in moving forward with the next steps for further enhancements to the business. 

## Reference Architecture Diagram
![RAD](/workspaces/TechCatalyst_capston3/docs/RAD.JPG)


## Overall Processs
### Data Description

NYC Taxi Corporation has three types of data: Yellow Taxi Data, Green Taxi Data and, High Volume For-Hire (HVFHV) Data. All three are sourced from the NYC Taxi and Limousine Commission by technology providers authorized under the Taxicab & Livery Passenger Enhancement Programs (TPEP/LPEP).

* add column descriptions?

### Data Extracting from Raw AWS S3 Bucket
The first step in our project for NYC Taxi Corporation is to extract the Raw data from AWS S3. The data is stored in the *capstone-techcatalyst-raw* bucket. Within this bucket there are three folders (green_taxi, hvfhv and yellow_taxi) which store the three types of data respectively. Our team decided on the Databricks platform  using Apache Spark for big data analytics because it allows for various data source connections, is cloud-native, offers robust data storage and finally includes built-in security controls. 
