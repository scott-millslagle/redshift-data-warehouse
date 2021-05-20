# Migrating Sparkify's user and song activity data to the cloud

## Overview
Sparkify is a new startup that has built a music streaming app. As their user base expands at a massive scale they need to scale quickly in order to keep up with the influx of new data. To keep up with the businesses expanding data demands, Sparkify has decided to migrate their data and processes to the cloud. Presently their data is is stored in S3 buckets, in directories of JSON logs on user activity and metadata on the songs in their app. As such, the Sparkify analytics team doesn't have an easy way to query their data. This project addresses that issue by migrating the data to an Amazon Redshift data warehouse. This data warehouse will allow business users to query postgres tables designed to optimize queries on song play analysis. 

## Database Schema
Sparkify's Redshift data warehouse contains one fact table and four dimension tables, shown below. 
! [Sparkify fact and dimension schema](/Sparkify fact and dimension schema.png)


## ETL process
Log data from directories of JSON data generated by the Sparkify app on user and song data is stored as JSON files in two S3 buckets. This JSON data is copied to two staging tables in Redshift, transformed, and inserted into final tables.
            
## Technologies used
1. Python 3
2. Amazon Cloud Services (Redshift, S3, and EC2)
3. PostgreSQL

## Files 
- infrustructure_as_code.ipynb This notebook containts  
- sql_queries.py This file contaisn SQL code to drop, create, and insert into tables
- creat_tables.py This files creates the tables using sql_queries.py
- etl.py This file process the data and inserts the data into the table

## Runnig the scripts
1. Run the cells in infrustructure_as_code.ipynb
2. Run ``python3 sql_queries.py`` from the command line
3. Run ``python3 creat_tables.py`` from the command line
4. Run ``python3 etl.py`` from the command line

### Credits
Code written by Scott Millslagle


