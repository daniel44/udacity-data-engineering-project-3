### Sparkify Music Streaming Database & ETL

##### Purpose and goals of the datawarehouse
The datawarehouse will enable Sparkify to analyze the songs and user activity through diverse Data Analysis techniques, with the end goal of understanding user patterns and tailoring the product to meet the market needs.
The datawarehouse will be created in the cloud (AWS) and will read the data from the data lake in S3

##### Database schema design
The database was designed with a Star schema, which enables high-speed analytic queries at a low resource cost. Since the data falls into S3, it was decided to utilize Amazon redshift as the backend technology. The database design is as follows:

###### Fact table
1. Songplay

###### Dimension tables
1. Songs
2. Artists
3. Users
4. Time

The tables described above will capture the information collected (Please refer to secion Data pipeline for further details) and expose it through Redshift.

##### Data pipeline

###### Source data
The data is sourced is from two different datasets:

1. A set of JSON files that include general information about a song and its corresponding artist. The files are partitioned by the first three letters of each song's track ID. 
2. A set of JSON files that include activity logs from the Sparkify app.

Both datasets are stored in thr clod in Amazon S3

###### Database creation and constraints

A redshift cluster was created with the tables described above The tables were created using the SQL included in sql_queries.py and executed through the create_tables.py script.

The following redshift cluster was created:

![redshift_cluster](/cluster.png)

###### ETL pipeline

The data pipeline include the following steps:

1. Read both datasets from S3 through a python script
2. Load the data into staging tables in redshift
3. Transorm the data through SQL queries in redshift utilizing a python command to call the script
4. Insert the data in the corresponding final tables within the database

The ETL script is executed through the etl.py file

##### Analysis and Data Quality results

The following queries were conducted for Data Quality purposes:

#of rows in each table
![numberrows](/number_rows.png)

Top 10 rows in the fact table
![Facttable](/fact.png)
