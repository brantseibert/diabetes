# diabetes
A test case illustrating the use of Spark on real-time IoT data.

# challenge
The standard of care for Type 2 Diabetes includes monitoring average blood insulin level over three months.  New advances in medicine and data engineering allow for granular monitoring and care recommendations.  Our challenge is to ingest data from personal insulin monitors, securely transmit that data to the cloud, apply data science analysis, and display current status to the patient.

# solution
The solution transmits data from a Raspberry Pi, which simulates an OEM diabetes monitor. The first iteration of the solution ingests data using Amazon AWS IoT to identify, authenticate, and move raw JSON files to patient specific S3 buckets. Subsequent iterations use Kafka to ingest streams from multiple patients, maintain fault tolerance, and allow multiple consumers to subscribe. Kinesis is also tested as an alternative to Kafka. 

The raw JSON files are pulled from S3 using a Databricks implementation of Spark 2.1 Structured Streaming, transformed into time delimited Parquet partitions, and saved back to S3. Finally, the Parquet files are visualized as a stream by publishing a Databricks Notebook to a web browser.
