# diabetes
A test case illustrating the use of Spark on real-time IoT data.

# challenge
The standard of care for Type 2 Diabetes includes monitoring average blood insulin level over three months.  New advances in medicine and data engineering allow for granular monitoring and care recommendations.  Our challenge is to ingest data from personal health monitors, securely transmit that data to the cloud, apply data science analysis, and display current status to the patient and care givers.

# solution
The first prototype uses Python code on a Raspberry Pi to generate mock patient data streams, simulating an OEM diabetes monitor. We ingest JSON data as MQTT packets into Amazon AWS IoT. Subsequent iterations used a direct connection to Kafka running on an EC2 instance which allows us to ingest streams from multiple patients, maintain fault tolerance, and enable multiple consumers to subscribe. Kinesis was also tested as an alternative to Kafka. 

From Kafka, Kinesis, or AWS IoT, the next step is to subscribe to topics and ingest the data into longer term storage in S3. A small Spark 2.1 cluster running on EC2 instances is used for this purpose.  We wrote code for Spark in Scala which reads JSON streams from a Kafka topic, transforming and joining the data into normalized formats, then writing out the stream to Parquet files on S3 that are partitioned by time and patientID.

For analysis and display of the patient condition, the Parquet files are analyzed in Databricks Notebooks running Spark 2.2. The notebooks use regression and classification algorithms to score the current status of the patient and prioritize patients for home care visits.
