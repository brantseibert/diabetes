// Databricks notebook source
spark

// COMMAND ----------

//AWS access                                                 *
val AccessKey = "AKIAIN7HM37OZU6N76SA"
val SecretKey = "e2KQEbP0QiM/YOM3EFBBtyFbQtEus3JWMg7KZT5z"
val EncodedSecretKey = SecretKey.replace("/", "%2F")
val AwsBucketName = "ai-diabetes-patients"
val MountName = "diabetes-patients"
dbutils.fs.mount(s"s3a://$AccessKey:$EncodedSecretKey@$AwsBucketName", s"/mnt/$MountName")

// COMMAND ----------

display(dbutils.fs.ls("/mnt/diabetes-patients/"))

// COMMAND ----------

val diabetesPatients = spark.read
.option("header", "false")
.csv("/mnt/diabetes-patients/diabetesPatients.csv")

// COMMAND ----------

import org.apache.spark.sql.functions._
val diabetesPatients2 = diabetesPatients.withColumn("Timestamp_val",current_timestamp())
diabetesPatients2.show()

// COMMAND ----------

