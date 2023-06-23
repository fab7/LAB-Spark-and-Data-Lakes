# README

This directory contains the STEDI project deliverables.

## Project Overview

The STEDI Human Balance Analytics project builds a data lakehouse solution for extracting and curating the data produced by the STEDI Step Trainer sensors and its associated mobile app. The lakehouse is built on AWS and the resulting data are to be used by Data Scientists to  train a learning model.

## S3 Landing Zone

The raw data coming from 3 various sources are stored on AWS S3 in the following 3 directories:
- s3://fab-se4s-bucket/stedi/accelerometer/landing
- s3://fab-se4s-bucket/stedi/customer/landing
- s3://fab-se4s-bucket/stedi/step_trainer/landing
    
## Python Scripts using Spark

### Customer Landind To Trusted
Sanitizes the customer data from the Website (Landing Zone) to create **customer_trusted** data that only stores the customer records:
- who agreed to share their data for **research purposes** and,
- who have **accelerometer data** available.

The corresponding Glue job is `./stedi/customer_landing_to_trusted.py`. It joins the customer and accelerometer landing tables on emails, and it applies a privacy policy on customer's data by filtering out the customers that did not give their approval to use their data for research purposes (i.e. the field `shareWithResearchAsOfDate` is set and is not zero).

**[INFO]** The join with the accelerometer data is doing much more than what is initially asked in the question. However, it avoids ending up with empty tables at the end of the project.   

![Alt text](./img/CustomerLandingToTrusted.png "Fig.1")<p align="center">*Fig.1 - Customer Landing To Trusted*</p>

###  Accelerometer Trusted Zone
The `./stedi/accelerometer_landing_to_trusted_zone.py` script sanitizes the accelerometer data to  only store sensor records from customers who agreed to share their data for research purposes. The Glue job consists of two transforms:
- a **JOIN** based on the fields `customer_trusted.email` == `accelerometer_landing.user`
- a **DROP_FIELDS** that keeps only relevant sensor fields.

![Alt text](./img/AccelerometerTrustedZone.png "Fig.2")<p align="center">*Fig.2 - Accelerometer Trusted Zone*</p>

###  Customer Curated Zone
The ` stedi/customer_trusted_to_curated.py` script 
sanitizes the trusted customer data to only include customers who have accelerometer data and have agreed to share their data for research.
The Glue job consists of the following transforms:
- a **JOIN** of trusted customers with accelerator records based on `email` ==  `user`
- a **DROP** of the unused record fields (i.e. the accelerometer data).

**Note:** The Glue job that is created here is redundant with the way I created the `customer_trusted` table at the beginning of the project. This job was re-created here to align with the requested deliverables.   

![Alt text](./img/CustomerCurated.png "Fig.3")<p align="center">*Fig.3 - Customer Curated Zone*</p>

### Step Trainer Trusted
The `stedi/trainer_trusted_to_curated.py` script sanitizes the step trainer data with records that contain the data from curated customers and who have agreed to share their data for research (customers_curated). The Glue job consists of the following JOIN and DROP transforms:
- a **JOIN** of curated_customers with step-trainer records based on `serialNumber` ==  `serialNumber`
- a **DROP** of the unused customer fields.

![Alt text](./img/StepTrainerTrusted.png "Fig.4")<p align="center">*Fig.4 - Step Trainer Trusted.png*</p>

### Machine Learning Curated
The `stedi/machine_learning_curated.py` script 
aggregates the table that has the step trainer readings, and the table that has the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data.

![Alt text](./img/MachineLearningCurated.png "Fig.5") <p align="center">*Fig.5 - Machine Learning Curated*</p>

