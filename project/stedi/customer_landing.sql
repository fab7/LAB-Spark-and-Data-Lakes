CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_landing` (
  `customername`             STRING,
  `email`                    STRING,
  `phone`                    STRING,
  `birthday`                 STRING,
  `serialnumber`             STRING,
  `registrationdate`         BIGINT,
  `lastupdatedate`           BIGINT,
  `sharewithpublicasofdate`  BIGINT,
  `sharewithfriendsasofdate` BIGINT
) COMMENT "The raw landing zone for ingested data from the customers."
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://fab-se4s-bucket/stedi/customer/landing/'
TBLPROPERTIES ('classification'='json');
