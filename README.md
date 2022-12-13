# Example of Hopsworks Online Feature Store and Flink real time feature engineering pipeline 

#### Step 1:
```bash
git clone https://github.com/logicalclocks/flink-online-fs.git
cd flink-online-fs
mvn clean package
```

#### Step 2:
On Hopsworks cluser execute 
- `setup/1_create_feature_groups.ipynb` to create feature groups
- `setup/2_create_topic_with_schema.ipynb` to create source kafka topic

#### Step 3:
From Hopsworks jobs UI start
- producer job and from Flink jobs UI upload jar file  `target/hops-examples-flink-3.1.0-SNAPSHOT.jar` 
- submit job with class path `io.hops.examples.flink.fraud.SimProducer` with arguments `-topicName 
  credit_card_transactions -batchSize 1`

#### Step 4:
From Hopsworks jobs UI start
- consumer job and from Flink jobs UI upload jar file  `target/hops-examples-flink-3.1.0-SNAPSHOT.jar`
- submit job with class path `io.hops.examples.flink.examples.TransactionFraudExample` with arguments `-featureGroupName card_transactions_10m_agg -featureGroupVersion 1 -sourceTopic credit_card_transactions`


#### Step 5:
From Hopsworks jobs UI start backfill job `card_transactions_10m_agg_1_offline_fg_backfill`