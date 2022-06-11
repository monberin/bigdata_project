# WIKI STREAMING PROJECT

## System Model
![Untitled Diagram drawio](https://user-images.githubusercontent.com/56642774/173186970-583320ee-16c7-4110-bd95-68b1963edd54.png)
## DB tables
![image](https://user-images.githubusercontent.com/56642774/173187576-6dd158a2-f1c1-45b5-9da9-755120025a97.png)

## How it works
First of all, we read the streaming data from https://stream.wikimedia.org/v2/stream/page-create
Every entry is then sent to Kafka Producer, to ensure that no message gets lost. 
After that, we use Cassandra Client and, through Kafka Consumer, we push our processed data to the database.
We used Flask app as our facade service and to send GET requests easily.
#### Task A
For task A, we pull the data for the previous 6 hours from the database and process them. This code authomatically runs every time the current hour changes, so, once every hour. Then, the updated data is written into tables a1, a2, a3. (Not our initial idea, as we were planning just to write to some json file locally, but we both had issues with permissions to create/modify files, and were not able to figure out how to fix it).
Those tables are only updated once an hour.
#### Task B
For task B, we run selects every time a request is sent through API, and the tables with data are being updated constantly with every new entry.

## How to run
(all in separate terminals)


Reading data from stream to Kafka
```
cd ./stream_reader
sh run-kafka-cluster.sh
sh run_transactions.sh
```

Reading from Kafka to Cassandra, Cassandra Client
```
cd ./cassandra_part
sh run-cassandra-cluster.sh
sh run_batches_reading.sh  
```

Task A spark processing
```
cd ./spark_processing
sh spark.sh
```

Flask, REST API
```
cd ./cassandra_part
sh rest.sh
```

## Some query results (the system was running for 6+ hours)
### Category B
#### The list of existing domains for which pages were created
![Screenshot from 2022-06-11 13-37-37](https://user-images.githubusercontent.com/56642774/173187988-79a11fab-f876-4286-b7c8-dc05aaf2631e.png)
####  All the pages which were created by the user with a specified user_id
![Screenshot from 2022-06-11 13-39-12](https://user-images.githubusercontent.com/56642774/173187990-c8adfd46-ac50-4cca-a1a8-4cd66a19cd9e.png)
![Screenshot from 2022-06-11 13-40-00](https://user-images.githubusercontent.com/56642774/173187992-493b2eba-afa7-414f-9b10-907cd483160a.png)
![Screenshot from 2022-06-11 13-46-03](https://user-images.githubusercontent.com/56642774/173187994-a4262b9d-4d33-4a46-bcad-7d80da59c460.png)
#### The number of articles created for a specified domain
![Screenshot from 2022-06-11 13-47-17](https://user-images.githubusercontent.com/56642774/173187996-5749a6fb-96ce-4de1-93a9-db5dce5e0919.png)
![Screenshot from 2022-06-11 13-47-35](https://user-images.githubusercontent.com/56642774/173187999-5d4eefdd-b95a-4937-bb51-b096332bf0af.png)
![Screenshot from 2022-06-11 13-47-53](https://user-images.githubusercontent.com/56642774/173188004-2f3439bb-68d3-49f7-ba38-0352557e8508.png)
![Screenshot from 2022-06-11 13-48-34](https://user-images.githubusercontent.com/56642774/173188005-a899105d-1392-4ceb-9f15-6c0c8ac9b764.png)
#### The page with the specified page_id 
![Screenshot from 2022-06-11 13-49-52](https://user-images.githubusercontent.com/56642774/173188009-4fc043e3-5623-435e-9040-0eec8b7de04f.png)
![Screenshot from 2022-06-11 13-50-17](https://user-images.githubusercontent.com/56642774/173188014-72ac9416-2701-4f2e-a1ba-6b5ddd4180eb.png)
#### Id, name, and the number of created pages of all the users who created at least one page in a specified time range
![Screenshot from 2022-06-11 13-52-28](https://user-images.githubusercontent.com/56642774/173188016-ff17e256-0e24-41ba-941b-11b5faa669b6.png)
![Screenshot from 2022-06-11 13-53-08](https://user-images.githubusercontent.com/56642774/173188018-15cfacd2-8656-4fdf-9a4e-0ca81c868708.png)

### Category A
#### The aggregated statistics containing the number of created pages for each Wikipedia domain for each hour in the last 6 hours, excluding the last hour.
![Screenshot from 2022-06-11 14-02-07](https://user-images.githubusercontent.com/56642774/173188021-cebdc29c-e0c3-4e27-8082-de2fbeacc52f.png)
#### The statistics about the number of pages created by bots for each of the domains for the last 6 hours, excluding the last hour
![Screenshot from 2022-06-11 14-02-20](https://user-images.githubusercontent.com/56642774/173188024-280cb770-b054-45ac-ae3c-6217e42fbcb9.png)
#### Top 20 users that created the most pages during the last 6 hours,
excluding the last hour.
![Screenshot from 2022-06-11 14-03-02](https://user-images.githubusercontent.com/56642774/173188028-ab23e6d7-5f3c-4a10-8e29-149999304b63.png)





