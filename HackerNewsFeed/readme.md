## Streaming HackerNews Comments with Pyspark and Kafka


Steps:

1. Start zookeeper server:

    ```
    C:\kafka\bin\windows> zookeeper-server-start.bat ../../config/zookeeper.properties
    ```

2. Start Kafka Server:

    ```
    C:\kafka\bin\windows> kafka-server-start.bat ../../config/server.properties
    ```

3. Create topic for storing HackerNews data:

    ```
    kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic hncomments
    ```

4. Create a MySQL DB for storing the transformed comments:

    ```sql
    CREATE DATABASE hncomments;
    USE hncomments;

    CREATE TABLE comments(
        id INT NOT NULL AUTO_INCREMENT,
        comment TEXT,
        comment_id TEXT,
        PRIMARY KEY (id));
    ```

    You then need to edit ```sqlconfig.json``` to update the username and password for accessing your local SQL server. 

4. Begin collecting HackerNews data with ```rssfeed.py```:

    ```
    spark\hackernewsETL> python rssfeed.py
    ```

    You should see something like this when ```rssfeed.py``` starts:

    ```
    Found 20 new entries after Fri, 01 Jan 2021 18:03:42 GMT.
    ```

    meaning that 20 new comments have been found. 


5. Begin streaming comments with ```kafkatopyspark.py```:

    ```
    spark\hackernewsETL> spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.6 kafkatopyspark.py
    ```

    As comments start to pour in, the Spark pipeline will normalize them and store them in your database. Here's what the output of a transformed batch looks like:

    ```
    ====== 2021-01-01 10:42:45 ======
    +--------------------+----------+
    |             comment|comment_id|
    +--------------------+----------+
    |i just got my fir...|  25605455|
    |>wood burners hav...|  25605454|
    |thanks. someone h...|  25605453|
    |i think i just re...|  25605452|
    |> if my date of b...|  25605451|
    |yeah, i wanted to...|  25605450|
    |probably true. th...|  25605449|
    |plenty of vendors...|  25605447|
    |printing money re...|  25605446|
    |i never had any p...|  25605445|
    |-Ç/hour is my lim...|  25605444|
    |i'm not very fami...|  25605443|
    |i do this as well...|  25605442|
    |yes, but i would ...|  25605441|
    |debian on less ra...|  25605440|
    |doordash and post...|  25605439|
    |this one will end...|  25605438|
    |there's a place l...|  25605437|
    |you should also t...|  25605436|
    |i agree, some kin...|  25605435|
    +--------------------+----------+
    ```