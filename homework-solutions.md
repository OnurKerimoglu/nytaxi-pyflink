# Homework

## Question 1: Redpanda version
*What's the version of redpandas?*

After running  `docker compose up --build`, I accessed the redpanda container with `docker exec -it redpanda-1 /bin/bash`. There, issuing `rpk help` revealed that the version can be revealed with `rpk --version`, which yields `rpk version v24.2.18 (rev f9a22d4430)`

## Question 2. Creating a topic
*What's the output of the command for creating a topic? Include the entire output in your answer.*

Issuing ` rpk topic create green-trips` yields:
```
TOPIC        STATUS
green-trips  OK
```

## Question 3. Connecting to the Kafka server
*Provided that you can connect to the server, what's the output of `producer.bootstrap_connected()`*

The output is `True`.

## Question 4: Sending the Trip Data
*How much time did it take to send the entire dataset and flush?*

Sending all 476386 rows in the table and flushing took 239.42 seconds.


## Question 5: Build a Sessionization Window (2 points)
*Which pickup and drop off locations have the longest unbroken streak of taxi trips?*

Quering the aggregated table:
```
SELECT window_start, window_end, pu_do_id_pair, num_trips
FROM public.taxi_events_agg_pudo_5minsessiondotime
order by num_trips desc
limit 1;
```
yields:\\

| window_start            | window_end              | pu_do_id_pair | num_trips |
| ----------------------- | ----------------------- | ------------- | --------- |
| 2019-10-16 18:18:42.000 |	2019-10-16 19:26:16.000 | 95-95         | 44        |

