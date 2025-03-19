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

Now we need to send the data to the `green-trips` topic

Read the data, and keep only these columns:

* `'lpep_pickup_datetime',`
* `'lpep_dropoff_datetime',`
* `'PULocationID',`
* `'DOLocationID',`
* `'passenger_count',`
* `'trip_distance',`
* `'tip_amount'`

Now send all the data using this code:

```python
producer.send(topic_name, value=message)
```

For each row (`message`) in the dataset. In this case, `message`
is a dictionary.

After sending all the messages, flush the data:

```python
producer.flush()
```

Use `from time import time` to see the total time 

```python
from time import time

t0 = time()

# ... your code

t1 = time()
took = t1 - t0
```

How much time did it take to send the entire dataset and flush? 


## Question 5: Build a Sessionization Window (2 points)

Now we have the data in the Kafka stream. It's time to process it.

* Copy `aggregation_job.py` and rename it to `session_job.py`
* Have it read from `green-trips` fixing the schema
* Use a [session window](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/windows/) with a gap of 5 minutes
* Use `lpep_dropoff_datetime` time as your watermark with a 5 second tolerance
* Which pickup and drop off locations have the longest unbroken streak of taxi trips?


## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw6
- Deadline: See the website

