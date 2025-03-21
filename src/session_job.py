from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_taxi_events_aggSessionW_sink(t_env):
    table_name = 'taxi_events_agg_PUDO_5minSessionDOtime'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PU_DO_ID_pair VARCHAR,
            num_trips BIGINT,
            PRIMARY KEY (window_start, window_end, PU_DO_ID_pair) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    table_name = "taxi_events_source"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance FLOAT,
            tip_amount FLOAT,
            row_index INTEGER,
            PU_DO_ID_pair AS CONCAT(cast(PULocationID AS VARCHAR), '-', cast(DOLocationID AS VARCHAR)),
            dropoff_timestamp AS TO_TIMESTAMP(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK for dropoff_timestamp as dropoff_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_aggregation_session():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            # This lambda is your timestamp assigner:
            #   event -> The data record
            #   timestamp -> The previously assigned (or default) timestamp
            lambda event, timestamp: event[2]  # We treat the second tuple element as the event-time (ms).
        )
    )
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_taxi_events_aggSessionW_sink(t_env)

        t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT
            SESSION_START(dropoff_timestamp,  INTERVAL '5' MINUTE),
            SESSION_END(dropoff_timestamp,  INTERVAL '5' MINUTE),
            PU_DO_ID_pair,
            SUM(1) AS num_trips
        FROM {source_table}
        GROUP BY
        SESSION(dropoff_timestamp,  INTERVAL '5' MINUTE),
        PU_DO_ID_pair ;
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

if __name__ == '__main__':
    log_aggregation_session()
