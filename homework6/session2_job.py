from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.common import WatermarkStrategy, Duration

def create_taxi_events_sink(t_env):
    table_name = 'taxi_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            pickup_location_id INTEGER,
            dropoff_location_id INTEGER,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            num_of_trips BIGINT
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
    table_name = "events"
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            pickup_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, '{pattern}'),
            dropoff_timestamp AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK FOR dropoff_timestamp AS dropoff_timestamp - INTERVAL '5' SECOND
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

def session_streak_analysis():
    # Set up execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # 10 seconds
    env.set_parallelism(3)

    # Set up table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)
    
    # Configure to use filesystem for checkpoints to avoid memory issues
    t_env.get_config().get_configuration().set_string(
        "state.backend", "filesystem"
    )
    t_env.get_config().get_configuration().set_string(
        "state.checkpoints.dir", "file:///tmp/flink-checkpoints"
    )
    
    try:
        # First try to perform a count to verify data is being read
        source_table = create_events_source_kafka(t_env)
        sink_table = create_taxi_events_sink(t_env)
        
        # Debug: Print query plan
        print("Executing query to count records:")
        t_env.execute_sql(f"SELECT COUNT(*) FROM {source_table}").print()
        
        # Use a shorter session window for testing
        query = f"""
        INSERT INTO {sink_table}
        SELECT
            PULocationID AS pickup_location_id,
            DOLocationID AS dropoff_location_id,
            SESSION_START(dropoff_timestamp, INTERVAL '5' SECOND) AS window_start,
            SESSION_END(dropoff_timestamp, INTERVAL '5' SECOND) AS window_end,
            COUNT(*) AS num_of_trips
        FROM {source_table}
        GROUP BY 
            SESSION(dropoff_timestamp, INTERVAL '5' SECOND),
            PULocationID,
            DOLocationID
        """
        
        print("Executing main query:")
        t_env.execute_sql(query).wait()
        
    except Exception as e:
        print("Error processing taxi session streaks:", str(e))
        raise

if __name__ == '__main__':
    session_streak_analysis()