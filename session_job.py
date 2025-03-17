from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.common import WatermarkStrategy, Duration

def create_green_trips_source(t_env):
    table_name = "green_trips"
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
            dropoff_timestamp AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK FOR dropoff_timestamp AS dropoff_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false', 
            'json.ignore-parse-errors' = 'true'     
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def create_aggregated_trips_sink(t_env):
    table_name = 'aggregated_trips'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_trips BIGINT,
            session_duration BIGINT,
            PRIMARY KEY (PULocationID, DOLocationID, session_start) NOT ENFORCED
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

def session_job():
    # Set up execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60 * 1000)  # 60 seconds
    env.set_parallelism(1)

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
        # Create source and sink tables
        source_table = create_green_trips_source(t_env)
        sink_table = create_aggregated_trips_sink(t_env)
        
        # Debug: Print query plan
        print("Executing query to count records:")
        t_env.execute_sql(f"SELECT COUNT(*) FROM {source_table}").print()
        
        # Query to find sessions and their durations
        query = f"""
        INSERT INTO {sink_table}
        SELECT
            PULocationID,
            DOLocationID,
            SESSION_START(dropoff_timestamp, INTERVAL '5' MINUTE) AS session_start,
            SESSION_END(dropoff_timestamp, INTERVAL '5' MINUTE) AS session_end,
            COUNT(*) AS num_trips,
            TIMESTAMPDIFF(SECOND, SESSION_START(dropoff_timestamp, INTERVAL '5' MINUTE), 
                         SESSION_END(dropoff_timestamp, INTERVAL '5' MINUTE)) AS session_duration
        FROM {source_table}
        GROUP BY 
            SESSION(dropoff_timestamp, INTERVAL '5' MINUTE),
            PULocationID,
            DOLocationID
        """
        
        print("Executing main query:")
        t_env.execute_sql(query).wait()
        
    except Exception as e:
        print("Error processing taxi sessions:", str(e))
        raise

if __name__ == '__main__':
    session_job()
