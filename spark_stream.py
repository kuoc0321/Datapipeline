import logging
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType
from pyspark.sql.functions import col, from_json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages',
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.2.24") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'user_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully")
    except Exception as e:
        logging.warning(f"Kafka DataFrame could not be created because: {e}")
    return spark_df


def create_mysql_connection():
    try:
        conn = mysql.connector.connect(
            database='user_db',
            user='root',
            password='123',
            host='localhost',
            port='3306'
        )
        cursor = conn.cursor()
        logging.info("MySQL connection created successfully")
        return conn, cursor
    except Exception as e:
        logging.error(f"Could not create MySQL connection due to {e}")
        return None, None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

    logging.info("Selection DataFrame created from Kafka")
    return sel


def write_to_mysql(batch_df, batch_id):
    conn, cursor = create_mysql_connection()

    if conn is not None:

        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id VARCHAR(255) PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            gender VARCHAR(10),
            address VARCHAR(255),
            post_code VARCHAR(20),
            email VARCHAR(100),
            username VARCHAR(50),
            registered_date VARCHAR(50),
            phone VARCHAR(20),
            picture VARCHAR(255)
        )
        """
        cursor.execute(create_table_query)  
        logging.info("Table 'users' checked/created in MySQL.")

        for row in batch_df.collect():
            insert_query = """
                INSERT INTO users (id, first_name, last_name, gender, address, post_code, email, username, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (row.id, row.first_name, row.last_name, row.gender,
                                           row.address, row.post_code, row.email, row.username,
                                           row.registered_date, row.phone, row.picture))
            logging.info(f"Inserted {row.first_name} {row.last_name} into MySQL")

        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()  # Close the connection after the batch is processed
        logging.info(f"Batch {batch_id} processed and data inserted into MySQL")
    else:
        logging.error("Failed to create MySQL connection for writing data.")


if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)

            # Write the output to MySQL
            query = selection_df.writeStream \
                .outputMode("append") \
                .foreachBatch(write_to_mysql) \
                .start() \
                .awaitTermination()

            logging.info("Streaming query started successfully.")
