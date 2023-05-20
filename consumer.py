import pika
import mysql.connector
import json
import sys

# class to store Message Queue Details


class MessageQueueDetails:
    def __init__(self, hostname, username, password, exchange, queue):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.exchange = exchange
        self.queue = queue


# class to store Database Details


class DatabaseDetails:
    def __init__(self, hostname, username, password, database):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database


def main():
    message_queue_details = MessageQueueDetails(
        hostname="candidatemq.n2g-dev.net",
        username="cand_t4a4",
        password="fGOuTMHTHlINkAg5",
        exchange="cand_t4a4",
        queue="cand_t4a4_results",
    )

    database_details = DatabaseDetails(
        hostname="candidaterds.n2g-dev.net",
        username="cand_t4a4",
        password="fGOuTMHTHlINkAg5",
        database="cand_t4a4",
    )

    credentials = pika.PlainCredentials(
        username=message_queue_details.username, password=message_queue_details.password
    )

    connection_parameters = pika.ConnectionParameters(
        host=message_queue_details.hostname, credentials=credentials
    )

    connection = pika.BlockingConnection(connection_parameters)

    channel = connection.channel()

    channel.queue_bind(
        exchange=message_queue_details.exchange, queue=message_queue_details.queue
    )

    # connect to mysql database
    try:
        db_conn = mysql.connector.connect(
            host=database_details.hostname,
            database=database_details.database,
            user=database_details.username,
            password=database_details.password,
        )

        if db_conn.is_connected():
            db_Info = db_conn.get_server_info()

            print("Connected to MySQL Server version ", db_Info)

            cursor = db_conn.cursor()

            cursor.execute("select database();")

            record = cursor.fetchone()

            print("You're connected to database: ", record)

    except mysql.connector.Error as connection_error:
        print("Error while connecting to MySQL", connection_error)

    try:
        # create table to store the data
        mySql_create_table_query = "CREATE TABLE data (Id int NOT NULL AUTO_INCREMENT, Value varchar(250) NOT NULL, Timestamp varchar(250) NOT NULL, PRIMARY KEY (Id))"

        cursor = db_conn.cursor()

        cursor.execute(mySql_create_table_query)

        print("Data Table created successfully ")

    except mysql.connector.Error as create_table_error:
        print("Failed to create table in MySQL: {}".format(create_table_error))

    # function that gets the message from the queue and stores it in the database
    def callback(ch, method, properties, body):
        data = json.loads(body)

        print("Value: " + data["value"] + " , " + "Timestamp: " + data["timestamp"])

        try:
            mySql_insert_query = "INSERT INTO data (Value, Timestamp) VALUES (%s, %s)"

            values = (data["value"], data["timestamp"])

            cursor.execute(mySql_insert_query, values)

            db_conn.commit()

            print("Record inserted successfully into Data table")

        except mysql.connector.Error as insert_error:
            print("Failed to insert into MySQL table {}".format(insert_error))

    channel.basic_consume(
        queue=message_queue_details.queue, on_message_callback=callback, auto_ack=True
    )

    print("Waiting for data. To exit press CTRL+C")

    try:
        channel.start_consuming()

    except KeyboardInterrupt:
        print("Consumer Stopped")

        channel.stop_consuming()

    connection.close()

    print("Connection to RabbitMQ closed")

    cursor.close()

    db_conn.close()

    print("MySQL connection closed")

    sys.exit(0)


if __name__ == "__main__":
    main()
