import requests
import json
import pika
import sys

# class to store Message Queue Details


class MessageQueueDetails:
    def __init__(self, hostname, username, password, exchange):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.exchange = exchange


# function to convert Hexadecimal string to Decimal string


def hex_to_dec(data):
    return str(int(data, 16))


# function to create the routing key


def create_routing_key(data):
    routing_key = (
        hex_to_dec(data["gatewayEui"])
        + "."
        + hex_to_dec(data["profileId"])
        + "."
        + hex_to_dec(data["endpointId"])
        + "."
        + hex_to_dec(data["clusterId"])
        + "."
        + hex_to_dec(data["attributeId"])
    )

    return routing_key


# function to create the message body


def create_body(data):
    body = {"value": str(data["value"]), "timestamp": str(data["timestamp"])}

    return body


# function to fetch data from the api, format the data and return the created routing key and message body


def fetch_and_format_api_data(hostname):
    try:
        response = requests.get(url=hostname)
    except requests.exceptions.RequestException as request_error:
        print("Error while trying to get data from api", request_error)

        sys.exit(0)

    response_data = response.text

    json_data = json.loads(response_data)

    routing_key = create_routing_key(json_data)

    body = create_body(json_data)

    return routing_key, body


def main():
    hostname_api = "https://xqy1konaa2.execute-api.eu-west-1.amazonaws.com/prod/results"

    message_queue_details = MessageQueueDetails(
        hostname="candidatemq.n2g-dev.net",
        username="cand_t4a4",
        password="fGOuTMHTHlINkAg5",
        exchange="cand_t4a4",
    )

    credentials = pika.PlainCredentials(
        username=message_queue_details.username, password=message_queue_details.password
    )

    connection_parameters = pika.ConnectionParameters(
        host=message_queue_details.hostname, credentials=credentials
    )

    connection = pika.BlockingConnection(connection_parameters)

    channel = connection.channel()

    print("Sending data. To exit press CTRL+C")

    # loop to consume data from api and send them to an exchange on a RabbitMQ instance for filtering
    while True:
        try:
            # get and format data from api
            routing_key, body = fetch_and_format_api_data(hostname_api)

            print("Value: " + body["value"] + " , " "Timestamp: " + body["timestamp"])

            body = json.dumps(body)

            # send data to RabbitMQ exchange
            channel.basic_publish(
                exchange=message_queue_details.exchange,
                routing_key=routing_key,
                body=body,
            )

        except KeyboardInterrupt:
            print("Producer Stopped")

            break

    connection.close()

    print("Connection to RabbitMQ closed")

    sys.exit(0)


if __name__ == "__main__":
    main()
