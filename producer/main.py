import json
import uuid

from kafka import KafkaProducer


def send_stock_price_data():
    """
    Sends stock price data to a Redpanda topic using a Kafka producer.

    The function reads stock price data from a JSON file, generates a message key,
    and sends the data to the specified Redpanda topic.

    Args:
        None

    Returns:
        None
    """

    topic = "stock_price_topic"
    producer = KafkaProducer(
        bootstrap_servers="localhost:19092",
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Input stock price data file - JSON format
    source_file = "stock_price_data.json"

    # Open the file and read the stock price data
    with open(source_file, "r") as file:
        stock_price_records = json.load(file)
        for data in stock_price_records:
            # Generate message key (optional)
            message_key = str(uuid.uuid4())

            # Send the stock price data to the Redpanda topic
            future = producer.send(topic, key=message_key, value=data)
            # Block until a single message is sent (or timeout in 15 seconds)
            result = future.get(timeout=15)

            print(
                "Message sent, partition: ",
                result.partition,
                ", offset: ",
                result.offset,
            )


# Call the function to send the stock price data
send_stock_price_data()
