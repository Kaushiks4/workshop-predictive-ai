import random
from uuid import uuid4
from datetime import datetime, timedelta

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

credit_card_numbers = [
    4774993836989662,
    4738273984732749,
    4739023369472686,
    4742020908432434,
    4743519677912308,
    4757008603231174,
    4751762910051615,
    4754760449011363,
    4760755526930859
]
def read_schema():
    schema_config = {}
    with open("schema.properties") as sp:
     for line in sp:
        if len(line) != 0 and line[0] != "#":
           parameter, value = line.strip().split('=', 1)
           schema_config[parameter] = value.strip()
    return schema_config

def read_config():
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

class Transaction(object):
    """
    Transaction record

    Args:
        transaction_id (int): Transaction ID

        transaction_timestamp (str): Transaction occured timestamp

        credit_card_number (long): Credit card number of the customer used for the transaction

        amount (str): Amount

        location (str): Transaction occured location
    """

    def __init__(self, transaction_id, transaction_timestamp, credit_card_number, amount, location):
        self.transaction_id = transaction_id
        self.transaction_timestamp = transaction_timestamp
        self.credit_card_number = credit_card_number
        # address should not be serialized, see user_to_dict()
        self.amount = amount
        self.location = location


def transaction_to_dict(transaction, ctx):
    """
    Returns a dict representation of a Transaction instance for serialization.

    Args:
        transaction (Transaction): Transaction instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with transaction attributes to be serialized.
    """

    # Transactoin._address must not be serialized; omit from dict
    return dict(transaction_id=transaction.transaction_id,
                transaction_timestamp=transaction.transaction_timestamp,
                credit_card_number=transaction.credit_card_number,
                amount=transaction.amount,
                location=transaction.location)


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for Transaction record {}: {}".format(msg.key(), err))
        return
    print('Transaction record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def generate_dummy_transactions(num_transactions, num_fraudulent):
    locations = ["Tokyo", "Rio", "Berlin", "Oslo", "Stockholm","Nairobi","Spain","Portugal"]
    base_timestamp = datetime.now()
    transactions = []
    
    # Generate normal transactions
    for i in range(num_transactions - num_fraudulent):
        random_hours = random.randint(1,5)
        random_minutes = random.randint(10,50)
        random_seconds = random.randint(10,50)
        transaction_id = i+100001
        credit_card_number = random.choice(credit_card_numbers)  # Pick a random credit card number
        amount = random.randint(500, 2000)  # Random amount between $500 and $2000
        location = random.choice(locations)  # Pick a random location
        timestamp = ' '.join((base_timestamp - timedelta(hours=random_hours,minutes=random_minutes,seconds=random_seconds)).isoformat().split('T'))[:-3]
        transactions.append(Transaction(transaction_id=transaction_id,transaction_timestamp=timestamp,credit_card_number=credit_card_number, amount=amount, location=location))
        

    # Generate fraudulent transactions
    for j in range(num_fraudulent):
        #Create a unique transaction ID
        transaction_id = i+100001+j 

        #Selecting the credit card number in the last iteration of the previous choice.
        credit_card_number = credit_card_number
        
        # High amount to simulate fraud
        amount = random.randint(5000, 10000)
        
        # Pick a random location
        location = random.choice(locations)

        # Generate a timestamp
        timestamp = ' '.join((base_timestamp + timedelta(minutes=random.uniform(0, 10))).isoformat().split('T'))[:-3]
        
        transactions.append(Transaction(transaction_id=transaction_id,transaction_timestamp=timestamp,credit_card_number=credit_card_number, amount=amount, location=location))
    
    return transactions


def main(config, schema):
    topic = "transactions"

    schema_str = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "Transaction",
      "description": "Credit card transaction",
      "type": "object",
      "properties": {
        "amount": {
            "connect.index": 3,
            "connect.type": "int32",
            "type": "integer"
        },
        "credit_card_number": {
            "connect.index": 2,
            "connect.type": "int64",
            "type": "integer"
        },
        "location": {
            "connect.index": 4,
            "type": "string"
        },
        "transaction_id": {
            "connect.index": 0,
            "connect.type": "int32",
            "type": "integer"
        },
        "transaction_timestamp": {
            "connect.index": 1,
            "type": "string"
        }
      }
    }
    """
    schema_registry_conf = {'url': schema["schema.registry.url"],'basic.auth.user.info': schema["schema.registry.username"]+":"+schema["schema.registry.password"]}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, transaction_to_dict)

    producer = Producer(config)

    print("Producing transaction records to topic {}. ^C to exit.".format(topic))
    num_transactions = 100
    num_fraudulent = random.randint(5, 10)
    transactions = generate_dummy_transactions(num_transactions,num_fraudulent)
    for transaction in transactions:
       producer.produce(
          topic = topic,
          key=string_serializer(str(uuid4())),
          value=json_serializer(transaction, SerializationContext(topic, MessageField.VALUE)),
          on_delivery=delivery_report
       )
    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':
    config = read_config()
    schema_config = read_schema()
    main(config,schema_config)