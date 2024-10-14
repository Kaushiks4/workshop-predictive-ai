import json
import random
import logging

from confluent_kafka import Producer
from datetime import datetime, timedelta

# List of dummy credit card numbers
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

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def produce_transaction(producer,transaction_id, credit_card_number, amount, location, timestamp):
    record = {
        "transaction_id": transaction_id,
        "transaction_timestamp": timestamp,
        "credit_card_number": credit_card_number,
        "amount": amount,
        "location": location
    }
    
    producer.produce('transactions', value=json.dumps(record))
    producer.flush()  # Ensure the message is sent

def generate_dummy_transactions(producer,num_transactions, num_fraudulent):
    locations = ["Tokyo", "Rio", "Berlin", "Oslo", "Stockholm","Nairobi","Spain","Portugal"]
    base_timestamp = datetime.now()
    
    # Generate normal transactions
    for i in range(num_transactions - num_fraudulent):
        random_hours = random.randint(1,5)
        random_minutes = random.randint(10,50)
        random_seconds = random.randint(10,50)
        transaction_id = i+100001
        credit_card_number = random.choice(credit_card_numbers)  # Pick a random credit card number
        amount = random.randint(100, 2000)  # Random amount between $10 and $500
        location = random.choice(locations)  # Pick a random location
        timestamp = ' '.join((base_timestamp - timedelta(hours=random_hours,minutes=random_minutes,seconds=random_seconds)).isoformat().split('T'))[:-3]
        produce_transaction(producer,transaction_id, credit_card_number, amount, location,timestamp)

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
        
        produce_transaction(producer,transaction_id, credit_card_number, amount, location,timestamp)

if __name__ == "__main__":
    # Set the logging info
    logging.basicConfig(level=logging.INFO)

    # Set up the config.
    config = read_config()
    producer = Producer(config)
   
   # Generate 100 dummy transactions with 5-10 fraudulent transactions
    num_transactions = 100
    num_fraudulent = random.randint(5, 10)  # Randomly select number of fraudulent transactions
    generate_dummy_transactions(producer,num_transactions, num_fraudulent)
    logging.info("Generated 100 transactions complete..!")