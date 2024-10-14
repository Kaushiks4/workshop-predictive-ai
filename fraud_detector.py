import json
from datetime import datetime
from confluent_kafka import Consumer, Producer

# Initialize the Kafka consumer
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

def identify_fraud(total_amount, transaction_count, average_spending):
    # Rule-based fraud detection logic
    # Example rules:
    if transaction_count >= 2 and total_amount > average_spending:  # Excessive number of transactions
        return True
    return False  # Default to not fraudulent

def produce_fraudulent_transaction(producer,credit_card_number,customer_email,amount, timestamp,average_spend,transactions_count):
    record = {"prompt_message": f"Generate a very short message saying the transaction is likely to be fraud with the details, credit card number {credit_card_number} customer {customer_email} total spend {amount}  average spend {average_spend} total number of transactions {transactions_count} time period {timestamp}"}
    producer.produce('fraudulent_transactions5', value=json.dumps(record))
    producer.flush()

def run_fraud_detection(producer,consumer):
    try:
        while True:
            msg = consumer.poll(1.0)  # Timeout
            if msg is None:
                print("Polling for messages...")
                continue
            if msg.error():
                print("Error: {}".format(msg.error()))
                continue

            key = json.loads(msg.key()[5:].decode('utf-8'))
            feature = json.loads(msg.value()[5:].decode('utf-8'))
            
            total_amount = feature['total_amount']
            transaction_count = feature['transaction_count']
            average_spending = feature['average_spending_amount']
            time_range = datetime.fromtimestamp(feature["window_start"]/1000.0).isoformat() + " to " + datetime.fromtimestamp(feature["window_end"]/1000.0).isoformat()
            
            is_fraudulent = identify_fraud(total_amount, transaction_count, average_spending)
            
            if is_fraudulent:
                produce_fraudulent_transaction(
                    producer=producer,
                    credit_card_number=key["credit_card_number"],
                    customer_email=feature['customer_email'],
                    amount=total_amount,
                    average_spend=average_spending,
                    transactions_count=transaction_count,
                    timestamp=time_range
                )
                print(f"Fraud detected for transaction for credit card: {key['credit_card_number']} {feature}")
    except KeyboardInterrupt:
        pass
    finally:
        # closes the consumer connection
        consumer.close()

if __name__ == "__main__":
    # Parse the properties file
    config = read_config()
    
    # Setup consumers
    consumer = Consumer(config)
    consumer.subscribe(['feature_set5'])
    
    # Initialize the Kafka producer for fraudulent transactions
    producer = Producer(config)

    # Run the fraud detection consumer
    run_fraud_detection(producer,consumer)