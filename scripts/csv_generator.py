import csv
import random
from faker import Faker
from datetime import datetime

# Initialize Faker
fake = Faker()

def generate_transaction_data(num_rows):
    """
    Generates a list of dictionaries, where each dictionary
    represents a single transaction row.
    """
    transactions = []
    product_categories = ['Electronics', 'Groceries', 'Apparel', 'Home Goods', 'Beauty']
    
    for _ in range(num_rows):
        # Generate fake data for each field
        transaction_id = fake.uuid4()
        timestamp = datetime.now()
        customer_id = fake.uuid4()
        customer_name = fake.name()
        product_name = fake.word()
        product_category = random.choice(product_categories)
        quantity = random.randint(1, 10)
        price = round(random.uniform(5.00, 500.00), 2)
        total_amount = round(quantity * price, 2)
        payment_method = random.choice(['Credit Card', 'Debit Card', 'Cash', 'Online Payment'])
        store_location = fake.city()

        transactions.append({
            'transaction_id': transaction_id,
            'timestamp': timestamp,
            'customer_id': customer_id,
            'customer_name': customer_name,
            'product_name': product_name,
            'product_category': product_category,
            'quantity': quantity,
            'price': price,
            'total_amount': total_amount,
            'payment_method': payment_method,
            'store_location': store_location
        })
    
    return transactions

def write_to_csv(data, filename='/opt/airflow/data/transactions.csv'):
    """
    Writes a list of dictionaries to a CSV file.
    """
    if not data:
        print("No data to write.")
        return
        
    # Get the column headers from the first dictionary's keys
    headers = data[0].keys()
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Successfully generated {len(data)} rows and saved to {filename}.")

if __name__ == '__main__':
    NUM_ROWS = 1000  # You can change this to generate more or less data
    data_to_write = generate_transaction_data(NUM_ROWS)
    write_to_csv(data_to_write)