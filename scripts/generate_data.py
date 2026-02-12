import pandas as pd
from faker import Faker
import random
import os

fake = Faker()

def generate_data(output_path):
    print("Generating synthetic retail data...")
    data = []
    for _ in range(1000):
        # Generate random data for each column
        record = {
            "transaction_id": fake.uuid4(),
            "customer_id": random.randint(1, 100),
            "product_category": random.choice(['Electronics', 'Clothing', 'Home', 'Toys']),
            "amount": round(random.uniform(10, 500), 2),
            "transaction_date": fake.date_time_this_year().isoformat()
        }

        # Data Quality Scenario
        # Introduce missing values randomly
        if random.random() < 0.05:
            record["amount"] = -100 # Invalid negative amount
        if random.random() < 0.05:
            record["product_category"] = None # Missing category

        data.append(record)

    # Save to CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    print(f"Data saved to {output_path}")

if __name__ == "__main__":
    generate_data("/opt/airflow/data/raw_sales.csv")