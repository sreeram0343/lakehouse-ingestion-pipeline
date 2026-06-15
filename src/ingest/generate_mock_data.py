import os
import json
import random
import argparse
from datetime import datetime, timedelta
import boto3
from botocore.client import Config

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "supersecretpassword")
BUCKET_NAME = "raw"

# Initialize MinIO client
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

# Synthetic data helpers
COUNTRIES = ["USA", "Canada", "UK", "Germany", "France", "Japan", "Australia", "India"]
CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Beauty"]
ORDER_STATUSES = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]

PRODUCT_NAMES = {
    "Electronics": ["Smartphone", "Laptop", "Wireless Headphones", "Smartwatch", "Bluetooth Speaker"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Sneakers", "Socks"],
    "Home & Kitchen": ["Coffee Maker", "Blender", "Air Fryer", "Vacuum Cleaner", "Toaster"],
    "Books": ["Science Fiction Novel", "Historical Biography", "Cooking Guide", "Self-Help Book", "Mystery Thriller"],
    "Beauty": ["Moisturizer", "Sunscreen", "Perfume", "Shampoo", "Lip Balm"]
}

def generate_base_customers(num_customers=50, as_of_date=datetime.now()):
    customers = []
    for i in range(1, num_customers + 1):
        cust_id = f"CUST_{i:03d}"
        signup_date = as_of_date - timedelta(days=random.randint(10, 365))
        customers.append({
            "customer_id": cust_id,
            "name": f"Customer {i}",
            "email": f"customer_{i}@example.com",
            "country": random.choice(COUNTRIES),
            "signup_date": signup_date.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": signup_date.strftime("%Y-%m-%d %H:%M:%S")
        })
    return customers

def generate_base_products():
    products = []
    prod_idx = 1
    for category, names in PRODUCT_NAMES.items():
        for name in names:
            prod_id = f"PROD_{prod_idx:03d}"
            price = round(random.uniform(5.0, 1200.0), 2)
            inventory = random.randint(10, 500)
            products.append({
                "product_id": prod_id,
                "name": name,
                "category": category,
                "price": price,
                "inventory_count": inventory,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            prod_idx += 1
    return products

def generate_orders(customers, products, num_orders, order_date):
    orders = []
    order_items = []
    
    start_time = datetime.strptime(order_date, "%Y-%m-%d")
    
    for i in range(1, num_orders + 1):
        # Create timestamps throughout the day
        sec_offset = random.randint(0, 86399)
        o_time = start_time + timedelta(seconds=sec_offset)
        timestamp_str = o_time.strftime("%Y-%m-%d %H:%M:%S")
        
        order_id = f"ORD_{start_time.strftime('%Y%m%d')}_{i:03d}"
        customer = random.choice(customers)
        status = random.choice(ORDER_STATUSES)
        
        # Select items for the order
        items_count = random.randint(1, 4)
        selected_products = random.sample(products, items_count)
        
        total_amount = 0.0
        for item_idx, prod in enumerate(selected_products):
            quantity = random.randint(1, 3)
            unit_price = prod["price"]
            item_total = round(quantity * unit_price, 2)
            total_amount += item_total
            
            order_items.append({
                "order_item_id": f"{order_id}_{item_idx+1}",
                "order_id": order_id,
                "product_id": prod["product_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "updated_at": timestamp_str
            })
            
        orders.append({
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "order_date": timestamp_str,
            "total_amount": round(total_amount, 2),
            "status": status,
            "updated_at": timestamp_str
        })
        
    return orders, order_items

def save_to_minio(s3, data, path):
    payload = "\n".join([json.dumps(record) for record in data])
    print(f"Uploading {len(data)} records to s3://{BUCKET_NAME}/{path}")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=path,
        Body=payload,
        ContentType="application/json"
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Ingestion date (YYYY-MM-DD)", default=datetime.now().strftime("%Y-%m-%d"))
    args = parser.parse_args()
    
    s3 = get_s3_client()
    dt = datetime.strptime(args.date, "%Y-%m-%d")
    
    # 1. Customers Ingestion (SCD demonstration - we generate state of customers)
    # Check if customers file already exists in raw to load and modify
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key="customers/customers.json")
        lines = obj["Body"].read().decode("utf-8").split("\n")
        customers = [json.loads(line) for line in lines if line.strip()]
        print(f"Loaded {len(customers)} existing customers from MinIO.")
        
        # Modify 10% of customers (simulate address changes / email updates)
        updated_count = 0
        for cust in customers:
            if random.random() < 0.10:
                cust["country"] = random.choice(COUNTRIES)
                cust["updated_at"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                updated_count += 1
        print(f"Updated {updated_count} customer profiles.")
        
        # Add 2-5 new customers
        new_cust_count = random.randint(2, 5)
        last_id_num = int(customers[-1]["customer_id"].split("_")[1])
        for i in range(1, new_cust_count + 1):
            new_id = f"CUST_{last_id_num + i:03d}"
            customers.append({
                "customer_id": new_id,
                "name": f"Customer {last_id_num + i}",
                "email": f"customer_{last_id_num + i}@example.com",
                "country": random.choice(COUNTRIES),
                "signup_date": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": dt.strftime("%Y-%m-%d %H:%M:%S")
            })
        print(f"Added {new_cust_count} new customers.")
        
    except s3.exceptions.NoSuchKey:
        print("No existing customers found. Generating base customer set...")
        customers = generate_base_customers(50, dt)
        
    # 2. Products Ingestion
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key="products/products.json")
        lines = obj["Body"].read().decode("utf-8").split("\n")
        products = [json.loads(line) for line in lines if line.strip()]
        print(f"Loaded {len(products)} existing products from MinIO.")
        
        # Update price/inventory of some products
        updated_count = 0
        for prod in products:
            if random.random() < 0.15:
                prod["price"] = round(prod["price"] * random.choice([0.9, 0.95, 1.05, 1.1]), 2)
                prod["inventory_count"] = max(0, prod["inventory_count"] + random.randint(-20, 50))
                prod["updated_at"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                updated_count += 1
        print(f"Updated pricing/stock for {updated_count} products.")
    except s3.exceptions.NoSuchKey:
        print("No existing products found. Generating base catalog...")
        products = generate_base_products()
        
    # Save modified catalogs
    save_to_minio(s3, customers, "customers/customers.json")
    save_to_minio(s3, products, "products/products.json")
    
    # 3. Orders and Order Items (Transactional incremental data)
    num_orders = random.randint(10, 20)
    orders, order_items = generate_orders(customers, products, num_orders, args.date)
    
    # Save incremental transaction files
    year, month, day = args.date.split("-")
    save_to_minio(s3, orders, f"orders/year={year}/month={month}/day={day}/orders.json")
    save_to_minio(s3, order_items, f"order_items/year={year}/month={month}/day={day}/order_items.json")
    
    print("Mock ingestion step completed successfully!")

if __name__ == "__main__":
    main()
