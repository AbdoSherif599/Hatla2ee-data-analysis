# csv_to_mongodb.py

import pandas as pd
from pymongo import MongoClient

# -----------------------------
# CONFIGURATION
# -----------------------------
CSV_FILE = "cars.csv"          # Path to your CSV file
MONGO_URI = "mongodb://localhost:27017/"  # MongoDB connection URI
DB_NAME = "mydatabase"         # Database name
COLLECTION_NAME = "mycollection"  # Collection name

# -----------------------------
# STEP 1: READ CSV
# -----------------------------
try:
    df = pd.read_csv(CSV_FILE)
    print(f"CSV loaded successfully. {len(df)} records found.")
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit(1)

# -----------------------------
# STEP 2: CONNECT TO MONGODB
# -----------------------------
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print("Connected to MongoDB successfully.")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    exit(1)

# -----------------------------
# STEP 3: INSERT DATA INTO MONGODB
# -----------------------------
try:
    # Convert DataFrame to dictionary format for MongoDB
    data_dict = df.to_dict(orient="records")
    
    if data_dict:
        collection.insert_many(data_dict)
        print(f"{len(data_dict)} records inserted into '{COLLECTION_NAME}' collection.")
    else:
        print("No data to insert.")
except Exception as e:
    print(f"Error inserting data into MongoDB: {e}")
    exit(1)
