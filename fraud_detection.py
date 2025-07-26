import pandas as pd
import mysql.connector
import os

# Step 1: Connect to MySQL
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Srujan@143",
        database="bankdb"
    )
    cursor = conn.cursor()
    print("✅ Connected to MySQL successfully.")
except mysql.connector.Error as err:
    print(f"❌ Error: {err}")
    exit()

# Step 2: Load CSVs (make sure these are in the same folder as this script)
try:
    customers = pd.read_csv('customers.csv')
    accounts = pd.read_csv('accounts.csv')
    transactions = pd.read_csv('transactions.csv')
    print("✅ CSV files loaded successfully.")
except FileNotFoundError as e:
    print(f"❌ File not found: {e}")
    exit()

# Step 3: Upload Data to MySQL
def upload_to_mysql(df, table_name):
    for _, row in df.iterrows():
        placeholders = ','.join(['%s'] * len(row))
        sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        try:
            cursor.execute(sql, tuple(row))
        except mysql.connector.Error as e:
            print(f"⚠️ Error inserting into {table_name}: {e}")
    conn.commit()

print("⏳ Uploading data to MySQL...")
upload_to_mysql(customers, "customers")
upload_to_mysql(accounts, "accounts")
upload_to_mysql(transactions, "transactions")
print("✅ Data uploaded successfully.")

# Step 4: Fraud Detection Logic
# Rule: Flag debit transactions > 40,000 as suspicious
fraud = transactions[
    (transactions['transaction_type'].str.lower() == 'debit') &
    (transactions['amount'] > 40000)
]

# Step 5: Save fraud alerts
fraud_file = os.path.join(os.getcwd(), 'fraud_alerts.csv')
fraud.to_csv(fraud_file, index=False)
print(f"✅ Fraud transactions saved to: {fraud_file}")
print("\n📌 Detected Fraud Transactions:\n", fraud)

# Step 6: Close the connection
conn.close()
print("🔒 MySQL connection closed.")

