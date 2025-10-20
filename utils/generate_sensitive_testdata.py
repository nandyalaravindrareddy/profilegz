import pandas as pd
import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# === 1️⃣ Create synthetic restricted & SAR-style data ===
records = [
    {
        "customer_id": "CUST001",
        "name": "John Doe",
        "email": "john.doe@example.com",
        "phone": "+1-202-555-0198",
        "address": "742 Evergreen Terrace, Springfield, USA",
        "ssn": "541-29-4433",
        "pan": "ABCDE1234F",
        "aadhaar": "5678 1234 9876",
        "passport": "M1234567",
        "credit_card": "4111 1111 1111 1111",
        "iban": "GB29NWBK60161331926819",
        "account_number": "123456789012",
        "dob": "1985-07-12",
        "country": "USA",
        "balance": 12540.75,
        "sar_narrative": "Customer made multiple cash deposits just under $10,000 in different branches within a week. Possible structuring pattern."
    },
    {
        "customer_id": "CUST002",
        "name": "Priya Singh",
        "email": "priya.singh@gmail.com",
        "phone": "+91-9876543210",
        "address": "12 MG Road, Bengaluru, India",
        "ssn": "",
        "pan": "AQJPK1234L",
        "aadhaar": "1234 5678 9123",
        "passport": "Z1234567",
        "credit_card": "5500 0000 0000 0004",
        "iban": "",
        "account_number": "765432100987",
        "dob": "1990-02-23",
        "country": "India",
        "balance": 9860.25,
        "sar_narrative": "Frequent international remittances to a high-risk jurisdiction with no clear business purpose."
    },
    {
        "customer_id": "CUST003",
        "name": "Sophia Chen",
        "email": "sophia.chen@aliyun.cn",
        "phone": "+86-139-8888-1234",
        "address": "88 Xuhui District, Shanghai, China",
        "ssn": "",
        "pan": "",
        "aadhaar": "",
        "passport": "E9876543",
        "credit_card": "3400 0000 0000 009",
        "iban": "DE89370400440532013000",
        "account_number": "998877665544",
        "dob": "1992-03-16",
        "country": "China",
        "balance": 18760.00,
        "sar_narrative": "Customer linked to shell companies receiving large incoming wires flagged in previous SARs."
    },
    {
        "customer_id": "CUST004",
        "name": "Liam O’Connor",
        "email": "liam.oconnor@outlook.ie",
        "phone": "+353-89-234-5678",
        "address": "45 Temple Bar, Dublin, Ireland",
        "ssn": "",
        "pan": "",
        "aadhaar": "",
        "passport": "P7654321",
        "credit_card": "6011 0009 9013 9424",
        "iban": "IE29AIBK93115212345678",
        "account_number": "112233445566",
        "dob": "1987-11-05",
        "country": "Ireland",
        "balance": 20990.10,
        "sar_narrative": "Offshore transfers to multiple beneficiaries with shared addresses — possible layering activity."
    }
]

df = pd.DataFrame(records)

# === 2️⃣ Write CSV ===
df.to_csv("sensitive_customers.csv", index=False)
print("✅ CSV file generated: sensitive_customers.csv")

# === 3️⃣ Write Parquet ===
table = pa.Table.from_pandas(df)
pq.write_table(table, "sensitive_customers.parquet")
print("✅ Parquet file generated: sensitive_customers.parquet")

# === 4️⃣ Write Avro ===
schema = {
    "type": "record",
    "name": "SensitiveCustomer",
    "fields": [{"name": col, "type": "string"} for col in df.columns]
}

# Convert all non-string types to string for Avro compatibility
records_str = [{k: str(v) for k, v in rec.items()} for rec in records]

with open("sensitive_customers.avro", "wb") as out:
    fastavro.writer(out, schema, records_str)
print("✅ Avro file generated: sensitive_customers.avro")
