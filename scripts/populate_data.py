import boto3
import random
from datetime import datetime, timedelta

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("QueryAnalytics")

# Categories & Departments
CATEGORIES = ["benefits", "policies", "IT", "recruitment", "payroll"]
DEPARTMENTS = ["HR", "IT", "Finance", "Admin", "Legal"]
SENIORITY = ["junior", "mid", "senior"]

# Start date: Jan 1st, 2025
start_date = datetime(2025, 1, 1)
end_date = datetime.now()

# Generate Random Data
num_records = 500  # Adjust as needed
sample_data = []

for _ in range(num_records):
    query_time = start_date + timedelta(
        seconds=random.randint(0, int((end_date - start_date).total_seconds()))
    )
    response_time = query_time + timedelta(
        minutes=random.randint(1, 10)
    )  # 1-10 mins later

    item = {
        "user_id": f"user{random.randint(1000, 9999)}",
        "timestamp": query_time.isoformat(),
        "category": random.choice(CATEGORIES),
        "satisfaction": random.randint(1, 5),
        "resolved": random.choice(["true", "false"]),
        "query_timestamp": query_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "response_timestamp": response_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "department": random.choice(DEPARTMENTS),
        "seniority": random.choice(SENIORITY),
        "new_user": random.choice(["true", "false"]),
    }
    sample_data.append(item)

# Insert Data Using Batch Write
with table.batch_writer() as batch:
    for item in sample_data:
        batch.put_item(Item=item)

print(f"✅ {num_records} random records inserted successfully!")
