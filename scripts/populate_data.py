import boto3
from datetime import datetime

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("QueryAnalytics")

# Sample Data
sample_data = [
    {
        "user_id": "user123",
        "timestamp": datetime.utcnow().isoformat(),
        "category": "benefits",
        "satisfaction": 4,
        "resolved": "true",
        "query_timestamp": "2025-03-15T09:55:00Z",
        "response_timestamp": "2025-03-15T09:59:00Z",
        "department": "HR",
        "seniority": "mid",
        "new_user": "false",
    },
    {
        "user_id": "user456",
        "timestamp": datetime.utcnow().isoformat(),
        "category": "policies",
        "satisfaction": 5,
        "resolved": "false",
        "query_timestamp": "2025-03-15T10:05:00Z",
        "response_timestamp": "2025-03-15T10:09:00Z",
        "department": "IT",
        "seniority": "senior",
        "new_user": "true",
    },
]

# Insert Data
for item in sample_data:
    table.put_item(Item=item)

print("✅ Sample data inserted successfully!")
