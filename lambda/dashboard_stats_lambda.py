import json
import os
import boto3
from datetime import datetime, timedelta

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])


def handler(event, context):
    """
    Lambda function to return key analytics:
    - Active Users (last 7 days)
    - Total Queries
    - Average Satisfaction Score (as a percentage)
    - Average Response Time
    """
    try:
        # Define the start date for active users (last 7 days)
        now = datetime.now()
        start_date = now - timedelta(days=7)

        # Fetch all items from DynamoDB
        response = table.scan()
        items = response.get("Items", [])

        # Process metrics
        result = compute_analytics(items, start_date)

        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


def compute_analytics(items, start_date):
    """
    Computes:
    - Active users in the last 7 days
    - Total queries
    - Average satisfaction score (as a percentage)
    - Average response time
    """

    total_queries = len(items)
    unique_users = set()
    total_satisfaction = 0
    total_response_time = 0
    response_time_count = 0
    valid_satisfaction_count = 0

    for item in items:
        # Active Users (users who queried in the last 7 days)
        timestamp = item.get("timestamp")
        user_id = item.get("user_id")

        if user_id and timestamp:
            try:
                unique_users.add(user_id)
            except ValueError:
                pass  # Skip invalid timestamps

        # Satisfaction Score (only count valid ones)
        satisfaction = int(item.get("satisfaction", 0))
        if 1 <= satisfaction <= 5:  # Assuming 5 is the max rating
            total_satisfaction += satisfaction
            valid_satisfaction_count += 1

        # Response Time Calculation
        query_time = item.get("query_timestamp")
        response_time = item.get("response_timestamp")

        if query_time and response_time:
            try:
                start_dt = datetime.strptime(query_time, "%Y-%m-%dT%H:%M:%SZ")
                end_dt = datetime.strptime(response_time, "%Y-%m-%dT%H:%M:%SZ")
                total_response_time += (end_dt - start_dt).total_seconds()
                response_time_count += 1
            except ValueError:
                pass  # Skip invalid timestamps

    # Calculate Average Satisfaction Percentage
    avg_satisfaction_percentage = (
        round((total_satisfaction / (valid_satisfaction_count * 5)) * 100, 2)
        if valid_satisfaction_count
        else 0
    )

    return {
        "active_users": len(unique_users),
        "total_queries": total_queries,
        "average_satisfaction": avg_satisfaction_percentage,  # Now in percentage
        "average_response_time": (
            round(total_response_time / response_time_count, 2)
            if response_time_count
            else 0
        ),
    }
