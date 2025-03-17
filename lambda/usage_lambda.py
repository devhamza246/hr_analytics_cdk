import json
import os
import boto3
from datetime import datetime, timedelta

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])


def handler(event, context):
    """
    Lambda function to fetch usage metrics from DynamoDB.
    Supports filtering by custom time range or predefined ranges (7d, 30d).
    """

    try:
        # Extract query parameters from API Gateway request
        query_params = event.get("queryStringParameters", {}) or {}
        start_date = query_params.get("start_date")  # e.g., "2024-03-01"
        end_date = query_params.get("end_date")  # e.g., "2024-03-10"
        time_range = query_params.get(
            "range", "7d"
        )  # Default to last 7 days if no custom range

        # Determine start and end timestamps
        start_time, end_time = get_time_range(start_date, end_date, time_range)
        start_timestamp = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_timestamp = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Query DynamoDB with time filter
        response = table.scan(
            FilterExpression="#ts BETWEEN :start AND :end",
            ExpressionAttributeNames={"#ts": "timestamp"},  # Alias for reserved keyword
            ExpressionAttributeValues={
                ":start": start_timestamp,
                ":end": end_timestamp,
            },
        )

        items = response.get("Items", [])
        result = process_usage_metrics(items)

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,GET",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
            },
            "body": json.dumps(result),
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,GET",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
            },
            "body": json.dumps({"error": str(e)}),
        }


def get_time_range(start_date, end_date, time_range):
    """
    Determines the start and end datetime based on custom dates or predefined ranges.
    """

    try:
        if start_date and end_date:
            # Convert input strings to datetime objects
            start_time = datetime.strptime(start_date, "%Y-%m-%d")
            end_time = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(
                days=1
            )  # Include full last day
        else:
            # Handle predefined ranges
            end_time = datetime.now()
            if time_range == "7d":
                start_time = end_time - timedelta(days=7)
            elif time_range == "30d":
                start_time = end_time - timedelta(days=30)
            elif time_range == "3m":
                start_time = end_time - timedelta(days=90)  # 3 months
            elif time_range == "6m":
                start_time = end_time - timedelta(days=180)  # 6 months
            elif time_range == "9m":
                start_time = end_time - timedelta(days=270)  # 9 months
            else:
                # Default to last 7 days if the range is invalid
                start_time = end_time - timedelta(days=7)

        return start_time, end_time

    except ValueError:
        # Fallback to default 7-day range if date parsing fails
        return datetime.now() - timedelta(days=7), datetime.now()


def process_usage_metrics(items):
    """
    Computes:
    - Daily/weekly active users
    - Query volume over time
    - Average queries per user
    """

    total_queries = len(items)
    user_activity = {}  # Tracks daily active users
    query_volume = {}  # Tracks queries per day
    unique_users = set()

    for item in items:
        user_id = item.get("user_id")
        timestamp = item.get("timestamp")  # e.g., "2024-03-10T14:30:00Z"

        if user_id and timestamp:
            try:
                date_str = timestamp.split("T")[0]  # Extract date part
                unique_users.add(user_id)

                # Count daily active users
                if date_str not in user_activity:
                    user_activity[date_str] = set()
                user_activity[date_str].add(user_id)

                # Count daily query volume
                query_volume[date_str] = query_volume.get(date_str, 0) + 1
            except ValueError:
                pass  # Skip invalid timestamps

    # Convert sets to counts
    daily_active_users = {date: len(users) for date, users in user_activity.items()}
    avg_queries_per_user = total_queries / len(unique_users) if unique_users else 0

    return {
        "total_queries": total_queries,
        "unique_users": len(unique_users),
        "avg_queries_per_user": round(avg_queries_per_user, 2),
        "daily_active_users": daily_active_users,
        "query_volume": query_volume,
    }
