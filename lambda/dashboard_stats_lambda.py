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
    - Active Users (last 7 days or custom range)
    - Total Queries
    - Average Satisfaction Score (as a percentage)
    - Average Response Time
    """

    try:
        # Extract query parameters from API Gateway request
        query_params = event.get("queryStringParameters", {}) or {}
        start_date = query_params.get("start_date")  # e.g., "2024-03-01"
        end_date = query_params.get("end_date")  # e.g., "2024-03-10"
        time_range = query_params.get("range", "7d")  # Default to last 7 days

        # Determine the date range
        start_time, end_time = get_time_range(start_date, end_date, time_range)
        start_timestamp = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_timestamp = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Query DynamoDB for items in the specified range
        response = table.scan(
            FilterExpression="#ts BETWEEN :start AND :end",
            ExpressionAttributeNames={"#ts": "timestamp"},  # Alias for reserved keyword
            ExpressionAttributeValues={
                ":start": start_timestamp,
                ":end": end_timestamp,
            },
        )

        items = response.get("Items", [])
        result = compute_analytics(items)

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


def compute_analytics(items):
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
