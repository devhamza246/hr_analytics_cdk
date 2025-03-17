import json
import os
import boto3
from datetime import datetime, timedelta

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])


def handler(event, context):
    """
    Lambda function to fetch user demographic analytics from DynamoDB.
    Supports filtering by time range or custom dates.
    """

    try:
        # Extract query parameters from API Gateway request
        query_params = event.get("queryStringParameters", {}) or {}
        start_date = query_params.get("start_date")  # e.g., "2024-03-01"
        end_date = query_params.get("end_date")  # e.g., "2024-03-10"
        time_range = query_params.get("range", "7d")  # Default to last 7 days

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
        result = process_demographic_metrics(items)

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


def get_start_date(range_str):
    """
    Converts range string (e.g., '7d', '30d') into a datetime object.
    """
    try:
        days = int(range_str.replace("d", ""))
        return datetime.now() - timedelta(days=days)
    except ValueError:
        return datetime.now() - timedelta(days=7)  # Default to 7 days


def filter_by_date_range(items, start_date):
    """
    Filters items to only include those within the specified date range.
    """
    filtered_items = []
    for item in items:
        timestamp = item.get("timestamp")  # Example: "2024-03-10T14:30:00Z"
        if timestamp:
            try:
                item_date = datetime.fromisoformat(timestamp.replace("Z", ""))
                if item_date >= start_date:
                    filtered_items.append(item)
            except ValueError:
                pass  # Skip invalid timestamps
    return filtered_items


def process_demographic_metrics(items):
    """
    Computes demographic analytics as percentages:
    - Department usage breakdown
    - Usage by seniority level
    - New vs. returning users
    """

    total_items = len(items)
    if total_items == 0:
        return {
            "department_usage": {},
            "seniority_usage": {},
            "new_vs_returning": {},
        }

    department_usage = {}
    seniority_usage = {"junior": 0, "mid": 0, "senior": 0, "unknown": 0}
    new_vs_returning = {"new": 0, "returning": 0}

    for item in items:
        # Department Usage
        dept = item.get("department", "unknown")
        department_usage[dept] = department_usage.get(dept, 0) + 1

        # Seniority Level Usage
        seniority = item.get("seniority", "unknown").lower()
        if seniority in seniority_usage:
            seniority_usage[seniority] += 1
        else:
            seniority_usage["unknown"] += 1  # Catch all undefined seniority levels

        # New vs. Returning Users
        is_new_user = item.get("new_user", "false").lower() == "true"
        new_vs_returning["new" if is_new_user else "returning"] += 1

    # Convert counts to percentages
    department_usage = {
        k: round((v / total_items) * 100, 2) for k, v in department_usage.items()
    }
    seniority_usage = {
        k: round((v / total_items) * 100, 2) for k, v in seniority_usage.items()
    }
    new_vs_returning = {
        k: round((v / total_items) * 100, 2) for k, v in new_vs_returning.items()
    }

    return {
        "department_usage": department_usage,
        "seniority_usage": seniority_usage,
        "new_vs_returning": new_vs_returning,
    }
