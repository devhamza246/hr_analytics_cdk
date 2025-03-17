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
    Supports filtering by time range.
    """

    try:
        # Get query parameters
        query_params = event.get("queryStringParameters", {}) or {}
        date_range = query_params.get("range", "7d")

        # Convert range to actual dates
        start_date = get_start_date(date_range)

        # Scan DynamoDB and filter by timestamp
        response = table.scan()
        items = response.get("Items", [])

        # Filter data based on the selected time range
        filtered_items = filter_by_date_range(items, start_date)

        # Process and return results
        result = process_demographic_metrics(filtered_items)

        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


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
