import json
import os
import boto3
from datetime import datetime
from collections import defaultdict

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])


def handler(event, context):
    """
    Lambda function to fetch performance metrics from DynamoDB
    and return structured data for frontend charting.
    """

    try:
        # Query all items from DynamoDB
        response = table.scan()
        items = response.get("Items", [])

        # Process metrics
        result = process_performance_metrics(items)

        return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


def process_performance_metrics(items):
    """
    Computes:
    - Satisfaction Rate over time
    - Query Resolution Rate over time
    - Average Response Time over time
    """

    performance_data = defaultdict(
        lambda: {
            "satisfaction_rate": 0,
            "resolution_rate": 0,
            "average_response_time": 0,
            "total_queries": 0,
        }
    )

    for item in items:
        date_key = item.get("timestamp", "")[:10]  # Extract YYYY-MM-DD
        if not date_key:
            continue

        # Satisfaction Rate
        satisfaction = int(item.get("satisfaction", 0))
        is_satisfied = 1 if satisfaction >= 4 else 0

        # Resolution Rate
        is_resolved = 1 if item.get("resolved", "false") == "true" else 0

        # Response Time Calculation
        start_time = item.get("query_timestamp")
        end_time = item.get("response_timestamp")

        response_time = None
        if start_time and end_time:
            try:
                start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
                end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")
                response_time = (end_dt - start_dt).total_seconds()
            except ValueError:
                pass

        # Update Daily Metrics
        performance_data[date_key]["total_queries"] += 1
        performance_data[date_key]["satisfaction_rate"] += is_satisfied
        performance_data[date_key]["resolution_rate"] += is_resolved
        if response_time is not None:
            performance_data[date_key]["average_response_time"] += response_time

    # Calculate final percentages and average times
    chart_data = []
    for date, data in sorted(performance_data.items()):
        total_queries = data["total_queries"]
        chart_data.append(
            {
                "date": date,
                "satisfaction_rate": (
                    round((data["satisfaction_rate"] / total_queries) * 100, 2)
                    if total_queries
                    else 0
                ),
                "resolution_rate": (
                    round((data["resolution_rate"] / total_queries) * 100, 2)
                    if total_queries
                    else 0
                ),
                "average_response_time": (
                    round(data["average_response_time"] / total_queries, 2)
                    if total_queries
                    else 0
                ),
            }
        )

    return {"chart_data": chart_data}
