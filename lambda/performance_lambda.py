import json
import os
import boto3
from datetime import datetime

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])

def handler(event, context):
    """
    Lambda function to fetch performance metrics from DynamoDB.
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
    - Satisfaction Rate
    - Query Resolution Rate
    - Average Response Time
    """

    total_queries = len(items)
    satisfied_count = 0
    resolution_count = 0
    total_response_time = 0
    response_time_count = 0  # To track valid response times

    for item in items:
        # Satisfaction Rate (ratings 4 & 5)
        if item.get("satisfaction", 0) >= 4:
            satisfied_count += 1

        # Resolution Rate (queries resolved without human intervention)
        if item.get("resolved", False):
            resolution_count += 1

        # Response Time Calculation
        start_time = item.get("query_timestamp")
        end_time = item.get("response_timestamp")

        if start_time and end_time:
            try:
                start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
                end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")

                response_time = (end_dt - start_dt).total_seconds()
                total_response_time += response_time
                response_time_count += 1

            except ValueError:
                pass  # Skip if timestamps are incorrect

    return {
        "total_queries": total_queries,
        "satisfaction_rate": round((satisfied_count / total_queries) * 100, 2) if total_queries else 0,
        "resolution_rate": round((resolution_count / total_queries) * 100, 2) if total_queries else 0,
        "average_response_time": round(total_response_time / response_time_count, 2) if response_time_count else 0
    }
