import json
import os
import boto3
from datetime import datetime, timedelta
from collections import defaultdict

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])


def handler(event, context):
    """
    Lambda function to fetch performance metrics from DynamoDB
    and return structured data for frontend charting with optional filtering.
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
        result = process_performance_metrics(items)

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
