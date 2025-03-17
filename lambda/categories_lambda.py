import json
import os
import boto3
from datetime import datetime, timedelta

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])


def handler(event, context):
    """
    Lambda function to fetch category metrics and trending topics.
    Supports filtering by custom date range or predefined last 7/14 days.
    """

    try:
        # Extract query parameters from API Gateway request
        query_params = event.get("queryStringParameters", {}) or {}
        start_date = query_params.get("start_date")  # e.g., "2024-03-01"
        end_date = query_params.get("end_date")  # e.g., "2024-03-10"
        time_range = query_params.get("range", "7d")  # Default to last 7 days

        # Determine the date range
        now = datetime.now()
        recent_start, recent_end = get_time_range(start_date, end_date, time_range)
        previous_start = recent_start - timedelta(days=7)  # Previous 7-day period

        # Fetch data from DynamoDB
        recent_items = get_items_between(recent_start, recent_end)
        previous_items = get_items_between(previous_start, recent_start)

        # Compute category metrics
        recent_category_counts = count_categories(recent_items)
        previous_category_counts = count_categories(previous_items)

        # Compute percentage distribution
        category_distribution = compute_percentage_distribution(recent_category_counts)

        # Calculate trending categories
        trending_topics = calculate_trending(
            recent_category_counts, previous_category_counts
        )

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,GET",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
            },
            "body": json.dumps(
                {
                    "category_distribution": category_distribution,
                    "top_5_categories": sorted(
                        category_distribution, key=lambda x: x["count"], reverse=True
                    )[:5],
                    "trending_topics": trending_topics,
                }
            ),
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
            start_time = datetime.strptime(start_date, "%Y-%m-%d")
            end_time = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(
                days=1
            )  # Include full last day
        else:
            end_time = datetime.now()
            if time_range == "7d":
                start_time = end_time - timedelta(days=7)
            elif time_range == "14d":
                start_time = end_time - timedelta(days=14)
            elif time_range == "30d":
                start_time = end_time - timedelta(days=30)
            elif time_range == "3m":
                start_time = end_time - timedelta(days=90)  # 3 months
            else:
                start_time = end_time - timedelta(days=7)  # Default to last 7 days

        return start_time, end_time

    except ValueError:
        return datetime.now() - timedelta(days=7), datetime.now()


def get_items_between(start_time, end_time):
    """Fetch items from DynamoDB within a time range."""
    response = table.scan(
        FilterExpression="#ts BETWEEN :start AND :end",
        ExpressionAttributeNames={"#ts": "timestamp"},
        ExpressionAttributeValues={
            ":start": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            ":end": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )
    return response.get("Items", [])


def count_categories(items):
    """Count occurrences of each category."""
    counts = {}
    for item in items:
        category = item.get("category", "unknown")
        counts[category] = counts.get(category, 0) + 1
    return counts


def compute_percentage_distribution(category_counts):
    """Compute percentage distribution for each category."""
    total = sum(category_counts.values())
    if total == 0:
        return []

    return [
        {
            "category": category,
            "count": count,
            "percentage": round((count / total) * 100, 2),
        }
        for category, count in category_counts.items()
    ]


def calculate_trending(recent_counts, previous_counts):
    """Calculate category growth percentage."""
    trending = []
    for category, recent_count in recent_counts.items():
        previous_count = previous_counts.get(category, 0)
        if previous_count > 0:
            growth = ((recent_count - previous_count) / previous_count) * 100
        else:
            growth = float("inf")  # New category
        trending.append({"category": category, "growth": growth})

    # Sort by highest growth
    trending.sort(key=lambda x: x["growth"], reverse=True)
    return trending
