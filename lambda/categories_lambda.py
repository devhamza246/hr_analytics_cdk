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
    """

    try:
        # Set time ranges
        now = datetime.now()
        recent_start = now - timedelta(days=7)  # Last 7 days
        previous_start = now - timedelta(days=14)  # 7–14 days ago

        # Fetch data from DynamoDB
        recent_items = get_items_between(recent_start, now)
        previous_items = get_items_between(previous_start, recent_start)

        # Compute category metrics
        recent_category_counts = count_categories(recent_items)
        previous_category_counts = count_categories(previous_items)

        # Calculate trending categories
        trending_topics = calculate_trending(
            recent_category_counts, previous_category_counts
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "category_distribution": recent_category_counts,
                    "top_5_categories": sorted(
                        recent_category_counts.items(), key=lambda x: x[1], reverse=True
                    )[:5],
                    "trending_topics": trending_topics,
                }
            ),
        }

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


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
