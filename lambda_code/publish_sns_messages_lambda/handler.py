import json
import os
import random
import string

import boto3

SNS_NUM_MESSAGES_PER_MINUTE = json.loads(os.environ["SNS_NUM_MESSAGES_PER_MINUTE"])
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
client = boto3.client("sns")

SECTORS = [
    "Agriculture",
    "Chemical",
    "Construction",
    "Education",
    "Financial services",
    "Professional services",
    "Food, drink, tobacco",
    "Forestry",
    "Health services",
    "Tourism",
    "Mining",
    "Media",
    "Oil and gas",
    "Telecommunications",
    "Shipping",
    "Textile",
    "Transportation",
    "Utilities",
]


def lambda_handler(event, context) -> None:
    messages = [
        {
            "TICKER_SYMBOL": "".join(
                random.choice(string.ascii_uppercase) for _ in range(4)
            ),
            "SECTOR": random.choice(SECTORS),
            "CHANGE": round((random.random() - 0.5) * 10, 2),
            "PRICE": random.randrange(-100, 100),
        }
        for _ in range(SNS_NUM_MESSAGES_PER_MINUTE)
    ]
    failed_messages = []
    for mini_batch in [
        messages[idx : idx + 10] for idx in range(0, len(messages), 10)
    ]:  # publish_batch() supports max of 10 elements
        response = client.publish_batch(
            TopicArn=SNS_TOPIC_ARN,
            PublishBatchRequestEntries=[
                {"Id": str(i), "Message": json.dumps(message)}
                for i, message in enumerate(mini_batch)
            ],
        )
        failed = response["Failed"]
        failed_messages.extend(failed)
        if failed_messages:
            print(failed)
    if failed_messages:
        raise Exception(failed_messages)
