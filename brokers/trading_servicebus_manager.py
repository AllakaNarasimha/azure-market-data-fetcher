import os
import json
import logging
import threading
from azure.servicebus import ServiceBusClient, ServiceBusMessage

from utils.blob_utils import BlobUtils

logger = logging.getLogger(__name__)


class TradingTopics:
    RAW_CANDLE = "raw-candle"
    INDICATOR_DATA = "indicator-data"
    ORDER_REQUEST = "order-request"
    ORDER_STATUS = "order-status"
    OPTION_CHAIN = "option-chain"
    
class TradingServiceBusManager:
    _client = None
    _lock = threading.Lock()

    @classmethod
    def publish_message(cls, topic_name: str, payload: dict):
        # 1. Initialize client inline if it doesn't exist yet
        if cls._client is None:
            with cls._lock:
                if cls._client is None:
                    connection_string = os.environ.get("AZURE_SERVICE_BUS")
                    if not connection_string:
                        raise ValueError("AZURE_SERVICE_BUS environment variable is not set.")
                    cls._client = ServiceBusClient.from_connection_string(connection_string)

        # 2. Publish the message
        try:
            with cls._client.get_topic_sender(topic_name=topic_name) as sender:
                sender.send_messages(ServiceBusMessage(json.dumps(payload)))
                logger.info(
                    "Published message to topic: %s (ENVIRONMENT=%r, WEBSITE_SLOT_NAME=%r)",
                    topic_name, BlobUtils.environment(), BlobUtils.website_slot_name(),
                )
        except Exception as e:
            logger.error(
                "Error publishing to %s (ENVIRONMENT=%r, WEBSITE_SLOT_NAME=%r): %s",
                topic_name, BlobUtils.environment(), BlobUtils.website_slot_name(), str(e),
            )
            raise e