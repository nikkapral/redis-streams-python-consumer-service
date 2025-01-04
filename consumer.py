import argparse
import asyncio
import logging
import signal
import uuid

from redis import ResponseError
from redis.asyncio import Redis

# Logging settings
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class ConsumerService:
    """
    Service for Redis Stream processing
    """

    def __init__(
        self,
        redis_client: Redis,
        stream_name: str,
        consumer_group_name: str,
    ):
        """
        Initialization of service

        :param redis_client: Redis client instance
        :param stream_name: Name of the Redis stream to be connected to
        :param consumer_group_name: Name of the consumer group
        """
        self.redis_client = redis_client
        self.stream_name = stream_name
        self.consumer_group_name = consumer_group_name
        self.consumer_name = f"{self.__class__.__name__}_{uuid.uuid4()}"

        self.__init_log()

    def __init_log(self) -> None:
        """
        Logs service initialization
        """
        logger.info(f"consumer {self.consumer_name} initialized")

    async def create_consumer_group(self) -> None:
        """
        Creates consumer group if it doesn`t exist
        """
        try:
            await self.redis_client.xgroup_create(
                name=self.stream_name,
                groupname=self.consumer_group_name,
                id="$",  # Only new messages
                mkstream=True,  # Creates stream if it doesn`t exist
            )
            logger.info(f"Consumer group {self.consumer_group_name} created")
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(
                    f"Consumer group {self.consumer_group_name} "
                    "already exists. Continue..."
                )
            else:
                logger.info(
                    f"Error initializing consumer group {self.consumer_group_name}"
                )
                raise

    async def listen(self, stop_event: asyncio.Event) -> None:
        """
        Creates infinite cycle listening to Redis entries
        """
        await self.create_consumer_group()
        while not stop_event.is_set():
            try:
                messages = await self.redis_client.xreadgroup(
                    groupname=self.consumer_group_name,
                    consumername=self.consumer_name,
                    streams={self.stream_name: ">"},
                    count=5,
                    block=5000,
                )
                if messages:
                    for stream, message in messages:
                        for id, message_body in message:
                            logger.info(
                                f"Message accepted"
                                "-> "
                                f"Message id: {id}"
                                f"Message stream {stream}"
                                f"Message: {message}"
                            )
                            await self.redis_client.xack(
                                self.stream_name,
                                self.consumer_group_name,
                                id,
                            )
                            logger.info(f"Message {id} acked")

            except Exception as e:
                logger.error(f"Exception occured: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def run(self):
        """
        Opens event-message listener
        """
        stop_event = asyncio.Event()

        def _signal_handler():
            logger.info("Recieved stop signal")
            stop_event.set()

        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, _signal_handler)
            loop.add_signal_handler(signal.SIGTERM, _signal_handler)
        except NotImplementedError:
            logger.warning("Signal handlers are not implemented on this platform")

        await self.listen(stop_event)

    async def close(self):
        """
        Closes redis stream connection
        """
        await self.redis_client.aclose()
        logger.info("Redis client connection closed")


async def main(host: str, port: int, stream_name: str, consumer_group_name: str):
    """
    Main async func to start a consumer
    """

    # Creating redis client with decoded responses
    redis_client: Redis = Redis(
        host=host,
        port=port,
        decode_responses=True,
    )

    # Creating UserStorageService
    service: ConsumerService = ConsumerService(
        redis_client=redis_client,
        stream_name=stream_name,
        consumer_group_name=consumer_group_name,
    )

    try:
        await service.run()
    finally:
        await service.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redis Stream service parser")

    parser.add_argument(
        "--host", type=str, default="localhost", help="Redis host (default: localhost)"
    )
    parser.add_argument(
        "--port", type=int, default=6379, help="Redis port (default: 6379)"
    )
    parser.add_argument(
        "--stream_name",
        type=str,
        default="user_stream",
        help="Redis Stream name (default: user_stream)",
    )
    parser.add_argument(
        "--group_name",
        type=str,
        default="user_group",
        help="Redis consumer group name (default: user_group)",
    )

    args = parser.parse_args()

    asyncio.run(
        main(
            host=args.host,
            port=args.port,
            stream_name=args.stream_name,
            consumer_group_name=args.group_name,
        )
    )
