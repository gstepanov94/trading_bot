import logging
from logging.handlers import RotatingFileHandler
import os


def setup_logger(name: str) -> logging.Logger:
    FORMAT = "[%(asctime)s | %(name)s %(module)s:%(lineno)s] | %(message)s"
    TIME_FORMAT = "%d.%m.%Y %I:%M:%S %p"
    if not os.path.exists("logs"):
        os.makedirs("logs")
    logging.basicConfig(
        handlers=[
            RotatingFileHandler(
                "logs/trading_log.log", maxBytes=10000000, backupCount=5
            )
        ],
        format=FORMAT,
        datefmt=TIME_FORMAT,
        level=logging.DEBUG,
    )
    logging.getLogger("pydantic").setLevel(logging.ERROR)
    logging.getLogger("websockets").setLevel(logging.ERROR)
    logging.getLogger("asyncio").setLevel(logging.ERROR)
    logger = logging.getLogger(name)
    return logger
