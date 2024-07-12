import logging


def init_logger(name: str, level: int = logging.INFO):
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s - [%(levelname)s] - %(name)s - %(funcName)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.setLevel(level)

    return logger
