import logging


logger = logging.getLogger(__name__)
handler = logging.StreamHandler()

formatter = logging.Formatter("%(asctime)s - [%(levelname)s] - %(name)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.setLevel(logging.INFO)
