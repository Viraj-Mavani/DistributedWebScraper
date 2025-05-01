import logging


def setup(verbose: bool = False) -> logging.Logger:
    """
    Configure and return a logger.

    :param verbose: If True, set level to DEBUG; otherwise INFO.
    :return: Configured root logger.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=level
    )
    return logging.getLogger()
