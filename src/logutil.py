"""Central logging configuration driven by LOG_LEVEL."""

import logging
import os


def setup_logging(default_level: str = "INFO") -> None:
    """Configure root logging once; respect LOG_LEVEL environment variable."""
    level_name = os.getenv("LOG_LEVEL", default_level).upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    if root.handlers:
        root.setLevel(level)
        for handler in root.handlers:
            handler.setLevel(level)
        return
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
