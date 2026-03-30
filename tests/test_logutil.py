"""Logging helper respects LOG_LEVEL."""

import logging
import os

import logutil


def test_setup_logging_respects_log_level(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "ERROR")
    logutil.setup_logging(default_level="INFO")
    root = logging.getLogger()
    assert root.level == logging.ERROR
