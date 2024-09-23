"""Lag tests 2024-09-23"""
import pytest
from unittest.mock import patch,MagicMock
from src.lag_monitor import get_consumer_lag
def test_lag_returns_dict():
    with patch("src.lag_monitor.AdminClient") as ma, patch("src.lag_monitor.Consumer") as mc:
        ma.return_value.list_topics.return_value.topics={"test":MagicMock(partitions={0:None})}
        mc.return_value.committed.return_value=[MagicMock(offset=10)]
        mc.return_value.get_watermark_offsets.return_value=(0,15)
        result=get_consumer_lag("localhost:9092","grp",["test"])
        assert isinstance(result,dict)
