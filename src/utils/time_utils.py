import time
from datetime import datetime

from src.utils.logger_utils import get_logger
from src.constants.time_constants import TimeConstants

logger = get_logger('Time utils')


def round_timestamp(timestamp, round_time=86400):
    timestamp = int(timestamp)
    timestamp_unit_day = timestamp / round_time
    recover_to_unit_second = int(timestamp_unit_day) * round_time
    return recover_to_unit_second


def convert_timestamp(timestamp, offset):
    timestamp = timestamp + offset * 3600
    timestamp = timestamp % 86400
    timestamp = timestamp // 3600

    return timestamp


def map_epoch_time_to_day(epoch_time):
    date = datetime.utcfromtimestamp(epoch_time)

    day_index = date.weekday()

    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return days[day_index]
