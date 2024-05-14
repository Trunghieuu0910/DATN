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


def timestamps_in_round(t1, t2, round_time=86400):
    return int(t1 / round_time) == int(t2 / round_time)


def round_timestamp_for_log(value_logs, round_time=86400):
    """
    input a value_logs
        value_logs: {
            timestamp: value
        }
    return a logs with rounded-up timestamp
    """
    return_logs = dict()
    now = int(time.time())
    for timestamp in value_logs:
        rounded_timestamp = round_timestamp(timestamp, round_time=round_time)
        if rounded_timestamp < int(timestamp):
            rounded_timestamp += round_time  # rounding up
        if rounded_timestamp <= now:
            return_logs[rounded_timestamp] = value_logs[timestamp]
    return return_logs


def get_timestamps_intersection(timestamps_lists, start_time=None, frequency=TimeConstants.MINUTES_15):
    """ input a timestamps_lists:
    [
        [timestamp_1,...,timestamp_n],
        [timestamps_1,..., timestamp_m].
        ...
        [timestamps_1,..., timestamp_p]
    ]
    return a unifying list of timestamps within now - duration
    """
    first_timestamps = list()
    last_timestamps = list()
    all_timestamps = list()
    # sort if necessary
    for index, timestamps in enumerate(timestamps_lists):
        timestamps_lists[index].sort()
    # get the start_timestamp and end_timestamp
    for timestamps in timestamps_lists:
        first_timestamps.append(int(timestamps[0]))
        last_timestamps.append(int(timestamps[-1]))
    start_timestamp = max(first_timestamps)
    end_timestamp = min(last_timestamps)
    # check if the result timetamps is greater than start_time
    if start_time is not None:
        if start_timestamp < start_time:
            start_timestamp = round_timestamp(start_time, round_time=frequency)
            if start_timestamp < start_time:  # rounding up
                start_timestamp += frequency
    if start_timestamp > end_timestamp:
        logger.error(f"Start timestamp: {start_timestamp} is greater than End timestamp: {end_timestamp}")
        return None
    # create the unified list of all_timestamps:
    while start_timestamp <= end_timestamp:
        all_timestamps.append(start_timestamp)
        start_timestamp += frequency
    return all_timestamps


def pretty_date(t):
    """
    Get a datetime object or a int() Epoch timestamp and return a
    pretty string like 'an hour ago', 'Yesterday', '3 months ago',
    'just now', etc
    """

    now = datetime.now()
    if type(t) is int:
        diff = now - datetime.fromtimestamp(t)
    elif isinstance(t, datetime):
        diff = now - t
    else:
        diff = 0

    second_diff = diff.seconds
    day_diff = diff.days

    if day_diff < 0:
        return ''

    if day_diff == 0:
        if second_diff < 10:
            return "just now"
        if second_diff < 60:
            return str(second_diff) + " seconds ago"
        if second_diff < 120:
            return "a minute ago"
        if second_diff < 3600:
            return str(second_diff // 60) + " minutes ago"
        if second_diff < 7200:
            return "an hour ago"
        if second_diff < 86400:
            return str(second_diff // 3600) + " hours ago"

    if day_diff == 1:
        return "yesterday"
    if day_diff < 7:
        return str(day_diff) + " days ago"
    if day_diff < 31:
        return str(day_diff // 7) + " weeks ago"
    if day_diff < 365:
        return str(day_diff // 30) + " months ago"
    return str(day_diff // 365) + " years ago"


def pretty_time(duration):
    second_diff = int(duration)
    day_diff = int(duration / 86400)

    if day_diff < 0:
        return ''

    if day_diff == 0:
        if second_diff < 10:
            return "few seconds"
        if second_diff < 60:
            return str(second_diff) + " seconds"
        if second_diff < 120:
            return "minute"
        if second_diff < 3600:
            return str(second_diff // 60) + " minutes"
        if second_diff < 7200:
            return "hour"
        if second_diff < 86400:
            return str(second_diff // 3600) + " hours"

    if day_diff == 1:
        return "24 hours"
    if day_diff < 7:
        return str(day_diff) + " days"
    if day_diff < 31:
        return str(day_diff // 7) + " weeks"
    if day_diff < 365:
        return str(day_diff // 30) + " months"
    return str(day_diff // 365) + " years"


def human_readable_time(timestamp):
    t = datetime.fromtimestamp(timestamp)
    return t.strftime('%m-%d-%Y %H:%M:%S %Z')


def convert_timestamp(timstamp, offset):
    timstamp = timstamp + offset * 3600
    timstamp = timstamp % 86400
    timstamp = timstamp // 3600

    return timstamp


def map_epoch_time_to_day(epoch_time):
    date = datetime.utcfromtimestamp(epoch_time)

    day_index = date.weekday()

    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return days[day_index]
