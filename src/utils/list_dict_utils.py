import copy
import math
import time
from bisect import bisect
from typing import Dict, Union

import numpy as np

from src.constants.time_constants import TimeConstants
from src.utils.time_utils import round_timestamp


def coordinate_logs(
        change_logs,
        start_time=0, end_time=None, frequency=None,
        fill_start_value=False, default_start_value=0
):
    if end_time is None:
        end_timestamp = int(time.time())
    else:
        end_timestamp = end_time

    logs = {}
    last_timestamp = 0
    pre_value = default_start_value
    for t, v in change_logs.items():
        if t is None:
            continue

        if t < start_time:
            pre_value = v
        elif start_time <= t <= end_timestamp:
            last_timestamp = _filter_timestamp_in_range(logs, t, v, last_timestamp, frequency)
        elif t > end_timestamp:
            break

    logs = _fill_margin(logs, start_time, end_time, fill_start_value, pre_value, end_timestamp)
    return logs


def _filter_timestamp_in_range(logs: dict, t, v, last_timestamp, frequency):
    if frequency:
        if round_timestamp(t, frequency) != round_timestamp(last_timestamp, frequency):
            logs[t] = v
            last_timestamp = t
    else:
        logs[t] = v

    return last_timestamp


def _fill_margin(logs: dict, start_time, end_time, fill_start_value, pre_value, end_timestamp):
    if (start_time not in logs) and fill_start_value and (pre_value is not None):
        logs[start_time] = pre_value

    logs = sort_log(logs)

    last_value = list(logs.values())[-1] if logs else None
    if (end_time is None) and (last_value is not None):
        logs[end_timestamp] = last_value

    return logs


def combined_logs(*logs, handler_func=sum, default_value=0):
    timestamps = set()
    for log in logs:
        timestamps.update(list(log.keys()))
    timestamps = sorted(timestamps)
    combined = {}
    current_values = [default_value] * len(logs)
    for t in timestamps:
        for idx, log in enumerate(logs):
            current_values[idx] = log.get(t, current_values[idx])

        combined[t] = handler_func(current_values)

    return combined


def get_logs_in_time(change_logs, start_time=0, end_time=None, start_with_zero=False, start=False):
    if end_time is None:
        end_time = int(time.time())

    logs = {}
    pre_v = None
    for t, v in change_logs.items():
        if t is None:
            continue

        if t <= start_time:
            pre_v = v
        elif t <= end_time:
            logs[t] = v

    if start:
        if pre_v is not None:
            logs[start_time] = pre_v
        elif start_with_zero:
            logs[start_time] = 0

    logs = dict(sorted(logs.items(), key=lambda x: x[0]))
    return logs


def get_value_in_logs(logs: dict, timestamp: int = None, default=0):
    if timestamp is None:
        return list(logs.values())[-1] if logs else default

    value = default
    for t, v in logs.items():
        if t > timestamp:
            return value
        value = v
    return value


def expand_dict_recursion(data, keys=None, depth=0):
    if depth > 10:
        return {}

    if keys is None:
        keys = list(data.keys())

    addresses = {}
    for key in keys:
        if type(data[key]) == dict:
            sub_keys = list(data[key].keys())
            sub_addresses = expand_dict_recursion(data, sub_keys, depth=depth + 1)
            t_addresses = set()
            for t, v in sub_addresses.items():
                t_addresses.update(v)

            addresses[key] = list(t_addresses)

        elif type(data[key]) == list:
            addresses[key] = data[key]

    return addresses


def sort_log(log: dict):
    if not log:
        return {}
    sorted_log = {int(t): v for t, v in log.items()}
    sorted_log = dict(sorted(sorted_log.items(), key=lambda x: x[0]))
    return sorted_log


def get_rank(value, ranges: list, ranks: list = None):
    idx = bisect(ranges, value)
    if ranks is not None:
        idx = ranks[idx]
    return idx


def fill_null_logs(logs, frequency=TimeConstants.MINUTES_15, forward=True, original_frequency=None):
    """Fill null for logs. Default is forward fill
    """
    timestamps = sorted(list(logs.keys()))
    end_time = timestamps[0]
    start_time = timestamps[-1]

    if original_frequency is None:
        original_frequency = frequency

    if not forward:
        original_frequency = -original_frequency

    t = end_time
    while t <= start_time:
        nearest_timestamp = t
        while nearest_timestamp not in logs:
            nearest_timestamp -= original_frequency
        logs[t] = logs[nearest_timestamp]
        t += frequency

    return logs


def transpose_dict(d: Dict[Union[str, int], dict]):
    data = {}
    for k, v in d.items():
        for k_, v_ in v.items():
            if k_ not in data:
                data[k_] = {}
            data[k_][k] = v_
    return data


def merge_logs(logs_dict: dict, default_value=None):
    keys = set()
    for logs in logs_dict.values():
        keys.update(logs.keys())
    keys = sorted(list(keys))

    data = {}
    values = {name: default_value for name in logs_dict}
    for key in keys:
        for name, logs in logs_dict.items():
            values[name] = logs.get(key, values[name])
        data[key] = copy.deepcopy(values)

    return data


def get_change_rate(old_value, new_value):
    if old_value == 0:
        if new_value < 0:
            change_rate = -1
        elif new_value > 0:
            change_rate = 1
        else:
            change_rate = 0
    else:
        delta = new_value - old_value
        change_rate = delta / old_value
    return change_rate


def get_percentage_on_top(value, histogram):
    histogram = {int(t): v for t, v in histogram.items()}
    histogram = dict(sorted(histogram.items(), reverse=True))

    top_cnt = 0
    for k, cnt in histogram.items():
        if k > value:
            top_cnt += cnt
        else:
            break
    total = sum(histogram.values())
    top_score = top_cnt + 1
    top_percentage = 100 * top_cnt / total if total else 0
    return top_score, top_percentage


def combined_token_change_logs_func(values):
    amount = 0
    value_in_usd = 0
    for value in values:
        if value is not None:
            amount += value['amount']
            value_in_usd += value['valueInUSD']
    return {'amount': amount, 'price': value_in_usd / amount if amount else 0}


def get_logs_change_rate(change_logs, duration):
    current_time = int(time.time())
    change_logs = coordinate_logs(
        change_logs, start_time=current_time - duration,
        end_time=current_time, fill_start_value=True
    )
    if not change_logs:
        return None

    values = list(change_logs.values())
    return get_change_rate(values[0], values[-1])


def get_logs_changed(change_logs, duration):
    current_time = int(time.time())
    change_logs = coordinate_logs(
        change_logs, start_time=current_time - duration,
        end_time=current_time, fill_start_value=True
    )
    if not change_logs:
        return None

    values = list(change_logs.values())
    return values[-1] - values[0]


def check_intersect(l1: list, l2: list):
    l_ = {x: True for x in l2}
    intersect = []
    for x in l1:
        if x in l_:
            intersect.append(x)
    return intersect


def get_histogram_with_standard(values: list, n_bins=10):
    if not values:
        return {}

    values = sorted(values)

    percents = []
    for i in range(1, n_bins):
        t = 100 * i / n_bins
        p = 100 / (1 + math.exp(-1.70096 * (4 * (0.02 * t - 1))))
        percents.append(p)

    a = np.array(values)
    ranges = [0]
    for p in percents:
        r = np.percentile(a, p)
        ranges.append(round(float(r), 3))

    histogram = {}
    for idx, r in enumerate(ranges):
        end_value = ranges[idx + 1] if idx < len(ranges) - 1 else ranges[-1]
        histogram[r] = {'startValue': r, 'endValue': end_value, 'numberOfTransactions': 0}

    idx = 0
    for v in values:
        while (idx < len(ranges) - 1) and (v >= ranges[idx + 1]):
            idx += 1

        r = ranges[idx]
        histogram[r]['numberOfTransactions'] += 1

    return list(histogram.values())


def get_value_with_default(d: dict, key, default=None):
    """
    The get_value_with_default function is a helper function that allows us to
    get the value of a key in a dictionary, but if the key does not exist or the value of the key is None, it will
    return the default value. This is useful for when we want to check whether something
    exists in our data structure without having to explicitly write code that checks
    for its existence. For example:

    Args:
        d:dict: Specify the dictionary to be used
        key: Retrieve the value from a dictionary
        default: Set a default value if the key is not found in the dictionary or the value of the key is None

    Returns:
        The value of the key if it exists in the dictionary or the value of the key is None,
        otherwise it returns default

    Doc Author:
        Trelent
    """
    if not d:
        return default

    v = d.get(key)
    if v is None:
        v = default
    return v


def get_last_value_with_default(d: Union[dict, list], default=None):
    if isinstance(d, dict):
        d = list(d.values())

    if not isinstance(d, list):
        return default

    if not d:
        return default

    return d[-1]


def delete_none(_dict):
    """Delete None values recursively from all the dictionaries"""
    for key, value in list(_dict.items()):
        if isinstance(value, dict):
            delete_none(value)
        elif value is None:
            del _dict[key]
        elif isinstance(value, list):
            for v_i in value:
                if isinstance(v_i, dict):
                    delete_none(v_i)

    return _dict


def flatten_dict(d):
    out = {}
    for key, val in d.items():
        if isinstance(val, dict):
            val = [val]
        if isinstance(val, list):
            array = []
            for subdict in val:
                if not isinstance(subdict, dict):
                    array.append(subdict)
                else:
                    deeper = flatten_dict(subdict).items()
                    out.update({str(key) + '.' + str(key2): val2 for key2, val2 in deeper})
            if array:
                out.update({str(key): array})
        else:
            out[str(key)] = val
    return out
