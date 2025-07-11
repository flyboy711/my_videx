# -*- coding: utf-8 -*-
"""
Copyright (c) 2024 Bytedance Ltd. and/or its affiliates
SPDX-License-Identifier: MIT
"""
import base64
import json
import logging
from collections import defaultdict
from typing import List, Optional, Union, Dict, Any, Tuple
from pydantic import BaseModel, PlainSerializer, BeforeValidator
from typing_extensions import Annotated

from sub_platforms.sql_server.common.pydantic_utils import PydanticDataClassJsonMixin
from sub_platforms.sql_server.databases.mysql.mysql_command import MySQLVersion
from sub_platforms.sql_server.env.rds_env import Env
from sub_platforms.sql_server.meta import Table, Column
from sub_platforms.sql_server.videx import videx_logging
from sub_platforms.sql_server.videx.videx_utils import BTreeKeySide, target_env_available_for_videx, parse_datetime, \
    data_type_is_int, reformat_datetime_str

MEANINGLESS_INT = -1357

# MySQL will pass 'NULL' to the rec_in_ranges function.
# Note that this NULL is distinct from "NULL"—the latter is a string with the value 'NULL'.
NULL_STR = 'NULL'


def decode_base64(raw):
    """
    'base64' is an identifier indicating the data encoding method, meaning the following data is encoded using Base64.
    'type254': In MySQL, data type number 254 typically represents CHAR type, but the specific meaning may depend on your context.
    Args:
        raw:

    Returns:

    """

    decode_type, char_type, s = raw.split(":")
    assert decode_type == "base64" and char_type == "type254"
    base64_bytes = s.encode('utf-8')
    message_bytes = base64.b64decode(base64_bytes)
    return message_bytes.decode('utf-8')


def is_base64(str_in_base4: bool, raw):
    if not str_in_base4:
        return False
    if len(raw.split(":")) != 3:
        return False
    decode_type, char_type, s = raw.split(":")
    if decode_type == "base64" and char_type == "type254":
        return True
    return False


def convert_str_by_type(raw, data_type: str, str_in_base4: bool = True):
    """

    Args:
        raw:
        data_type:
        str_in_base4: if True，str is base64, need to decode

    Returns:

    """
    if raw == NULL_STR:
        return None

    NULL_STR_SET = {NULL_STR, 'None'}
    if data_type_is_int(data_type):
        if raw in NULL_STR_SET:
            return None
        return int(float(raw))
    elif data_type in ['float', 'double']:
        if raw in NULL_STR_SET:
            return None
        return float(raw)
    elif data_type in ['string', 'str', 'varchar', 'char']:
        # "base64:type254:YXhhaGtyc2I="
        if is_base64(str_in_base4, raw):
            res = decode_base64(raw)
        else:
            res = str(raw)
        # res = res.strip(' ') # we cannot strip the space at sides, as the parameter might be ' xx '.
        if (res.startswith("`") and res.endswith("`")) or \
                (res.startswith("'") and res.endswith("'")) or \
                (res.startswith('"') and res.endswith('"')):
            res = res[1:-1]
        return res
    elif data_type in ['datetime', 'date']:
        if '0000-00-00' in str(raw) or '1-01-01 00:00:00' in str(raw):
            return raw
        return reformat_datetime_str(str(raw))
    elif data_type == 'decimal':
        # we omit the point part in decimal
        return float(raw)
    elif data_type == 'json':
        # TODO: Temporarily handle JSON as a string for now. But in fact, we should parse the JSON and
        #  then perform function processing.
        return str(raw)
    else:
        # datetime,
        raise ValueError(f"Not support data type: {data_type}")


def large_number_encoder(x):
    MIN_LONG = -2 ** 63
    MAX_LONG = 2 ** 63 - 1
    if isinstance(x, int) and (x > MAX_LONG or x < MIN_LONG):
        return {"bigint": str(x)}
    return x


def large_number_decoder(y):
    if isinstance(y, dict) and "bigint" in y:
        return int(y["bigint"])
    return y


class HistogramBucket(BaseModel, PydanticDataClassJsonMixin):
    min_value: Annotated[Union[int, float, str, bytes], PlainSerializer(large_number_encoder), BeforeValidator(large_number_decoder)]
    max_value: Annotated[Union[int, float, str, bytes], PlainSerializer(large_number_encoder), BeforeValidator(large_number_decoder)]
    # cumulative_frequency: float
    cum_freq: float
    row_count: float  # note，row_count is "ndv" in bucket，we use float since algorithm may return non-integer
    size: int = 0


def init_bucket_by_type(bucket_raw: list, data_type: str, hist_type: str) -> HistogramBucket:
    """
    init HistogramBucket

    Args:
        bucket_raw:
            {
                "min_value": "base64:type254:YXhhaGtyc2I=",
                "max_value": "base64:type254:ZHZ1bXV1eWVh",
                "cum_freq": 0.1,
                "row_count": 8
            },
        data_type: string, int, decimal, ...
        hist_type:

    Returns:

    """
    if hist_type == 'singleton':
        assert len(bucket_raw) == 2, f"Singleton bucket must have 2 elements, got {len(bucket_raw)}"

    if len(bucket_raw) == 2:
        min_value, max_value, cum_freq, row_count = bucket_raw[0], bucket_raw[0], bucket_raw[1], 1
    elif len(bucket_raw) == 4:
        min_value, max_value, cum_freq, row_count = bucket_raw
    else:
        raise NotImplementedError(f"Not support bucket with len!=2, 4 yet: {bucket_raw}")
    min_value, max_value = convert_str_by_type(min_value, data_type), convert_str_by_type(max_value, data_type)
    bucket = HistogramBucket(min_value=min_value, max_value=max_value, cum_freq=cum_freq, row_count=row_count)
    return bucket


class HistogramStats(BaseModel, PydanticDataClassJsonMixin):
    """
    bucket.min_value <= bucket.max_value,
    and buckets are increasing, bucket[i].max_value <= bucket[i + 1].min_value
    buckets may have gaps, e.g., [1,2], [3,4]
    however, this doesn't necessarily mean gaps, but rather adjacent non-overlapping boundaries.

    For double values, between buckets:
    [
      6.22951651565178,
      8.72513181167602,
      0.1002,
      2004
    ],
    [
      8.72524160458256,
      9.18321476620723,
      0.2004,
      2004
    ],
    adjacent boundaries can be considered non-overlapping and without gaps

    for int：
    {
    "min_value": 2401,
    "max_value": 2700,
    "cum_freq": 0.9,
    "row_count": 300
    },
    {
    "min_value": 2701,
    "max_value": 3000,
    "cum_freq": 1,
    "row_count": 300
    }
    """
    # table_rows: int
    buckets: Optional[List[HistogramBucket]]
    data_type: Optional[str]
    histogram_type: Optional[str]
    null_values: Optional[float] = 0
    collation_id: Optional[int] = MEANINGLESS_INT
    last_updated: Optional[str] = str(MEANINGLESS_INT)
    sampling_rate: Optional[float] = MEANINGLESS_INT
    number_of_buckets_specified: Optional[int] = MEANINGLESS_INT

    def model_post_init(self, __context: Any) -> None:
        if int(self.null_values) == MEANINGLESS_INT:
            self.null_values = 0
        assert self.null_values >= 0, f"null_values must >= 0, got {self.null_values}"
        for b in self.buckets:
            b.min_value = convert_str_by_type(b.min_value, self.data_type)
            b.max_value = convert_str_by_type(b.max_value, self.data_type)
        if len(self.buckets) > 0:
            # check: sum(freq(buckets[-1] + null ratio) should be almost 1. if not, scale it.
            if abs(self.null_values + self.buckets[-1].cum_freq - 1) > 0.01:
                scale_factor = self.buckets[-1].cum_freq / (1 - self.null_values)
                for bucket in self.buckets:
                    bucket.cum_freq = bucket.cum_freq * scale_factor
                self.buckets[-1].cum_freq = 1

    def find_nearest_key_pos(self, value, side: BTreeKeySide) -> Union[int, float]:
        """
        Scan from left to right, find the first bucket that contains the value.

        Args:
            value: the value to search
            side: the boundary of the key.
                left: the left bound of the key.
                right: the right bound of the key.

        Returns:

        """
        value = convert_str_by_type(value, self.data_type, str_in_base4=False)  # histogram is base4 encoding，but request is raw string

        if value is None:
            if side == BTreeKeySide.left:
                return 0
            elif side == BTreeKeySide.right:
                return self.null_values
            else:
                raise ValueError(f"only support key pos side left and right, but get {side}")

        # convert to 0
        if value > self.buckets[-1].max_value:
            key_cum_freq = 1
        elif value < self.buckets[0].min_value:
            key_cum_freq = 0
        else:
            key_cum_freq = None
            for i in range(len(self.buckets)):
                if i < len(self.buckets) and (self.buckets[i].max_value < value < self.buckets[i + 1].min_value):
                    logging.warning(f"!!!!!!!!! value(={value})%s is "
                                    f"between buckets-{i} and {i + 1}: {self.buckets[i]}, {self.buckets[i + 1]}")
                    value = self.buckets[i].max_value
                cur: HistogramBucket = self.buckets[i]
                if cur.min_value <= value <= cur.max_value:
                    # a float number between [0, 1], it's the width of one value in the bucket,
                    # 1 means that all values in the bucket are same.
                    one_value_width: float
                    # a float number between [0, 1], it's the offset of one value in the bucket,
                    # 0 means that the value is the min value in the bucket, 1 means that the value is the max value in the bucket.
                    one_value_offset: float

                    # TODO we use the uniform distribution assumption temporarily.
                    # Under the uniform distribution, the width of a value is at least 1 / bucket_ndv.
                    one_value_width = 1 / cur.row_count

                    if cur.min_value == cur.max_value:
                        one_value_width, one_value_offset = 1, 0
                    else:
                        if data_type_is_int(self.data_type):
                            one_value_width = max(1 / (int(cur.max_value) - int(cur.min_value) + 1), one_value_width)
                            one_value_offset = (value - cur.min_value) / (cur.max_value + 1 - cur.min_value)
                        elif self.data_type in ['float', 'double', 'decimal']:
                            # we thought the width of float number can be close to 0 temporarily
                            one_value_offset = (value - cur.min_value) / (cur.max_value - cur.min_value)
                        elif self.data_type in ['string', 'varchar', 'char']:
                            # Strings only support comparison and do not support addition or subtraction,
                            # so we only compare the two ends.
                            # For values that are neither the minimum (min) nor the maximum (max), we take 1/2.
                            if value == cur.min_value:
                                one_value_offset = 0
                            elif value == cur.max_value:
                                one_value_offset = 1
                            else:
                                one_value_offset = 0.5
                        elif self.data_type in ['date']:
                            # In MySQL, columns of the DATE type contain only the year, month, and day components,
                            # excluding the time (i.e., hours, minutes, and seconds).
                            # According to the official MySQL documentation,
                            # the format for date values should be 'YYYY-MM-DD'.
                            # However, formats such as YYYYMMDD, YY-MM-DD and even timestamps are also supported:
                            # e.g. SELECT L_SHIPDATE FROM lineitem WHERE FROM_UNIXTIME(1672531200) < L_SHIPDATE LIMIT 5;
                            # But in the underlying implementation, all are converted to the format YYYY-MM-DD.
                            min_date = parse_datetime(cur.min_value).date()
                            max_date = parse_datetime(cur.max_value).date()
                            value_date = parse_datetime(value).date()

                            total_days = (max_date - min_date).days + 1
                            one_value_width = max(1 / total_days, one_value_width)
                            one_value_offset = (value_date - min_date).days / total_days

                        elif self.data_type in ['datetime']:
                            min_datetime = parse_datetime(cur.min_value)
                            max_datetime = parse_datetime(cur.max_value)
                            value_datetime = parse_datetime(value)

                            total_seconds = int((max_datetime - min_datetime).total_seconds())
                            one_value_width = max(1 / total_seconds, one_value_width)
                            if total_seconds != 0:
                                one_value_offset = (value_datetime - min_datetime).total_seconds() / total_seconds
                            else:
                                one_value_offset = 0
                        else:
                            raise NotImplementedError(f"data_type {self.data_type} not supported")
                        # the case that one_value_offset is at the right boundary
                        one_value_offset = min(one_value_offset, 1 - one_value_width)

                    if side == BTreeKeySide.left:
                        pos_in_bucket = one_value_offset
                    elif side == BTreeKeySide.right:
                        pos_in_bucket = one_value_offset + one_value_width
                    else:
                        raise ValueError(f"only support key pos side left and right, but get {side}")

                    pre_cum_freq = 0 if i == 0 else self.buckets[i - 1].cum_freq
                    key_cum_freq = pre_cum_freq + (cur.cum_freq - pre_cum_freq) * pos_in_bucket
                    break
        assert key_cum_freq is not None

        # MySQL histogram frequency is inconsistent with the in-equation condition.
        # We follow the in-equation format, i.e.
        # 0, null_values(ratio), null_values + buckets[0].min, null_values + buckets[-1].max(almost 1)
        return key_cum_freq + self.null_values

    @staticmethod
    def init_from_mysql_json(data: dict):
        """
        Init from data that is obtained from mysql, but not json or dataclass
        """
        buckets: List[HistogramBucket] = []
        for bucket_raw in data['buckets']:
            bucket = init_bucket_by_type(bucket_raw, data['data-type'], data['histogram-type'])
            buckets.append(bucket)
        return HistogramStats(
            # table_rows=table_rows,
            buckets=buckets,
            data_type=data['data-type'],
            null_values=data['null-values'],
            collation_id=data.get('collation-id', None),
            last_updated=data.get('last-updated', None),
            sampling_rate=data.get('sampling-rate', MEANINGLESS_INT),  # a special value indicating no sampling rate
            histogram_type=data['histogram-type'],
            number_of_buckets_specified=data['number-of-buckets-specified']
        )


def query_histogram(env: Env, dbname: str, table_name: str, col_name: str) -> Union[HistogramStats, None]:
    """

    Args:
        dbname:
        table_name:
        col_name:

    Returns:

    """
    sql = f"SELECT HISTOGRAM FROM information_schema.column_statistics " \
          f"WHERE SCHEMA_NAME = '{dbname}' AND TABLE_NAME = '{table_name}' AND COLUMN_NAME ='{col_name}'"
    res = env.query_for_dataframe(sql)
    if len(res) == 0:
        return None
    assert len(res) == 1 and 'HISTOGRAM' in res.iloc[0].to_dict(), f"Invalid result from query_histogram: {res}"
    hist_dict = json.loads(res.iloc[0].to_dict()['HISTOGRAM'])

    return HistogramStats.init_from_mysql_json(data=hist_dict)


def update_histogram(env: Env, dbname: str, table_name: str, col_name: str,
                     n_buckets: int = 32, hist_mem_size: int = None) -> bool:
    """

    Args:
        env:
        dbname:
        table_name:
        col_name:
        n_buckets:

    Returns:
        success if return true

    """
    n_buckets = max(1, min(1024, int(n_buckets)))

    conn = env.mysql_util.get_connection()
    with conn.cursor() as cursor:
        if hist_mem_size is not None:
            cursor.execute(f'SET histogram_generation_max_mem_size={hist_mem_size};')
        sql = f"ANALYZE TABLE `{dbname}`.`{table_name}` UPDATE HISTOGRAM ON {col_name} WITH {n_buckets} BUCKETS;"
        logging.debug(sql)
        cursor.execute(sql)
        res = cursor.fetchone()
        if res is not None and len(res) == 4:
            if 'Histogram statistics created for column' in res[3]:
                return True
        conn.commit()

    raise Exception(f"meet error when query: {res}")


def drop_histogram(env: Env, dbname: str, table_name: str, col_name: str) -> bool:
    """

    Args:
        dbname:
        table_name:
        col_name:

    Returns:

    """
    sql = f"ANALYZE TABLE `{dbname}`.`{table_name}` DROP HISTOGRAM ON {col_name};"
    logging.debug(sql)
    res = env.query_for_dataframe(sql)
    if res is not None and len(res) == 1:
        msg = res.iloc[0].to_dict().get('Msg_text')
        return 'Histogram statistics removed for column' in msg
    return False


def _format_value_by_type_in_sql(value, data_type_upper):
    """ format value by type in sql"""
    if value is None:
        return "NULL"

    if 'INT' in data_type_upper:
        return str(int(value))
    elif 'FLOAT' in data_type_upper or 'DOUBLE' in data_type_upper or 'DECIMAL' in data_type_upper:
        return str(float(value))
    elif 'DATE' in data_type_upper:
        return f"'{value}'"
    elif 'DATETIME' in data_type_upper:
        return f"'{value}'"
    elif 'CHAR' in data_type_upper or 'TEXT' in data_type_upper:
        return f"'%s'" % value.replace("'", "''")
    else:
        return str(value)


def _get_uniform_buckets(env: Env, db_name, table_name, col_name, min_value, max_value, data_type_upper, n_buckets):
    """use uniform distribution to generate buckets"""
    if 'INT' in data_type_upper:
        min_val = int(min_value)
        max_val = int(max_value)

        # make sure there are at least n_buckets buckets
        step = max(1, (max_val - min_val) // n_buckets)

        bounds = [min_val]
        for i in range(1, n_buckets):
            bounds.append(min_val + i * step)
        bounds.append(max_val)

    elif 'FLOAT' in data_type_upper or 'DOUBLE' in data_type_upper or 'DECIMAL' in data_type_upper:
        min_val = float(min_value)
        max_val = float(max_value)
        step = (max_val - min_val) / n_buckets

        bounds = []
        for i in range(n_buckets + 1):
            bounds.append(min_val + i * step)

    # date and char
    elif 'CHAR' in data_type_upper or 'TEXT' in data_type_upper or 'DATE' in data_type_upper or 'DATETIME' in data_type_upper:
        # random sampling some data, note that it's costly for online instance
        sample_sql = f"""
        SELECT {col_name} FROM {db_name}.{table_name} 
        WHERE {col_name} IS NOT NULL
        ORDER BY RAND() LIMIT 1000
        """
        sample_df = env.query_for_dataframe(sample_sql)

        if len(sample_df) <= 1:
            bounds = [min_value, max_value]
        else:
            sorted_samples = sorted(sample_df[col_name].tolist())

            # init bounds
            bounds = [min_value]

            if len(sorted_samples) < n_buckets - 1:
                # if the sampling size is less than n_buckets, use all unique samples as boundaries
                for sample in sorted_samples:
                    if min_value < sample < max_value and sample not in bounds:
                        bounds.append(sample)
            else:
                # choose the almost equal-width samples as boundaries
                step = len(sorted_samples) // n_buckets
                for i in range(1, n_buckets):
                    idx = min(i * step, len(sorted_samples) - 1)
                    sample = sorted_samples[idx]
                    if min_value < sample < max_value and sample not in bounds:
                        bounds.append(sample)

            if bounds[-1] != max_value:
                bounds.append(max_value)
    else:
        raise ValueError(f"Unsupported data_type: {data_type_upper}")

    if len(bounds) < 2:
        bounds = [min_value, max_value]

    if len(bounds) < n_buckets + 1:
        logging.warning(f"Generated boundary points ({len(bounds)}) are fewer than required ({n_buckets+1}). "
                        f"Existing boundaries will be used.")

    result = []
    # scan all boundaries, use select distinct count to generate bucket infomation
    for i in range(len(bounds) - 1):
        lower = bounds[i]
        upper = bounds[i + 1]

        lower_str = _format_value_by_type_in_sql(lower, data_type_upper)
        upper_str = _format_value_by_type_in_sql(upper, data_type_upper)

        # the first bucket：min_val <= c < bound1
        # the lst bucket：bound_{n-1} <= c <= max_val
        # middle buckets：bound_i <= c < bound_{i+1}
        if i == 0:
            left_op = ">="
        else:
            left_op = ">="

        if i == len(bounds) - 2:
            right_op = "<="
        else:
            right_op = "<"

        bucket_sql = f"""
        SELECT COUNT(1) as bucket_count, COUNT(DISTINCT {col_name}) as bucket_ndv,
        MIN({col_name}) as actual_min, MAX({col_name}) as actual_max
        FROM {db_name}.{table_name}
        WHERE {col_name} {left_op} {lower_str} AND {col_name} {right_op} {upper_str}
        """
        bucket_df = env.query_for_dataframe(bucket_sql)

        if not bucket_df.empty and bucket_df['bucket_count'].iloc[0] > 0:
            bucket_count = int(bucket_df['bucket_count'].iloc[0])
            bucket_ndv = int(bucket_df['bucket_ndv'].iloc[0])

            actual_min = bucket_df['actual_min'].iloc[0]
            actual_max = bucket_df['actual_max'].iloc[0]

            if actual_min is not None and actual_max is not None:
                result.append((str(actual_min), str(actual_max), bucket_count, bucket_ndv))
                logging.debug(f" {col_name=} bucket[{i}]: [{actual_min}, {actual_max}], bucket_count: {bucket_count}, bucket_ndv: {bucket_ndv}")

    return result


def get_bucket_bounds(env: Env, table_name, col_name,
                      min_value, max_value,
                      data_type, n_buckets,
                      ndv=None) -> List[Tuple[str, str, int, int]]:
    """
    Given the maximum and minimum values, data type.

    If ndv is null, first fetch ndv;
    If ndv is small, a group by can be performed;
    Otherwise, randomly sample n data entries, then get boundary values from these n entries; then use the following SQL to get information for each bucket:
        SELECT COUNT(1) as bucket_count, COUNT(DISTINCT {col_name}) as bucket_ndv,
        min({col_name}) as actual_min, max({col_name}) as actual_max
        FROM {db_name}.{table_name}
        WHERE {col_name} {left_op} {l_str} AND {col_name} {right_op} {u_str}

    Notes:
    1. For int, float, datetime, date, generation can be based on a uniform distribution;
    2. For string, random sampling is needed, and then generation;

    Args:
        env:
        table_name:
        col_name:
        min_value:
        max_value:
        data_type:
        n_buckets:
        ndv:

    Returns:
        a list with length <= n_buckets: [(lower_bound, upper_bound, bucket_count, bucket_ndv), ...],
        if ndv < n_buckets, return buckets where lower=upper
        lower_bound in str format
        upper_bound in str format
        bucket_count:
        bucket_ndv:
    """
    db_name = env.default_db
    data_type_upper = data_type.upper()

    # obtain ndv if it's None
    if ndv is None:
        ndv_sql = f"SELECT COUNT(DISTINCT {col_name}) as ndv FROM {db_name}.{table_name}"
        ndv_df = env.query_for_dataframe(ndv_sql)
        ndv = ndv_df['ndv'].iloc[0]
        logging.debug(f"{table_name=} {col_name=} ndv is None, force fetch it, {ndv=}")

    # if ndv is very small, use group by to get the value count
    if ndv <= n_buckets:
        logging.debug(f"{table_name=} {col_name=} {ndv=} < {n_buckets=}, use group by")
        small_ndv_sql = f"""
        SELECT {col_name} as value, COUNT(1) as bucket_count, 1 as bucket_ndv 
        FROM {db_name}.{table_name} 
        WHERE {col_name} IS NOT NULL 
        GROUP BY {col_name} 
        ORDER BY {col_name}
        """
        small_ndv_df = env.query_for_dataframe(small_ndv_sql)

        result = []
        for _, row in small_ndv_df.iterrows():
            value = row['value']
            result.append((value, value, int(row['bucket_count']), 1))

        return result

    return _get_uniform_buckets(env, db_name, table_name, col_name, min_value, max_value, data_type_upper, n_buckets)


def force_generate_histogram_by_sdc_for_col(env: Env, db_name: str, table_name: str, col_name: str,
                                            n_buckets: int, hist_mem_size: int = None,
                                            ndv: int = None,
                                            ) -> HistogramStats:
    """
    force generate histogram using sdc(SELECT DISTINCT COUNT). it may be very time-consuming.
    Args:
        env:
        db_name:
        table_name:
        col_name:
        n_buckets:
        hist_mem_size:

    Returns:
        initialize HistogramStats from json dict:
        {
            "buckets": [{
                    "min_value": "0000",
                    "max_value": "0000",
                    "cum_freq": 0.7035317292809906,
                    "row_count": 1
                },
            ],
            "data_type": None,
            "histogram_type": "brute_force_calc",
            "null_values": None,
            "collation_id": MEANINGLESS_INT,
            "sampling_rate": 1.0,
            "number_of_buckets_specified": None
        }
    """
    res_dict = {
        "buckets": [
        ],
        "data-type": None,
        "histogram-type": "brute_force_equi_width",
        "null-values": None,
        "collation-id": MEANINGLESS_INT,
        "sampling-rate": 1.0,
        "number-of-buckets-specified": None
    }
    column = env.get_column_meta(db_name, table_name, col_name)
    if not column:
        raise ValueError(f"column not found: {db_name}")
    data_type = column.data_type

    # Find the minimum and maximum values in the column
    _df = env.query_for_dataframe(f"SELECT MIN({col_name}) as min, MAX({col_name}) as max FROM {db_name}.{table_name}")
    min_val, max_val = _df['min'][0], _df['max'][0]

    # Calculate the bucket size
    null_values = env.mysql_util.query_for_value(
        f"SELECT COUNT(1) FROM {db_name}.{table_name} WHERE {col_name} IS NULL;")
    total_rows = env.mysql_util.query_for_value(f"SELECT COUNT(1) FROM {db_name}.{table_name}")
    null_values = null_values / total_rows if total_rows > 0 else 0  # null_values is in [0, 1]
    n_buckets = min(total_rows, n_buckets)
    if total_rows > 0 and data_type_is_int(data_type):
        n_buckets = min(n_buckets, max_val - min_val + 1)

    res_dict['data-type'] = data_type
    res_dict['null-values'] = null_values

    logging.debug(f"{table_name=} {col_name=} {data_type=} {total_rows=} {null_values=}")

    if n_buckets == 0 or total_rows == 0:
        logging.warning(f"brute-force generate histogram, but meet 0: {n_buckets=} {total_rows=}")
        res_dict['number-of-buckets-specified'] = 0
        return HistogramStats.init_from_mysql_json(res_dict)

    res_dict['number-of-buckets-specified'] = n_buckets

    bucket_list = get_bucket_bounds(env, table_name, col_name, min_val, max_val, data_type, n_buckets, ndv)
    # Calculate the cumulative frequency and NDV for each bucket
    cum_freq = 0
    for actual_min, actual_max, bucket_count, bucket_ndv in bucket_list:
        if bucket_ndv == 0:
            continue

        # Calculate cumulative frequency
        cum_freq += bucket_count / total_rows

        # Add the histogram bucket details
        res_dict["buckets"].append([
            str(actual_min),
            str(actual_max),
            cum_freq,
            max(1, int(bucket_ndv)),
        ])

    return HistogramStats.init_from_mysql_json(res_dict)


def fetch_col_histogram(env: Env, dbname: str, table_name: str, col_name: str, n_buckets: int = 32,
                        force: bool = False, hist_mem_size: int = None, ndv: int = None) -> HistogramStats:
    """
    fetch or generate histogram for a column
    Args:
        env: MySQL env
        dbname:
        table_name:
        col_name:
        n_buckets: number of buckets
        force: if force is False, and histogram exists, return it

    Returns:

    """
    if not force:
        hist: HistogramStats = query_histogram(env, dbname, table_name, col_name)
        if hist is not None:
            if len(hist.buckets) == n_buckets:
                return hist
            else:
                logging.debug(f"hist(`{dbname}`.`{table_name}`.`{col_name=}`) exists, "
                              f"but n_bucket mismatch (exists={len(hist.buckets)} != {n_buckets}), re-generate.")
        else:
            logging.debug(f"Histogram(`{dbname}`.`{table_name}`.`{col_name=}`) not found. "
                          f"Generating with {n_buckets} n_buckets")
    else:
        logging.debug(
            f"Force Generating Histogram for `{dbname}`.`{table_name}`.`{col_name}` with {n_buckets} n_buckets")
    # generate histogram and return. if failed, use force_generate_histogram_for_col to generate
    try:
        res_update = update_histogram(env, dbname, table_name, col_name, n_buckets, hist_mem_size)
    except Exception as e:
        if 'is covered by a single-part unique index' in str(e):
            logging.info(f"Column is covered single uk, force generate: {dbname=}, {table_name=}, {col_name=}")
            return force_generate_histogram_by_sdc_for_col(env, dbname, table_name, col_name, n_buckets, ndv=ndv)
        else:
            logging.error(f"uncatched: {dbname=}, {table_name=}, {col_name=}")
            raise
    assert res_update, 'Failed to update histogram'
    return query_histogram(env, dbname, table_name, col_name)


def generate_fetch_histogram(env: Env, target_db: str, all_table_names: List[str],
                             n_buckets: int, force: bool,
                             drop_hist_after_fetch: bool,
                             hist_mem_size: int,
                             ret_json: bool = False,
                             ndv_single_dict: dict = None,
                             ) -> Dict[str, Dict[str, Union[HistogramStats, dict]]]:
    """
    generate histogram for all specifed tables

    Args:
        env: MySQL
        target_db:
        all_table_names:
        n_buckets:
        force:
        ret_json: True: return json, False: return HistogramStats
        ndv_single_dict: table_name -> col -> ndv

    Returns:
        lower_table -> column -> HistogramStats

    """
    if not target_env_available_for_videx(env):
        raise Exception(f"given env ({env.instance=}) is not in BLACKLIST, cannot generate_fetch_histogram directly")

    ndv_single_dict = ndv_single_dict or {}

    only_sfw_fetch = env.get_version() == MySQLVersion.MySQL_57

    res_tables = defaultdict(dict)
    for table_name in all_table_names:
        table_meta: Table = env.get_table_meta(target_db, table_name)
        # print(table_meta)
        for c_id, col in enumerate(table_meta.columns):
            col: Column
            ndv = ndv_single_dict.get(table_name, {}).get(col.name, None)
            hist = None
            try:
                logging.info(f"Generating Histogram for `{target_db}`.`{table_name}`.`{col.name}` "
                             f"with {n_buckets} n_buckets")
                if only_sfw_fetch:
                    hist = force_generate_histogram_by_sdc_for_col(env, target_db, table_name, col.name, n_buckets,
                                                                   ndv=ndv)
                else:
                    hist = fetch_col_histogram(env, target_db, table_name, col.name, n_buckets, force=force,
                                               hist_mem_size=hist_mem_size,
                                               ndv=ndv,
                                               )
            finally:
                if not only_sfw_fetch and drop_hist_after_fetch:
                    try:
                        drop_histogram(env, target_db, table_name, col.name)
                    except Exception as e:
                        logging.error(f"drop histogram failed for {target_db}.{table_name}.{col.name}, {e}")

            if hist is not None and ret_json:
                hist = hist.to_dict()
            res_tables[str(table_name).lower()][col.name] = hist
    return res_tables


if __name__ == '__main__':
    videx_logging.initial_config()
    # some database with tpch
    from sub_platforms.sql_server.env.rds_env import Env, OpenMySQLEnv
    # from sub_platforms.sql_server.benchmark.bench_utils import TPCH_UT_INS_80
    my_env = OpenMySQLEnv.from_db_instance(TPCH_UT_INS_80)

    # varchar(44)
    hist = force_generate_histogram_by_sdc_for_col(my_env, 'tpch_rong', 'lineitem', col_name='L_COMMENT', n_buckets=16, ndv=4580554)
    print(hist.buckets)
    # int
    hist = force_generate_histogram_by_sdc_for_col(my_env, 'tpch_rong', 'lineitem', col_name='L_LINENUMBER', n_buckets=16, )
    print(hist)
    # date
    hist = force_generate_histogram_by_sdc_for_col(my_env, 'tpch_rong', 'lineitem', col_name='L_SHIPDATE', n_buckets=16, )
    print(hist)
    # decimal
    hist = force_generate_histogram_by_sdc_for_col(my_env, 'tpch_rong', 'lineitem', col_name='L_DISCOUNT', n_buckets=16, )
    print(hist)
