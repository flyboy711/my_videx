# -*- coding: utf-8 -*-
"""
Copyright (c) 2024 Bytedance Ltd. and/or its affiliates
SPDX-License-Identifier: MIT
"""
import json
import logging
import os
import pickle
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Dict, Union, Tuple, Set

import msgpack

from sub_platforms.sql_optimizer.env.rds_env import Env, OpenMySQLEnv
from sub_platforms.sql_optimizer.meta import TableId, Index, IndexColumn

# VIDEX obtains four statistical information through fetch_all_meta_for_videx.
# All four functions will directly access the original database.
# Especially, ndv and histogram impose a heavy load on the original database.
# Exercise caution: fetch_all_meta_for_videx should not be executed on the online production environment.
VIDEX_IP_WHITE_LIST = [
    '127.0.0.1',
    'localhost',
]


def target_env_available_for_videx(env: Env) -> bool:
    """
    Check whether the given env supports videx to directly collect raw statistical information,
    such as analyze table update histogram, or select count(distinct)
    """
    if not isinstance(env, OpenMySQLEnv):
        return False
    host = env.instance.split(':')[0]
    return host in VIDEX_IP_WHITE_LIST


class BTreeKeyOp(Enum):
    """
    Corresponds to MySQL HaRKeyFunction
    """
    EQ = ("=", "HA_READ_KEY_EXACT")
    GTE = (">=", "HA_READ_KEY_OR_NEXT")
    LTE = ("<=", "HA_READ_KEY_OR_PREV")
    GT = (">", "HA_READ_AFTER_KEY")
    LT = ("<", "HA_READ_BEFORE_KEY")
    HA_READ_PREFIX = ("=x%", "HA_READ_PREFIX")
    HA_READ_PREFIX_LAST = ("last_x%", "HA_READ_PREFIX_LAST")
    HA_READ_PREFIX_LAST_OR_PREV = ("<=last_x%",)
    HA_READ_MBR_CONTAIN = ("HA_READ_MBR_CONTAIN",)
    HA_READ_MBR_INTERSECT = ("HA_READ_MBR_INTERSECT",)
    HA_READ_MBR_WITHIN = ("HA_READ_MBR_WITHIN",)
    HA_READ_MBR_DISJOINT = ("HA_READ_MBR_DISJOINT",)
    HA_READ_MBR_EQUAL = ("HA_READ_MBR_EQUAL",)
    HA_READ_INVALID = ("HA_READ_INVALID",)
    Unknown = ("Unknown",)

    @staticmethod
    def init(value: str):
        """
        Get the HaRKeyFunction enumeration object corresponding to the value or name

        Args:
            value (str):

        Returns:
            BTreeKeyOp: Corresponds to HaRKeyFunction enum item，if missing，return Unknown

        """
        if not hasattr(BTreeKeyOp, "_value_map"):
            BTreeKeyOp._value_map = {}
            for member in BTreeKeyOp:
                for v in member.value:
                    BTreeKeyOp._value_map[v] = member
                    BTreeKeyOp._value_map[member.name] = member
        return BTreeKeyOp._value_map.get(value, BTreeKeyOp.Unknown)


class BTreeKeySide(Enum):
    left = 'left'
    right = 'right'

    @staticmethod
    def from_op(op: Union[str, BTreeKeyOp]):
        if isinstance(op, str):
            op = BTreeKeyOp.init(op)
        if op in [BTreeKeyOp.LT, BTreeKeyOp.EQ]:
            return BTreeKeySide.left
        elif op in [BTreeKeyOp.GT]:
            return BTreeKeySide.right
        else:
            raise ValueError(f"Invalid given btree op: {op}")


@dataclass
class RangeCond:
    """
    Single-column query condition.
    For example, c2 < 3 and c2 > 1, which is converted from the more basic min-key and max-key
    """
    col: str
    data_type: str
    min_value: str = None
    max_value: str = None
    min_op: str = None  # for print，including "None", "=", "<", "<="
    max_op: str = None  # for print，including "None", "=", ">", ">="
    """
    The original operators sent by the MySQL interface doesn't care about min or max, 
    but indicates whether the position of the key appears on the left or right side of the value.
    
    In the original MySQL, `<` or `=` represents "left", and `>` represents "right".
    Some modifications have been made here to facilitate strategy operations:
    1. `<` and `=` are unified as "left".
    2. For the first few columns of a multi-column index, special processing will be done on the `pos_op` 
    corresponding to "=", and it will be unified as "right".
    
    Note that if the first n - 1 columns are not "=", it will make InnoDB search very difficult to understand. 
    Therefore, MySQL also prohibits this situation.
    We now ensure that the case where the first n - 1 columns are "=" is handled correctly, 
    and other cases will be dealt with later.

    """
    min_key_pos_side: BTreeKeySide = None  # None, left, right
    max_key_pos_side: BTreeKeySide = None  # None, left, right

    @staticmethod
    def _check_op_and_side(op: str, is_min: bool):
        if is_min:
            MIN_VALID_OP = {"=", ">", ">="}
            assert op in MIN_VALID_OP, f"Invalid min_op: {op}, valid: {MIN_VALID_OP}"
            # assert side in [BTREE_KEY_SIDE_LEFT, BTREE_KEY_SIDE_RIGHT], f"Invalid min key pos side: {side}"
        else:
            MAX_VALID_OP = {"=", "<", "<="}
            assert op in MAX_VALID_OP, f"Invalid max_op: {op}, valid: {MAX_VALID_OP}"
            # assert side in [BTREE_KEY_SIDE_LEFT, BTREE_KEY_SIDE_RIGHT], f"Invalid max key pos side: {side}"

    def __post_init__(self):
        if self.min_value is not None:
            self._check_op_and_side(self.min_op, is_min=True)
        if self.max_value is not None:
            self._check_op_and_side(self.min_op, is_min=False)

    def add_min(self, op: str, value: str, side: BTreeKeySide):
        self.min_value = value
        self.min_op = op
        self.min_key_pos_side = side
        self._check_op_and_side(self.min_op, is_min=True)

    def add_max(self, op: str, value: str, side: BTreeKeySide):
        self.max_value = value
        self.max_op = op
        self.max_key_pos_side = side
        self._check_op_and_side(self.max_op, is_min=False)

    def valid(self):
        return self.min_op is not None or self.max_op is not None

    def has_min(self):
        return self.min_op is not None

    def has_max(self):
        return self.max_op is not None

    def is_singlepoint(self) -> bool:
        """
        The naming refers to SEL_ARG::is_singlepoint sql/range_optimizer/tree.h:820
        Actually, it is to determine whether it is an equality query.

        Returns:

        """
        return self.min_op == "="

    def __eq__(self, other):
        if not isinstance(other, RangeCond):
            return False
        if not self.valid() or not other.valid():
            return False
        return self.col == other.col and self.min_op == other.min_op and self.max_op == other.max_op \
            and self.min_value == other.min_value and self.max_value == other.max_value

    def all_possible_strs(self) -> List[str]:
        res = []
        REVERSE_OP = {">": "<", ">=": "<="}

        if self.min_op == '=':
            res.append(f"{self.col} = {self.min_value}")
            res.append(f"{self.min_value} = {self.col}")
            # return res
        elif self.min_op is not None and self.max_op is not None:
            # like 2 < col < 3
            rev_min_op = REVERSE_OP.get(self.min_op, "!!!!!")
            rev_max_op = REVERSE_OP.get(self.max_op, "!!!!!")
            res.append(f"{self.min_value} {rev_min_op} {self.col} {self.max_op} {self.max_value}")
            res.append(f"{self.max_op} {rev_max_op} {self.col} {self.min_op}  {self.min_value} ")
            # return res
        elif self.min_op is not None:
            rev_min_op = REVERSE_OP.get(self.min_op, "!!!!!")
            # col < v
            res.append(f"{self.col} {self.min_op} {self.min_value}")
            # v < col
            res.append(f"{self.min_value} {rev_min_op} {self.col}")
        if self.max_op is not None:
            rev_max_op = REVERSE_OP.get(self.max_op, "!!!!!")
            # col > v
            res.append(f"{self.col} {self.max_op} {self.max_value}")
            # v > col
            res.append(f"{self.max_value} {rev_max_op} {self.col}")
            # v > col > 'NULL'
            res.append(f"{self.max_value} {rev_max_op} {self.col} > 'NULL'")
            # 'NULL' < col < v
            res.append(f"'NULL' < {self.col} {self.max_op} {self.max_value}")

        return res

    def __repr__(self):
        res = self.all_possible_strs()
        if res is None or len(res) == 0:
            return "None"
        return res[0]

    def to_print_full(self) -> str:
        res = (f"{self.__repr__()}; "
               f"min_side: {self.min_key_pos_side.value if self.min_key_pos_side else 'None'}, "
               f"max_side: {self.max_key_pos_side.value if self.max_key_pos_side else 'None'}")
        return res

    @staticmethod
    def construct_eq(col: str, data_type: str, value: str) -> 'RangeCond':
        return RangeCond(col=col, data_type=data_type,
                         min_value=value, min_op="=", min_key_pos_side=BTreeKeySide.left,
                         max_value=value, max_op="=", max_key_pos_side=BTreeKeySide.right,
                         )


@dataclass
class IndexRangeCond:
    """
    For example, ranges = [RangeCond(c1 = 3), RangeCond(c2 < 3 and c2 > 1)]

    Translate handler:: records_in_range to List[RangeCond].
    The above example is converted from the more basic min-key and max-key.

    """
    index_name: str
    ranges: List[RangeCond]

    def ranges_to_str(self):
        return " AND ".join(map(str, self.ranges))

    def __repr__(self):
        return f"{self.index_name}: {self.ranges_to_str()}"

    def to_print_full(self) -> str:
        return f"{self.index_name}: " + " AND ".join(map(lambda k: k.to_print_full(), self.ranges))

    def __eq__(self, other):
        if not isinstance(other, IndexRangeCond):
            return False
        return self.index_name == other.index_name and self.ranges == other.ranges

    @staticmethod
    def from_dict(min_key: dict, max_key: dict, get_data_type: callable = None,
                  index_meta: Index = None,
                  ) -> 'IndexRangeCond':
        """
        Examples:

        EXPLAIN select I_PRICE from ITEM where I_IM_ID = 3
        KEY: idx_I_IM_ID   MIN_KEY: { =  I_IM_ID(3), }, MAX_KEY: { >  I_IM_ID(3), }

        req_json = {"item_type": "videx_request",
                    "properties": {"dbname": "tpcc",
                                   "function": "virtual ha_rows ha_innobase::records_in_range(uint, key_range*, key_range*)",
                                   "table_name": "ITEM",
                                   "target_storage_engine": "INNODB"}, "data": [
                {"item_type": "min_key",
                 "properties": {"index_name": "idx_I_IM_ID", "length": "4", "operator": "="},
                 "data": [
                     {"item_type": "column_and_bound", "properties": {"column": "I_IM_ID", "value": "3"}, "data": []}]},
                {"item_type": "max_key",
                 "properties": {"index_name": "idx_I_IM_ID", "length": "4", "operator": ">"},
                 "data": [{"item_type": "column_and_bound", "properties": {"column": "I_IM_ID", "value": "3"},
                           "data": []}]}]}

            min_key, max_key = req_json['data']

        Args:
            min_key:
            max_key:

        Returns: List[RangeCond]
        """
        # TODO consider desc index when generating IndexRangeCond
        if get_data_type is None:
            get_data_type = lambda x: "Unknown"
        if 'index_name' in min_key['properties']:
            index_name = min_key['properties']['index_name']
        elif 'index_name' in max_key['properties']:
            index_name = max_key['properties']['index_name']
        else:
            index_name = 'INVALUD !!!'
        res = IndexRangeCond(index_name=index_name, ranges=[])

        n_col = max(len(min_key['data']), len(max_key['data']))
        if abs(len(min_key['data']) - len(max_key['data']) > 1):
            # It's abnormal，the delta between min_key and max_key is at most 1
            logging.error("min_key and max_key length can only differ by 1."
                          f"given min_key: {min_key}, max_key: {max_key}")
            return res

        for c in range(n_col):
            is_desc_idx_col = False
            if index_meta and len(index_meta.columns) > c:
                index_col_meta = index_meta.columns[c]
                if index_col_meta.is_desc:
                    is_desc_idx_col = True

            # col = min_key['data'][c]["properties"]['column']
            # value = min_key['data'][c]["properties"]['value']
            # res.ranges.append(RangeCond.construct_eq(col, get_data_type(col), value))
            has_min = c < len(min_key['data'])
            has_max = c < len(max_key['data'])
            if has_min:
                col = min_key['data'][c]["properties"]['column']
            elif has_max:
                col = max_key['data'][c]["properties"]['column']
            else:
                logging.error(f"receive boundary without min and max: min_key: {min_key}, max_key: {max_key}")
                return res

            min_op = min_key['properties']['operator'] if has_min else None
            max_op = max_key['properties']['operator'] if has_max else None
            min_value = min_key['data'][c]["properties"]['value'] if has_min else None
            max_value = max_key['data'][c]["properties"]['value'] if has_max else None
            # refer to SEL_ARG::is_singlepoint
            if has_min and has_max and min_value == max_value:
                # =
                res.ranges.append(RangeCond.construct_eq(col, data_type=get_data_type(col), value=min_value))
            else:
                last_range = RangeCond(col=col, data_type=get_data_type(col))
                # add min bound
                if has_min:
                    if min_op == "=":
                        if not is_desc_idx_col:
                            # >= min_value.
                            last_range.add_min(">=", min_value, BTreeKeySide.left)
                        else:
                            # c >= v -> c <= v
                            last_range.add_max("<=", min_value, BTreeKeySide.right)
                    elif min_op == ">":
                        if not is_desc_idx_col:
                            last_range.add_min(">", min_value, BTreeKeySide.right)
                        else:
                            # c > v -> c < v
                            last_range.add_max("<", min_value, BTreeKeySide.left)
                    else:
                        pass
                # add max bound
                if has_max:
                    if max_op == ">":
                        if not is_desc_idx_col:
                            # <= max_value
                            last_range.add_max("<=", max_value, BTreeKeySide.right)
                        else:
                            last_range.add_min(">=", max_value, BTreeKeySide.left)
                    elif max_op == "<":
                        if not is_desc_idx_col:
                            last_range.add_max("<", max_value, BTreeKeySide.left)
                        else:
                            last_range.add_min(">", max_value, BTreeKeySide.right)
                    else:
                        pass
                res.ranges.append(last_range)
        return res

    def match(self, range_str: str, ignore_range_after_neq: bool) -> bool:
        """

        Args:
            range_str:
            ignore_range_after_neq: In the case of a multi-column query in btree, if the first column is not an equality,
                the subsequent columns will be ignored, so there is no match here either.
            @See sub_platforms.sql_server.videx.strategy.VidexModelInnoDB.__init__
        Returns:

        """
        gt_range_str_list = range_str.split(' AND ')
        cmp_ranges = self.get_valid_ranges(ignore_range_after_neq)

        if len(gt_range_str_list) != len(cmp_ranges):
            return False
        for cond, gt_range_str in zip(cmp_ranges, gt_range_str_list):
            all_strs = cond.all_possible_strs()
            if gt_range_str.strip() not in all_strs:
                return False
        return True

    def get_valid_ranges(self, ignore_range_after_neq: bool) -> List[RangeCond]:
        """

        Args:
            ignore_range_after_neq: In the case of a multi-column query in btree, if the first column is not an equality,
                the subsequent columns will be ignored, so there is no match here either.
                @See sub_platforms.sql_server.videx.strategy.VidexModelInnoDB.__init__
        Returns:

        """
        ranges = []
        if ignore_range_after_neq:
            # 从 0 向后找，找到第一个非等值列。保留等值列和第一个非等值列，抛弃后续的 ranges
            for range_cond in self.ranges:
                ranges.append(range_cond)
                if not range_cond.is_singlepoint():
                    break
        else:
            ranges = self.ranges
        return ranges


@dataclass
class GT_Table_Return:
    """
    Input a list of gt rec_in_ranges, which is parsed from the trace.
    It contains all the gt in a table. This is for debugging and used to compare the online effects.

    Refer to：sub_platforms.sql_server.videx.metadata.extract_rec_in_range_gt_from_explain
    Args:
        gt_rec_in_ranges: {
            "idx_1": [
                {"range_str": "'1995-01-01' <= O_ORDERDATE <= '1996-12-31'", "rows": 1282},
                {"range_str": "P_TYPE < 5", "rows": 123},
            ],
            "idx_2": [
                {"range_str": "O_ORDERDATE < '1995-01-01'", "rows": 1282},
                {"range_str": "P_TYPE >= 5", "rows": 123},
            ]
        }
    """
    idx_gt_pair_dict: Dict[str, list] = field(default_factory=lambda: defaultdict(list))

    @staticmethod
    def parse_raw_gt_rec_in_range_list(raw_gt_rec_in_range_list: List[dict]) -> Dict[str, 'GT_Table_Return']:
        """

        Returns:
            Dict[str, GT_Table_Return]: table -> GT_Table_Return
        """
        gt_rr_dict = defaultdict(GT_Table_Return)
        for rr in raw_gt_rec_in_range_list:
            """
              {
                "table": "`nation` `n1`"
                "index": "1_2_idx_I_IM_ID", 
                "ranges": ["I_IM_ID = 70","I_IM_ID = 80"],
                "rows": 21, "cost": 3.12041, 
              }
            """
            if "ranges" not in rr:
                continue
            index_name = rr["index"]
            # len > 1的话，意味着 gt_rr 包含 or 或者 in 条件，按照均匀分布推导出 rows
            per_rows = int(rr["rows"]) / len(rr["ranges"])

            tables = rr["table"].split(" ")
            for table in tables:
                table = table.strip('`').lower()
                for ranges_str in rr["ranges"]:
                    gt_rr_dict[table].idx_gt_pair_dict[index_name].append({"range_str": ranges_str, "rows": per_rows})
        return gt_rr_dict

    def find(self, range_cond: IndexRangeCond, ignore_range_after_neq: bool = True) -> Union[int, None]:
        """

        Args:
            range_cond:
            ignore_range_after_neq:

        Returns:

        """
        ranges_str = range_cond.ranges_to_str()
        if range_cond.index_name in self.idx_gt_pair_dict:
            gt_index_ranges = self.idx_gt_pair_dict[range_cond.index_name]
            for gt_item in gt_index_ranges:
                # gt_item: {"range_str": ranges_str, "rows": per_rows}
                if range_cond.match(gt_item["range_str"], ignore_range_after_neq):
                    return int(gt_item["rows"])

            logging.warning(f"NOGT: rec_in_ranges. found index but not gt."
                            f"given index: {range_cond.index_name}, given range: {ranges_str}, "
                            f"gt ranges: {gt_index_ranges}")
        else:
            logging.warning(f"NOGT: rec_in_ranges. index not in gt. "
                            f"given index: {range_cond.index_name}, given range: {ranges_str}, "
                            f"gt index keys: {list(self.idx_gt_pair_dict.keys())}")


def load_json_from_file(filename: str):
    if not os.path.exists(filename):
        return None

    with open(filename, 'r') as file:
        data = json.load(file)
    return data


def dump_json_to_file(filename: str, data: Union[dict, list], indent: int = 4):
    parent_path = os.path.dirname(os.path.abspath(filename))
    if not os.path.exists(parent_path):
        os.makedirs(parent_path)

    with open(filename, 'w') as file:
        json.dump(data, file, indent=indent)


def data_type_is_int(data_type: str) -> bool:
    # mysql int contains "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT",
    return 'INT' in data_type.upper()


def reformat_datetime_str(datetime_input: Union[str, int], fmt='%Y-%m-%d %H:%M:%S.%f') -> str:
    """
    Args:
        datetime_input:
        fmt:

    Returns:

    """
    if isinstance(datetime_input, str) and datetime_input == 'NULL':
        return datetime.min.strftime(fmt)
    return datetime.strftime(parse_datetime(datetime_input), fmt)


def parse_datetime(datetime_input: Union[str, int]) -> datetime:
    """
    Convert a string or an integer to a datetime object. It can handle MySQL date and datetime formats,
    as well as integer timestamps (in seconds or nanoseconds).
    Args:
        datetime_input: A string or an integer representing a date/time or a timestamp.
    Returns:
        datetime: The corresponding datetime object.
    Raises:
        ValueError: If the input is neither in a date/time format nor an integer timestamp, an exception will be raised.。
    """
    if isinstance(datetime_input, str):
        datetime_str = datetime_input.strip('\'"')

        try:
            datetime_int = int(datetime_str)
            return parse_timestamp(datetime_int)
        except ValueError:
            pass
    elif isinstance(datetime_input, int):
        return parse_timestamp(datetime_input)
    else:
        raise ValueError("Input must be a string or an integer")

    for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f',
                "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue

    raise ValueError("No suitable format found for '{}'".format(datetime_input))


from datetime import datetime, timedelta


def parse_timestamp(timestamp):
    """
    convert int timestamp into datetime
    """
    length = len(str(timestamp))
    if length <= 10:
        return datetime.fromtimestamp(timestamp)
    elif length <= 13:
        milliseconds = timestamp % 1_000
        return datetime.fromtimestamp(timestamp // 1_000).replace(microsecond=milliseconds * 1000)
    elif length <= 16:
        microseconds = timestamp % 1_000_000
        return datetime.fromtimestamp(timestamp // 1_000_000).replace(microsecond=microseconds)
    elif length <= 19:
        nanoseconds = timestamp % 1_000_000_000
        base_datetime = datetime.fromtimestamp(timestamp // 1_000_000_000)
        return base_datetime + timedelta(microseconds=nanoseconds // 1000)
    else:
        raise ValueError("Integer timestamp length is not compatible: '{}'".format(timestamp))


if __name__ == '__main__':
    pass
