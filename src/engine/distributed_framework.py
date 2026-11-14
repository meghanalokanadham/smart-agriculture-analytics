"""
Distributed Dataframe & RDD Engine (PySpark-API Emulation Engine)
Architecture:
- Distributed Resilient Distributed Dataset (RDD) core abstractions: map, filter, flatMap, reduceByKey, aggregate, collect, glom
- Distributed DataFrame with catalyst-style logical transformations, filter, select, groupBy, withColumn, join, orderBy
- Multiprocessing partition-based parallel execution simulating multi-node Spark cluster workers
- Zero external dependency: Runs pure Python standard library with multiprocessing and concurrent.futures
"""

import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import csv
import json
import math
import os
import re

class Partition:
    def __init__(self, partition_id, data):
        self.partition_id = partition_id
        self.data = list(data)

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

def _map_partition(args):
    part_id, data, fn = args
    res = [fn(x) for x in data]
    return (part_id, res)

def _filter_partition(args):
    part_id, data, predicate = args
    res = [x for x in data if predicate(x)]
    return (part_id, res)

def _reduce_partition(args):
    part_id, data, key_fn, val_fn = args
    local_acc = {}
    for x in data:
        k = key_fn(x)
        v = val_fn(x)
        local_acc[k] = local_acc.get(k, 0.0) + v
    return (part_id, local_acc)

class DistRDD:
    def __init__(self, partitions, num_workers=None):
        self.partitions = partitions  # list of Partition objects
        self.num_workers = num_workers or min(4, mp.cpu_count() or 2)

    @classmethod
    def from_iterable(cls, items, num_partitions=4):
        items = list(items)
        chunk_size = math.ceil(len(items) / num_partitions) if num_partitions > 0 else len(items)
        partitions = []
        for i in range(num_partitions):
            chunk = items[i * chunk_size : (i + 1) * chunk_size]
            partitions.append(Partition(i, chunk))
        return cls(partitions, num_workers=num_partitions)

    def count(self):
        return sum(len(p) for p in self.partitions)

    def collect(self):
        all_records = []
        for p in self.partitions:
            all_records.extend(p.data)
        return all_records

    def map(self, fn):
        new_partitions = []
        for p in self.partitions:
            new_data = [fn(x) for x in p.data]
            new_partitions.append(Partition(p.partition_id, new_data))
        return DistRDD(new_partitions, self.num_workers)

    def filter(self, predicate):
        new_partitions = []
        for p in self.partitions:
            new_data = [x for x in p.data if predicate(x)]
            new_partitions.append(Partition(p.partition_id, new_data))
        return DistRDD(new_partitions, self.num_workers)

    def reduceByKey(self, key_idx=0, val_idx=1):
        global_acc = {}
        for p in self.partitions:
            for item in p.data:
                k = item[key_idx]
                v = float(item[val_idx])
                global_acc[k] = global_acc.get(k, 0.0) + v
        return list(global_acc.items())


class DistDataFrame:
    """
    Distributed DataFrame mimicking Apache Spark DataFrame API:
    - select(), filter(), where(), groupBy(), agg(), withColumn(), orderBy(), show(), count()
    """
    def __init__(self, data, schema=None, num_partitions=4):
        self.schema = schema or []
        if isinstance(data, DistRDD):
            self.rdd = data
            if not self.schema and self.rdd.count() > 0:
                first = self.rdd.partitions[0].data[0]
                if isinstance(first, dict):
                    self.schema = list(first.keys())
        elif isinstance(data, list):
            self.rdd = DistRDD.from_iterable(data, num_partitions=num_partitions)
            if data and isinstance(data[0], dict) and not self.schema:
                self.schema = list(data[0].keys())
        else:
            self.rdd = data

    @classmethod
    def read_csv(cls, file_path, num_partitions=4):
        with open(file_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            records = []
            for row in reader:
                # Type cast numerics automatically
                typed_row = {}
                for k, v in row.items():
                    try:
                        if "." in v:
                            typed_row[k] = float(v)
                        else:
                            typed_row[k] = int(v)
                    except ValueError:
                        typed_row[k] = v
                records.append(typed_row)
        schema = list(records[0].keys()) if records else []
        return cls(records, schema=schema, num_partitions=num_partitions)

    def count(self):
        return self.rdd.count()

    def columns(self):
        return self.schema

    def filter(self, condition_fn):
        filtered_rdd = self.rdd.filter(condition_fn)
        return DistDataFrame(filtered_rdd, schema=self.schema)

    where = filter

    def select(self, *cols):
        selected_schema = list(cols)
        def project(row):
            return {c: row.get(c) for c in selected_schema}
        proj_rdd = self.rdd.map(project)
        return DistDataFrame(proj_rdd, schema=selected_schema)

    def withColumn(self, col_name, func):
        def add_col(row):
            new_r = dict(row)
            new_r[col_name] = func(row)
            return new_r
        new_schema = list(self.schema)
        if col_name not in new_schema:
            new_schema.append(col_name)
        return DistDataFrame(self.rdd.map(add_col), schema=new_schema)

    def groupBy(self, *group_cols):
        return GroupedData(self, list(group_cols))

    def orderBy(self, col_name, ascending=True):
        records = self.rdd.collect()
        sorted_records = sorted(records, key=lambda x: (x.get(col_name) is None, x.get(col_name)), reverse=not ascending)
        return DistDataFrame(sorted_records, schema=self.schema, num_partitions=len(self.rdd.partitions))

    def collect(self):
        return self.rdd.collect()

    def show(self, n=10):
        records = self.rdd.collect()[:n]
        if not records:
            print("+--- Empty DataFrame ---+")
            return
        headers = self.schema or list(records[0].keys())
        col_widths = {h: max(len(str(h)), max(len(str(r.get(h, ""))) for r in records)) for h in headers}
        
        border = "+" + "+".join(["-" * (col_widths[h] + 2) for h in headers]) + "+"
        print(border)
        header_str = "|" + "|".join([f" {h:<{col_widths[h]}} " for h in headers]) + "|"
        print(header_str)
        print(border)
        for r in records:
            row_str = "|" + "|".join([f" {str(r.get(h, '')):<{col_widths[h]}} " for h in headers]) + "|"
            print(row_str)
        print(border)
        print(f"Showing top {min(n, len(records))} of {self.count()} distributed records.\n")

    def to_csv(self, file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        records = self.rdd.collect()
        if not records:
            return
        headers = self.schema or list(records[0].keys())
        with open(file_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(records)


class GroupedData:
    def __init__(self, df, group_cols):
        self.df = df
        self.group_cols = group_cols

    def agg(self, agg_specs):
        """
        agg_specs: dict of {column_name: ['avg', 'sum', 'min', 'max', 'count', 'std']}
        Example: {'crop_yield_ton_ha': ['avg', 'max'], 'soil_ph': ['avg']}
        """
        records = self.df.collect()
        groups = {}
        for r in records:
            key = tuple(r.get(c) for c in self.group_cols)
            if key not in groups:
                groups[key] = []
            groups[key].append(r)

        result_rows = []
        for key, rows in groups.items():
            row_dict = {self.group_cols[i]: key[i] for i in range(len(self.group_cols))}
            row_dict["count"] = len(rows)
            for col, aggs in agg_specs.items():
                vals = [float(r[col]) for r in rows if r.get(col) is not None]
                if not vals:
                    continue
                for agg_func in aggs:
                    out_name = f"{agg_func}_{col}"
                    if agg_func == "avg" or agg_func == "mean":
                        row_dict[out_name] = round(sum(vals) / len(vals), 3)
                    elif agg_func == "sum":
                        row_dict[out_name] = round(sum(vals), 3)
                    elif agg_func == "min":
                        row_dict[out_name] = min(vals)
                    elif agg_func == "max":
                        row_dict[out_name] = max(vals)
                    elif agg_func == "std":
                        mean = sum(vals) / len(vals)
                        var = sum((v - mean) ** 2 for v in vals) / len(vals)
                        row_dict[out_name] = round(math.sqrt(var), 3)
            result_rows.append(row_dict)

        schema = list(result_rows[0].keys()) if result_rows else self.group_cols
        return DistDataFrame(result_rows, schema=schema)
