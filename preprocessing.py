from os import *
from itertools import product
from random import sample
from psycopg2 import connect, sql
import random

import networkx as nx
from config.base import project_root

from functools import wraps
from typing import Any, Callable, List, Optional

import time
import matplotlib.pyplot as plt

from annotation import *
from queryplan import *


def parse(query):
    columns = []

    # # Replace all occurences of VARY(X) with x < $i, where i is the placeholder counter
    # while query.find("VARY") != -1:
    #     start = query.find("VARY(")
    #     subStr = query[start:]
    #     end = subStr.find(")")
    #     res = subStr[5:end] + " < (%f)"
    #     columns.append(subStr[5:end])
    #     query = query[:start] + res + query[start + end + 1 :]

    output = {"bounds": [], "query": query, "error": False, "error_message": ""}

    if not len(query):
        output["error"] = True
        output["error_message"] = "Query is empty."

    for column in columns:
        table = query_processor.find_table(column)
        temp_bounds = query_processor.find_bounds(column)
        valid_col = query_processor.is_col_numeric(table, column)

        if not table:
            output["error"] = True
            return output

        if not valid_col:
            output["error"] = True
            output["error_message"] = (
                "Invalid query. Column {} is non-numeric and cannot be varied."
            ).format(column)
            return output

        # create buckets using min max
        if not temp_bounds and table:
            temp_bounds = query_processor.find_alt_partitions(table, column)
            output["bounds"].append(temp_bounds)
            continue

        output["bounds"].append(temp_bounds)

    return output

def permutate(bounds: list, query: str):
    potential_plans = []
    perm_bounds = list(product(*bounds))

    for b in perm_bounds:
        potential_plans.append(query % tuple(map(float, b)))

    if len(potential_plans) > 100:
        return sample(potential_plans, 100)

    return potential_plans


class QueryProcessor:
    def __init__(self):
        self.conn = self.set_up_db_connection()
        self.cursor = self.conn.cursor()

    def set_up_db_connection(self):
        return connect(
            dbname=os.getenv("POSTGRES_DBNAME"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )

    def wrap_single_transaction(func):
        # decorator to wrap a function to create a new cursor per function call
        @wraps(func)
        def inner_func(self, *args, **kwargs):
            try:
                self.cursor = self.conn.cursor()
                ans = func(self, *args, **kwargs)
                self.conn.commit()
                return ans
            except Exception as err:
                print(f"Exception encountered, rolling back: {err}")
                self.conn.rollback()

        return inner_func

    def tear_down_db_connection(self):
        self.conn.close()
        self.cursor.close()

    @wrap_single_transaction
    def explain(self, query: str) -> QueryPlan:
        # get execution plan of statement from postgresql
        self.cursor.execute("EXPLAIN (FORMAT JSON) " + query)
        plan = self.cursor.fetchall()
        query_plan_dict: dict = plan[0][0][0]["Plan"]
        return QueryPlan(query_plan_dict, query)

    def topNplans(
        self, plans: List[str], topN: int, **kwargs
    ) -> List[QueryPlan]:
        # select top K plans from
        query_plans = [self.explain(plan) for plan in plans]
        unique_query_plans = []
        seen_query_plans = set()
        for query_plan in query_plans:
            if query_plan not in seen_query_plans:
                unique_query_plans.append(query_plan)
                seen_query_plans.add(query_plan)
        return sorted(unique_query_plans, **kwargs)[:topN]

    @wrap_single_transaction
    def find_table(self, column: str) -> str:
        # Find table names that the columns can be found in
        findTableQuery = sql.SQL(
            """
            SELECT t.table_schema, t.table_name FROM information_schema.tables t
            inner join information_schema.columns c on c.table_name = t.table_name
            and c.table_schema = t.table_schema where c.column_name = %s
            and t.table_schema not in ('information_schema', 'pg_catalog')
            and t.table_type = 'BASE TABLE' order by t.table_schema;
            """
        )

        self.cursor.execute(findTableQuery, [column])

        result = self.cursor.fetchall()

        # Nothing found
        if len(result) == 0:
            return None

        table = result[0][1]

        return table

    @wrap_single_transaction
    def is_col_numeric(self, table: str, column: str) -> bool:
        # check if column data type is numeric within table
        if not table:
            return False

        col_datatype_query = sql.SQL(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_name = %s
            and column_name = %s;
            """
        )

        self.cursor.execute(col_datatype_query, [table, column])
        col_datatype = self.cursor.fetchall()

        if len(col_datatype) == 0:
            return False

        return col_datatype[0][0] == "numeric"

    @wrap_single_transaction
    def find_bounds(self, column: str) -> list:
        # get the bounds of histogram from pg_stats with 10 buckets
        table = self.find_table(column)

        if not table:
            return None

        # analyze column with psycopg2
        col_query = (
            "SELECT * FROM pg_stats WHERE tablename='{}' and attname='{}'"
        ).format(table, column)
        self.cursor.execute(col_query)
        analyze_fetched = self.cursor.fetchall()[0]

        short_bounds = []
        # analyze_fetched[9] = histogram_bounds
        all_bounds = analyze_fetched[9]

        # all bounds empty
        if not all_bounds:
            return None

        # Get all bounds
        all_bounds = all_bounds[1:-1].split(",")
        inc = len(all_bounds) // 10
        for i in range(inc, len(all_bounds), inc):
            short_bounds.append(all_bounds[i])

        return short_bounds

    @wrap_single_transaction
    def find_alt_partitions(self, table: str, column: str) -> list:
        # used to manually find partition for histogram bounds when it cannot be queried directly from pg_stats
        col_query = (
            "SELECT * FROM pg_stats WHERE tablename='{}' and attname='{}'"
        ).format(table, column)
        self.cursor.execute(col_query)
        analyze_fetched = self.cursor.fetchall()[0]

        # analyze_fetched[6] = n_distict
        distinct = analyze_fetched[6]

        # skip bucket creation when there are less than 10 distinct values
        if 0 <= distinct <= 10:
            dist_query = ("SELECT DISTINCT {} FROM {};").format(column, table)
            self.cursor.execute(dist_query)
            distinct_vals = self.cursor.fetchall()

            return [val[0] for val in distinct_vals]
        # bucket creation to make 10 if there are less than 10 distinct values
        min_query = ("SELECT MIN({}) FROM {}").format(column, table)
        max_query = ("SELECT MAX({}) FROM {}").format(column, table)

        self.cursor.execute(min_query)
        min_result = self.cursor.fetchall()[0][0]

        self.cursor.execute(max_query)
        max_result = self.cursor.fetchall()[0][0]

        inc = (max_result - min_result) / 10
        return [min_result + inc * i for i in range(1, 11)]


query_processor = QueryProcessor()

