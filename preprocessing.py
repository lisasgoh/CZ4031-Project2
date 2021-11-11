from os import *
from psycopg2 import connect, sql
from config.base import project_root
from functools import wraps
from annotation import *
from interface import *


def check(query):
    # function to check if query is valid
    output = {"query": query, "error": False, "error_message": ""}

    if not len(query):
        output["error"] = True
        output["error_message"] = "Query is empty."

    if not query_processor.query_valid(query):
        output["error"] = True
        output["error_message"] = "Query is invalid."
        return output

    return output

class QueryProcessor:
    def __init__(self):
        self.conn = self.start_db_connection()
        self.cursor = self.conn.cursor()

    def start_db_connection(self):
        return connect(
            dbname=os.getenv("POSTGRES_DBNAME"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )

    def wrap_single_transaction(func):
        # decorator to create cursor each time function is called
        @wraps(func)
        def inner_func(self, *args, **kwargs):
            try:
                self.cursor = self.conn.cursor()
                ans = func(self, *args, **kwargs)
                self.conn.commit()
                return ans
            except Exception as error:
                print(f"Exception encountered, rolling back: {error}")
                self.conn.rollback()

        return inner_func

    def stop_db_connection(self):
        self.conn.close()
        self.cursor.close()

    @wrap_single_transaction
    def explain(self, query: str) -> QueryPlan:
        # get execution plan of statement from postgresql
        self.cursor.execute("EXPLAIN (FORMAT JSON) " + query)
        plan = self.cursor.fetchall()
        query_plan_dict: dict = plan[0][0][0]["Plan"]
        return QueryPlan(query_plan_dict, query)

    @wrap_single_transaction
    def query_valid(self, query: str):
        # check if query is valid
        output = True
        self.cursor.execute(query)
        try:
            result = self.cursor.fetchone()
        except:
            output = False
        return output


query_processor = QueryProcessor()

