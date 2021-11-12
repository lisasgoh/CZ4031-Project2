from os import *
from psycopg2 import connect, sql
from config.base import project_root
from functools import wraps
from annotation import *
from interface import *


def validate(query):
    """Check if the query is valid.

    Args:
        query (string): Query string that was entered by the user.

    Returns:
        dict: Output dict consisting of error status and error message.
    """
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
        """Establishes connection with PostgreSQL database.

        Returns:
            connection: Connection to the database.
        """
        return connect(
            dbname=os.getenv("POSTGRES_DBNAME"),
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )

    def wrap_single_transaction(func):
        """Decorator to create cursor each time the function is called.

        Args:
            func (function): Function to be wrapped

        Returns:
            function: Wrapped function
        """
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
        """Retrives execution plan of statement from PostgreSQL

        Args:
            query (str): Query string that was entered by the user.

        Returns:
            QueryPlan: An object consisting of all the necessary information in the QEP
            to be displayed to the user.
        """
        self.cursor.execute("EXPLAIN (FORMAT JSON) " + query)
        plan = self.cursor.fetchall()
        query_plan_dict: dict = plan[0][0][0]["Plan"]
        return QueryPlan(query_plan_dict)

    @wrap_single_transaction
    def query_valid(self, query: str):
        """Validate query by trying to fetch a single row from the result set.

        Args:
            query (str): Query string

        Returns:
            bool: Whether the query is valid.
        """
        self.cursor.execute(query)
        try:
            self.cursor.fetchone()
        except:
            return False
        return True


query_processor = QueryProcessor()
