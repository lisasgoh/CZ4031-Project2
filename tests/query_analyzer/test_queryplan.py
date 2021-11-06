import unittest

from query_analyzer.queryplan import QueryPlan


class TestQueryPlan(unittest.TestCase):
    def setup(self):
        self.qep_nested_json = {
            "Node Type": "T",
            "Total Cost": 10,
            "Plans": [
                {
                    "Node Type": "T1A",
                    "Total Cost": 100,
                    "Plans": [
                        {"Node Type": "T1B", "Total Cost": 200, "Plans": []}
                    ],
                },
                {"Node Type": "T2", "Total Cost": 20, "Plans": []},
            ],
        }
        self.qep_single_json = {
            "Node Type": "T",
            "Total Cost": 10,
            "Plans": [],
        }
        self.qep_nested = QueryPlan(self.qep_nested_json, None)
        self.qep_single = QueryPlan(self.qep_single_json, None)

    def test_graph_creation(self):
        self.assertEqual(len(self.qep_nested.graph.nodes), 4)
        self.assertEqual(len(self.qep_nested.graph.edges), 3)

    def test_graph_single_node_creation(self):
        self.assertEqual(len(self.qep_single.graph.nodes), 1)
        self.assertEqual(len(self.qep_single.graph.edges), 0)

    def test_cost_computation(self):
        self.assertEqual(self.qep_nested.calculate_total_cost(), 330)
        self.assertEqual(self.qep_single.calculate_total_cost(), 10)
