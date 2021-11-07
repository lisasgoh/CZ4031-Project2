import os
import time
from typing import List

import matplotlib.pyplot as plt
import networkx as nx
from config.base import project_root

from query_analyzer.explainer import Explainer
from query_analyzer.explainers.default_explain import default_explain


class Node:
    def __init__(self, query_plan):
        self.plans = []
        for key in query_plan:
            setattr(self,  key.lower().replace(" ", "_"), query_plan.get(key))
        explainer = Explainer.explainer_map.get(self.node_type, default_explain)
        self.explanation = explainer(query_plan)
        print("Query plan", query_plan)

    def __str__(self):
        name_string = f"{self.node_type}\ncost: {self.total_cost}"
        return name_string

class QueryPlan:
    """
    A query plan is a directed graph made up of several Nodes
    """

    def __init__(self, query_json, raw_query):
        self.graph = nx.DiGraph()
        self.root = Node(query_json)
        self._construct_graph(self.root)
        self.raw_query = raw_query

    def _construct_graph(self, cur_node):
        self.graph.add_node(cur_node)

        for child in cur_node.plans:
            child_node = Node(child)
            self.graph.add_edge(cur_node, child_node) 
             # add both curr_node and child_node if not present in graph
            self._construct_graph(child_node)

    def serialize_graph_operation(self) -> str:
        node_list = [self.root.node_type]
        for start, end in nx.edge_bfs(self.graph, self.root):
            node_list.append(end.node_type)
        return "#".join(node_list)

    def calculate_total_cost(self):
        total_cost = 0
        for node in self.graph.nodes:
            total_cost += node.total_cost
        return total_cost

    def calculate_plan_rows(self):
        plan_rows = 0
        for node in self.graph.nodes:
            plan_rows += node.plan_rows
        return plan_rows

    def calculate_num_nodes(self, node_type: str):
        node_count = 0
        for node in self.graph.nodes:
            if node.node_type == node_type:
                node_count += 1
        return node_count

    def save_graph_file(self):
        graph_name = f"graph_{str(time.time())}.png"
        filename = os.path.join(project_root, "static", graph_name)
        plot_formatter_position = get_tree_node_pos(self.graph, self.root)
        node_labels = {x: str(x) for x in self.graph.nodes}
        nx.draw(
            self.graph,
            plot_formatter_position,
            with_labels=True,
            labels=node_labels,
            font_size=6,
            node_size=300,
            node_color="skyblue",
            node_shape="s",
            alpha=1,
        )
        plt.savefig(filename)
        plt.clf()
        return graph_name

    def create_explanation(self, node: Node):
        # if not node.has_children:
        #     return [node.explanation]
        # else:
        result = []
        for child in self.graph[node]:
            result += self.create_explanation(child)
        result += [node.explanation]
        return result

    def __eq__(self, obj):
        return (
            isinstance(obj, QueryPlan)
            and obj.serialize_graph_operation()
            == self.serialize_graph_operation()
        )

    def __hash__(self):
        """Overrides the default implementation"""
        return hash(self.serialize_graph_operation())

def get_tree_node_pos(
    G, root=None, width=1.0, height=1, vert_gap=0.1, vert_loc=0, xcenter=0.5
):

    """
    From Joel's answer at https://stackoverflow.com/a/29597209/2966723.
    Licensed under Creative Commons Attribution-Share Alike

    If the graph is a tree this will return the positions to plot this in a
    hierarchical layout.

    G: the graph (must be a tree)

    root: the root node of current branch
    - if the tree is directed and this is not given,
      the root will be found and used
    - if the tree is directed and this is given, then
      the positions will be just for the descendants of this node.
    - if the tree is undirected and not given,
      then a random choice will be used.

    width: horizontal space allocated for this branch - avoids overlap with other branches

    vert_gap: gap between levels of hierarchy

    vert_loc: vertical location of root

    xcenter: horizontal location of root
    """
    if not nx.is_tree(G):
        raise TypeError(
            "cannot use hierarchy_pos on a graph that is not a tree"
        )

    if root is None:
        if isinstance(G, nx.DiGraph):
            root = next(
                iter(nx.topological_sort(G))
            )  # allows back compatibility with nx version 1.11
        else:
            root = random.choice(list(G.nodes))

    path_dict = dict(nx.all_pairs_shortest_path(G))
    max_height = 0
    for value in path_dict.values():
        max_height = max(max_height, len(value))
    vert_gap = height / max_height

    def _hierarchy_pos(
        G,
        root,
        width,
        vert_gap,
        vert_loc,
        xcenter,
        pos=None,
        parent=None,
        min_dx=0.05,
    ):
        """
        see hierarchy_pos docstring for most arguments

        pos: a dict saying where all nodes go if they have been assigned
        parent: parent of this branch. - only affects it if non-directed

        """

        if pos is None:
            pos = {root: (xcenter, vert_loc)}
        else:
            pos[root] = (xcenter, vert_loc)
        children = list(G.neighbors(root))
        if not isinstance(G, nx.DiGraph) and parent is not None:
            children.remove(parent)
        if len(children) != 0:
            dx = max(min_dx, width / len(children))
            nextx = xcenter - width / 2 - max(min_dx, dx / 2)
            for child in children:
                nextx += dx
                pos = _hierarchy_pos(
                    G,
                    child,
                    width=dx,
                    vert_gap=vert_gap,
                    vert_loc=vert_loc - vert_gap,
                    xcenter=nextx,
                    pos=pos,
                    parent=root,
                )
        return pos

    return _hierarchy_pos(G, root, width, vert_gap, vert_loc, xcenter)
