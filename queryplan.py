import os
import time
from typing import List

import matplotlib.pyplot as plt
import networkx as nx
from config.base import project_root
from annotation import *

class Node:
    def __init__(self, query_plan):
        self.plans = []
        for key in query_plan:
            setattr(self,  key.lower().replace(" ", "_"), query_plan.get(key))
        explainer = Annotation.annotation_dict.get(self.node_type, defaultAnnotation)
        self.explanation = explainer(query_plan)

    def __str__(self):
        return f"{self.node_type}\ncost: {self.total_cost}"

class QueryPlan:
    def __init__(self, query_json, raw_query):
        self.graph = nx.DiGraph()
        self.root = Node(query_json)
        self.construct_graph(self.root)
        self.total_cost = self.calculate_total_cost()
        self.plan_rows = self.calculate_plan_rows()
        self.num_seq_scan_nodes = self.calculate_num_nodes("Seq Scan")
        self.num_index_scan_nodes = self.calculate_num_nodes("Index Scan")
        self.explanation = self.create_explanation(self.root)

    def construct_graph(self, root):
        self.graph.add_node(root)
        for child in root.plans:
            child_node = Node(child)
            self.graph.add_edge(root, child_node) 
            self.construct_graph(child_node)

    def create_explanation(self, node: Node):
        result = []
        for child in self.graph[node]:
            result += self.create_explanation(child)
        result += [node.explanation]
        return result

    def serialize_graph_operation(self) -> str:
        node_list = [self.root.node_type]
        for start, end in nx.edge_bfs(self.graph, self.root):
            node_list.append(end.node_type)
        return "#".join(node_list)

    def calculate_num_nodes(self, node_type: str) -> int:
        num_nodes = 0
        for node in self.graph.nodes:
            if node.node_type == node_type:
                num_nodes += 1
        return num_nodes
    
    def calculate_plan_rows(self) -> int:
        plan_rows = 0
        for node in self.graph.nodes:
            plan_rows += node.plan_rows
        return plan_rows


    def calculate_total_cost(self) -> int:
        total_cost = 0
        for node in self.graph.nodes:
            total_cost += node.total_cost
        return total_cost

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
