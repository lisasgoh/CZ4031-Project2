from flask import Flask, redirect, render_template, request, url_for

import config.base
from preprocessing import *
from annotation import *

app = Flask(__name__)

# link for home page
@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")


@app.route("/result", methods=["POST", "GET"])
def explain():
    if request.method == "GET":
        return redirect("/")

    query = request.form["queryText"]
    output = parse(query)

    if output["error"]:
        error = "Query is invalid."

        if output["error_message"]:
            error = output["error_message"]

        html_context = {
            "query": error,
            "explanation_1": [error],
            "explanation_2": [error],
            "explanation_3": [error],
            "bounds": [error],
        }

        return render_template("index.html", **html_context)

    # get the 10 bounds
    bounds = output["bounds"]
    processed_query = output["query"]
    queries = permutate(bounds, processed_query)
    top_plans_by_cost = query_processor.topNplans(
        queries, topN=3, key=lambda x: x.calculate_total_cost()
    )

    graph_file_name = [None for ix in range(3)]
    total_costs = [None for ix in range(3)]
    total_plan_rows = [None for ix in range(3)]
    explanations = [None for ix in range(3)]
    seq_scans = [None for ix in range(3)]
    index_scans = [None for ix in range(3)]
    for ix, plan in enumerate(top_plans_by_cost):
        graph_file_name[ix] = plan.save_graph_file()
        explanations[ix] = plan.create_explanation(plan.root)
        total_costs[ix] = int(plan.calculate_total_cost())
        total_plan_rows[ix] = int(plan.calculate_plan_rows())
        seq_scans[ix] = int(plan.calculate_num_nodes("Seq Scan"))
        index_scans[ix] = int(plan.calculate_num_nodes("Index Scan"))

    # clean_up_static_dir(graph_file_name)

    html_context = {
        "query": query,
        "bounds": bounds,
        "graph_1": graph_file_name[0],
        "graph_2": graph_file_name[1],
        "graph_3": graph_file_name[2],
        "explanation_1": explanations[0],
        "explanation_2": explanations[1],
        "explanation_3": explanations[2],
        "total_cost_1": total_costs[0],
        "total_cost_2": total_costs[1],
        "total_cost_3": total_costs[2],
        "total_plan_rows_1": total_plan_rows[0],
        "total_plan_rows_2": total_plan_rows[1],
        "total_plan_rows_3": total_plan_rows[2],
        "total_seq_scan_1": seq_scans[0],
        "total_seq_scan_2": seq_scans[1],
        "total_seq_scan_3": seq_scans[2],
        "total_index_scan_1": index_scans[0],
        "total_index_scan_2": index_scans[1],
        "total_index_scan_3": index_scans[2],
    }

    return render_template("index.html", **html_context)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
