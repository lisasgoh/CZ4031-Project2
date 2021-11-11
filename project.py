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
    output = check(query)

    if output["error"]:
        error = "Query is invalid."

        if output["error_message"]:
            error = output["error_message"]

        html_context = {
            "query": error,
            "explanation_1": [error],
        }

        return render_template("index.html", **html_context)

    plan = query_processor.explain(output["query"])

    html_context = {
        "query": query,
        "graph_1": plan.save_graph_file(),
        "explanation_1": plan.create_explanation(plan.root),
        "total_cost_1": int(plan.calculate_total_cost()),
        "total_plan_rows_1": int(plan.calculate_plan_rows()),
        "total_seq_scan_1": int(plan.calculate_num_nodes("Seq Scan")),
        "total_index_scan_1": int(plan.calculate_num_nodes("Index Scan"))
    }

    return render_template("index.html", **html_context)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
