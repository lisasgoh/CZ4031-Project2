from flask import Flask, redirect, render_template, request, url_for

import config.base
from preprocessing import *
from annotation import *

app = Flask(__name__)

# GET endpoint for '/'
@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")


# GET and POST endpoint for '/result'
@app.route("/result", methods=["POST", "GET"])
def explain():
    if request.method == "GET":
        return redirect("/")

    query = request.form["queryText"]
    output = validate(query)

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
        "graph": plan.save_graph_file(),
        "explanation": plan.explanation,
        "total_cost": int(plan.total_cost),
        "total_plan_rows": int(plan.plan_rows),
        "total_seq_scan": int(plan.num_seq_scan_nodes),
        "total_index_scan": int(plan.num_index_scan_nodes),
    }

    return render_template("index.html", **html_context)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
