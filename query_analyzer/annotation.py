'''
Some links:
https://gitlab.com/postgres/postgres/blob/master/src/include/nodes/plannodes.h
https://docs.gitlab.com/ee/development/understanding_explain_plans.html
'''

class Color:
    BOLD = "<b>"
    END = "</b>"

def bold_string(string):
    return Color.BOLD + string + Color.END

def defaultAnnotation(query_plan):
    return f"The {bold_string(query_plan['Node Type'])} operation is performed."

def appendAnnotation(query_plan): # Append
    return f"The {query_plan['Node Type']} operation combines the results of the child sub-operations."

def functionScanAnnotation(query_plan): # Function Scan
    return f"The function {query_plan['Function Name']} is executed and the set of records are returned."

def limitAnnotation(query_plan): # Limit
    return f"The Limit operation takes {query_plan['Plan Rows']} records and disregard the remaining records."

def subqueryScanAnnotation(query_plan): # Subquery Scan
    return f"The {bold_string(query_plan['Node Type'])} operation reads on results from a subquery."

def valuesScanAnnotation(query_plan): # Values Scan
    return f"The {bold_string(query_plan['Node Type'])} operation reads the given constant values from the query."

def materializeAnnotation(query_plan): # Materialize
    return f"{query_plan['Node Type']} operation stores the results of child operations in memory for faster access by parent operations."

def nestedLoopAnnotation(query_plan): # Nested Loop
    return f"The {query_plan['Node Type']} operation implements a join where the first child node is run once, then for every row it produces, its partner is looked up in the second node."
    # return f"The join results between the {bold_string('nested loop')} scans of the suboperations are returned as new rows."

def uniqueAnnotation(query_plan):  # Unique
    return f"The {query_plan['Node Type']} operation removes duplicates from a sorted result set."

def hashAnnotation(query_plan): # Hash
    return f"The {query_plan['Node Type']} function hashes the query rows into memory, for use by its parent operation"

def aggregateAnnotation(query_plan): # Aggregate
    # For plans of the aggregate type: SortAggregate, HashAggregate, PlainAggregate
    strategy = query_plan["Strategy"]

    if strategy == "Sorted":
        result = f"The {query_plan['Node Type']} operation sorts the tuples based on their keys, "

        if "Group Key" in query_plan:
            result += f" where the tuples are {bold_string('aggregated')} by the following keys: "

            for key in query_plan["Group Key"]:
                result += bold_string(key) + ","
            
            result = result[:-1]
            result += "."

        if "Filter" in query_plan:
            result += f" where the tuples are filtered by {bold_string(query_plan['Filter'].replace('::text', ''))}."

        return result

    elif strategy == "Hashed":
        result = f"The {query_plan['Node Type']} operation hashes all rows based on these key(s): "

        for key in query_plan["Group Key"]:
            result += bold_string(key.replace("::text", "")) + ", "

        result += f"which are then {bold_string('aggregated')} into a bucket given by the hashed key."

        return result

    elif strategy == "Plain":
        return f"The result is {bold_string('aggregated')} with the {query_plan['Node Type']} operation."

    else:
        raise ValueError("Annotation does not work: " + strategy)

def cteScanAnnotation(query_plan): # CTE Scan
    result = (f"The {query_plan['Node Type']} operation is performed on the table {bold_string(str(query_plan['CTE Name']))} which the results are stored in memory for use later. ")

    if "Index Cond" in query_plan:
        result += " The condition(s) are " + bold_string(query_plan["Index Cond"].replace("::text", ""))

    if "Filter" in query_plan:
        result += " and then filtered by " + bold_string(
            query_plan["Filter"].replace("::text", "")
        )

    result += "."
    return result

def groupAnnotation(query_plan): # Group
    result = f"The {query_plan['Node Type']} operation groups the results from the previous operation together with the following keys: "

    for i, key in enumerate(query_plan["Group Key"]):
        result += bold_string(key.replace("::text", ""))
        if i == len(query_plan["Group Key"]) - 1:
            result += "."
        else:
            result += ", "
    
    return result

def indexScanAnnotation(query_plan): # Index Scan
    result = (f"The {query_plan['Node Type']} operation is performed using an index table {bold_string(query_plan['Index Name'])}")

    if "Index Cond" in query_plan:
        result += " with the following conditions: " + bold_string(query_plan["Index Cond"].replace("::text", ""))
    
    result += f", and the {query_plan['Relation Name']} table and fetches rows pointed by indices matched in the scan."

    if "Filter" in query_plan:
        result += (f"The result is then filtered by {bold_string(query_plan['Filter'].replace('::text', ''))}.")
    
    return result

def index_onlyScanAnnotation(query_plan):
    result = "The {query_plan['Node Type']} function is conducted with an index table " + bold_string(query_plan["Index Name"])

    if "Index Cond" in query_plan:
        result += " with condition(s) " + bold_string(query_plan["Index Cond"].replace("::text", ""))

    result += ". It then returns the matches found in index table scan as the result."
    
    if "Filter" in query_plan:
        result += (f" The result is then filtered by {bold_string(query_plan['Filter'].replace('::text', ''))}.")

    return result

def mergeJoinAnnotation(query_plan): # Merge Join
    result = f"The {query_plan['Node Type']} operation joins the results from sub-operations"

    if "Merge Cond" in query_plan:
        result += " with condition " + bold_string(query_plan["Merge Cond"].replace("::text", ""))

    if "Join Type" == "Semi":
        result += " but only the row from the left relation is returned"

    result += "."

    return result

def SetOpAnnotation(query_plan): # SetOp
    result = f"The {query_plan['Node Type']} operation finds the "
    command = bold_string(str(query_plan["Command"]))

    if command == "Except" or command == "Except All":
        result += "differences "

    else:
        result += "similarities "

    result += f"between the two previously scanned tables."

    return result

def sequentialScanAnnotation(query_plan): # Sequential Scan
    result = f"The {query_plan['Node Type']} operation performs a scan on relation "

    if "Relation Name" in query_plan:
        result += bold_string(query_plan["Relation Name"])

    if "Alias" in query_plan:
        if query_plan["Relation Name"] != query_plan["Alias"]:
            result += f" with an alias of {query_plan['Alias']}"

    if "Filter" in query_plan:
        result += f" and filtered with the condition {query_plan['Filter'].replace('::text', '')}"

    result += "."

    return result

def sortAnnotation(query_plan): # Sort
    result = f"The {query_plan['Node Type']} operation sorts the rows "

    if "DESC" in query_plan["Sort Key"]:
        result += (
            bold_string(str(query_plan["Sort Key"].replace("DESC", "")))+ " in descending order")

    elif "INC" in query_plan["Sort Key"]:
        result += (bold_string(str(query_plan["Sort Key"].replace("INC", "")))+ " in increasing order")

    else:
        result += bold_string(str(query_plan["Sort Key"]))

    result += "."

    return result

def hashJoinAnnotation(query_plan): # Hash Join
    result = f"The {query_plan['Node Type']} operation joins the results from the previous operations using a hash {bold_string(query_plan['Join Type'])} {bold_string('Join')}"

    if "Hash Cond" in query_plan:
        result += f" on the condition: {query_plan['Hash Cond'].replace('::text', '')}"

    result += "."

    return result

class Annotation(object):
    '''
    List of possible node types based on this source: 
    https://www.pgmustard.com/docs/explain
    '''

    annotation_dict = {
        "Aggregate": aggregateAnnotation,
        "Append": appendAnnotation,
        "CTE Scan": cteScanAnnotation,
        "Function Scan": functionScanAnnotation,
        "Group": groupAnnotation,
        "Index Scan": indexScanAnnotation,
        "Index Only Scan": index_onlyScanAnnotation,
        "Limit": limitAnnotation,
        "Materialize": materializeAnnotation,
        "Unique": uniqueAnnotation,
        "Merge Join": mergeJoinAnnotation,
        "SetOp": SetOpAnnotation,
        "Subquery Scan": subqueryScanAnnotation,
        "Values Scan": valuesScanAnnotation,
        "Seq Scan": sequentialScanAnnotation,
        "Nested Loop": nestedLoopAnnotation,
        "Sort": sortAnnotation,
        "Hash": hashAnnotation,
        "Hash Join": hashJoinAnnotation,
    }

if __name__ == "__main__":

    # For testing only
    query_plan = {'Node Type': 'Values Scan'}
    annotation = Annotation().annotation_dict.get(query_plan['Node Type'], defaultAnnotation(query_plan))(query_plan)
    print(annotation)