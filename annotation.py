'''
Some links:
https://gitlab.com/postgres/postgres/blob/master/src/include/nodes/plannodes.h
https://docs.gitlab.com/ee/development/understanding_explain_plans.html
'''

# Create class to define constants for use to format the redrered QEP annotation onto the web interface.
class FontFormat:
    BOLD_START = "<b>"
    BOLD_END = "</b>"
    ITALIC_START = "<em>"
    ITALIC_END = "</em>"

# Function to append the HTML tags to make the word(s) bold
def bold(string):
    return FontFormat.BOLD_START + string + FontFormat.BOLD_END

# Function to append the HTML tags to make the word(s) in italics
def italics(string):
    return FontFormat.ITALIC_START + string + FontFormat.ITALIC_END

# Default Annotation if none of the node types are identified in the annotation functions listed below
def defaultAnnotation(query_plan):
    return f"The {italics(query_plan['Node Type'])} operation is performed."

# Append
def appendAnnotation(query_plan):
    return f"The {italics(query_plan['Node Type'])} operation combines the results of the child sub-operations."

# Function Scan
def functionScanAnnotation(query_plan):
    return f"The function {italics(query_plan['Function Name'])} is executed and the set of records are returned."

# Limit
def limitAnnotation(query_plan):
    return f"The {italics(query_plan['Function Name'])} operation takes {bold(query_plan['Plan Rows'])} records and disregard the remaining records."

# Subquery Scan
def subqueryScanAnnotation(query_plan):
    return f"The {italics(query_plan['Node Type'])} operation reads on the results from a subquery."

# Values Scan
def valuesScanAnnotation(query_plan):
    return f"The {italics(query_plan['Node Type'])} operation reads the given constant values from the query."

# Materialize
def materializeAnnotation(query_plan):
    return f"The {italics(query_plan['Node Type'])} operation stores the results of child operations in memory for faster access by parent operations."

# Nested Loop
def nestedLoopAnnotation(query_plan):
    return f"The {italics(query_plan['Node Type'])} operation implements a join or lookup where the first child node is run once, then for every row it produces, its partner is looked up in the second node."

# Unique
def uniqueAnnotation(query_plan):
    return f"The {italics(query_plan['Node Type'])} operation removes duplicates from a sorted result set."

# Hash
def hashAnnotation(query_plan):
    return f"The {italics(query_plan['Node Type'])} function hashes the query rows into memory, for use by its parent operation."

# Gather Merge  
def gatherMergeAnnotation(query_plan):
    return f"The {italics(query_plan['Node Type'])} operation combines the output table from sub-operations by executing the operation in parallel."

# Aggregate
def aggregateAnnotation(query_plan):

    # Obtain strategy from query plan
    strategy = query_plan["Strategy"]

    # Sorted strategy
    if strategy == "Sorted":
        result = f"The {italics(query_plan['Node Type'])} operation sorts the tuples based on their keys, "

        # Obtain the attributes that the records are grouped by
        if "Group Key" in query_plan:
            result += f" where the tuples are {bold('aggregated')} by the following keys: "

            for key in query_plan["Group Key"]:
                result += bold(key) + ","
            
            result = result[:-1]
            result += "."

        # Get the filtered attribute and remove unnecessary strings
        if "Filter" in query_plan:
            result += f" where the tuples are filtered by {bold(query_plan['Filter'].replace('::text', ''))}."

        return result

    # Hashed strategy
    elif strategy == "Hashed":
        result = f"The {italics(query_plan['Node Type'])} operation {bold('hashes')} all rows based on these key(s): "

        # Obtain the attributes that the records are grouped by
        for key in query_plan["Group Key"]:

            # Remove unnecessary strings
            result += bold(key.replace("::text", "")) + ", "

        result += f"which are then {bold('aggregated')} into a bucket given by the hashed key."

        return result

    # Plain strategy
    elif strategy == "Plain":
        return f"The result is {bold('aggregated')} with the {italics(query_plan['Node Type'])} operation."

    # If the strategy value is neither of the above
    else:
        raise ValueError("Annotation does not work: " + strategy)

# CTE Scan
def cteScanAnnotation(query_plan):
    result = (f"The {italics(query_plan['Node Type'])} operation is performed on the table {bold(str(query_plan['CTE Name']))} which the results are stored in memory for use later. ")

    # Get the index condition and remove unnecessary strings
    if "Index Cond" in query_plan:
        result += " The condition(s) are " + bold(query_plan["Index Cond"].replace("::text", ""))

    # Get the filtered attribute and remove unnecessary strings
    if "Filter" in query_plan:
        result += " and then further filtered by " + bold(query_plan["Filter"].replace("::text", ""))

    result += "."
    return result

# Group
def groupAnnotation(query_plan):
    result = f"The {italics(query_plan['Node Type'])} operation groups the results from the previous operation together with the following keys: "

    # Obtain the attributes that the records are grouped by
    for i, key in enumerate(query_plan["Group Key"]):

        # Remove unnecessary strings
        result += bold(key.replace("::text", ""))

        # Condition checks to put comma or full stop after attribute
        if i == len(query_plan["Group Key"]) - 1:
            result += "."
        else:
            result += ", "
    
    return result

# Index Scan
def indexScanAnnotation(query_plan):
    result = (f"The {italics(query_plan['Node Type'])} operation scans the index for rows")

    # Get the index condition and remove unnecessary strings
    if "Index Cond" in query_plan:
        result += " which match the following conditions: " + bold(query_plan["Index Cond"].replace("::text", ""))
    
    result += f", and then reads the records from the table that match the conditions."

    # Get the filtered attribute and remove unnecessary strings
    if "Filter" in query_plan:
        result += f" The result is further filtered by {bold(query_plan['Filter'].replace('::text', ''))}."
    
    return result

# Index-Only Scan
def index_onlyScanAnnotation(query_plan):
    result = f"The {italics(query_plan['Node Type'])} function is conducted using an index table {bold(query_plan['Index Name'])}"

    # Get the index condition and remove unnecessary strings
    if "Index Cond" in query_plan:
        result += " with condition(s) " + bold(query_plan["Index Cond"].replace("::text", ""))

    result += ". The records obtained from the index table is returned as the result."
    
    # Get the filtered attribute and remove unnecessary strings
    if "Filter" in query_plan:
        result += f" The result is further filtered by {bold(query_plan['Filter'].replace('::text', ''))}."

    return result

# Merge Join
def mergeJoinAnnotation(query_plan):
    result = f"The {italics(query_plan['Node Type'])} operation joins the results that have been sorted on join keys from sub-operations"

    # Get the merge condition and remove unnecessary strings
    if "Merge Cond" in query_plan:
        result += " with condition " + bold(query_plan['Merge Cond'].replace("::text", ""))

    # Check the join type
    if "Join Type" == "Semi":
        result += " but only the records from the left relation is returned as the result"
        
    result += "."

    return result

# SetOp
def SetOpAnnotation(query_plan):
    result = f"The {italics(query_plan['Node Type'])} operation finds the "

    # Get the command
    command = bold(str(query_plan["Command"]))

    # SQL 'Except' command
    if command == "Except" or command == "Except All":
        result += "differences"

    # SQL 'Intercept' command
    else:
        result += "similarities"

    result += " in records between the two previously scanned tables."

    return result

# Sequential Scan
def sequentialScanAnnotation(query_plan): 
    result = f"The {italics(query_plan['Node Type'])} operation performs a scan on relation "

    # Get the relation name from query plan
    if "Relation Name" in query_plan:
        result += bold(query_plan["Relation Name"])

    # Get the alias from query plan if it is not the same as relation name
    if "Alias" in query_plan:
        if query_plan["Relation Name"] != query_plan["Alias"]:
            result += f" with an alias of {bold(query_plan['Alias'])}"

    # Get the filtered attribute and remove unnecessary strings
    if "Filter" in query_plan:
        result += f" and then filtered with the condition {bold(query_plan['Filter'].replace('::text', ''))}"

    result += "."

    return result

# Sort
def sortAnnotation(query_plan):
    result = f"The {italics(query_plan['Node Type'])} operation sorts the rows "

    # If the specified sort key is DESC
    if "DESC" in query_plan["Sort Key"]:
        result += (bold(str(query_plan["Sort Key"].replace("DESC", "")))+ " in descending order")

    # If the specified sort key is INC
    elif "INC" in query_plan["Sort Key"]:
        result += (bold(str(query_plan["Sort Key"].replace("INC", "")))+ " in increasing order")

    # Otherwise specify the attribute
    else:
        result += f"based on {bold(str(query_plan['Sort Key']))}"

    result += "."

    return result

 # Hash Join
def hashJoinAnnotation(query_plan):
    result = f"The {italics(query_plan['Node Type'])} operation joins the results from the previous operations using a hash {bold(query_plan['Join Type'])} {bold('Join')}"

    # Get the hash condition and remove unnecessary strings
    if "Hash Cond" in query_plan:
        result += f" on the condition: {bold(query_plan['Hash Cond'].replace('::text', ''))}"

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
        "Gather Merge": gatherMergeAnnotation,
    }

if __name__ == "__main__":

    # For testing only
    query_plan = {'Node Type': 'Values Scan'}
    annotation = Annotation().annotation_dict.get(query_plan['Node Type'], defaultAnnotation(query_plan))(query_plan)
    print(annotation)