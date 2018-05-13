# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import json

def aggregate(cuisine):
    name = cuisine[0]
    score = 0
    neg_rev = 0
    reviews = list(cuisine[1])
    total_rev = len(reviews)
    for v in reviews:
      if v["evaluation"] == "Positive":
        score += v["points"]
      elif v["evaluation"] == "Negative":
        score -= v["points"]
        neg_rev += 1
    avg_pnts = float(score)/total_rev
    return (name,(total_rev, neg_rev, score, avg_pnts))
  
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, result_dir, percentage_f):
  # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(result_dir, True)
    # Read in the json files from dir
    input_rdd = spark.read.json(dataset_dir).rdd
    # persist rdd in memory to be re-used
    input_rdd.persist()
    # Group all reviews by cuisine
    grouped_rdd = input_rdd.groupBy(lambda x: x['cuisine'])
    # Agreggate values for each cuisine 
    aggregated_rdd = grouped_rdd.map(aggregate)
    # Get total average reviews
    avg = float(input_rdd.count()) / grouped_rdd.count()
    # Filter based on total amount of reviews and negative reviews percentage
    filtered_rdd = aggregated_rdd.filter(lambda x: x[1][0] >= avg and float(x[1][1])/x[1][0] < percentage_f)
    # Sort results descendingly by average points per cuisine
    sorted_rdd = filtered_rdd.sortBy(lambda x: -x[1][3])    
    # Save results as text files
    sorted_rdd.saveAsTextFile(result_dir)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We provide the path to the input folder (dataset) and output folder (Spark job result)
    source_dir = "/FileStore/tables/A02/my_dataset/"
    result_dir = "/FileStore/tables/A02/my_result/"

    # 2. We add any extra variable we want to use
    percentage_f = 10

    # 3. We remove the monitoring and output directories
    dbutils.fs.rm(result_dir, True)

    # 4. We call to our main function
    my_main(source_dir, result_dir, percentage_f)
