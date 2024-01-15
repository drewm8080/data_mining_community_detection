import os
import sys
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from graphframes import *

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 pyspark-shell"

def preprocessing(file):
    partitions = 20
    text_rdd = sc.textFile(file,partitions)
    text_rdd_header = text_rdd.first()
    data_rdd = text_rdd.filter(lambda row: row != text_rdd_header)
    seperate_rdd = data_rdd.map(lambda row: tuple(row.split(',')[:2]))
    data_dictionary =seperate_rdd.groupByKey().mapValues(lambda values: set(values)).collectAsMap()
    return data_dictionary


def nodes_edges(data_dict, threshold):
    list_of_edge = []
    list_of_nodes = []
    # checking each combination
    keys = list(data_dict.keys())
    # generating all possible keys
    for i in range(len(keys)):
        for j in range(i+1, len(keys)):
            node1, node2 = keys[i], keys[j]
            intersection = data_dict[node1] & data_dict[node2]
            # making sure the length of intersection is greater than the threshold 
            if len(intersection) >= threshold:
                list_of_edge.extend([(node1, node2), (node2, node1)])
                if node1 not in list_of_nodes:
                    list_of_nodes.append(node1)
                if node2 not in list_of_nodes:
                    list_of_nodes.append(node2)
    
    # Create a list of tuples
    list_of_tuples_nodes = []
    for node in list_of_nodes:
        list_of_tuples_nodes.append((node,))
    
    return list_of_tuples_nodes, list_of_edge



def communities(nodes, edges):
    # Create SparkSession from the existing SparkContext
    spark_session = SparkSession(sc)

    # Create DataFrame for nodes and edges
    nodes_df = spark_session.createDataFrame(nodes, ["id"])
    edges_df = spark_session.createDataFrame(edges, ["src", "dst"])

    # Create a GraphFrame
    g = GraphFrame(nodes_df, edges_df)

    # Run the Label Propagation Algorithm
    result = g.labelPropagation(maxIter=5)

    # Return the result
    return result

def write_output(communities_df, output_filepath):
    # Transform DataFrame to RDD
    communities_rdd = communities_df.rdd

    # Group by label and sort
    communities_sorted = communities_rdd.map(lambda row: (row['label'], row['id'])).groupByKey().mapValues(sorted) 
    final_communities_sorted = communities_sorted.sortBy(lambda x: (len(x[1]), x[1])).map(lambda x: x[1]).collect()

    # Write output to file
    with open(output_filepath, 'w') as file:
        for community in final_communities_sorted:
            file.write(', '.join(f"'{user_id}'" for user_id in community))
            file.write('\n')



if __name__ == '__main__':
    time_start = time.time()
    input_filepath = '/Users/andrewmoore/Desktop/DSCI 553/DSCI 553 HW 4/ub_sample_data.csv'
    threshold = 4
    output_filepath = '/Users/andrewmoore/Desktop/DSCI 553/DSCI 553 HW 4/finalized_output.csv'
    sc= SparkContext('local[*]','task1') 
    sc.setLogLevel('ERROR')
    data_dictionary = preprocessing(input_filepath)
    nodes, edges = nodes_edges(data_dictionary,threshold)
    # Count the number of nodes
    communities_df = communities(nodes, edges)
    final_output = write_output(communities_df,output_filepath)



    
    
    
    
    
    time_end = time.time()
    duration= time_end-time_start
    final_time = print('Duration:',duration)