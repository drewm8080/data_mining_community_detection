import os
import sys
import time
from pyspark import SparkContext, SparkConf
from copy import deepcopy
from itertools import combinations

def preprocessing(file):
    # reading in the data 
    partitions = 20
    text_rdd = sc.textFile(file,partitions)
    text_rdd_header = text_rdd.first()
    data_rdd = text_rdd.filter(lambda row: row != text_rdd_header)
    # seperating out the data 
    seperate_rdd = data_rdd.map(lambda row: (row.split(',')[0], row.split(',')[1]))
    # grouping the data 
    data_dictionary =seperate_rdd .groupByKey().mapValues(set).collectAsMap()
    return data_dictionary



def nodes_edges(data_dict, threshold):
    list_of_edge = []
    list_of_nodes = []
    # getting all possible nodes
    keys = list(data_dict.keys())
    for i in range(len(keys)):
        for j in range(len(keys)):
            if i != j:  
                node1, node2 = keys[i], keys[j]
                intersection = data_dict[node1] & data_dict[node2]
                # only if greater/equal to threshold
                if len(intersection) >= threshold:
                    list_of_edge.extend([(node1, node2)])
                    # only getting node 1 
                    if node1 not in list_of_nodes:
                        list_of_nodes.append(node1)
    return list_of_nodes, list_of_edge



def create_graph(edges):
    graph = {}
    # For each edge, add the nodes to the graph and update their neighbors

    for edge in edges:
        # first node not in graph
        if edge[0] not in graph:
            graph[edge[0]] = [edge[1]]
        # second node of the edge is not already a neighbor of the first node
        else:
            if edge[1] not in graph[edge[0]]:
                graph[edge[0]].append(edge[1])
        # now doing it for second node 
        if edge[1] not in graph:
            graph[edge[1]] = [edge[0]]
        else:
            if edge[0] not in graph[edge[1]]:
                graph[edge[1]].append(edge[0])
    return graph

def calculate_shortest_paths(graph, start_node):
    road = dict.fromkeys(graph, 0)
    total_dis = dict.fromkeys(graph, float('inf'))
    preceeding_node = dict.fromkeys(graph, [])
    credit = dict.fromkeys(graph, 1)
    
    road[start_node] = 1
    total_dis[start_node] = 0

    nodes_finalized, nodes_to_be_searched = [], [start_node]
    i = 0

    while i < len(nodes_to_be_searched):
        currently_searched_node = nodes_to_be_searched[i]
        nodes_finalized.append(currently_searched_node)
        for n in graph[currently_searched_node]:
            total_distance = total_dis[currently_searched_node] + 1
            if total_distance < total_dis[n]:
                road[n] = road[currently_searched_node]
                total_dis[n] = total_distance
                nodes_to_be_searched.append(n)
                preceeding_node[n] = [currently_searched_node]
            elif total_distance == total_dis[n]:
                road[n] += road[currently_searched_node]
                if currently_searched_node not in preceeding_node[n]:
                    preceeding_node[n].append(currently_searched_node)
        i += 1
    return road, preceeding_node, nodes_finalized, credit

def edge_betweenness(graph):
    betweeness = {}
    for beginning_node in graph:
        # getting the data
        path_counts, preceeding_node, nodes_finalized, edge_credit = calculate_shortest_paths(graph, beginning_node)
        # moving backqward from last processed node 
        current_index_processed = len(nodes_finalized) - 1

        while current_index_processed >= 0:
            node_to_process = nodes_finalized[current_index_processed]
            # getting all the pre
            for previous_nodes in preceeding_node[node_to_process]:
                # calculating the division of shortest paths from previous paths to current node
                # this is the picking up of the node and letting the nodes dangle down
                path_fraction = edge_credit[node_to_process] * path_counts[previous_nodes] / path_counts[node_to_process]
                pair_of_edges = tuple(sorted((previous_nodes, node_to_process)))
                # adding them together
                betweeness[pair_of_edges] = betweeness.get(pair_of_edges, 0) + path_fraction
                #adding the edge credits
                edge_credit[previous_nodes] = edge_credit[previous_nodes]+path_fraction
            current_index_processed = current_index_processed- 1
    # normalizing
    for edge in betweeness:
        betweeness[edge] = betweeness[edge]*0.5
    return betweeness





def edge_betweenness(graph):
    betweeness = {}
    for beginning_node in graph:
        # getting the data
        path_counts, preceeding_node, nodes_finalized, edge_credit = calculate_shortest_paths(graph, beginning_node)
        # moving backqward from last processed node 
        current_index_processed = len(nodes_finalized) - 1

        while current_index_processed >= 0:
            node_to_process = nodes_finalized[current_index_processed]
            # getting all the pre
            for previous_nodes in preceeding_node[node_to_process]:
                # calculating the division of shortest paths from previous paths to current node
                # this is the picking up of the node and letting the nodes dangle down
                path_fraction = edge_credit[node_to_process] * path_counts[previous_nodes] / path_counts[node_to_process]
                pair_of_edges = tuple(sorted((previous_nodes, node_to_process)))
                # adding them together
                betweeness[pair_of_edges] = betweeness.get(pair_of_edges, 0) + path_fraction
                #adding the edge credits
                edge_credit[previous_nodes] = edge_credit[previous_nodes]+path_fraction
            current_index_processed = current_index_processed- 1
    # normalizing
    for edge in betweeness:
        betweeness[edge] = betweeness[edge]*0.5
    return betweeness

def optimalCommunity(graph):
    # creating deep copies
    neighbors = deepcopy(graph)
    modified_graph = deepcopy(neighbors)
    total_edges = sum([len(v) for v in graph.values()]) / 2

    best_communities = 0
    optimal_modularity = 0

    while True:
        betweenness = edge_betweenness(modified_graph)
        max_betweenness = max(betweenness.values())

        # create_communities logic
        pruned_tree = deepcopy(modified_graph)

        # finding community
        visited_nodes = []
        communities = []
        for node in pruned_tree:
            if node not in visited_nodes:
                community = []
                nodes_to_visit = list((node,))
                # continuing until all the nodes in a community have been visited
                while nodes_to_visit:
                    # remove from stack
                    current_node = nodes_to_visit.pop()
                    if current_node not in visited_nodes:
                        # make sure the node has been visited
                        visited_nodes.append(current_node)
                        # create the community
                        community.append(current_node)
                        # add all the visitors
                        nodes_to_visit.extend(set(pruned_tree[current_node]) - set(visited_nodes))
                communities.append(community)

       # assess_modularity logic
        mod_score = 0
        for community in communities:
            for node1, node2 in combinations(community, 2):
                if node1 in neighbors[node2]:
                    connection = 1
                else:
                    connection = 0
                mod_score =mod_score+ connection - (len(neighbors[node1]) * len(neighbors[node2])) / (2 * total_edges)

        # getting the new modularity score
        modularity = mod_score / (2 * total_edges)


        # updating the modularity 
        if modularity > optimal_modularity:
            optimal_modularity = modularity
            best_communities = communities

        # Filter out edges with high betweenness
        edges_to_remove = [(x, y) for (x, y), betweness in betweenness.items() if betweness >= max_betweenness]

        # Remove these edges from the graph
        for n_1, n_2 in edges_to_remove:
            modified_graph[n_1].remove(n_2)
            modified_graph[n_2].remove(n_1)

        # Calculate the number of remaining edges
        remaining_edges = sum(len(neighbors) for neighbors in modified_graph.values()) / 2

        # Stop the loop if there are no more edges left
        if remaining_edges == 0:
            break

    return best_communities








def write_to_file(betweenness, output_filepath):
    # writng output
    with open(output_filepath, 'w') as f:
        for edge, value in sorted(betweenness.items(), key=lambda x: (-x[1], x[0])):
            if value != 0:
                f.write(f"{edge},{round(value, 5)}\n")

def write_communities_to_file(communities, output_filepath):
    # writing output
    with open(output_filepath, 'w') as f:
        for community in sorted(communities, key=lambda x: (len(x), sorted(x)[0])):
            f.write(', '.join(f"'{user_id}'" for user_id in sorted(community)) + '\n')


if __name__ == '__main__':
    time_start = time.time()
    input_filepath = '/Users/andrewmoore/Desktop/DSCI 553/DSCI 553 HW 4/test_user_business.csv'
    threshold = 4
    output_filepath = '/Users/andrewmoore/Desktop/DSCI 553/DSCI 553 HW 4/finalized_output_1.txt'
    output_file_path_2 = '/Users/andrewmoore/Desktop/DSCI 553/DSCI 553 HW 4/finalized_output_2.txt'
    sc= SparkContext('local[*]','task2') 
    sc.setLogLevel('ERROR')
    # preprocess data
    data_dictionary = preprocessing(input_filepath)
    # get the nodes and edges 
    nodes, edges = nodes_edges(data_dictionary,threshold)
    # create the graph
    graph = create_graph(edges)
    # get the betweeness
    betweenness = edge_betweenness(graph)
    write_to_file(betweenness, output_filepath)
    # get the best community
    communities = optimalCommunity(graph)
    write_communities_to_file(communities, output_file_path_2)



        
    time_end = time.time()
    duration= time_end-time_start
    final_time = print('Duration:',duration)