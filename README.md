# Project Description

## Datasets
I generated a sub-dataset, ub_sample_data.csv, from the Yelp review dataset.

## Tasks
### 4.1 Graph Construction
I constructed a social network graph by assuming that each node is uniquely labeled and that links are undirected and unweighted. Each node represents a user, and there is an edge between two nodes if the number of common businesses reviewed by two users is greater than or equivalent to the filter threshold.

### 4.2 Task1: Community Detection Based on GraphFrames
I explored the Spark GraphFrames library to detect communities in the network graph. I used the Label Propagation Algorithm (LPA) provided by the library to detect communities.

#### 4.2.1 Execution Detail
I used the version 0.6.0 of GraphFrames for Python and followed the provided instructions to install the package.

#### 4.2.2 Output Result
I saved the result of communities in a txt file, following the specified format.

### 4.3 Task 2: Community Detection Based on Girvan-Newman algorithm
I implemented the Girvan-Newman algorithm to detect communities in the network graph using Spark RDD and standard Python or Scala libraries.

#### 4.3.1 Betweenness Calculation
I calculated the betweenness of each edge in the original graph and saved the result in a txt file, following the specified format.

#### 4.3.2 Community Detection
I divided the graph into suitable communities, which reached the global highest modularity, following the Girvan-Newman algorithm. I saved the result in a txt file, following the specified format.
