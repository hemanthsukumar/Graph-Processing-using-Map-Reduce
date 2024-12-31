### Project Description: Graph Partitioning using Multi-Source BFS

The goal of this project is to partition a directed graph into K clusters using a Map-Reduce framework. The process involves selecting K random graph vertices (called centroids) and using breadth-first search (BFS) to assign the vertices to the nearest centroid. The process is repeated for several iterations until the clusters stabilize.

#### **Input Format:**
The graph is represented as a text file where each line represents a graph vertex and its adjacent vertices (neighbors). Each line is structured as:
```
vertex_id, adjacent_vertex1, adjacent_vertex2, ..., adjacent_vertexN
```
For example:
```
1, 2, 3, 4, 5, 6, 7
```
This indicates that vertex `1` is connected to vertices `2`, `3`, `4`, `5`, `6`, and `7`.

#### **Vertex Representation:**
The graph vertices are represented by the `Vertex` class:
```java
class Vertex {
  long id;           // Vertex ID
  Vector adjacent;   // List of adjacent vertices
  long centroid;     // ID of the centroid the vertex belongs to
  short depth;       // BFS depth
}
```
The class has a constructor:
```java
Vertex(long id, Vector adjacent, long centroid, short depth)
```

#### **Map-Reduce Workflow:**

This project consists of three Map-Reduce tasks:

1. **First Map-Reduce Job (Reading the Graph):**
   - **Mapper:**
     - Parse each line to get the vertex ID and its adjacent vertices.
     - Select the first 10 vertices as centroids (initial centroid assignment).
     - Emit each vertex along with its neighbors, centroid ID, and BFS depth (initialized to 0).
   - **Reducer:**
     - No reducer is needed for this step as each vertex is emitted as-is.

2. **Second Map-Reduce Job (BFS for Cluster Assignment):**
   - **Mapper:**
     - Emit each vertex ID with its associated data.
     - If the vertex is a centroid, propagate its ID to its adjacent vertices.
     - The BFS depth is incremented for each iteration (from 1 to 8).
   - **Reducer:**
     - For each vertex, select the nearest centroid based on the minimum BFS depth.
     - Update the vertex's centroid assignment and depth.
   
3. **Third Map-Reduce Job (Calculating Cluster Sizes):**
   - **Mapper:**
     - Emit the centroid ID and a count of `1` for each vertex.
   - **Reducer:**
     - Sum the counts for each centroid and output the total number of vertices assigned to each centroid.

#### **Steps for Execution:**

1. **Map-Reduce Job 1 (Initial Graph Read):**
   - Input: Graph data (e.g., `small-graph.txt`).
   - Output: Intermediate results stored in the directory `args[1]/i0` (using SequenceFileOutputFormat).

2. **Map-Reduce Job 2 (BFS Iteration):**
   - This job must be repeated 8 times, each iteration reading from `args[1]/i{i}` and writing to `args[1]/i{(i+1)}`.
   - In the first iteration, the BFS depth is 1, in the second iteration it's 2, and so on.
   - After 8 iterations, the final output will be stored in `args[1]/i8`.

3. **Map-Reduce Job 3 (Cluster Sizes Calculation):**
   - Input: Final vertex assignments from `args[1]/i8`.
   - Output: Cluster sizes stored in `args[2]`.

#### **Configuration:**
- **Input Graph Path** (`args[0]`): Path to the input graph file (e.g., `small-graph.txt`).
- **Intermediate Directory** (`args[1]`): Directory for storing intermediate results.
- **Output Directory** (`args[2]`): Directory for storing the final output (cluster sizes).

#### **Example Execution:**

1. **Local Mode (Small Graph Test):**
   - Run the first Map-Reduce job:
     ```bash
     ~/hadoop-3.2.2/bin/hadoop jar target/*.jar GraphPartition small-graph.txt tmp output
     ```
   - Intermediate results will be saved in `tmp/i0`.

2. **Distributed Mode (Large Graph Test on Expanse):**
   - Run the first Map-Reduce job on Expanse:
     ```bash
     sbatch multiply.local.run
     ```
   - Repeat the second Map-Reduce job 8 times, and then run the final cluster size calculation.

#### **Notes:**
- **Repeat the Second Job**: You will need to repeat the second Map-Reduce job 8 times. Each iteration assigns centroid IDs to vertices using BFS.
- **Intermediate Results**: All intermediate results should be stored using SequenceFileOutputFormat, which ensures efficient binary storage of the data.
