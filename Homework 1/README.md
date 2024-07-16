## Homework 1 - Triangle Counting
In the homework you must implement and test in Spark two MapReduce algorithms to count the number of distinct triangles in an undirected graph $G=(V,E)$, where a triangle is defined by 3 vertices $u,v,w\in V$, such that $(u,v),(v,w),(w,u)\in E$. 

![image](https://github.com/enricobolzonello/bigdata_homeworks/assets/73855124/54e673b1-a94b-4f7a-93a2-df0c9fda2cc4)

Both algorithms use an integer parameter $C\ge 1$, which is used to partition the data.

### ALGORITHM 1: 
Define a hash function ℎ𝐶 which maps each vertex 𝑢 in 𝑉 into a color $h_C(u)$ in $[0,C-1]$. To this purpose, we advise you to use the hash function
$$h_C(u)=(((a\cdot u+b)mod\: p)mod C)$$
where $p=8191$ (which is prime), $a$ is a random integer in $[1,p-1]$, and $b$ is a random integer in $[0,p-1]$.

##### Round 1:
* Create $C$ subsets of edges, where, for $0\leq i < C$, the i-th subset, $E(i)$ consist of all edges $(u,v)\in E$ such that $h_C(u)=h_C(v)=i$. Note that if the two endpoints of an edge have different colors, the edge does not belong to any $E(i)$ and will be ignored by the algorithm.
* Compute the number $t(i)$ triangles formed by edges of $E(i)$, separately for each $0\leq i < C$.
##### Round 2: 
* Compute and return $t_{final}=C^2\sum_{0\leq i < C}t(i)$ as final estimate of the number of triangles in $G$.

In the homework you must develop an implementation of this algorithm as a method/function `MR_ApproxTCwithNodeColors`.

### ALGORITHM 2:
##### Round 1:
* Partition the edges at random into $C$ subsets $E(0),E(1),...,E(C-1)$. Note that, unlike the previous algorithm, now every edge ends up in some $E(i).
* Compute the number $t(i)$ of triangles formed by edges of $E(i)$, separately for each $0\le i < C$.
##### Round 2: 
* Compute and return $t_{final}=C^2\sum_{0\leq i < C}t(i)$ as final estimate of the number of triangles in G.

In the homework you must develop an implementation of this algorithm as a method/function `MR_ApproxTCwithSparkPartitions` that, in Round 1, uses the partitions provided by Spark, which you can access through method mapPartitions.

### DATA FORMAT. 
To implement the algorithms assume that the vertices (set $V$) are represented as 32-bit integers, and that the graph $G$ is given in input as the set of edges $E$ stored in a file. Each row of the file contains one edge stored as two integers (the edge's endpoints) separated by comma (','). Each edge of $E$ appears exactly once in the file and $E$ does not contain multiple copies of the same edge.
