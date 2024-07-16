## Homework 3 - Count Sketch with Spark Streaming
You must write a program which receives in input the following 6 command-line arguments (in the given order):
* An integer _D_: the number of rows of the count sketch
* An integer _W_: the number of columns of the count sketch
* An integer _left_: the left endpoint of the interval of interest
* An integer _right_: the right endpoint of the interval of interest
* An integer _K_: the number of top frequent items of interest
* An integer _portExp_: the port number
The program must read the first (approximately) 10M items of the stream $\Sigma$ generated from the remote machine at port _portExp_ and compute the following statistics. Let _R_ denote the interval _[left,right]_ and let $\Sigma_R$ be the substream consisting of all items of $\Sigma$ belonging to $R$.

The program must compute:
* A $D\times W$ count sketch for $\Sigma_R$
* The exact frequencies of all distinct items of $\Sigma_R$
* The true second moment $F_2$ of |\Sigma_R|. To avoid large numbers, normalize $F_2$ by dividing it by $|\Sigma_R|^2$.
* The approximate second moment $\tilde{F}_2$ of $\Sigma_R$ using count sketch, also normalized by dividing it by $|\Sigma_R|^2$.
* The average relative error of the frequency estimates provided by the count sketch where the average is computed over the items of $u\in \Sigma_R$ whose true frequency is $f_u\ge \phi(K)$, where $\phi(K)$ is the $K$-th largest frequency of the items of $\Sigma_R$. Recall that if $\tilde{f}_u$ is the estimated frequency for $u$, the relative error of is $|f_u-\tilde{f}_u| / f_u$.
