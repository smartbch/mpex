## EVM-Chain TLP Analysis

In the field of CPU research, an important concept is Instruction-level parallelism (ILP). It is defined as: A measure of the average number of instructions in a program that a processor might be able to execute at the same time. There are a lot of researches on ILP. According to the conclusion of the classic paper _[The Limits of Instruction Level Parallelism in SPEC95 Applications](https://citeseerx.ist.psu.edu/document?repid=rep1&type=pdf&doi=b76002eb9dc434d0c4cb00ab71bbd76be6c05d0c)_, the ILP of common one-point applications can be obtained after some exploiting strategies. It can reach dozens or even hundreds.

In the field of Blockchain, we can define a similiar concept Transaction-level parallelism (TLP) as：A measure of the average number of transactions in a block that a full-node might be able to execute at the same time.

Concurrent EVM is a topic that has received a lot of attention, and TLP has a very basic significance for concurrent EVM: If the concurrency of on-chain applications is low, the benefits of concurrent EVM optimization are limited. Unfortunately, to date, there have been no detailed, reproducible TLP analyses of the typical workload of EVM chains.

We are building a research platform for TLP, analyzing TLP under different exploiting strategies through open source code and open data, so as to better understand the workload on EVM chains, and provide necessary technical reserves for the development of high-concurrency EVM chains in the future. On top of the research we build, other teams can develop new code and bring in new data to analyze more exploiting strategies and more workload.

### Input Data

We analyze the historical transaction data of the blockchain. Each transaction that has ever been executed on the chain has a definite read and write list of which addresses and slots it has read and overwritten. We use the following method to obtain the read and write list.

First, get the full read/write storage slot list and read/write account list of a transaction through the following rpc endpoint (it cannot distinguish read/write, but only lists all the accounts and storage involved in the execution of the transaction):

```
curl https://rpc.ankr.com/eth/{your_api_key} -d '{"jsonrpc":"2.0","method":"debug_traceTransaction","params":[ <txhash>, { "tracer": "prestateTracer" }],"id":1}'
```

Then, we can get the storage slot write information and account write information of all transactions in a block through the following rpc endpoint.

```
curl https://rpc.ankr.com/eth/{your_api_key} -d '{"jsonrpc":"2.0","method":"trace_replayBlockTransactions","params":[<block_number>, ["stateDiff"]],"id":1}'
```

Based on the results returned by these two RPC endpoints, it is possible to obtain which accounts were written in a transaction, which storage slots were written, which accounts were read, and which storage slots were read. That's the read and write information that we need to do our analysis.

### Analysis Results

The sequence of transactions with read and write lists is processed by our analysis algorithm to form a series of bundles. Transactions within each bundle, which do not collide with each other, can be executed simultaneously, and the results of execution can be committed in any order.

For the same sequence of transactions, the fewer the number of bundles obtained, the more transactions contained within each bundle on average, which means that we can exploit a higher TLP from the sequence.

### Pre-process Coinbase

The EVM instruct COINBASE returns the beneficiary account of the current block. The Gas Fee of the current transaction will be transferred to the beneficiary account of the block at the end of the transaction. In other words, the beneficiary account is updated in each transaction. Because all transactions in a block share the same beneficiary account, they all have a write-to-write collision with each other, resulting in any two transactions not being able to execute simultaneously. In other words, if it is fully compatible with the Ethereum Yellow Book specification for Coinbase, then the TLP will always be equal to 1, with no concurrency available for exploiting.

To remove this restriction, a different mechanism is needed to reward the beneficiary account: the Gas Fee for each transaction is collected and not immediately sent to the beneficiary account. After all transactions in the block have been executed, the collected Gas Fee is transferred to the beneficiary account. This completely avoids the possibility of a write-to-write collision happening on the beneficiary's account.

We did a simple pre-processing to the acquired transaction data: we deleted the write operation to coinbase's balance. Without such pre-processing, the subsequent analysis of TLP would be meaningless.

### Keep the order of transactions in a block

Any algorithm that schedules and executes transactions out of order must guarantee that the final effect is exactly the same as the algorithm that executes transactions in a certain order. This order is the consensus order. If there is no consensus order, and each node of the blockchain execute transactions in its own order, then there is no consensus between the nodes about the state on the chain.

The simplest and most straightforward case is: not scheduling and out-of-order transactions at all, which is equivalent to using the order of transactions by the Block Proposer as the "consensus order". So let's start from this case. Its algorithm for output bundles is:

1. Set the current bundle to empty
2. Inspect each transaction in the sequence
	1. If the transaction collides with any transaction in the current bundle, the current bundle is outputted and then cleared to empty
	2. If there is no Collision, the transaction is added to the current bundle
3. If the current bundle is not empty, the current bundle is outputted

#### Spererate read&write or not

For two transactions A and B, the addresses and slots they read are denoted ReadSetA and ReadSetB respectively, and the addresses and slots they write are denoted WriteSetA and WriteSetB respectively. A and B cannot be executed at the same time if:

1. Write-to-Write Collision: The intersection of WriteSetA and WriteSetB is not empty
2. Write-to-Read Collision: The intersection of WriteSetA and ReadSetB is not empty, or the intersection of ReadSetA and WriteSetB is not empty

Note that Read-to-Read Collision does NOT exist. If the intersection of ReadSetA and ReadSetB is not empty, A and B can still be executed simultaneously.

For the sake of research completeness, we also look at the case where we don't distinguish between ReadSet and WriteSet, that is, we put all the addresses and slots to be read and written into one collection. This means that we add ReadSet's content to WriteSet, and then only consider collision between WriteSet.

#### Whether to exclude WETH's influence

In the WETH contracts, namely the 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 ERC20 contract, is responsible to wrap ETH for generating WETH, and unwrap WETH to release ETH. During the wrap process, its balance increases; and in the process of unwrap, its balance will decrease.

A large number of Uniswap-V2/V3 trading pairs, as well as other DEX trading pairs which borrow code from Uniswap-V2/V3, have required WETH as an intermediary when trading ETH. Therefore, when these trading pairs are used to buy and sell tokens, the WETH contract's balance will be changed frequently, resulting in many transactions with write-to-write collision.

In our experiments, we found that the WETH contract is the most significant factor limiting the Ethereum TLP. However, this limitation will be eased in the future, as Uniswap-V4 has abandoned the use of WETH as an intermediary for trading ETH. To show what TLP can be exploited in the future if this restriction is eased, we did special experiments to exclude the balance-change operations of the WETH contract from WriteSet.

#### Experimental result

We analyzed 747,272 transactions in blocks between 20338950 and 20343510 and analyzed the average sizes of the bundles obtained under different conditions. The results are as follows:

- KeepWETH/OnlyRnW：2.65
- KeepWETH/RdO+RnW：3.23
- IgnoreWETH/OnlyRnW：3.32
- IgnoreWETH/RdO+RnW：5.80

IgnoreWETH indicates excluding the balance-change operations of the WETH contract from WriteSet, and KeepWETH indicates that they are not excluded. RdO+RnW indicates that ReadSet and WriteSet are seperated, and OnlyRnW indicates that ReadSet and WriteSet are not seperated (that is, ReadSet's content is added to Writeset).

### Reorder transactions in the block

It is perfectly feasible not to treat the order of transactions by a Block Proposer as a "consensus order." As long as all the nodes reorder the transactions according to a deterministic algorithm, getting exactly the same, new "consensus order", the blockchain will work perfectly fine.

The sequence of instructions in the program executed by the CPU cannot be arbitrarily reordered, otherwise the function of the program will be wrong. But there is a lot of freedom in changing the order of blockchain transactions.

During a block interval (or longer time), the order in which users' transactions will eventually be packaged in the block is something that cannot be predicted in advance, and something that the users cannot control. If we accept that Block Proposer reorders these transactions according to its own interests, and even intentionally ignores some of them, then, in the interest of the public and in order for the blockchain to support higher throughput, we can also accept the reordering of transactions with deterministic algorithms.

Here, we consider the simplest deterministic reordering algorithm, which maximizes the number of transactions contained in the bundle by maintaining multiple bundles:

1. Initialize N bundles and set them to empty
2. Inspect each transaction in a sequence
	1. Analyze whether there is collision between this transaction and transactions in the N bundles
	2. If this transaction does not collide with any other transaction in a bundle X, it is added to bundle X
	3. If this transaction cannot be inserted into any bundle, that is, all bundles collide with it at least one transaction, then:
		1. Select the bundle that contains the most transactions, output it, and replace it with a new empty bundle
		2. Add this transaction to the newly created empty bundle
3. Inspect the N bundles one by one and output the non-empty ones

We experimented with three cases where N=32/64/128:

- N=32：20.54
- N=64：22.44
- N=128：23.74

### Use Bloom Filter to accelerate collision analysis

To determine whether a new transaction can be inserted into a bundle, it is necessary to check whether all transactions in this bundle have collision with a new transaction, which is a very time-consuming process. For more efficient checking, we use two Bloom filters to record the sets that are read-only by all transactions in a bundle, and the sets that are read and written by them, respectively.

Because the Bloom Filter has False Positives, the cost of accelerated checking is the introduction of collisions that do not exist, resulting in a slightly lower TLP.

We experimented with three cases where N=32/64/128:

- N=32：19.44
- N=64：21.07
- N=128：22.16

Compared with the previous results, it can be seen that the loss caused by False Positive is relatively small.

### Test the throughput of Bloom filter

We recommend to use the bloomfilter-based scheduler in production, because it is the most efficient. We test the throughput for N=32/64/128:

- N=32：412k/sec
- N=64：408k/sec
- N=128：299k/sec

Since 32 and 64 are both within the CPU's general purpose register size (64), they have roughly the same throughput. The case of 128 costs more CPU L1 cache bandwidth, so it's slower.

### Aggregates transactions from the same account into a task

If several transactions in the same block are sent by the same account, they cannot be executed simultaneously due to a Write-to-Write Collision. Such collision can be avoided by aggregating these transactions into a single task and executing them serially within the task.

We require a task to contain a maximum of 4 transactions. On this basis, we analyze the scheduling algorithm based on Bloom Filter.

- N=32：20.04
- N=64：21.64
- N=128：22.69

As you can see, aggregating tasks can slightly increase the TLP.

In practice, a major obstacle to reordering scheduling algorithms is that several transactions issued by the same account must be executed in an nonce-increasing sequence, and reordering disrupts this sequence. We remove this barrier by requiring Proposer to package blocks such that all transactions sent by the same account are included in one task, which is then scheduled as a whole.

## Conclusion

Strictly speaking, the TLP of Ethereum is exactly 1, which means no concurrency is possible. After we fix the COINBASE problem, TLP can rise to 3.23. If the mainstream DEXs can avoid using WETH (like Uniswap-V4), TLP can rise to 5.80. If we are allowed to reorder the transactions with a deterministic scheduler, TLP can rise to 21. The Bloomfilter-based scheduler is very efficient and can schedule more than 400k tx per second.
