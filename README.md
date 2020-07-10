# horoscope

horoscope is an optimizer inspector for DBMS.



## Get Started

1. Run TiDB

    Recommand [TiUP](https://tiup.io).

2. Initialize TPCH Database

    Recommand [go-tpc](https://github.com/pingcap/go-tpc).
    ```bash
    git clone https://github.com/pingcap/go-tpc.git
    cd go-tpc
    make
    ./bin/go-tpc tpch --sf=1 prepare
    ```

3. Build Horoscope

    ```bash
    git clone https://github.com/chaos-mesh/horoscope.git
    cd horoscope
    make
    ```

4. Start Benching

    ```bash
    bin/horo bench -p -w benchmark/tpch
    ```

## Usage

```
NAME:
   horoscope - An optimizer inspector for DBMS

USAGE:
   horo [global options] command [command options] [arguments...]

COMMANDS:
   bench       bench the optimizer
   query, q    Execute a query
   explain, e  Explain a query
   info, i     Show database information
   help, h     Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --dsn DSN, -d DSN          DSN of target db (default: "root:@tcp(localhost:4000)/test?charset=utf8")
   --round ROUND, -r ROUND    Execution ROUND of each query (default: 1)
   --json, -j                 Format log with json formatter (default: false)
   --file FILE, -f FILE       FILE to store log
   --verbose LEVEL, -v LEVEL  LEVEL of log: trace|debug|info|warn|error|fatal|panic (default: "info")
   --help, -h                 show help (default: false)
```

## Summary report

There will generate A summary report after `bench` sub-command is finished.

```txt
+-----+-------------+------------------------+--------------------------+---------------+---------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ID  | #PLAN SPACE | DEFAULT EXECUTION TIME | BEST PLAN EXECUTION TIME | EFFECTIVENESS | BETTER OPTIMAL PLANS                                                                                          | QUERY                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
+-----+-------------+------------------------+--------------------------+---------------+---------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| q3  |          11 | 11108.8ms ± 3%        | 6868.8ms ± 4%           | 72.7%         | #6(71.1%),#10(68.4%),#11(61.8%)                                                                               | SELECT l_orderkey,sum(l_extendedprice*(1-l_discount)) AS revenue,o_orderdate,o_shippriority FROM ((customer) JOIN orders) JOIN lineitem WHERE c_mktsegment="AUTOMOBILE" AND c_custkey=o_custkey AND l_orderkey=o_orderkey AND o_orderdate<"1995-03-13" AND l_shipdate>"1995-03-13" GROUP BY l_orderkey,o_orderdate,o_shippriority ORDER BY revenue DESC,o_orderdate LIMIT 10                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
+-----+-------------+------------------------+--------------------------+---------------+---------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

* `ID`: query id
* `#PLAN SPACE`: the plan space size of a query
* `DEFAULT EXECUTION TIME`: the execution time of default plan, giving in the format of "Mean ±Diff", "Mean" is the mean value of `round` rounds, and "Diff" is the lower/upper bound of the mean value
* `BEST PLAN EXECUTION TIME`: the execution time of default plan
* `EFFECTIVENESS`: the percent of the execution time of the default plan better than others on plan space
* `BETTER OPTIMAL PLANS`: gives the better plan, each item is giving in the format of "nth_plan id(execution time / default execution time)"
* `QUERY`: the query

## Dataset

We integrate the SQL queries of TPCH, TPCDS, SSB, and JOB benchmarks on the repo, the user can refer [tidb-bench](https://github.com/pingcap/tidb-bench) to import the dataset.

For the JOB benchmark, [join-order-benchmark](https://github.com/gregrahn/join-order-benchmark) is helpful.
