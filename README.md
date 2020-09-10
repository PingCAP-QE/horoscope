# horoscope

horoscope is an optimizer inspector for DBMS.

## Get Started

1. Run TiDB

    Recommend [TiUP](https://tiup.io).

2. Initialize TPCH Database

    Recommend [go-tpc](https://github.com/pingcap/go-tpc).
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
   init, i     initialize workload
   bench       Bench the optimizer
   gen, g      Generate a dynamic bench scheme
   query, q    Execute a query
   hint, H     Explain hint of a query
   explain, e  Explain analyze a query
   info        Show database information
   index       Add indexes for tables
   card        test the cardinality estimations
   split, s    Split data into several slices
   load        Load data in a directory
   help, h     Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --dsn DSN, -d DSN          set DSN of target db (default: "root@tcp(localhost:4000)/test")
   --workload DIR, -w DIR     workload DIR of horo (default: "workload")
   --json, -j                 format log with json formatter (default: false)
   --file FILE, -f FILE       set FILE to store log
   --verbose LEVEL, -v LEVEL  set LEVEL of log: trace|debug|info|warn|error|fatal|panic (default: "info")
   --max-open-conns numbers   the max numbers of connections (default: 100)
   --max-idle-conns numbers   the max numbers of idle connections (default: 20)
   --max-lifetime seconds     the max seconds of connections lifetime (default: 10)
   --not-save                 do not save options (default: false)
   --help, -h                 show help (default: false)
```

### Bench effectiveness

```sh
bin/horo -r 4 bench -p -c -w benchmark/tpch
```

### Bench cardinality estimation

For example, measures the EMQ(exact match queries) row cnt error on `customer.C_NAME` for total 100 seconds.

```sh
bin/horo card -columns 'customer.C_NAME' -type emq -timeout 100s
```

## Summary report

There will generate a summary report after `bench` sub-command is finished.

```txt
+-----+-------------+------------------------+--------------------------+---------------+---------------------------------+--------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ID  | #PLAN SPACE | DEFAULT EXECUTION TIME | BEST PLAN EXECUTION TIME | EFFECTIVENESS | BETTER OPTIMAL PLANS            | ESTROW Q-ERROR                                                     | QUERY                                                                                                                                                                                                                                                                                                                                                                        |
+-----+-------------+------------------------+--------------------------+---------------+---------------------------------+--------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| q3  |          11 | 12061.0ms ±11%         | 5401.8ms ±19%            | 72.7%         | #6(44.8%),#10(66.4%),#11(47.3%) | count:3, median:1.0, 90th:7173270.0, 95th:7173270.0, max:7173270.0 | SELECT l_orderkey,sum(l_extendedprice*(1-l_discount)) AS revenue,o_orderdate,o_shippriority FROM ((customer) JOIN orders) JOIN lineitem WHERE c_mktsegment="AUTOMOBILE" AND c_custkey=o_custkey AND l_orderkey=o_orderkey AND o_orderdate<"1995-03-13" AND l_shipdate>"1995-03-13" GROUP BY l_orderkey,o_orderdate,o_shippriority ORDER BY revenue DESC,o_orderdate LIMIT 10 |
+-----+-------------+------------------------+--------------------------+---------------+---------------------------------+--------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

* `ID`: query id
* `#PLAN SPACE`: the plan space size of a query
* `DEFAULT EXECUTION TIME`: the execution time of default plan, giving in the format of "Mean ±Diff", "Mean" is the mean value of `round` rounds, and "Diff" is the lower/upper bound of the mean value
* `BEST PLAN EXECUTION TIME`: the execution time of the best plan
* `EFFECTIVENESS`: the percent of the execution time of the default plan better than others on plan space
    * We use Pd to represent the default plan generated for the query, Pi as one of plan on plan space
    * If execution time(Pi) < 0.9 * execution time(Pd), Pi is a better plan
* `BETTER OPTIMAL PLANS`: gives the better plan, each item is giving in the format of "nth_plan id(execution time / default execution time)"
* `ESTROW Q-ERROR`: Base table row cnt estimation q-error for each query
* `QUERY`: the query

## Dataset

We integrate the SQL queries of TPCH, TPCDS, SSB, and JOB benchmarks on the repo, you can use [go-tpc](https://github.com/pingcap/go-tpc) and [tidb-bench](https://github.com/pingcap/tidb-bench) to import the dataset.

For the JOB benchmark, [join-order-benchmark](https://github.com/gregrahn/join-order-benchmark) is helpful.

## Index selection fuzz

Refer to [index selection fuzz](doc/index_selection.md)
