# horoscope

horoscope is an optimizer inspector for DBMS.



## Get Started

1. Run TiDB

    Recommand [TiUP](https://tiup.io).

2. Initialize TCPH Database

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

4. Start Test

    ```bash
    bin/horo tpch -p
    ```

## Usage

```
NAME:
   horoscope - An optimizer inspector for DBMS

USAGE:
   horo [global options] command [command options] [arguments...]

COMMANDS:
   tpch      Test DSN with TPCH
   query, q  Execute a query
   help, h   Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --dsn DSN, -d DSN          DSN of target db (default: "root:@tcp(localhost:4000)/test?charset=utf8")
   --round ROUND, -r ROUND    Execution ROUND of each query (default: 1)
   --json, -j                 Format log with json formatter (default: false)
   --file FILE, -f FILE       FILE to store log
   --verbose LEVEL, -v LEVEL  LEVEL of log: trace|debug|info|warn|error|fatal|panic (default: "info")
   --help, -h                 show help (default: false)

```

