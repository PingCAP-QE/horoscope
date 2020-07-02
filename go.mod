module github.com/chaos-mesh/horoscope

go 1.13

require (
	github.com/go-sql-driver/mysql v1.5.0
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/pingcap/go-tpc v1.0.3 // indirect
	github.com/pingcap/parser v0.0.0-20200612092132-17a1160e5a81
	github.com/pingcap/tidb v2.0.11+incompatible
	github.com/pingcap/tipb v0.0.0-20200610045017-b69a98cfcf6b // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.6.1
	github.com/urfave/cli/v2 v2.2.0
	golang.org/x/sys v0.0.0-20200610111108-226ff32320da // indirect
	google.golang.org/protobuf v1.24.0 // indirect
)

replace github.com/pingcap/tidb => github.com/pingcap/tidb v0.0.0-20200317142013-5268094afe05
