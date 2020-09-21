module github.com/chaos-mesh/horoscope

go 1.13

require (
	github.com/aclements/go-moremath v0.0.0-20190830160640-d16893ddf098
	github.com/go-openapi/strfmt v0.19.5 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/golang-collections/go-datastructures v0.0.0-20150211160725-59788d5eb259
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/jedib0t/go-pretty v4.3.0+incompatible
	github.com/magiconair/properties v1.8.0
	github.com/pingcap/parser v0.0.0-20200921041333-cd2542b7a8a2
	github.com/pingcap/tidb v1.1.0-beta.0.20200921050610-4ec101d7e329
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.6.1
	github.com/urfave/cli/v2 v2.2.0
	golang.org/x/perf v0.0.0-20200318175901-9c9101da8316
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	google.golang.org/protobuf v1.24.0 // indirect
)

//replace github.com/pingcap/tidb => github.com/pingcap/tidb v0.0.0-20200317142013-5268094afe05
replace github.com/pingcap/parser => github.com/Hexilee/parser v0.0.0-20200921032941-e3585adbb4a1
