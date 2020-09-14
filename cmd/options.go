// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"time"

	"github.com/chaos-mesh/horoscope/pkg/executor"
	"github.com/chaos-mesh/horoscope/pkg/generator"
)

var (
	options = Options{
		Main: MainOptions{
			Workload: "workload",
			Dsn:      "root:@tcp(localhost:4000)/test?charset=utf8",
			Verbose:  "info",
			Pool: executor.PoolOptions{
				MaxOpenConns:   100,
				MaxIdleConns:   20,
				MaxLifeSeconds: 10,
			},
		},
		Bench: BenchOptions{
			ReportFmt: "table",
			Round:     1,
		},
		Card: CardOptions{
			Typ: "emq",
		},
		Generate: GenerateOptions{
			Mode:        "bench",
			Queries:     20,
			AndOpWeight: 3,
			Generator: generator.Options{
				MaxTables:            1,
				MaxByItems:           3,
				MinDurationThreshold: 10 * time.Millisecond,
				Limit:                100,
				AggregateWeight:      0.5,
			},
		},
		Index: IndexOptions{
			MaxIndexes:    10,
			CompoundLevel: 1,
		},
		Split: SplitOptions{
			Slices:    100,
			BatchSize: 100,
		},
	}
)

type (
	Options struct {
		Main     MainOptions     `json:"main"`
		Bench    BenchOptions    `json:"bench"`
		Card     CardOptions     `json:"card"`
		Query    QueryOptions    `json:"query"`
		Generate GenerateOptions `json:"generate"`
		Index    IndexOptions    `json:"index"`
		Info     InfoOptions     `json:"info"`
		Load     LoadOptions     `json:"load"`
		Split    SplitOptions    `json:"split"`
	}

	MainOptions struct {
		Workload      string               `json:"workload"`
		Dsn           string               `json:"dsn"`
		JsonFormatter bool                 `json:"json_formatter"`
		LogFile       string               `json:"log_file"`
		Verbose       string               `json:"verbose"`
		Pool          executor.PoolOptions `json:"pool"`
	}

	BenchOptions struct {
		Round                   uint   `json:"round"`
		NeedPrepare             bool   `json:"need_prepare"`
		DisableCollectCardError bool   `json:"disable_collect_card_error"`
		NoVerify                bool   `json:"no_verify"`
		ReportFmt               string `json:"report_fmt"`
	}

	CardOptions struct {
		Columns string        `json:"columns"`
		Typ     string        `json:"type"`
		Timeout time.Duration `json:"timeout"`
	}

	QueryOptions struct {
		PlanID int64 `json:"plan_id"`
	}

	GenerateOptions struct {
		Queries     int               `json:"queries"`
		AndOpWeight int               `json:"and_op_weight"`
		Mode        string            `json:"mode"`
		Generator   generator.Options `json:"generator"`
	}

	IndexOptions struct {
		MaxIndexes     int  `json:"max_indexes"`
		CompoundLevel  int  `json:"compound_level"`
		ReserveIndexes bool `json:"reserve_indexes"`
	}

	InfoOptions struct {
		Table string `json:"table"`
	}

	LoadOptions struct {
		DataSource string `json:"data_source"`
	}

	SplitOptions struct {
		Group       string `json:"group"`
		Slices      uint   `json:"slices"`
		BatchSize   uint   `json:"batch_size"`
		UseBitArray bool   `json:"use_bit_array"`
	}
)
