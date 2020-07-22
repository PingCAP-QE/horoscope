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

package keymap

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const keyMaps = `
title.id <=> aka_title.movie_id
		 <=> cast_info.movie_id
		 <=> complete_cast.movie_id
		 <=> movie_companies.movie_id
		 <=> movie_info.movie_id
		 <=> movie_keyword.movie_id
		 <=> movie_link.movie_id;

name.id <=> cast_info.person_id
		<=> aka_name.person_id
		<=> person_info.person_id;
`

func TestParse(t *testing.T) {
	maps, err := Parse(keyMaps)
	assert.Nil(t, err)
	assert.Len(t, maps, 2)
	assert.Len(t, maps[0].ForeignKeys, 7)
	assert.Len(t, maps[1].ForeignKeys, 3)
	assert.Equal(t, Key{Table: "title", Column: "id"}, *maps[0].PrimaryKey)
	assert.Equal(t, Key{Table: "aka_title", Column: "movie_id"}, *maps[0].ForeignKeys[0])
	assert.Equal(t, Key{Table: "movie_link", Column: "movie_id"}, *maps[0].ForeignKeys[6])
	assert.Equal(t, Key{Table: "name", Column: "id"}, *maps[1].PrimaryKey)
	assert.Equal(t, Key{Table: "cast_info", Column: "person_id"}, *maps[1].ForeignKeys[0])
	assert.Equal(t, Key{Table: "person_info", Column: "person_id"}, *maps[1].ForeignKeys[2])
}
