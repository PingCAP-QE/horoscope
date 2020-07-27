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
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"
)

type (
	Key struct {
		Table, Column string
	}

	KeyMap struct {
		PrimaryKey  *Key
		ForeignKeys []*Key
	}
)

var keyRegex *regexp.Regexp

func init() {
	var err error
	keyRegex, err = regexp.Compile(`(\w+)\.(\w+)`)
	if err != nil {
		panic(err.Error())
	}
}

func parseKey(rawKey string) (*Key, error) {
	segments := keyRegex.FindStringSubmatch(rawKey)
	if len(segments) != 3 {
		return nil, fmt.Errorf("invalid key: %s", rawKey)
	}
	return &Key{Table: segments[1], Column: segments[2]}, nil
}

func parseLine(line string) (keyMap KeyMap, err error) {
	keys := strings.Split(line, "<=>")
	if len(keys) < 2 {
		err = fmt.Errorf("invalid keymap line: %s", line)
		return
	}
	keyList := make([]*Key, 0, len(keys))
	for _, rawKey := range keys {
		var key *Key
		key, err = parseKey(strings.TrimSpace(rawKey))
		if err != nil {
			return
		}
		keyList = append(keyList, key)
	}
	keyMap = KeyMap{
		PrimaryKey:  keyList[0],
		ForeignKeys: keyList[1:],
	}
	return
}

func Parse(contents string) (maps []KeyMap, err error) {
	lines := strings.Split(contents, ";")
	maps = make([]KeyMap, 0, len(lines))
	for _, line := range lines {
		var keyMap KeyMap
		trimedLine := strings.TrimSpace(line)
		if trimedLine != "" {
			keyMap, err = parseLine(trimedLine)
			if err != nil {
				return
			}
			maps = append(maps, keyMap)
		}
	}
	return
}

func ParseFile(filename string) (maps []KeyMap, err error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}
	maps, err = Parse(string(data))
	return
}

func (key *Key) String() string {
	return fmt.Sprintf("%s.%s", key.Table, key.Column)
}
