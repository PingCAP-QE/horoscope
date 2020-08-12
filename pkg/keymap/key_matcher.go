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

type (
	KeyMatcher struct {
		inner map[TablePair]map[KeyPair]bool
	}

	TablePair struct {
		t1, t2 string
	}

	KeyPair struct {
		K1, K2 Key
	}
)

func NewTablePair(t1, t2 string) TablePair {
	if t2 < t1 {
		return NewTablePair(t2, t1)
	}
	return TablePair{t1: t1, t2: t2}
}

func NewKeyPair(k1, k2 Key) KeyPair {
	if k2.Table < k1.Table {
		return NewKeyPair(k2, k1)
	}
	return KeyPair{K1: k1, K2: k2}
}

func NewKeyMatcher(keymaps []KeyMap) *KeyMatcher {
	matcher := &KeyMatcher{
		inner: make(map[TablePair]map[KeyPair]bool),
	}

	for _, kp := range keymaps {
		pk := kp.PrimaryKey
		for _, fk := range kp.ForeignKeys {
			tablePair := NewTablePair(pk.Table, fk.Table)
			keyPair := NewKeyPair(*pk, *fk)
			if matcher.inner[tablePair] == nil {
				matcher.inner[tablePair] = make(map[KeyPair]bool)
			}
			matcher.inner[tablePair][keyPair] = true
		}
	}

	return matcher
}

func (matcher KeyMatcher) Match(t1, t2 string) map[KeyPair]bool {
	return matcher.inner[NewTablePair(t1, t2)]
}

func (matcher KeyMatcher) MatchRandom(t1, t2 string) *KeyPair {
	for pair := range matcher.Match(t1, t2) {
		return &pair
	}
	return nil
}

func (matcher KeyMatcher) MatchKey(k1, k2 Key) bool {
	return matcher.Match(k1.Table, k2.Table)[NewKeyPair(k1, k2)]
}
