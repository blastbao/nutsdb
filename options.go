// Copyright 2019 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

// EntryIdxMode represents entry index mode.
type EntryIdxMode int

const (

	// HintKeyValAndRAMIdxMode represents ram index (key and value) mode.
	//
	// 内存索引
	HintKeyValAndRAMIdxMode EntryIdxMode = iota

	// HintKeyAndRAMIdxMode represents ram index (only key) mode.
	//
	// 内存索引
	HintKeyAndRAMIdxMode

	// HintBPTSparseIdxMode represents b+ tree sparse index mode.
	//
	// B+ 树磁盘索引
	HintBPTSparseIdxMode
)

// Options records params for creating DB object.
type Options struct {

	// Dir represents Open the database located in which dir.
	// 数据库位于的目录。
	Dir string

	// EntryIdxMode represents using which mode to index the entries.
	// 用那种索引模式。
	EntryIdxMode EntryIdxMode

	// RWMode represents the read and write mode.
	// RWMode includes two options: FileIO and MMap.
	// FileIO represents the read and write mode using standard I/O.
	// MMap represents the read and write mode using mmap.
	//
	//
	// 读写模式：标准IO、MMAP
	RWMode      RWMode

	// 段大小
	SegmentSize int64


	// NodeNum represents the node number.
	// Default NodeNum is 1. NodeNum range [1,1023].
	NodeNum int64

	// SyncEnable represents if call Sync() function.
	// if SyncEnable is false, high write performance but potential data loss likely.
	// if SyncEnable is true, slower but persistent.
	SyncEnable bool

	// StartFileLoadingMode represents when open a database which RWMode to load files.
	StartFileLoadingMode RWMode
}

var defaultSegmentSize int64 = 8 * 1024 * 1024

// DefaultOptions represents the default options.
var DefaultOptions = Options{
	EntryIdxMode:         HintKeyValAndRAMIdxMode,
	SegmentSize:          defaultSegmentSize,
	NodeNum:              1,
	RWMode:               FileIO,
	SyncEnable:           true,
	StartFileLoadingMode: MMap,
}
