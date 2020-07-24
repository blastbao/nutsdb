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

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/blastbao/nutsdb/ds/list"
	"github.com/blastbao/nutsdb/ds/set"
	"github.com/blastbao/nutsdb/ds/zset"
	"github.com/xujiajun/utils/filesystem"
	"github.com/xujiajun/utils/strconv2"
)

var (

	// ErrDBClosed is returned when db is closed.
	ErrDBClosed = errors.New("db is closed")

	// ErrBucket is returned when bucket is not in the HintIdx.
	ErrBucket = errors.New("err bucket")

	// ErrEntryIdxModeOpt is returned when set db EntryIdxMode option is wrong.
	ErrEntryIdxModeOpt = errors.New("err EntryIdxMode option set")

	// ErrFn is returned when fn is nil.
	ErrFn = errors.New("err fn")

)

const (
	// DataDeleteFlag represents the data delete flag
	DataDeleteFlag uint16 = iota

	// DataSetFlag represents the data set flag
	DataSetFlag

	// DataLPushFlag represents the data LPush flag
	DataLPushFlag

	// DataRPushFlag represents the data RPush flag
	DataRPushFlag

	// DataLRemFlag represents the data LRem flag
	DataLRemFlag

	// DataLPopFlag represents the data LPop flag
	DataLPopFlag

	// DataRPopFlag represents the data RPop flag
	DataRPopFlag

	// DataLSetFlag represents the data LSet flag
	DataLSetFlag

	// DataLTrimFlag represents the data LTrim flag
	DataLTrimFlag

	// DataZAddFlag represents the data ZAdd flag
	DataZAddFlag

	// DataZRemFlag represents the data ZRem flag
	DataZRemFlag

	// DataZRemRangeByRankFlag represents the data ZRemRangeByRank flag
	DataZRemRangeByRankFlag

	// DataZPopMaxFlag represents the data ZPopMax flag
	DataZPopMaxFlag

	// DataZPopMinFlag represents the data aZPopMin flag
	DataZPopMinFlag
)

const (
	// UnCommitted represents the tx unCommitted status
	UnCommitted uint16 = 0

	// Committed represents the tx committed status
	Committed uint16 = 1

	// Persistent represents the data persistent flag
	Persistent uint32 = 0

	// ScanNoLimit represents the data scan no limit flag
	ScanNoLimit int = -1
)

const (
	// DataStructureSet represents the data structure set flag
	DataStructureSet uint16 = iota

	// DataStructureSortedSet represents the data structure sorted set flag
	DataStructureSortedSet

	// DataStructureBPTree represents the data structure b+ tree flag
	DataStructureBPTree

	// DataStructureList represents the data structure list flag
	DataStructureList
)

type (

	// DB represents a collection of buckets that persist on disk.
	DB struct {
		opt                     Options   // the database options


		BPTreeIdx               BPTreeIdx // Hint Index

		BPTreeRootIdxes         []*BPTreeRootIdx


		BPTreeKeyEntryPosMap    map[string]int64 // key = bucket+key  val = EntryPos
		bucketMetas             BucketMetasIdx
		SetIdx                  SetIdx
		SortedSetIdx            SortedSetIdx
		ListIdx                 ListIdx
		ActiveFile              *DataFile

		// 内存索引: 存储key=>record
		ActiveBPTreeIdx         *BPTree
		// 内存索引: 存储已提交的事务ID
		ActiveCommittedTxIdsIdx *BPTree

		committedTxIds          map[uint64]struct{}
		MaxFileID               int64
		mu                      sync.RWMutex
		KeyCount                int // total key number ,include expired, deleted, repeated.
		closed                  bool
		isMerging               bool
	}

	// BPTreeIdx represents the B+ tree index
	BPTreeIdx map[string]*BPTree

	// SetIdx represents the set index
	SetIdx map[string]*set.Set

	// SortedSetIdx represents the sorted set index
	SortedSetIdx map[string]*zset.SortedSet

	// ListIdx represents the list index
	ListIdx map[string]*list.List

	// Entries represents entries
	Entries []*Entry

	// BucketMetasIdx represents the index of the bucket's meta-information
	BucketMetasIdx map[string]*BucketMeta
)

// Open returns a newly initialized DB object.
func Open(opt Options) (*DB, error) {
	db := &DB{
		BPTreeIdx:               make(BPTreeIdx),
		SetIdx:                  make(SetIdx),
		SortedSetIdx:            make(SortedSetIdx),
		ListIdx:                 make(ListIdx),
		ActiveBPTreeIdx:         NewTree(),
		MaxFileID:               0,
		opt:                     opt,
		KeyCount:                0,
		closed:                  false,
		committedTxIds:          make(map[uint64]struct{}),
		BPTreeKeyEntryPosMap:    make(map[string]int64),
		bucketMetas:             make(map[string]*BucketMeta),
		ActiveCommittedTxIdsIdx: NewTree(),
	}

	// 目录不存在则创建
	if ok := filesystem.PathIsExist(db.opt.Dir); !ok {
		if err := os.MkdirAll(db.opt.Dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 检查索引模式是否合法
	if err := db.checkEntryIdxMode(); err != nil {
		return nil, err
	}

	//  B+ 树磁盘索引
	if opt.EntryIdxMode == HintBPTSparseIdxMode {

		// 创建 ../bpt/root 目录
		bptRootIdxDir := db.opt.Dir + "/" + bptDir + "/root"
		if ok := filesystem.PathIsExist(bptRootIdxDir); !ok {
			if err := os.MkdirAll(bptRootIdxDir, os.ModePerm); err != nil {
				return nil, err
			}
		}

		// 创建 ../bpt/txid 目录
		bptTxIDIdxDir := db.opt.Dir + "/" + bptDir + "/txid"
		if ok := filesystem.PathIsExist(bptTxIDIdxDir); !ok {
			if err := os.MkdirAll(bptTxIDIdxDir, os.ModePerm); err != nil {
				return nil, err
			}
		}

		// 创建 ../meta/bucket 目录
		bucketMetaDir := db.opt.Dir + "/meta/bucket"
		if ok := filesystem.PathIsExist(bucketMetaDir); !ok {
			if err := os.MkdirAll(bucketMetaDir, os.ModePerm); err != nil {
				return nil, err
			}
		}
	}

	//
	if err := db.buildIndexes(); err != nil {
		return nil, fmt.Errorf("db.buildIndexes error: %s", err)
	}

	return db, nil
}

// 检查索引模式是否合法
func (db *DB) checkEntryIdxMode() error {

	// 是否存在 ".dat" 数据文件
	hasDataFlag   := false
	// 是否存在 "bpt" 子目录
	hasBptDirFlag := false

	files, err := ioutil.ReadDir(db.opt.Dir)
	if err != nil {
		return err
	}

	for _, f := range files {
		// 扩展名检查，判断是否为 ".dat" 数据文件
		if path.Ext(path.Base(f.Name())) == DataSuffix {
			hasDataFlag = true
			if hasBptDirFlag {
				break
			}
		}
		// 文件名检查，判断是否为 "bpt" 子目录
		if f.Name() == bptDir {
			hasBptDirFlag = true
		}
	}

	// 兼容性检查
	if db.opt.EntryIdxMode != HintBPTSparseIdxMode && hasDataFlag && hasBptDirFlag {
		return errors.New("not support HintBPTSparseIdxMode switch to the other EntryIdxMode")
	}

	// 兼容性检查
	if db.opt.EntryIdxMode == HintBPTSparseIdxMode && hasBptDirFlag == false && hasDataFlag == true {
		return errors.New("not support the other EntryIdxMode switch to HintBPTSparseIdxMode")
	}

	return nil
}

// Update executes a function within a managed read/write transaction.
func (db *DB) Update(fn func(tx *Tx) error) error {
	if fn == nil {
		return ErrFn
	}

	return db.managed(true, fn)
}

// View executes a function within a managed read-only transaction.
func (db *DB) View(fn func(tx *Tx) error) error {
	if fn == nil {
		return ErrFn
	}

	return db.managed(false, fn)
}





// Merge removes dirty data and reduce data redundancy,following these steps:
//
// 1. Filter delete or expired entry.
//
// 2. Write entry to activeFile if the key not exist，if exist miss this write operation.
//
// 3. Filter the entry which is committed.
//
// 4. At last remove the merged files.
//
// Caveat: Merge is Called means starting multiple write transactions, and it
// will effect the other write request. so execute it at the appropriate time.
//
//
//
//
//
//
//
func (db *DB) Merge() error {
	var (
		off                 int64
		pendingMergeFIds    []int
		pendingMergeEntries []*Entry
	)

	if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		return errors.New("not support mode `HintBPTSparseIdxMode`")
	}

	db.isMerging = true

	_, pendingMergeFIds = db.getMaxFileIDAndFileIDs()

	if len(pendingMergeFIds) < 2 {
		db.isMerging = false
		return errors.New("the number of files waiting to be merged is at least 2")
	}

	for _, pendingMergeFId := range pendingMergeFIds {
		off = 0
		f, err := NewDataFile(db.getDataPath(int64(pendingMergeFId)), db.opt.SegmentSize, db.opt.RWMode)
		if err != nil {
			db.isMerging = false
			return err
		}

		pendingMergeEntries = []*Entry{}

		for {
			if entry, err := f.ReadAt(int(off)); err == nil {
				if entry == nil {
					break
				}

				if db.isFilterEntry(entry) {
					off += entry.Size()
					if off >= db.opt.SegmentSize {
						break
					}
					continue
				}

				pendingMergeEntries = db.getPendingMergeEntries(entry, pendingMergeEntries)

				off += entry.Size()
				if off >= db.opt.SegmentSize {
					break
				}

			} else {
				if err == io.EOF {
					break
				}
				f.rwManager.Close()
				return fmt.Errorf("when merge operation build hintIndex readAt err: %s", err)
			}
		}

		if err := db.reWriteData(pendingMergeEntries); err != nil {
			f.rwManager.Close()
			return err
		}

		if err := os.Remove(db.getDataPath(int64(pendingMergeFId))); err != nil {
			db.isMerging = false
			f.rwManager.Close()
			return fmt.Errorf("when merge err: %s", err)
		}

		f.rwManager.Close()
	}

	return nil
}

// Backup copies the database to file directory at the given dir.
func (db *DB) Backup(dir string) error {
	err := db.View(func(tx *Tx) error {
		return filesystem.CopyDir(db.opt.Dir, dir)
	})
	if err != nil {
		return err
	}

	return nil
}

// Close releases all db resources.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDBClosed
	}

	db.closed = true

	db.ActiveFile.rwManager.Close()

	db.ActiveFile = nil

	db.BPTreeIdx = nil

	return nil
}

// setActiveFile sets the ActiveFile (DataFile object).
func (db *DB) setActiveFile() (err error) {
	// file_id => file_path
	filepath := db.getDataPath(db.MaxFileID)
	// 构造文件解析器
	db.ActiveFile, err = NewDataFile(filepath, db.opt.SegmentSize, db.opt.RWMode)
	if err != nil {
		return
	}
	// 变量赋值
	db.ActiveFile.fileID = db.MaxFileID
	return nil
}

// getMaxFileIDAndFileIds returns max fileId and fileIds.
//
//
//
func (db *DB) getMaxFileIDAndFileIDs() (maxFileID int64, dataFileIds []int) {
	// 读取目录下所有文件
	files, _ := ioutil.ReadDir(db.opt.Dir)
	if len(files) == 0 {
		return 0, nil
	}
	// 如果是 ".dat" 结尾的数据文件，从文件名中解析出 ID 添加到数组中
	maxFileID = 0
	for _, f := range files {
		// 文件后缀（扩展名）
		fileSuffix := path.Ext(path.Base(f.Name()))
		// 检查是不是 ".dat"
		if fileSuffix != DataSuffix {
			continue
		}
		// 去除 ".dat" 后缀后，得到 file_id ，添加到 dataFileIds 数组中
		id := strings.TrimSuffix(f.Name(), DataSuffix)
		idVal, _ := strconv2.StrToInt(id)
		dataFileIds = append(dataFileIds, idVal)
	}
	// 为空，直接返回
	if len(dataFileIds) == 0 {
		return 0, nil
	}
	// 排序
	sort.Ints(dataFileIds)
	// 取最大值
	maxFileID = int64(dataFileIds[len(dataFileIds)-1])
	// 返回
	return
}

// getActiveFileWriteOff returns the write offset of activeFile.
func (db *DB) getActiveFileWriteOff() (off int64, err error) {
	off = 0
	for {

		// 从文件 offset 处读取一个 Entry
		if item, err := db.ActiveFile.ReadAt(int(off)); err == nil {

			// 读完就结束
			if item == nil {
				break
			}

			// 继续读下一个 Entry
			off += item.Size()

			// set ActiveFileActualSize
			//
			// 更新成员变量
			db.ActiveFile.ActualSize = off

		} else {
			if err == io.EOF {
				break
			}
			return -1, fmt.Errorf("when build activeDataIndex readAt err: %s", err)
		}
	}

	return
}

func (db *DB) parseDataFiles(dataFileIds []int) (unconfirmedRecords []*Record, committedTxIds map[uint64]struct{}, err error) {


	var (
		off int64
		e   *Entry
	)

	committedTxIds = make(map[uint64]struct{})


	// B+ 树磁盘索引，取最后一个数据文件
	if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		dataFileIds = dataFileIds[len(dataFileIds)-1:]
	}


	for _, dataID := range dataFileIds {

		off = 0
		fileID := int64(dataID)

		// 打开 file_id 文件
		f, err := NewDataFile(db.getDataPath(fileID), db.opt.SegmentSize, db.opt.StartFileLoadingMode)
		if err != nil {
			return nil, nil, err
		}

		for {

			// 从数据文件 offset 处读取一个 Entry
			if entry, err := f.ReadAt(int(off)); err == nil {

				if entry == nil {
					break
				}

				e = nil

				// 内存索引
				if db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode {
					e = &Entry{
						Key:   entry.Key,
						Value: entry.Value,
						Meta:  entry.Meta,
					}
				}

				// 已提交
				if entry.Meta.status == Committed {

					// 记录已提交的事务 ID
					committedTxIds[entry.Meta.txID] = struct{}{}

					// 把 txID 写入到已提交事务的索引上
					_ = db.ActiveCommittedTxIdsIdx.Insert(
						[]byte(strconv2.Int64ToStr(int64(entry.Meta.txID))), 	// txID
						nil,													// nil
						&Hint{
							meta: &MetaData{
								Flag: DataSetFlag,								// Set Data
							},
						}, CountFlagEnabled)									//

				}

				// 未确认的记录
				unconfirmedRecords = append(unconfirmedRecords, &Record{
					H: &Hint{
						key:     entry.Key,
						fileID:  fileID,
						meta:    entry.Meta,
						dataPos: uint64(off),
					},
					E: e, // 空
				})

				// B+ 树磁盘索引
				if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
					// 记录 bucket + key 对应的 Entry 的数据文件偏移
					db.BPTreeKeyEntryPosMap[string(entry.Meta.bucket)+string(entry.Key)] = off
				}

				off += entry.Size()

			} else {
				if err == io.EOF {
					break
				}
				if off >= db.opt.SegmentSize {
					break
				}
				f.rwManager.Close()
				return nil, nil, fmt.Errorf("when build hintIndex readAt err: %s", err)
			}
		}

		f.rwManager.Close()
	}

	return
}

func (db *DB) buildBPTreeRootIdxes(dataFileIds []int) error {


	var off int64

	dataFileIdsSize := len(dataFileIds)
	if dataFileIdsSize == 1 {
		return nil
	}

	for i := 0; i < len(dataFileIds[0:dataFileIdsSize-1]); i++ {
		fileID := dataFileIds[i]
		off = 0
		for {

			// 读取 file_id 对应的 B+ 树索引文件
			bs, err := ReadBPTreeRootIdxAt(db.getBPTRootPath(int64(fileID)), off)
			if err == io.EOF || err == nil && bs == nil {
				break
			}

			// 失败报错
			if err != nil {
				return err
			}

			// 成功，则追加存储到 db.BPTreeRootIdxes 中
			if err == nil && bs != nil {
				db.BPTreeRootIdxes = append(db.BPTreeRootIdxes, bs)
				off += bs.Size()
			}

		}
	}

	db.committedTxIds = nil

	return nil
}

func (db *DB) buildBPTreeIdx(bucket string, r *Record) error {

	if _, ok := db.BPTreeIdx[bucket]; !ok {
		db.BPTreeIdx[bucket] = NewTree()
	}

	if err := db.BPTreeIdx[bucket].Insert(r.H.key, r.E, r.H, CountFlagEnabled); err != nil {
		return fmt.Errorf("when build BPTreeIdx insert index err: %s", err)
	}

	return nil
}

func (db *DB) buildActiveBPTreeIdx(r *Record) error {
	// Key = bucket + key
	newKey := append(r.H.meta.bucket, r.H.key...)
	// 插入到 B+ 树索引中
	if err := db.ActiveBPTreeIdx.Insert(newKey, r.E, r.H, CountFlagEnabled); err != nil {
		return fmt.Errorf("when build BPTreeIdx insert index err: %s", err)
	}
	return nil
}

func (db *DB) buildBucketMetaIdx() error {

	// 检查索引模式是不是 B+ 树
	if db.opt.EntryIdxMode == HintBPTSparseIdxMode {

		// 获取 meta 目录下所有文件
		files, err := ioutil.ReadDir(db.getBucketMetaPath())
		if err != nil {
			return err
		}

		// 读取 "bucket_xxx.meta" 文件内容，存储到 db.bucketMetas[bucket_xxx] 上
		if len(files) != 0 {

			for _, f := range files {

				fileSuffix := path.Ext(path.Base(f.Name()))
				if fileSuffix != BucketMetaSuffix {
					continue
				}

				bucketName := strings.TrimSuffix(f.Name(), BucketMetaSuffix)
				bucketMeta, err := ReadBucketMeta(db.getBucketMetaFilePath(bucketName))
				if err != nil {
					return err
				}
				db.bucketMetas[bucketName] = bucketMeta
			}
		}
	}

	return nil
}

func (db *DB) buildOtherIdxes(bucket string, r *Record) error {


	if r.H.meta.ds == DataStructureSet {
		if err := db.buildSetIdx(bucket, r); err != nil {
			return err
		}
	}

	if r.H.meta.ds == DataStructureSortedSet {
		if err := db.buildSortedSetIdx(bucket, r); err != nil {
			return err
		}
	}

	if r.H.meta.ds == DataStructureList {
		if err := db.buildListIdx(bucket, r); err != nil {
			return err
		}
	}

	return nil
}

// buildHintIdx builds the Hint Indexes.
func (db *DB) buildHintIdx(dataFileIds []int) error {

	// 未确认记录、已提交记录
	unconfirmedRecords, committedTxIds, err := db.parseDataFiles(dataFileIds)

	db.committedTxIds = committedTxIds

	if err != nil {
		return err
	}


	if len(unconfirmedRecords) == 0 {
		return nil
	}


	// 遍历所有未确认的记录
	for _, r := range unconfirmedRecords {


		// 如果记录对应的 txID 已经提交
		if _, ok := db.committedTxIds[r.H.meta.txID]; ok {

			//
			bucket := string(r.H.meta.bucket)

			// B+ 树
			if r.H.meta.ds == DataStructureBPTree {

				// 设置状态为 "Committed"
				r.H.meta.status = Committed

				// B+ 树磁盘索引
				if db.opt.EntryIdxMode == HintBPTSparseIdxMode {
					// 插入到 B+ 树中
					if err = db.buildActiveBPTreeIdx(r); err != nil {
						return err
					}
				// B+ 树内存索引
				} else {
					// 插入到 B+ 树中
					if err = db.buildBPTreeIdx(bucket, r); err != nil {
						return err
					}
				}
			}

			//
			if err = db.buildOtherIdxes(bucket, r); err != nil {
				return err
			}

			db.KeyCount++
		}
	}

	// B+ 树磁盘索引
	if HintBPTSparseIdxMode == db.opt.EntryIdxMode {
		if err = db.buildBPTreeRootIdxes(dataFileIds); err != nil {
			return err
		}
	}

	return nil
}

// buildSetIdx builds set index when opening the DB.
func (db *DB) buildSetIdx(bucket string, r *Record) error {


	if _, ok := db.SetIdx[bucket]; !ok {
		db.SetIdx[bucket] = set.New()
	}

	if r.E == nil {
		return ErrEntryIdxModeOpt
	}

	if r.H.meta.Flag == DataSetFlag {
		if err := db.SetIdx[bucket].SAdd(string(r.E.Key), r.E.Value); err != nil {
			return fmt.Errorf("when build SetIdx SAdd index err: %s", err)
		}
	}

	if r.H.meta.Flag == DataDeleteFlag {
		if err := db.SetIdx[bucket].SRem(string(r.E.Key), r.E.Value); err != nil {
			return fmt.Errorf("when build SetIdx SRem index err: %s", err)
		}
	}

	return nil
}

// buildSortedSetIdx builds sorted set index when opening the DB.
func (db *DB) buildSortedSetIdx(bucket string, r *Record) error {
	if _, ok := db.SortedSetIdx[bucket]; !ok {
		db.SortedSetIdx[bucket] = zset.New()
	}

	if r.H.meta.Flag == DataZAddFlag {
		keyAndScore := strings.Split(string(r.E.Key), SeparatorForZSetKey)
		if len(keyAndScore) == 2 {
			key := keyAndScore[0]
			score, _ := strconv2.StrToFloat64(keyAndScore[1])
			if r.E == nil {
				return ErrEntryIdxModeOpt
			}
			_ = db.SortedSetIdx[bucket].Put(key, zset.SCORE(score), r.E.Value)
		}
	}
	if r.H.meta.Flag == DataZRemFlag {
		_ = db.SortedSetIdx[bucket].Remove(string(r.E.Key))
	}
	if r.H.meta.Flag == DataZRemRangeByRankFlag {
		start, _ := strconv2.StrToInt(string(r.E.Key))
		end, _ := strconv2.StrToInt(string(r.E.Value))
		_ = db.SortedSetIdx[bucket].GetByRankRange(start, end, true)
	}
	if r.H.meta.Flag == DataZPopMaxFlag {
		_ = db.SortedSetIdx[bucket].PopMax()
	}
	if r.H.meta.Flag == DataZPopMinFlag {
		_ = db.SortedSetIdx[bucket].PopMin()
	}

	return nil
}

//buildListIdx builds List index when opening the DB.
func (db *DB) buildListIdx(bucket string, r *Record) error {
	if _, ok := db.ListIdx[bucket]; !ok {
		db.ListIdx[bucket] = list.New()
	}

	if r.E == nil {
		return ErrEntryIdxModeOpt
	}

	switch r.H.meta.Flag {
	case DataLPushFlag:
		_, _ = db.ListIdx[bucket].LPush(string(r.E.Key), r.E.Value)
	case DataRPushFlag:
		_, _ = db.ListIdx[bucket].RPush(string(r.E.Key), r.E.Value)
	case DataLRemFlag:
		count, _ := strconv2.StrToInt(string(r.E.Value))
		if _, err := db.ListIdx[bucket].LRem(string(r.E.Key), count); err != nil {
			return ErrWhenBuildListIdx(err)
		}
	case DataLPopFlag:
		if _, err := db.ListIdx[bucket].LPop(string(r.E.Key)); err != nil {
			return ErrWhenBuildListIdx(err)
		}
	case DataRPopFlag:
		if _, err := db.ListIdx[bucket].RPop(string(r.E.Key)); err != nil {
			return ErrWhenBuildListIdx(err)
		}
	case DataLSetFlag:
		keyAndIndex := strings.Split(string(r.E.Key), SeparatorForListKey)
		newKey := keyAndIndex[0]
		index, _ := strconv2.StrToInt(keyAndIndex[1])
		if err := db.ListIdx[bucket].LSet(newKey, index, r.E.Value); err != nil {
			return ErrWhenBuildListIdx(err)
		}
	case DataLTrimFlag:
		keyAndStartIndex := strings.Split(string(r.E.Key), SeparatorForListKey)
		newKey := keyAndStartIndex[0]
		start, _ := strconv2.StrToInt(keyAndStartIndex[1])
		end, _ := strconv2.StrToInt(string(r.E.Value))
		if err := db.ListIdx[bucket].Ltrim(newKey, start, end); err != nil {
			return ErrWhenBuildListIdx(err)
		}
	}

	return nil
}

// ErrWhenBuildListIdx returns err when build listIdx
func ErrWhenBuildListIdx(err error) error {
	return fmt.Errorf("when build listIdx LRem err: %s", err)
}




// buildIndexes builds indexes when db initialize resource.
//
//
//
func (db *DB) buildIndexes() (err error) {

	var (
		maxFileID   int64
		dataFileIds []int
	)

	// 读取数据库目录中 ".dat" 结尾的数据文件，从文件名中解析出一组 file_ids，排序后返回
	maxFileID, dataFileIds = db.getMaxFileIDAndFileIDs()

	// init db.ActiveFile
	db.MaxFileID = maxFileID

	// set ActiveFile
	//
	// 具有最大 file_id 的数据文件，是当前最新的文件，打开它
	if err = db.setActiveFile(); err != nil {
		return
	}

	if dataFileIds == nil && maxFileID == 0 {
		return
	}

	// 获取最新写入偏移
	if db.ActiveFile.writeOff, err = db.getActiveFileWriteOff(); err != nil {
		return
	}

	// 初始化 buckets 相关
	if err = db.buildBucketMetaIdx(); err != nil {
		return
	}

	// build hint index
	return db.buildHintIdx(dataFileIds)
}


// managed calls a block of code that is fully contained in a transaction.
//
//
func (db *DB) managed(writable bool, fn func(tx *Tx) error) error {

	var tx *Tx

	// 开启事务
	tx, err := db.Begin(writable)
	if err != nil {
		return err
	}

	// 执行事务
	if err = fn(tx); err != nil {
		// 失败回滚
		if errRollback := tx.Rollback(); errRollback != nil {
			return errRollback
		}
		return err
	}

	// 提交事务
	if err = tx.Commit(); err != nil {
		// 失败回滚
		if errRollback := tx.Rollback(); errRollback != nil {
			return errRollback
		}
		return err
	}

	return nil
}

const bptDir = "bpt"

// getDataPath returns the data path at given fid.
func (db *DB) getDataPath(fID int64) string {
	return db.opt.Dir + "/" + strconv2.Int64ToStr(fID) + DataSuffix
}

func (db *DB) getMetaPath() string {
	return db.opt.Dir + "/meta"
}

func (db *DB) getBucketMetaPath() string {
	return db.getMetaPath() + "/bucket"
}

func (db *DB) getBucketMetaFilePath(name string) string {
	return db.getBucketMetaPath() + "/" + name + BucketMetaSuffix
}

func (db *DB) getBPTDir() string {
	return db.opt.Dir + "/" + bptDir
}

func (db *DB) getBPTPath(fID int64) string {
	return db.getBPTDir() + "/" + strconv2.Int64ToStr(fID) + BPTIndexSuffix
}
//
func (db *DB) getBPTRootPath(fID int64) string {
	return db.getBPTDir() + "/root/" + strconv2.Int64ToStr(fID) + BPTRootIndexSuffix
}

func (db *DB) getBPTTxIDPath(fID int64) string {
	return db.getBPTDir() + "/txid/" + strconv2.Int64ToStr(fID) + BPTTxIDIndexSuffix
}

func (db *DB) getBPTRootTxIDPath(fID int64) string {
	return db.getBPTDir() + "/txid/" + strconv2.Int64ToStr(fID) + BPTRootTxIDIndexSuffix
}

func (db *DB) getPendingMergeEntries(entry *Entry, pendingMergeEntries []*Entry) []*Entry {


	if entry.Meta.ds == DataStructureBPTree {
		if r, err := db.BPTreeIdx[string(entry.Meta.bucket)].Find(entry.Key); err == nil {
			if r.H.meta.Flag == DataSetFlag {
				pendingMergeEntries = append(pendingMergeEntries, entry)
			}
		}
	}

	if entry.Meta.ds == DataStructureSet {
		if db.SetIdx[string(entry.Meta.bucket)].SIsMember(string(entry.Key), entry.Value) {
			pendingMergeEntries = append(pendingMergeEntries, entry)
		}
	}

	if entry.Meta.ds == DataStructureSortedSet {
		keyAndScore := strings.Split(string(entry.Key), SeparatorForZSetKey)
		if len(keyAndScore) == 2 {
			key := keyAndScore[0]
			n := db.SortedSetIdx[string(entry.Meta.bucket)].GetByKey(key)
			if n != nil {
				pendingMergeEntries = append(pendingMergeEntries, entry)
			}
		}
	}

	if entry.Meta.ds == DataStructureList {
		items, _ := db.ListIdx[string(entry.Meta.bucket)].LRange(string(entry.Key), 0, -1)
		ok := false
		if entry.Meta.Flag == DataRPushFlag || entry.Meta.Flag == DataLPushFlag {
			for _, item := range items {
				if string(entry.Value) == string(item) {
					ok = true
					break
				}
			}
			if ok {
				pendingMergeEntries = append(pendingMergeEntries, entry)
			}
		}
	}

	return pendingMergeEntries
}

func (db *DB) reWriteData(pendingMergeEntries []*Entry) error {

	// 开启事务
	tx, err := db.Begin(true)
	if err != nil {
		db.isMerging = false
		return err
	}

	// 打开新文件
	dataFile, err := NewDataFile(db.getDataPath(db.MaxFileID+1), db.opt.SegmentSize, db.opt.RWMode)
	if err != nil {
		db.isMerging = false
		return err
	}
	db.ActiveFile = dataFile
	db.MaxFileID++

	// 遍历 Entries
	for _, e := range pendingMergeEntries {

		// 逐个 put
		err := tx.put(string(e.Meta.bucket), e.Key, e.Value, e.Meta.TTL, e.Meta.Flag, e.Meta.timestamp, e.Meta.ds)

		// 失败则回滚
		if err != nil {
			tx.Rollback()
			db.isMerging = false
			return err
		}
	}

	// 提交
	_ = tx.Commit()
	return nil
}

func (db *DB) isFilterEntry(entry *Entry) bool {
	if  entry.Meta.Flag == DataDeleteFlag ||
		entry.Meta.Flag == DataRPopFlag ||
		entry.Meta.Flag == DataLPopFlag ||
		entry.Meta.Flag == DataLRemFlag ||
		entry.Meta.Flag == DataLTrimFlag ||
		entry.Meta.Flag == DataZRemFlag ||
		entry.Meta.Flag == DataZRemRangeByRankFlag ||
		entry.Meta.Flag == DataZPopMaxFlag ||
		entry.Meta.Flag == DataZPopMinFlag ||
		IsExpired(entry.Meta.TTL, entry.Meta.timestamp) {
		return true
	}

	return false
}
