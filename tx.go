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
	"os"
	"strings"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/blastbao/nutsdb/ds/list"
	"github.com/blastbao/nutsdb/ds/set"
	"github.com/blastbao/nutsdb/ds/zset"
	"github.com/xujiajun/utils/strconv2"
)

var (
	// ErrKeyAndValSize is returned when given key and value size is too big.
	ErrKeyAndValSize = errors.New("key and value size too big")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx is closed")

	// ErrTxNotWritable is returned when performing a write operation on
	// a read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrKeyEmpty is returned if an empty key is passed on an update function.
	ErrKeyEmpty = errors.New("key cannot be empty")

	// ErrBucketEmpty is returned if bucket is empty.
	ErrBucketEmpty = errors.New("bucket is empty")

	// ErrRangeScan is returned when range scanning not found the result
	ErrRangeScan = errors.New("range scans not found")

	// ErrPrefixScan is returned when prefix scanning not found the result
	ErrPrefixScan = errors.New("prefix scans not found")

	// ErrPrefixSearchScan is returned when prefix and search scanning not found the result
	ErrPrefixSearchScan = errors.New("prefix and search scans not found")

	// ErrNotFoundKey is returned when key not found int the bucket on an view function.
	ErrNotFoundKey = errors.New("key not found in the bucket")
)

// Tx represents a transaction.
type Tx struct {
	id                     uint64
	db                     *DB
	writable               bool
	pendingWrites          []*Entry
	ReservedStoreTxIDIdxes map[int64]*BPTree
}

// Begin opens a new transaction.
// Multiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
// All transactions must be closed by calling Commit() or Rollback() when done.
func (db *DB) Begin(writable bool) (tx *Tx, err error) {
	// 创建事务对象
	tx, err = newTx(db, writable)
	if err != nil {
		return nil, err
	}

	// 事务加锁：写加独占锁、读加共享锁
	tx.lock()

	// 若数据库已关闭，释放锁，并报错
	if db.closed {
		tx.unlock()
		return nil, ErrDBClosed
	}
	return
}

// newTx returns a newly initialized Tx object at given writable.
func newTx(db *DB, writable bool) (tx *Tx, err error) {

	tx = &Tx{
		db:                     db,
		writable:               writable,
		pendingWrites:          []*Entry{},
		ReservedStoreTxIDIdxes: make(map[int64]*BPTree),
	}

	// 生成唯一事务 ID
	txID, err := tx.getTxID()
	if err != nil {
		return nil, err
	}

	tx.id = txID
	return
}

// getTxID returns the tx id.
func (tx *Tx) getTxID() (id uint64, err error) {
	node, err := snowflake.NewNode(tx.db.opt.NodeNum)
	if err != nil {
		return 0, err
	}

	id = uint64(node.Generate().Int64())
	return
}

// Commit commits the transaction, following these steps:
// 	1. check the length of pendingWrites.If there are no writes, return immediately.
// 	2. check if the ActiveFile has not enough space to store entry. if not, call rotateActiveFile function.
// 	3. write pendingWrites to disk, if a non-nil error,return the error.
// 	4. build Hint index.
// 	5. Unlock the database and clear the db field.
//
// 步骤：
// 	1. 检查 pendingWrites 的长度。如果没有，立即返回。
// 	2. 检查 ActiveFile 是否没有足够的空间来存储条目。如果没有足够空间，调用 rotateActiveFile 函数。
// 	3. 将 pendingWrites 写入磁盘，如果出现错误，报错返回。
// 	4. 建立 Hint 索引。
// 	5. 解锁数据库，清除 db 字段。
//
func (tx *Tx) Commit() error {

	var (
		off            int64
		e              *Entry
		bucketMetaTemp BucketMeta
	)

	if tx.db == nil {
		return ErrDBClosed
	}

	// 1.
	writesLen := len(tx.pendingWrites)
	if writesLen == 0 {
		tx.unlock()
		tx.db = nil
		return nil
	}

	//
	lastIndex := writesLen - 1
	countFlag := CountFlagEnabled
	if tx.db.isMerging {
		countFlag = CountFlagDisabled
	}

	//
	for i := 0; i < writesLen; i++ {

		entry := tx.pendingWrites[i]

		// 检查 entry 大小，超过限制则报错
		entrySize := entry.Size()
		if entrySize > tx.db.opt.SegmentSize {
			return ErrKeyAndValSize
		}

		// 确定 bucket
		bucket := string(entry.Meta.bucket)

		// 检查当前活跃数据文件是否足以容纳新的 entry ，若不足够则滚动文件，生成新文件
		if tx.db.ActiveFile.ActualSize + entrySize > tx.db.opt.SegmentSize {
			// 文件滚动，生成新数据文件
			if err := tx.rotateActiveFile(); err != nil {
				return err
			}
		}

		// ????? B+ 树索引
		if entry.Meta.ds == DataStructureBPTree {
			// 修改内存 Map ，本质是 Hash 表
			tx.db.BPTreeKeyEntryPosMap[string(entry.Meta.bucket)+string(entry.Key)] = tx.db.ActiveFile.writeOff
		}

		// [核心] 只有最后一个记录的状态为 "committed"
		if i == lastIndex {
			entry.Meta.status = Committed
		}

		// 记录下当前 off，用于后面更新索引。
		off = tx.db.ActiveFile.writeOff

		// [核心][数据落盘-1] 把 entry 追加写入到当前数据文件中
		if _, err := tx.db.ActiveFile.WriteAt(entry.Encode(), tx.db.ActiveFile.writeOff); err != nil {
			return err
		}
		// [核心][数据落盘-2] 调用 sync() 持久化
		if tx.db.opt.SyncEnable {
			if err := tx.db.ActiveFile.rwManager.Sync(); err != nil {
				return err
			}
		}

		// 修改文件写入偏移
		tx.db.ActiveFile.ActualSize += entrySize
		tx.db.ActiveFile.writeOff += entrySize

		// B+ 树
		if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
			bucketMetaTemp = tx.buildTempBucketMetaIdx(bucket, entry.Key, bucketMetaTemp)
		}

		// 将最后一个记录写入到数据文件之后，才可以提交事务
		if i == lastIndex {

			// 事务 ID
			txID := entry.Meta.txID

			// 索引模式：B+ 树索引
			if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {

				// 将事务 ID 写入到提交树
				if err := tx.buildTxIDRootIdx(txID, countFlag); err != nil {
					return err
				}

				// 更新 bucket 的信息，主要是 key 的范围
				if err := tx.buildBucketMetaIdx(bucket, entry.Key, bucketMetaTemp); err != nil {
					return err
				}

			// 索引模式：内存索引
			} else {
				// 把事务 ID 记录到已提交列表中
				tx.db.committedTxIds[txID] = struct{}{}
			}
		}

		e = nil
		if tx.db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode {
			e = entry
		}

		// [核心][索引落盘] 把 Entry 存储到 B+ 树数据索引上，可以理解为是内存操作，不会失败，如果失败则是宕机、重启会重新构建。
		if entry.Meta.ds == DataStructureBPTree {
			// off : Entry 在数据文件的偏移
			tx.buildBPTreeIdx(bucket, entry, e, off, countFlag)
		}
	}

	// 将 pendingWrites 添加到 Set/List/SortedSet 的内存索引中
	tx.buildIdxes(writesLen)

	// 数据库解锁
	tx.unlock()
	// 重置变量
	tx.db = nil
	tx.pendingWrites = nil
	tx.ReservedStoreTxIDIdxes = nil

	return nil
}

func (tx *Tx) buildTempBucketMetaIdx(bucket string, key []byte, bucketMetaTemp BucketMeta) BucketMeta {

	keySize := uint32(len(key))

	if bucketMetaTemp.start == nil {

		//
		bucketMetaTemp = BucketMeta{
			start: key,
			end: key,
			startSize: keySize,
			endSize: keySize,
		}

	} else {


		if compare(bucketMetaTemp.start, key) > 0 {
			bucketMetaTemp.start = key
			bucketMetaTemp.startSize = keySize
		}


		if compare(bucketMetaTemp.end, key) < 0 {
			bucketMetaTemp.end = key
			bucketMetaTemp.endSize = keySize
		}


	}



	return bucketMetaTemp
}

func (tx *Tx) buildBucketMetaIdx(bucket string, key []byte, bucketMetaTemp BucketMeta) error {

	start, startSize := bucketMetaTemp.start, uint32(len(bucketMetaTemp.start))
	end, endSize := bucketMetaTemp.end, uint32(len(bucketMetaTemp.end))

	var updateFlag bool

	bucketMeta, ok := tx.db.bucketMetas[bucket]
	if !ok {
		bucketMeta = &BucketMeta{
			start: start,
			end: end,
			startSize: startSize,
			endSize: endSize,
		}
		updateFlag = true
	} else {
		// start 变小，则更新 start
		if compare(bucketMeta.start, bucketMetaTemp.start) > 0 {
			bucketMeta.start = start
			bucketMeta.startSize = startSize
			updateFlag = true
		}
		// end 变大，则更新 end
		if compare(bucketMeta.end, bucketMetaTemp.end) < 0 {
			bucketMeta.end = end
			bucketMeta.endSize = endSize
			updateFlag = true
		}
	}

	// 需要更新 bucket meta
	if updateFlag {

		// 打开 bucket meta 磁盘文件
		fd, err := os.OpenFile(tx.db.getBucketMetaFilePath(bucket), os.O_CREATE|os.O_RDWR, 0644)
		defer fd.Close()
		if err != nil {
			return err
		}

		// 覆盖写入新 bucket meta 信息
		if _, err = fd.WriteAt(bucketMeta.Encode(), 0); err != nil {
			return err
		}

		// sync()
		if tx.db.opt.SyncEnable {
			if err = fd.Sync(); err != nil {
				return err
			}
		}

		// 重新读入到内存
		tx.db.bucketMetas[bucket] = bucketMeta
	}

	return nil
}


func (tx *Tx) buildTxIDRootIdx(txID uint64, countFlag bool) error {

	txIDStr := strconv2.IntToStr(int(txID))

	// 把 txID 插入到已提交的事务 ID B+ 树索引中
	tx.db.ActiveCommittedTxIdsIdx.Insert([]byte(txIDStr), nil, &Hint{meta: &MetaData{Flag: DataSetFlag}}, countFlag)


	// 如果在事务中，数据文件发生滚动，意味着事务 txID 写入的数据，出现在多个数据文件中。
	//
	// 每个数据文件都存在一个关联的 commited txid 索引，
	//
	//
	// 因此，在提交 txID 时，把 txID 添加到滚动的各个 file_id 数据文件关联的 commit txid 索引中，同时将其落盘。
	//
	if len(tx.ReservedStoreTxIDIdxes) > 0 {

		// 遍历这些被滚动的 TxID B+ 树索引
		for fID, txIDIdx := range tx.ReservedStoreTxIDIdxes {

			// 把 fID 关联的、用于存储已提交的 txid 的 B+ 树索引，存储到 /txid/file_id.bpttxid 文件中。
			txIDIdx.Insert([]byte(txIDStr), nil, &Hint{ meta: &MetaData{Flag: DataSetFlag} }, countFlag)
			txIDIdx.Filepath = tx.db.getBPTTxIDPath(fID)

			// 落盘
			err := txIDIdx.WriteNodes(tx.db.opt.RWMode, tx.db.opt.SyncEnable, 2)
			if err != nil {
				return err
			}

			// 刚刚落盘的这颗 B+ 树的元数据存储到 /txid/file_id.bptrtxid 文件中。
			txIDRootIdx := NewTree()
			txIDRootIdx.Insert([]byte(strconv2.Int64ToStr(txIDIdx.root.Address)), nil, &Hint{meta: &MetaData{Flag: DataSetFlag}}, countFlag)
			txIDRootIdx.Filepath = tx.db.getBPTRootTxIDPath(fID)
			err = txIDRootIdx.WriteNodes(tx.db.opt.RWMode, tx.db.opt.SyncEnable, 2)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// 将 tx.pendingWrites[] 添加到 Set/List/SortedSet 的内存索引中
func (tx *Tx) buildIdxes(writesLen int) {
	for i := 0; i < writesLen; i++ {
		entry := tx.pendingWrites[i]
		bucket := string(entry.Meta.bucket)
		if entry.Meta.ds == DataStructureSet {
			tx.buildSetIdx(bucket, entry)
		}
		if entry.Meta.ds == DataStructureSortedSet {
			tx.buildSortedSetIdx(bucket, entry)
		}
		if entry.Meta.ds == DataStructureList {
			tx.buildListIdx(bucket, entry)
		}
		tx.db.KeyCount++
	}
}

//
func (tx *Tx) buildBPTreeIdx(bucket string, entry, e *Entry, off int64, countFlag bool) {

	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {

		// 构造索引键：IndexKey := bucket + key
		idxKey := append([]byte(bucket), entry.Key...)

		// 插入到 B+ 树
		_ =	tx.db.ActiveBPTreeIdx.Insert(
			// 索引键
			idxKey,
			// 数据项
			e,
			// 索引项
			&Hint{
				fileID:  tx.db.ActiveFile.fileID,
				key:     idxKey,
				meta:    entry.Meta,
				dataPos: uint64(off),
			},
			// 计数标记
			countFlag,
		)

	} else {

		if _, ok := tx.db.BPTreeIdx[bucket]; !ok {
			tx.db.BPTreeIdx[bucket] = NewTree()
		}

		if tx.db.BPTreeIdx[bucket] == nil {
			tx.db.BPTreeIdx[bucket] = NewTree()
		}

		_ = tx.db.BPTreeIdx[bucket].Insert(
			entry.Key,
			e,
			&Hint{
				fileID:  tx.db.ActiveFile.fileID,
				key:     entry.Key,
				meta:    entry.Meta,
				dataPos: uint64(off),
			},
			countFlag,
		)

	}
}

func (tx *Tx) buildSetIdx(bucket string, entry *Entry) {
	if _, ok := tx.db.SetIdx[bucket]; !ok {
		tx.db.SetIdx[bucket] = set.New()
	}

	if entry.Meta.Flag == DataDeleteFlag {
		_ = tx.db.SetIdx[bucket].SRem(string(entry.Key), entry.Value)
	}

	if entry.Meta.Flag == DataSetFlag {
		_ = tx.db.SetIdx[bucket].SAdd(string(entry.Key), entry.Value)
	}
}

func (tx *Tx) buildSortedSetIdx(bucket string, entry *Entry) {
	if _, ok := tx.db.SortedSetIdx[bucket]; !ok {
		tx.db.SortedSetIdx[bucket] = zset.New()
	}

	switch entry.Meta.Flag {
	case DataZAddFlag:
		keyAndScore := strings.Split(string(entry.Key), SeparatorForZSetKey)
		key := keyAndScore[0]
		score, _ := strconv2.StrToFloat64(keyAndScore[1])
		_ = tx.db.SortedSetIdx[bucket].Put(key, zset.SCORE(score), entry.Value)
	case DataZRemFlag:
		_ = tx.db.SortedSetIdx[bucket].Remove(string(entry.Key))
	case DataZRemRangeByRankFlag:
		start, _ := strconv2.StrToInt(string(entry.Key))
		end, _ := strconv2.StrToInt(string(entry.Value))
		_ = tx.db.SortedSetIdx[bucket].GetByRankRange(start, end, true)
	case DataZPopMaxFlag:
		_ = tx.db.SortedSetIdx[bucket].PopMax()
	case DataZPopMinFlag:
		_ = tx.db.SortedSetIdx[bucket].PopMin()
	}
}

func (tx *Tx) buildListIdx(bucket string, entry *Entry) {

	if _, ok := tx.db.ListIdx[bucket]; !ok {
		tx.db.ListIdx[bucket] = list.New()
	}

	key, value := entry.Key, entry.Value
	switch entry.Meta.Flag {
	case DataLPushFlag:
		_, _ = tx.db.ListIdx[bucket].LPush(string(key), value)
	case DataRPushFlag:
		_, _ = tx.db.ListIdx[bucket].RPush(string(key), value)
	case DataLRemFlag:
		count, _ := strconv2.StrToInt(string(value))
		_, _ = tx.db.ListIdx[bucket].LRem(string(key), count)
	case DataLPopFlag:
		_, _ = tx.db.ListIdx[bucket].LPop(string(key))
	case DataRPopFlag:
		_, _ = tx.db.ListIdx[bucket].RPop(string(key))
	case DataLSetFlag:
		keyAndIndex := strings.Split(string(key), SeparatorForListKey)
		newKey := keyAndIndex[0]
		index, _ := strconv2.StrToInt(keyAndIndex[1])
		_ = tx.db.ListIdx[bucket].LSet(newKey, index, value)
	case DataLTrimFlag:
		keyAndStartIndex := strings.Split(string(key), SeparatorForListKey)
		newKey := keyAndStartIndex[0]
		start, _ := strconv2.StrToInt(keyAndStartIndex[1])
		end, _ := strconv2.StrToInt(string(value))
		_ = tx.db.ListIdx[bucket].Ltrim(newKey, start, end)
	}
}















// rotateActiveFile rotates log file when active file is not enough space to store the entry.
//
//
// 文件滚动
//
// 1. 调用 sync() 和 close() 关闭当前数据文件 /file_id.dat ，结束写入。
//
// 2. db.ActiveBPTreeIdx 中存储当前数据文件的 B+ 树索引，需要落到磁盘文件 /bpt/file_id.bptidx 上。
//
// 3. 将 db.ActiveBPTreeIdx 落到磁盘后，把它的元信息 BPTreeRootIdx 落到磁盘文件 /bpt/root/file_id.bptridx 上。
//
// 4. 至此，已经完成滚动前的落盘操作，下面执行一组内存操作：
//
//		4.1. 把滚动后的 B+ 树索引元数据 BPTreeRootIdx 添加到内存数组 tx.db.BPTreeRootIdxes 中
//		4.2. 把 db.ActiveBPTreeIdx 清空&重置
//		4.3. 把当前数据文件关联的已提交事务索引 db.ActiveCommittedTxIdsIdx 移动到 tx.ReservedStoreTxIDIdxes[fID] 中，然后清空&重置 db.ActiveCommittedTxIdsIdx
//
//
// 5. db.ActiveCommittedTxIdsIdx 中存储当前数据文件的 committed txid B+ 树索引，需要落到磁盘文件 /txid/file_id.bpttxid 上。
//
//
//
// 5. 完成滚动，递增文件 ID ，打开新的数据文件接受新的写入请求。
//
//
func (tx *Tx) rotateActiveFile() error {


	var err error

	fID := tx.db.MaxFileID
	tx.db.MaxFileID++

	if !tx.db.opt.SyncEnable && tx.db.opt.RWMode == MMap {
		if err := tx.db.ActiveFile.rwManager.Sync(); err != nil {
			return err
		}
	}

	// 关闭
	if err := tx.db.ActiveFile.rwManager.Close(); err != nil {
		return err
	}

	// B+ 树稀疏索引
	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {

		// 获取当前 fileId 对应的 b+ 树索引文件路径，"xxx.bptidx"
		tx.db.ActiveBPTreeIdx.Filepath = tx.db.getBPTPath(fID)
		tx.db.ActiveBPTreeIdx.enabledKeyPosMap = true
		tx.db.ActiveBPTreeIdx.SetKeyPosMap(tx.db.BPTreeKeyEntryPosMap)

		// 将 B+ 树落盘
		err = tx.db.ActiveBPTreeIdx.WriteNodes(tx.db.opt.RWMode, tx.db.opt.SyncEnable, 1)
		if err != nil {
			return err
		}

		// 将 B+ 树元数据落盘，主要是根结点信息
		BPTreeRootIdx := &BPTreeRootIdx{
			rootOff:   uint64(tx.db.ActiveBPTreeIdx.root.Address),
			fID:       uint64(fID),
			startSize: uint32(len(tx.db.ActiveBPTreeIdx.FirstKey)),
			endSize:   uint32(len(tx.db.ActiveBPTreeIdx.LastKey)),
			start:     tx.db.ActiveBPTreeIdx.FirstKey,
			end:       tx.db.ActiveBPTreeIdx.LastKey,
		}

		// 将 root 索引对象写入到文件 "xxx.bptridx" 中
		_, err := BPTreeRootIdx.Persistence(tx.db.getBPTRootPath(fID), 0, tx.db.opt.SyncEnable)
		if err != nil {
			return err
		}

		// 保存到内存数组中
		tx.db.BPTreeRootIdxes = append(tx.db.BPTreeRootIdxes, BPTreeRootIdx)

		// clear and reset BPTreeKeyEntryPosMap
		tx.db.BPTreeKeyEntryPosMap = nil
		tx.db.BPTreeKeyEntryPosMap = make(map[string]int64)

		// clear and reset ActiveBPTreeIdx
		tx.db.ActiveBPTreeIdx = nil
		tx.db.ActiveBPTreeIdx = NewTree()


		// 每个数据文件有唯一关联的、用于存储已提交的 txid 的 B+ 树索引，当数据文件发生滚动时，需要把这个索引也落盘。
		//
		// 当前数据文件 tx.db.ActiveFile 关联的索引即 db.ActiveCommittedTxIdsIdx ，所以当写事务发生滚动时，
		// 将 db.ActiveCommittedTxIdsIdx 暂存到 tx 的成员变量中，然后重置 db.ActiveCommittedTxIdsIdx 变量，
		// 等到 tx 提交时，再一次行将所有涉及到的 txid 索引落盘。
		//
		//
		// 把关联的已提交事务索引 db.ActiveCommittedTxIdsIdx 暂存起来，然后清空&重置，
		tx.ReservedStoreTxIDIdxes[fID] = tx.db.ActiveCommittedTxIdsIdx
		// clear and reset ActiveCommittedTxIdsIdx
		tx.db.ActiveCommittedTxIdsIdx = nil
		tx.db.ActiveCommittedTxIdsIdx = NewTree()
	}


	// 重置 ActiveFile
	path := tx.db.getDataPath(tx.db.MaxFileID)
	tx.db.ActiveFile, err = NewDataFile(path, tx.db.opt.SegmentSize, tx.db.opt.RWMode)
	if err != nil {
		return err
	}
	tx.db.ActiveFile.fileID = tx.db.MaxFileID
	return nil
}

// Rollback closes the transaction.
func (tx *Tx) Rollback() error {
	if tx.db == nil {
		return ErrDBClosed
	}
	// 释放数据库锁
	tx.unlock()
	// 清理变量
	tx.db = nil
	tx.pendingWrites = nil
	return nil
}

// lock locks the database based on the transaction type.
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

// Put sets the value for a key in the bucket.
// a wrapper of the function put.
func (tx *Tx) Put(bucket string, key, value []byte, ttl uint32) error {
	// DataSetFlag => Set Data
	// DataStructureBPTree => B+ tree
	return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().Unix()), DataStructureBPTree)
}

func (tx *Tx) checkTxIsClosed() error {
	if tx.db == nil {
		return ErrTxClosed
	}
	return nil
}

// put sets the value for a key in the bucket.
// Returns an error if tx is closed, if performing a write operation on a read-only transaction, if the key is empty.
//
//
// flag
//
//
func (tx *Tx) put(bucket string, key, value []byte, ttl uint32, flag uint16, timestamp uint64, ds uint16) error {

	// 检查数据库是否已关闭
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	// 检查事务是否可写
	if !tx.writable {
		return ErrTxNotWritable
	}

	// 检查 key 是否为空
	if len(key) == 0 {
		return ErrKeyEmpty
	}

	// 构造 entry
	entry := &Entry{
		Key:   key,
		Value: value,
		Meta: &MetaData{
			keySize:    uint32(len(key)),
			valueSize:  uint32(len(value)),
			timestamp:  timestamp,
			Flag:       flag,
			TTL:        ttl,
			bucket:     []byte(bucket),
			bucketSize: uint32(len(bucket)),
			status:     UnCommitted,			// 未提交
			ds:         ds,
			txID:       tx.id,
		},
	}

	// 将待写入的 entry 添加到 pending 队列
	tx.pendingWrites = append(tx.pendingWrites, entry)

	return nil
}
