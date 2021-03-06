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
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/xujiajun/utils/strconv2"
)


func getNewKey(bucket string, key []byte) []byte {
	newKey := []byte(bucket)
	newKey = append(newKey, key...)
	return newKey
}


func (tx *Tx) getByHintBPTSparseIdxInMem(bucket string, key []byte) (e *Entry, err error) {


	// Read in memory.


	// 从内存索引中读取 key 关联的 record
	r, err := tx.db.ActiveBPTreeIdx.Find(key)

	if err == nil && r != nil {
		// 查询事务 ID 是否已经提交
		if _, err := tx.db.ActiveCommittedTxIdsIdx.Find([]byte(strconv2.Int64ToStr(int64(r.H.meta.txID)))); err == nil {
			// 如果已经提交，去关联的数据文件中取出 offset 处的 entry 数据，并返回。
			dataPath := tx.db.getDataPath(r.H.fileID)
			dataFile, err := NewDataFile(dataPath, tx.db.opt.SegmentSize, tx.db.opt.RWMode)
			defer dataFile.rwManager.Close()
			if err != nil {
				return nil, err
			}
			return dataFile.ReadAt(int(r.H.dataPos))
		}

		// 尚未提交，报错
		return nil, ErrNotFoundKey
	}

	return nil, nil
}

func (tx *Tx) getByHintBPTSparseIdxOnDisk(bucket string, key []byte) (entry *Entry, err error) {

	// Read on disk.

	var bptSparseIdxGroup []*BPTreeRootIdx

	for _, bptRootIdxPointer := range tx.db.BPTreeRootIdxes {
		bptSparseIdxGroup = append(bptSparseIdxGroup, &BPTreeRootIdx{
			fID:     bptRootIdxPointer.fID,			// 关联数据文件的 fileID
			rootOff: bptRootIdxPointer.rootOff,		// 关联索引文件的 root offset
			start:   bptRootIdxPointer.start,		//
			end:     bptRootIdxPointer.end,			//
		})
	}

	// Sort the fid from largest to smallest, to ensure that the latest data is first compared.
	SortFID(bptSparseIdxGroup, func(p, q *BPTreeRootIdx) bool {
		return p.fID > q.fID
	})

	// IndexKey = bucket + key
	newKey := getNewKey(bucket, key)

	for _, idx := range bptSparseIdxGroup {

		// 区间查询： start <= IndexKey <= end
		if compare(newKey, idx.start) >= 0 && compare(newKey, idx.end) <= 0 {

			// 搜索 B+ 树，得到 newKey 在 data 文件中 offset，进而读取出对应 entry 。
			entry, err = tx.FindOnDisk(idx.fID, idx.rootOff, key, newKey)

			if err == nil && entry != nil {

				// 过滤已删除和已过期的 entry
				if entry.Meta.Flag == DataDeleteFlag || IsExpired(entry.Meta.TTL, entry.Meta.timestamp) {
					return nil, ErrNotFoundKey
				}

				// 若事务已经提交，则直接返回
				txIDStr := strconv2.Int64ToStr(int64(entry.Meta.txID))
				if _, err := tx.db.ActiveCommittedTxIdsIdx.Find([]byte(txIDStr)); err == nil {
					return entry, err
				}

				// 从 B+ 树中查找 txid ，成功返回 true ，代表已经提交
				if ok, _ := tx.FindTxIDOnDisk(idx.fID, entry.Meta.txID); !ok {
					return nil, ErrNotFoundKey
				}

				return entry, err
			}
		}
		continue
	}

	return nil, nil
}

func (tx *Tx) getByHintBPTSparseIdx(bucket string, key []byte) (e *Entry, err error) {

	newKey := getNewKey(bucket, key)

	// 内存查询
	entry, err := tx.getByHintBPTSparseIdxInMem(bucket, newKey)
	if entry != nil && err == nil {
		if entry.Meta.Flag == DataDeleteFlag || IsExpired(entry.Meta.TTL, entry.Meta.timestamp) {
			return nil, ErrNotFoundKey
		}
		return entry, err
	}

	// 磁盘查询
	entry, err = tx.getByHintBPTSparseIdxOnDisk(bucket, key)
	if entry != nil && err == nil {
		return entry, err
	}

	return nil, ErrNotFoundKey
}

func (tx *Tx) getAllByHintBPTSparseIdx(bucket string) (entries Entries, err error) {

	// 读取 Bucket Meta 信息
	bucketMeta, err := ReadBucketMeta(tx.db.getBucketMetaFilePath(bucket))
	if err != nil {
		return nil, err
	}

	//
	return tx.RangeScan(bucket, bucketMeta.start, bucketMeta.end)
}

// Get retrieves the value for a key in the bucket.
// The returned value is only valid for the life of the transaction.
func (tx *Tx) Get(bucket string, key []byte) (e *Entry, err error) {


	// 数据库已关闭？
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	// 索引模式？
	idxMode := tx.db.opt.EntryIdxMode

	// B+ 树索引？
	if idxMode == HintBPTSparseIdxMode {
		return tx.getByHintBPTSparseIdx(bucket, key)
	}

	//
	if idxMode == HintKeyValAndRAMIdxMode || idxMode == HintKeyAndRAMIdxMode {

		if idx, ok := tx.db.BPTreeIdx[bucket]; ok {

			r, err := idx.Find(key)
			if err != nil {
				return nil, err
			}

			if _, ok := tx.db.committedTxIds[r.H.meta.txID]; !ok {
				return nil, ErrNotFoundKey
			}

			if r.H.meta.Flag == DataDeleteFlag || r.IsExpired() {
				return nil, ErrNotFoundKey
			}

			if idxMode == HintKeyValAndRAMIdxMode {
				return r.E, nil
			}

			if idxMode == HintKeyAndRAMIdxMode {

				path := tx.db.getDataPath(r.H.fileID)
				df, err := NewDataFile(path, tx.db.opt.SegmentSize, tx.db.opt.RWMode)
				defer df.rwManager.Close()

				if err != nil {
					return nil, err
				}

				item, err := df.ReadAt(int(r.H.dataPos))
				if err != nil {
					return nil, fmt.Errorf("read err. pos %d, key %s, err %s", r.H.dataPos, string(key), err)
				}

				return item, nil
			}
		}
	}

	return nil, errors.New("not found bucket:" + bucket + ",key:" + string(key))
}

//GetAll returns all keys and values of the bucket stored at given bucket.
func (tx *Tx) GetAll(bucket string) (entries Entries, err error) {

	// 数据库已关闭？
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	entries = Entries{}

	idxMode := tx.db.opt.EntryIdxMode

	if idxMode == HintBPTSparseIdxMode {
		return tx.getAllByHintBPTSparseIdx(bucket)
	}

	if idxMode == HintKeyValAndRAMIdxMode || idxMode == HintKeyAndRAMIdxMode {
		if index, ok := tx.db.BPTreeIdx[bucket]; ok {
			records, err := index.All()
			if err != nil {
				return nil, ErrBucketEmpty
			}

			entries, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, entries, RangeScan)
			if err != nil {
				return nil, ErrBucketEmpty
			}
		}
	}

	if len(entries) == 0 {
		return nil, ErrBucketEmpty
	}

	return
}

// RangeScan query a range at given bucket, start and end slice.
func (tx *Tx) RangeScan(bucket string, start, end []byte) (es Entries, err error) {

	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {

		newStart, newEnd := getNewKey(bucket, start), getNewKey(bucket, end)

		// 内存索引：范围查询，得到一组索引项
		records, err := tx.db.ActiveBPTreeIdx.Range(newStart, newEnd)

		if err == nil && records != nil {

			// 根据索引项查找关联数据，存储到结果切片 es 中
			for _, r := range records {

				// 确定所属数据文件，并打开
				dataPath := tx.db.getDataPath(r.H.fileID)
				dataFile, err := NewDataFile(dataPath, tx.db.opt.SegmentSize, tx.db.opt.RWMode)
				if err != nil {
					dataFile.rwManager.Close()
					return nil, err
				}

				// 根据数据偏移读取 Entry
				if entry, err := dataFile.ReadAt(int(r.H.dataPos)); err == nil {
					es = append(es, entry)
				} else {
					dataFile.rwManager.Close()
					return nil, fmt.Errorf("HintIdx r.Hi.dataPos %d, err %s", r.H.dataPos, err)
				}

				// 关闭文件
				dataFile.rwManager.Close()
			}

		}


		// 磁盘索引：范围查询，得到一组索引项
		entries, err := tx.rangeScanOnDisk(bucket, start, end)
		if err != nil {
			return nil, err
		}
		es = append(es, entries...)

		if len(es) == 0 {
			return nil, ErrRangeScan
		}

		return processEntriesScanOnDisk(es), nil
	}




	if index, ok := tx.db.BPTreeIdx[bucket]; ok {

		// 内存索引：范围查询，得到一组索引项
		records, err := index.Range(start, end)
		if err != nil {
			return nil, ErrRangeScan
		}

		// 内存索引：范围查询，得到一组索引项
		es, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, es, RangeScan)
		if err != nil {
			return nil, ErrRangeScan
		}
	}

	if len(es) == 0 {
		return nil, ErrRangeScan
	}

	return
}

func (tx *Tx) rangeScanOnDisk(bucket string, start, end []byte) ([]*Entry, error) {


	var result []*Entry

	bptSparseIdxGroup := tx.db.BPTreeRootIdxes

	SortFID(bptSparseIdxGroup, func(p, q *BPTreeRootIdx) bool {
		return p.fID > q.fID
	})

	newStart, newEnd := getNewKey(bucket, start), getNewKey(bucket, end)

	for _, bptSparseIdx := range bptSparseIdxGroup {

		if compare(newStart, bptSparseIdx.start) <= 0 && compare(bptSparseIdx.start, newEnd) <= 0 ||
		   compare(newStart, bptSparseIdx.end)   <= 0 && compare(bptSparseIdx.end, newEnd)   <= 0 {

			entries, err := tx.findRangeOnDisk(int64(bptSparseIdx.fID), int64(bptSparseIdx.rootOff), start, end, newStart, newEnd)

			if err != nil {
				return nil, err
			}
			result = append(
				result,
				entries...,
			)
		}
	}

	return result, nil
}

func (tx *Tx) prefixScanOnDisk(bucket string, prefix []byte, offsetNum int, limitNum int) ([]*Entry, int, error) {
	var result []*Entry
	var off int

	bptSparseIdxGroup := tx.db.BPTreeRootIdxes
	SortFID(bptSparseIdxGroup, func(p, q *BPTreeRootIdx) bool {
		return p.fID > q.fID
	})

	newPrefix := getNewKey(bucket, prefix)
	leftNum := limitNum

	for _, bptSparseIdx := range bptSparseIdxGroup {
		if compare(newPrefix, bptSparseIdx.start) <= 0 || compare(newPrefix, bptSparseIdx.end) <= 0 {
			entries, voff, err := tx.findPrefixOnDisk(bucket, int64(bptSparseIdx.fID), int64(bptSparseIdx.rootOff), prefix, newPrefix, offsetNum, leftNum)
			if err != nil {
				return nil, off, err
			}

			leftNum -= len(entries)

			result = append(
				result,
				entries...,
			)

			off = off + voff

			if len(result) == limitNum {
				return result, off, nil
			}
		}
	}

	return result, off, nil
}

func (tx *Tx) prefixSearchScanOnDisk(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) ([]*Entry, int, error) {
	var result []*Entry
	var off int

	bptSparseIdxGroup := tx.db.BPTreeRootIdxes
	SortFID(bptSparseIdxGroup, func(p, q *BPTreeRootIdx) bool {
		return p.fID > q.fID
	})

	newPrefix := getNewKey(bucket, prefix)
	leftNum := limitNum

	for _, bptSparseIdx := range bptSparseIdxGroup {
		if compare(newPrefix, bptSparseIdx.start) <= 0 || compare(newPrefix, bptSparseIdx.end) <= 0 {
			entries, voff, err := tx.findPrefixSearchOnDisk(bucket, int64(bptSparseIdx.fID), int64(bptSparseIdx.rootOff), prefix, reg, newPrefix, offsetNum, leftNum)
			if err != nil {
				return nil, off, err
			}

			leftNum -= len(entries)

			result = append(
				result,
				entries...,
			)

			off = off + voff

			if len(result) == limitNum {
				return result, off, nil
			}
		}
	}

	return result, off, nil
}

func processEntriesScanOnDisk(entriesTemp []*Entry) (result []*Entry) {
	var entriesMap map[string]*Entry
	entriesMap = make(map[string]*Entry)

	for _, entry := range entriesTemp {
		if _, ok := entriesMap[string(entry.Key)]; !ok {
			entriesMap[string(entry.Key)] = entry
		}
	}

	keys, es := SortedEntryKeys(entriesMap)
	for _, key := range keys {
		if !IsExpired(es[key].Meta.TTL, es[key].Meta.timestamp) && es[key].Meta.Flag != DataDeleteFlag {
			result = append(result, es[key])
		}
	}

	return result
}

func (tx *Tx) getStartIndexForFindPrefix(fID int64, curr *BinaryNode, prefix []byte) (uint16, error) {
	var j uint16
	var entry *Entry

	for j = 0; j < curr.KeysNum; j++ {
		df, err := NewDataFile(tx.db.getDataPath(fID), tx.db.opt.SegmentSize, tx.db.opt.RWMode)
		if err != nil {
			return 0, err
		}

		entry, err = df.ReadAt(int(curr.Keys[j]))
		df.rwManager.Close()
		if err != nil {
			return 0, err
		}

		newKey := getNewKey(string(entry.Meta.bucket), entry.Key)
		if compare(newKey, prefix) >= 0 {
			break
		}
	}

	return j, nil
}

func (tx *Tx) findPrefixOnDisk(bucket string, fID, rootOff int64, prefix, newPrefix []byte, offsetNum int, limitNum int) (es []*Entry, off int, err error) {
	var (
		i, j  uint16
		entry *Entry
		curr  *BinaryNode
	)

	if curr, err = tx.FindLeafOnDisk(fID, rootOff, prefix, newPrefix); err != nil && curr == nil {
		return nil, off, err
	}

	if j, err = tx.getStartIndexForFindPrefix(fID, curr, newPrefix); err != nil {
		return nil, off, err
	}

	scanFlag := true
	numFound := 0
	filepath := tx.db.getBPTPath(fID)

	coff := 0

	for curr != nil && scanFlag {
		for i = j; i < curr.KeysNum; i++ {

			if coff < offsetNum {
				coff++
				continue
			}

			df, err := NewDataFile(tx.db.getDataPath(fID), tx.db.opt.SegmentSize, tx.db.opt.RWMode)
			if err != nil {
				return nil, off, err
			}

			entry, err = df.ReadAt(int(curr.Keys[i]))
			df.rwManager.Close()
			if err != nil {
				return nil, off, err
			}

			if !bytes.HasPrefix(entry.Key, prefix) || string(entry.Meta.bucket) != bucket {
				scanFlag = false
				break
			}

			es = append(es, entry)
			numFound++

			if limitNum > 0 && numFound == limitNum {
				scanFlag = false
				break
			}
		}

		address := curr.NextAddress
		if address == DefaultInvalidAddress {
			break
		}
		curr, err = ReadNode(filepath, address)
		if err != nil {
			return nil, off, err
		}
		j = 0
	}

	off = coff

	return
}

func (tx *Tx) findPrefixSearchOnDisk(bucket string, fID, rootOff int64, prefix []byte, reg string, newPrefix []byte, offsetNum int, limitNum int) (es []*Entry, off int, err error) {
	var (
		i, j  uint16
		entry *Entry
		curr  *BinaryNode
	)

	rgx, err := regexp.Compile(reg)
	if err != nil {
		return nil, off, ErrBadRegexp
	}

	if curr, err = tx.FindLeafOnDisk(fID, rootOff, prefix, newPrefix); err != nil && curr == nil {
		return nil, off, err
	}

	if j, err = tx.getStartIndexForFindPrefix(fID, curr, newPrefix); err != nil {
		return nil, off, err
	}

	scanFlag := true
	numFound := 0
	filepath := tx.db.getBPTPath(fID)

	coff := 0

	for curr != nil && scanFlag {
		for i = j; i < curr.KeysNum; i++ {

			if coff < offsetNum {
				coff++
				continue
			}

			df, err := NewDataFile(tx.db.getDataPath(fID), tx.db.opt.SegmentSize, tx.db.opt.RWMode)
			if err != nil {
				return nil, off, err
			}

			entry, err = df.ReadAt(int(curr.Keys[i]))
			df.rwManager.Close()
			if err != nil {
				return nil, off, err
			}

			if !bytes.HasPrefix(entry.Key, prefix) || string(entry.Meta.bucket) != bucket {
				scanFlag = false
				break
			}

			if !rgx.Match(bytes.TrimPrefix(entry.Key, prefix)) {
				continue
			}

			es = append(es, entry)
			numFound++

			if limitNum > 0 && numFound == limitNum {
				scanFlag = false
				break
			}
		}

		address := curr.NextAddress
		if address == DefaultInvalidAddress {
			break
		}
		curr, err = ReadNode(filepath, address)
		if err != nil {
			return nil, off, err
		}
		j = 0
	}

	off = coff

	return
}

func (tx *Tx) getStartIndexForFindRange(fID int64, curr *BinaryNode, start, newStart []byte) (uint16, error) {
	var entry *Entry
	var j uint16

	for j = 0; j < curr.KeysNum; j++ {
		df, err := NewDataFile(tx.db.getDataPath(fID), tx.db.opt.SegmentSize, tx.db.opt.RWMode)
		if err != nil {
			return 0, err
		}

		entry, err = df.ReadAt(int(curr.Keys[j]))
		df.rwManager.Close()

		if err != nil {
			return 0, err
		}

		newStartTemp := getNewKey(string(entry.Meta.bucket), entry.Key)
		if compare(newStartTemp, newStart) >= 0 {
			break
		}
	}

	return j, nil
}

func (tx *Tx) findRangeOnDisk(fID, rootOff int64, start, end, newStart, newEnd []byte) (es []*Entry, err error) {

	var (
		i, j  uint16
		entry *Entry
		curr  *BinaryNode
	)

	if curr, err = tx.FindLeafOnDisk(fID, rootOff, start, newStart); err != nil && curr == nil {
		return nil, err
	}

	if j, err = tx.getStartIndexForFindRange(fID, curr, start, newStart); err != nil {
		return nil, err
	}

	scanFlag := true
	filepath := tx.db.getBPTPath(fID)

	for curr != nil && scanFlag {
		for i = j; i < curr.KeysNum; i++ {

			df, err := NewDataFile(tx.db.getDataPath(int64(fID)), tx.db.opt.SegmentSize, tx.db.opt.RWMode)
			if err != nil {
				return nil, err
			}

			entry, err = df.ReadAt(int(curr.Keys[i]))
			df.rwManager.Close()

			if err != nil {
				return nil, err
			}

			newEndTemp := getNewKey(string(entry.Meta.bucket), entry.Key)

			if compare(newEndTemp, newEnd) > 0 {
				scanFlag = false
				break
			}

			es = append(es, entry)
		}

		address := curr.NextAddress
		if address == DefaultInvalidAddress {
			break
		}
		curr, err = ReadNode(filepath, address)
		if err != nil {
			return nil, err
		}

		j = 0
	}

	return
}

func (tx *Tx) prefixScanByHintBPTSparseIdx(bucket string, prefix []byte, offsetNum int, limitNum int) (es Entries, off int, err error) {
	newPrefix := getNewKey(bucket, prefix)
	records, voff, err := tx.db.ActiveBPTreeIdx.PrefixScan(newPrefix, offsetNum, limitNum)
	if err == nil && records != nil {
		for _, r := range records {
			path := tx.db.getDataPath(r.H.fileID)
			df, err := NewDataFile(path, tx.db.opt.SegmentSize, tx.db.opt.RWMode)
			if err != nil {
				df.rwManager.Close()
				return nil, off, err
			}
			if item, err := df.ReadAt(int(r.H.dataPos)); err == nil {
				es = append(es, item)
				if len(es) == limitNum {
					off = voff
					return es, off, nil
				}
			} else {
				df.rwManager.Close()
				return nil, off, fmt.Errorf("HintIdx r.Hi.dataPos %d, err %s", r.H.dataPos, err)
			}
			df.rwManager.Close()
		}
	}

	leftNum := limitNum - len(es)
	if leftNum > 0 {
		entries, voff, err := tx.prefixScanOnDisk(bucket, prefix, offsetNum, leftNum)
		if err != nil {
			return nil, off, err
		}
		es = append(es, entries...)
		off = voff
	}

	off = voff

	if len(es) == 0 {
		return nil, off, ErrPrefixScan
	}

	return processEntriesScanOnDisk(es), off, nil
}

func (tx *Tx) prefixSearchScanByHintBPTSparseIdx(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) (es Entries, off int, err error) {
	newPrefix := getNewKey(bucket, prefix)
	records, voff, err := tx.db.ActiveBPTreeIdx.PrefixSearchScan(newPrefix, reg, offsetNum, limitNum)
	if err == nil && records != nil {
		for _, r := range records {
			path := tx.db.getDataPath(r.H.fileID)
			df, err := NewDataFile(path, tx.db.opt.SegmentSize, tx.db.opt.RWMode)
			if err != nil {
				df.rwManager.Close()
				return nil, off, err
			}
			if item, err := df.ReadAt(int(r.H.dataPos)); err == nil {
				es = append(es, item)
				if len(es) == limitNum {
					off = voff
					return es, off, nil
				}
			} else {
				df.rwManager.Close()
				return nil, off, fmt.Errorf("HintIdx r.Hi.dataPos %d, err %s", r.H.dataPos, err)
			}
			df.rwManager.Close()
		}
	}

	leftNum := limitNum - len(es)
	if leftNum > 0 {
		entries, voff, err := tx.prefixSearchScanOnDisk(bucket, prefix, reg, offsetNum, leftNum)
		if err != nil {
			return nil, off, err
		}
		es = append(es, entries...)
		off = voff
	}

	off = voff

	if len(es) == 0 {
		return nil, off, ErrPrefixSearchScan
	}

	return processEntriesScanOnDisk(es), off, nil
}

// PrefixScan iterates over a key prefix at given bucket, prefix and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) (es Entries, off int, err error) {

	if err := tx.checkTxIsClosed(); err != nil {
		return nil, off, err
	}

	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		return tx.prefixScanByHintBPTSparseIdx(bucket, prefix, offsetNum, limitNum)
	}

	if idx, ok := tx.db.BPTreeIdx[bucket]; ok {
		records, voff, err := idx.PrefixScan(prefix, offsetNum, limitNum)
		if err != nil {
			off = voff
			return nil, off, ErrPrefixScan
		}

		es, err = tx.getHintIdxDataItemsWrapper(records, limitNum, es, PrefixScan)
		if err != nil {
			off = voff
			return nil, off, ErrPrefixScan
		}

		off = voff

	}

	if len(es) == 0 {
		return nil, off, ErrPrefixScan
	}

	return
}

// PrefixSearchScan iterates over a key prefix at given bucket, prefix, match regular expression and limitNum.
// LimitNum will limit the number of entries return.
func (tx *Tx) PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) (es Entries, off int, err error) {

	if err := tx.checkTxIsClosed(); err != nil {
		return nil, off, err
	}

	if tx.db.opt.EntryIdxMode == HintBPTSparseIdxMode {
		return tx.prefixSearchScanByHintBPTSparseIdx(bucket, prefix, reg, offsetNum, limitNum)
	}

	if idx, ok := tx.db.BPTreeIdx[bucket]; ok {
		records, voff, err := idx.PrefixSearchScan(prefix, reg, offsetNum, limitNum)
		if err != nil {
			off = voff
			return nil, off, ErrPrefixSearchScan
		}

		es, err = tx.getHintIdxDataItemsWrapper(records, limitNum, es, PrefixSearchScan)
		if err != nil {
			off = voff
			return nil, off, ErrPrefixSearchScan
		}

		off = voff

	}

	if len(es) == 0 {
		return nil, off, ErrPrefixSearchScan
	}

	return
}

// Delete removes a key from the bucket at given bucket and key.
func (tx *Tx) Delete(bucket string, key []byte) error {
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	return tx.put(bucket, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBPTree)
}

// getHintIdxDataItemsWrapper returns wrapped entries when prefix scanning or range scanning.
func (tx *Tx) getHintIdxDataItemsWrapper(records Records, limitNum int, es Entries, scanMode string) (Entries, error) {

	for _, r := range records {

		// 过滤删除和过期的索引项
		if r.H.meta.Flag == DataDeleteFlag || r.IsExpired() {
			continue
		}

		//
		if limitNum > 0 && len(es) < limitNum || limitNum == ScanNoLimit {
			idxMode := tx.db.opt.EntryIdxMode
			if idxMode == HintKeyAndRAMIdxMode {
				dataPath := tx.db.getDataPath(r.H.fileID)
				dataFile, err := NewDataFile(dataPath, tx.db.opt.SegmentSize, tx.db.opt.RWMode)
				if err != nil {
					return nil, err
				}
				if entry, err := dataFile.ReadAt(int(r.H.dataPos)); err == nil {
					es = append(es, entry)
				} else {
					dataFile.rwManager.Close()
					return nil, fmt.Errorf("HintIdx r.Hi.dataPos %d, err %s", r.H.dataPos, err)
				}
				dataFile.rwManager.Close()
			}

			if idxMode == HintKeyValAndRAMIdxMode {
				es = append(es, r.E)
			}
		}
	}

	return es, nil
}

// FindTxIDOnDisk returns if txId on disk at given fid and txID.
//
// 1. 根据 fileId 查找对应的 B+ 树索引的元数据
// 2. 根据 B+ 树索引元数据，定位到 B+ 树索引 root 结点
// 3. 从 root 开始深度搜索，查找 txID 所位于的 lead node
// 4. 顺序遍历 leaf node 查找和  txID 匹配的数据条目，找到则返回 true
func (tx *Tx) FindTxIDOnDisk(fID, txID uint64) (ok bool, err error) {

	var i uint16

	// 读取 ".bptrtxid" 文件，它存储着 B+ 树索引的元数据
	idxMetaPath := tx.db.getBPTRootTxIDPath(int64(fID))
	idxMetaNode, err := ReadNode(idxMetaPath, 0)

	if err != nil {
		return false, err
	}

	// 读取 ".bpttxid" 文件，它存储着 B+ 树的数据
	idxPath := tx.db.getBPTTxIDPath(int64(fID))
	rootAddress := idxMetaNode.Keys[0]

	curr, err := ReadNode(idxPath, rootAddress)
	if err != nil {
		return false, err
	}

	txIDStr := strconv2.IntToStr(int(txID))

	// 定位到 leaf node
	for curr.IsLeaf != 1 {
		i = 0
		for i < curr.KeysNum {
			if compare([]byte(txIDStr), []byte(strconv2.Int64ToStr(curr.Keys[i]))) >= 0 {
				i++
			} else {
				break
			}
		}

		address := curr.Pointers[i]
		curr, err = ReadNode(idxPath, int64(address))
	}

	if curr == nil {
		return false, ErrKeyNotFound
	}

	// 遍历 leaf node ，查找和 txID 匹配的目标记录
	for i = 0; i < curr.KeysNum; i++ {
		if compare([]byte(txIDStr), []byte(strconv2.Int64ToStr(curr.Keys[i]))) == 0 {
			break
		}
	}

	if i == curr.KeysNum {
		return false, ErrKeyNotFound
	}

	return true, nil
}

// FindOnDisk returns entry on disk at given fID, rootOff and key.
//
// 1. 根据 newKey 去深搜 B+ 树，确定 leaf Node
// 2. 遍历 leaf node 的 offsets ，去 data 文件逐个 entry 读取和比较
// 3. 找到 newKey 对应的 entry ，返回
//
func (tx *Tx) FindOnDisk(fID uint64, rootOff uint64, key, newKey []byte) (entry *Entry, err error) {
	var (
		bnLeaf *BinaryNode
		i      uint16
		dataFile *DataFile
	)

	// 从 root 结点深度搜索 B+ 树，定位到 newKey 所属的 leaf 结点
	bnLeaf, err = tx.FindLeafOnDisk(int64(fID), int64(rootOff), key, newKey)

	// 若没找到，即不存在
	if bnLeaf == nil {
		return nil, ErrKeyNotFound
	}

	// 遍历叶子结点，逐个根据 offset 读取 entry 进行比较判断，若找到 newKey 对应的 entry 则返回
	for i = 0; i < bnLeaf.KeysNum; i++ {

		dataFile, err = NewDataFile(tx.db.getDataPath(int64(fID)), tx.db.opt.SegmentSize, tx.db.opt.RWMode)
		if err != nil {
			return nil, err
		}

		entry, err = dataFile.ReadAt(int(bnLeaf.Keys[i]))
		dataFile.rwManager.Close()

		if err != nil {
			return nil, err
		}

		newKeyTemp := getNewKey(string(entry.Meta.bucket), entry.Key)
		if entry != nil && compare(newKey, newKeyTemp) == 0 {
			return entry, nil
		}
	}

	// 若没找到，即不存在
	if i == bnLeaf.KeysNum {
		return nil, ErrKeyNotFound
	}

	return
}

// FindLeafOnDisk returns binary leaf node on disk at given fId, rootOff and key.
func (tx *Tx) FindLeafOnDisk(fileID int64, rootOff int64, key, newKey []byte) (bn *BinaryNode, err error) {

	var i uint16
	var curr *BinaryNode

	dataPath := tx.db.getDataPath(fileID)
	indexPath := tx.db.getBPTPath(fileID)

	// 读取 root 结点
	curr, err = ReadNode(indexPath, rootOff)
	if err != nil {
		return nil, err
	}

	// not leaf node represents node address
	for curr.IsLeaf != 1 {

		i = 0

		for i < curr.KeysNum {

			dataFile, err := NewDataFile(dataPath, tx.db.opt.SegmentSize, tx.db.opt.RWMode)
			if err != nil {
				return nil, err
			}

			entry, err := dataFile.ReadAt(int(curr.Keys[i]))
			dataFile.rwManager.Close()

			if err != nil {
				return nil, err
			}

			newKeyTemp := getNewKey(string(entry.Meta.bucket), entry.Key)
			if compare(newKey, newKeyTemp) >= 0 {
				i++
			} else {
				break
			}
		}

		address := curr.Pointers[i]
		curr, err = ReadNode(indexPath, address)
	}

	return curr, nil
}
