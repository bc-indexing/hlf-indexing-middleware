/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package history

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/queryresult"
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	protoutil "github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// QueryExecutor is a query executor against the LevelDB history DB
type QueryExecutor struct {
	levelDB    *leveldbhelper.DBHandle
	blockStore *blkstorage.BlockStore
}

// GetHistoryForKey implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetHistoryForKey(namespace string, key string) (commonledger.ResultsIterator, error) {
	rangeScan := constructRangeScan(namespace, key)
	dbItr, err := q.levelDB.GetIterator(rangeScan.startKey, rangeScan.endKey)
	if err != nil {
		return nil, err
	}

	// By default, dbItr is in the orderer of oldest to newest and its cursor is at the beginning of the entries.
	// Need to call Last() and Next() to move the cursor to the end of the entries so that we can iterate
	// the entries in the order of newest to oldest.
	if dbItr.Last() {
		dbItr.Next()
	}
	return &historyScanner{rangeScan, namespace, key, dbItr, q.blockStore, 0, nil, -1}, nil
}

// historyScanner implements ResultsIterator for iterating through history results
type historyScanner struct {
	rangeScan    *rangeScan
	namespace    string
	key          string
	dbItr        iterator.Iterator
	blockStore   *blkstorage.BlockStore
	currentBlock uint64
	transactions []uint64
	txIndex      int
}

// Next iterates to the next key, in the order of newest to oldest, from history scanner.
// It decodes blockNumTranNumBytes to get blockNum and tranNum,
// loads the block:tran from block storage, finds the key and returns the result.
func (scanner *historyScanner) Next() (commonledger.QueryResult, error) {
	if scanner.txIndex <= -1 {
		if !scanner.dbItr.Prev() {
			return nil, nil
		}
		indexVal := scanner.dbItr.Value()
		currentBlock, transactions, err := decodeNewIndex(indexVal)
		if err != nil {
			return nil, err
		}
		scanner.currentBlock = currentBlock
		scanner.transactions = transactions
		scanner.txIndex = len(scanner.transactions) - 1
	}

	blockNum := scanner.currentBlock
	tranNum := scanner.transactions[scanner.txIndex]
	scanner.txIndex--

	logger.Debugf("Found history record for namespace:%s key:%s at blockNumTranNum %v:%v\n",
		scanner.namespace, scanner.key, blockNum, tranNum)

	// Get the transaction from block storage that is associated with this history record
	tranEnvelope, err := scanner.blockStore.RetrieveTxByBlockNumTranNum(blockNum, tranNum)
	if err != nil {
		return nil, err
	}

	// Get the txid, key write value, timestamp, and delete indicator associated with this transaction
	queryResult, err := getKeyModificationFromTran(tranEnvelope, scanner.namespace, scanner.key)
	if err != nil {
		return nil, err
	}
	if queryResult == nil {
		// should not happen, but make sure there is inconsistency between historydb and statedb
		logger.Errorf("No namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, scanner.key, blockNum, tranNum)
		return nil, errors.Errorf("no namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, scanner.key, blockNum, tranNum)
	}
	logger.Debugf("Found historic key value for namespace:%s key:%s from transaction %s",
		scanner.namespace, scanner.key, queryResult.(*queryresult.KeyModification).TxId)
	return queryResult, nil
}

func (scanner *historyScanner) Close() {
	scanner.dbItr.Release()
}

// GetHistoryForKeys implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetHistoryForKeys(namespace string, keys []string) (commonledger.ResultsIterator, error) {
	scanners := make(map[string]*historyScanner)
	for _, key := range keys {
		scanner, err := q.GetHistoryForKey(namespace, key)
		if err != nil {
			return nil, err
		}
		var ok bool
		scanners[key], ok = scanner.(*historyScanner)
		if !ok {
			return nil, errors.Errorf("Error converting commonledger.ResultsIterator to historyScanner")
		}
	}
	scanner := &multipleHistoryScanner{namespace, keys, scanners, 0}
	return scanner, nil
}

// historyScanner implements ResultsIterator for iterating through history results
type multipleHistoryScanner struct {
	namespace       string
	keys            []string
	scanners        map[string]*historyScanner
	currentKeyIndex int
}

func (scanner *multipleHistoryScanner) Next() (commonledger.QueryResult, error) {
	key := scanner.keys[scanner.currentKeyIndex]

	queryResult, err := scanner.scanners[key].Next()
	if err != nil {
		return nil, err
	}

	for queryResult == nil {
		scanner.currentKeyIndex++
		if scanner.currentKeyIndex >= len(scanner.keys) {
			return nil, nil
		}

		key := scanner.keys[scanner.currentKeyIndex]

		queryResult, err = scanner.scanners[key].Next()
		if err != nil {
			return nil, err
		}
	}

	logger.Debugf("Found historic key value for namespace:%s key:%s from transaction %s",
		scanner.namespace, key, queryResult.(*queryresult.KeyModification).TxId)

	return queryResult, nil
}

func (scanner *multipleHistoryScanner) Close() {
	for _, key := range scanner.keys {
		scanner.scanners[key].dbItr.Release()
	}
}

// getTxIDandKeyWriteValueFromTran inspects a transaction for writes to a given key
func getKeyModificationFromTran(tranEnvelope *common.Envelope, namespace string, key string) (commonledger.QueryResult, error) {
	logger.Debugf("Entering getKeyModificationFromTran %s:%s", namespace, key)

	// extract action from the envelope
	payload, err := protoutil.UnmarshalPayload(tranEnvelope.Payload)
	if err != nil {
		return nil, err
	}

	tx, err := protoutil.UnmarshalTransaction(payload.Data)
	if err != nil {
		return nil, err
	}

	_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])
	if err != nil {
		return nil, err
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, err
	}

	txID := chdr.TxId
	timestamp := chdr.Timestamp

	txRWSet := &rwsetutil.TxRwSet{}

	// Get the Result from the Action and then Unmarshal
	// it into a TxReadWriteSet using custom unmarshalling
	if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
		return nil, err
	}

	// look for the namespace and key by looping through the transaction's ReadWriteSets
	for _, nsRWSet := range txRWSet.NsRwSets {
		if nsRWSet.NameSpace == namespace {
			// got the correct namespace, now find the key write
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				if kvWrite.Key == key {
					return &queryresult.KeyModification{TxId: txID, Value: kvWrite.Value,
						Timestamp: timestamp, IsDelete: rwsetutil.IsKVWriteDelete(kvWrite)}, nil
				}
			} // end keys loop
			logger.Debugf("key [%s] not found in namespace [%s]'s writeset", key, namespace)
			return nil, nil
		} // end if
	} //end namespaces loop
	logger.Debugf("namespace [%s] not found in transaction's ReadWriteSets", namespace)
	return nil, nil
}

type versionScanner struct {
	rangeScan    *rangeScan
	namespace    string
	key          string
	dbItr        iterator.Iterator
	blockStore   *blkstorage.BlockStore
	currentBlock uint64
	transactions []uint64
	txIndex      int
	start        uint64
	end          uint64
}

func (q *QueryExecutor) GetVersionsForKey(namespace string, key string, start uint64, end uint64) (commonledger.ResultsIterator, error) {
	if end < start {
		return nil, errors.Errorf("Start: %d is not less than or equal to end: %d", start, end)
	}

	GIkey := []byte("_" + key)
	versionsBytes, err := q.levelDB.Get(GIkey)
	if err != nil {
		return nil, errors.Errorf("Error reading from history database for key: %s", key)
	}
	if versionsBytes != nil {
		maxVersion, _, err := util.DecodeOrderPreservingVarUint64(versionsBytes)
		if err != nil {
			return nil, errors.Errorf("Error decoding lasts known version for key: %s", key)
		}
		if maxVersion < start {
			return nil, errors.Errorf("Start: %d is greater than the last existing version: %d", start, maxVersion)
		}
	}

	rangeScan := constructRangeScan(namespace, key)
	endKey := append(rangeScan.startKey, util.EncodeOrderPreservingVarUint64(end+1)...)

	dbItr, err := q.levelDB.GetIterator(rangeScan.startKey, endKey)
	if err != nil {
		return nil, err
	}
	var (
		currentBlock uint64
		transactions []uint64
		txIndex      int
	)
	if dbItr.Last() {
		historyKey := dbItr.Key()
		firstVersionInBlock, err := rangeScan.decodeMinVersion(historyKey)
		if err != nil {
			return nil, err
		}
		indexVal := dbItr.Value()
		currentBlock, transactions, err = decodeNewIndex(indexVal)
		if err != nil {
			return nil, err
		}
		lastVersionInBlock := firstVersionInBlock + uint64(len(transactions)-1)
		if end > lastVersionInBlock {
			txIndex = int(len(transactions) - 1)
		} else {
			txIndex = int(end - firstVersionInBlock)
		}
	} else {
		txIndex = -1
	}

	return &versionScanner{rangeScan, namespace, key, dbItr, q.blockStore, currentBlock, transactions, txIndex, start, end}, nil
}

func (scanner *versionScanner) Next() (commonledger.QueryResult, error) {
	if scanner.txIndex <= -1 {
		if !scanner.dbItr.Prev() {
			return nil, nil
		}
		indexVal := scanner.dbItr.Value()
		currentBlock, transactions, err := decodeNewIndex(indexVal)
		if err != nil {
			return nil, err
		}
		scanner.currentBlock = currentBlock
		scanner.transactions = transactions
		scanner.txIndex = len(scanner.transactions) - 1

		logger.Debugf("Updated scanner for key: %s, block: %d, transactions: %v, txIndex: %d\n", scanner.key,
			scanner.currentBlock, scanner.transactions, scanner.txIndex)
	}

	historyKey := scanner.dbItr.Key()
	firstVersionInBlock, err := scanner.rangeScan.decodeMinVersion(historyKey)
	if err != nil {
		return nil, err
	}
	currentVersionNum := firstVersionInBlock + uint64(scanner.txIndex)
	logger.Debugf("First version in block: %d, Current version: %d\n", firstVersionInBlock, currentVersionNum)
	if currentVersionNum < scanner.start {
		logger.Debugf("First requested version %d found for key: %s", scanner.start, scanner.key)
		return nil, nil
	}

	blockNum := scanner.currentBlock
	tranNum := scanner.transactions[scanner.txIndex]
	scanner.txIndex--

	logger.Debugf("Found history record for namespace:%s key:%s at blockNumTranNum %v:%v\n, firstVersionInBlock: %d, currentVersion: %d\n",
		scanner.namespace, scanner.key, blockNum, tranNum, firstVersionInBlock, currentVersionNum)

	// Get the transaction from block storage that is associated with this history record
	tranEnvelope, err := scanner.blockStore.RetrieveTxByBlockNumTranNum(blockNum, tranNum)
	if err != nil {
		return nil, err
	}

	// Get the txid, key write value, timestamp, and delete indicator associated with this transaction
	queryResult, err := getKeyModificationFromTran(tranEnvelope, scanner.namespace, scanner.key)
	if err != nil {
		return nil, err
	}
	if queryResult == nil {
		// should not happen, but make sure there is inconsistency between historydb and statedb
		logger.Errorf("No namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, scanner.key, blockNum, tranNum)
		return nil, errors.Errorf("no namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, scanner.key, blockNum, tranNum)
	}
	logger.Debugf("Found historic key value for namespace:%s key:%s from transaction %s",
		scanner.namespace, scanner.key, queryResult.(*queryresult.KeyModification).TxId)
	return queryResult, nil
}

func (scanner *versionScanner) Close() {
	scanner.dbItr.Release()
}

// ---------------------------------------------

// GetUpdatesByBlockRange implements method in interface `ledger.HistoryQueryExecutor`
func (q *QueryExecutor) GetUpdatesByBlockRange(namespace string, start uint64, end uint64, updates uint64) (commonledger.ResultsIterator, error) {
	if end < start {
		return nil, errors.Errorf("Start: %d is not less than or equal to end: %d", start, end)
	}

	if end <= 0 || start <= 0 {
		return nil, errors.Errorf("Start: %d, end: %d cannot be less than 1", start, end)
	}

	scanner := &blockRangeScanner{namespace, q.levelDB, nil, q.blockStore, start, end, nil, -1, nil, 0, nil, 0}

	err := scanner.countKeyUpdates(updates)
	if err != nil {
		return nil, err
	}

	_, err = scanner.nextKey()
	if err != nil {
		return nil, err
	}

	logger.Debugf("Initialized block scanner over range from start: %d to end: %d seeking results with at least %d updates", start, end, updates)

	return scanner, nil
}

// blockRangeScanner implements ResultsIterator for iterating through history results
type blockRangeScanner struct {
	namespace     string
	levelDB       *leveldbhelper.DBHandle
	dbItr         iterator.Iterator
	blockStore    *blkstorage.BlockStore
	start         uint64
	end           uint64
	keys          []string
	keyIndex      int
	currentKeyItr iterator.Iterator
	blockNum      uint64
	transactions  []uint64
	txIndex       int
}

func (scanner *blockRangeScanner) Next() (commonledger.QueryResult, error) {
	if scanner.txIndex >= len(scanner.transactions) {
		keyExhausted, err := scanner.updateCurrentKeyData()
		if err != nil {
			return nil, err
		}
		if keyExhausted {
			hasNext, err := scanner.nextKey()
			if err != nil {
				return nil, err
			}
			if !hasNext {
				logger.Debugf("Query completed")
				// All iterators exhausted
				return nil, nil
			}
		}
	}
	key := scanner.keys[scanner.keyIndex]

	blockNum := scanner.blockNum
	tranNum := scanner.transactions[scanner.txIndex]
	scanner.txIndex++

	logger.Debugf("Found history record for namespace:%s key:%s at blockNumTranNum %v:%v\n",
		scanner.namespace, key, blockNum, tranNum)

	// Get the transaction from block storage that is associated with this history record
	tranEnvelope, err := scanner.blockStore.RetrieveTxByBlockNumTranNum(blockNum, tranNum)
	if err != nil {
		return nil, err
	}

	// Get the txid, key write value, timestamp, and delete indicator associated with this transaction
	queryResult, err := getKeyModificationFromTran(tranEnvelope, scanner.namespace, key)
	if err != nil {
		return nil, err
	}
	if queryResult == nil {
		// should not happen, but make sure there is inconsistency between historydb and statedb
		logger.Errorf("No namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, key, blockNum, tranNum)
		return nil, errors.Errorf("no namespace or key is found for namespace %s and key %s with decoded blockNum %d and tranNum %d", scanner.namespace, key, blockNum, tranNum)
	}
	logger.Debugf("Found historic key value for namespace:%s key:%s from transaction %s",
		scanner.namespace, key, queryResult.(*queryresult.KeyModification).TxId)
	return queryResult, nil
}

func (scanner *blockRangeScanner) Close() {
	scanner.currentKeyItr.Release()
}

func (scanner *blockRangeScanner) countKeyUpdates(updates uint64) error {
	keyCounts := make(map[string]int)
	for i := scanner.start; i <= scanner.end; i++ {
		nextBlockBytes, err := scanner.blockStore.RetrieveBlockByNumber(i)
		if err != nil {
			return err
		}
		for _, txEnvelopeBytes := range nextBlockBytes.Data.Data {
			tranEnvelope, err := protoutil.GetEnvelopeFromBlock(txEnvelopeBytes)
			if err != nil {
				return err
			}
			err = countKeyUpdatesForTran(tranEnvelope, scanner.namespace, keyCounts)
			if err != nil {
				return err
			}
		}
	}
	logger.Debugf("%d keys found meeting the update threshold in block range", len(keyCounts))
	for key, count := range keyCounts {
		logger.Debugf("Key: %s updated %d times\n", key, count)
		if count >= int(updates) {
			scanner.keys = append(scanner.keys, key)
		}
	}
	return nil
}

func (scanner *blockRangeScanner) nextKey() (bool, error) {
	logger.Debugf("Entering nextKey")
	if scanner.currentKeyItr != nil {
		scanner.currentKeyItr.Release()
	}
	scanner.keyIndex++
	if scanner.keyIndex >= len(scanner.keys) {
		return false, nil
	}
	key := scanner.keys[scanner.keyIndex]
	rangeScan := constructRangeScan(scanner.namespace, key)
	var err error
	scanner.currentKeyItr, err = scanner.levelDB.GetIterator(rangeScan.startKey, rangeScan.endKey)
	if err != nil {
		return false, err
	}
	for scanner.currentKeyItr.Next() {
		indexVal := scanner.currentKeyItr.Value()
		blockNum, transactions, err := decodeNewIndex(indexVal)
		if err != nil {
			return false, err
		}
		if blockNum >= scanner.start {
			scanner.blockNum = blockNum
			scanner.transactions = transactions
			scanner.txIndex = 0
			return true, nil
		}
	}
	// Shouldn't ever reach this line as we precheck that all keys are within block range
	return scanner.nextKey()
}

func (scanner *blockRangeScanner) updateCurrentKeyData() (bool, error) {
	logger.Debugf("Entering updateCurrentKeyData")
	if !scanner.currentKeyItr.Next() {
		return true, nil
	}
	indexVal := scanner.currentKeyItr.Value()
	blockNum, transactions, err := decodeNewIndex(indexVal)
	if err != nil {
		return true, err
	}
	if blockNum > scanner.end {
		return true, nil
	}
	scanner.blockNum = blockNum
	scanner.transactions = transactions
	scanner.txIndex = 0
	return false, nil
}

// getTxIDandKeyWriteValueFromTran inspects a transaction for writes to a given key
func countKeyUpdatesForTran(tranEnvelope *common.Envelope, namespace string, keys map[string]int) error {
	logger.Debugf("Entering getKeysFromTran %s", namespace)

	// extract action from the envelope
	payload, err := protoutil.UnmarshalPayload(tranEnvelope.Payload)
	if err != nil {
		return err
	}

	tx, err := protoutil.UnmarshalTransaction(payload.Data)
	if err != nil {
		return err
	}

	_, respPayload, err := protoutil.GetPayloads(tx.Actions[0])
	if err != nil {
		return err
	}

	txRWSet := &rwsetutil.TxRwSet{}

	// Get the Result from the Action and then Unmarshal
	// it into a TxReadWriteSet using custom unmarshalling
	if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
		return err
	}

	// Read the keys by looping through the transaction's ReadWriteSets
	for _, nsRWSet := range txRWSet.NsRwSets {
		if nsRWSet.NameSpace == namespace {
			// got the correct namespace, now find the key write
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				keys[kvWrite.Key] += 1
			} // end keys loop
			return nil
		} // end if
	} //end namespaces loop
	logger.Debugf("namespace [%s] not found in transaction's ReadWriteSets", namespace)
	return nil
}
