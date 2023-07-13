/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package history

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/queryresult"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
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
	return &historyScanner{rangeScan, namespace, key, dbItr, q.blockStore, -1, nil, nil}, nil
}

// historyScanner implements ResultsIterator for iterating through history results
type historyScanner struct {
	rangeScan    *rangeScan
	namespace    string
	key          string
	dbItr        iterator.Iterator
	blockStore   *blkstorage.BlockStore
	txIndex      int
	transactions []uint64
	currentBlock *common.Block
}

// Next iterates to the next key, in the order of newest to oldest, from history scanner.
// It decodes blockNumTranNumBytes to get blockNum and tranNum,
// loads the block:tran from block storage, finds the key and returns the result.
func (scanner *historyScanner) Next() (commonledger.QueryResult, error) {
	// call Prev because history query result is returned from newest to oldest
	if scanner.txIndex == -1 && !scanner.dbItr.Prev() {
		return nil, nil
	}

	historyKey := scanner.dbItr.Key()
	blockNum, err := scanner.rangeScan.decodeBlockNum(historyKey)
	if err != nil {
		return nil, err
	}
	if scanner.txIndex == -1 {
		// Retrieve new block
		scanner.currentBlock, err = scanner.blockStore.RetrieveBlockByNumber(blockNum)
		if err != nil {
			return nil, err
		}
		indexVal := scanner.dbItr.Value()
		_, _, scanner.transactions, err = decodeNewIndex(indexVal)
		if err != nil {
			return nil, err
		}
		scanner.txIndex = len(scanner.transactions) - 1
	}
	tranNum := scanner.transactions[scanner.txIndex]
	scanner.txIndex--

	logger.Debugf("Found history record for namespace:%s key:%s at blockNumTranNum %v:%v\n",
		scanner.namespace, scanner.key, blockNum, tranNum)

	// Index into stored block & get the tranEnvelope
	txEnvelopeBytes := scanner.currentBlock.Data.Data[tranNum]
	// Get the transaction from block storage that is associated with this history record
	tranEnvelope, err := protoutil.GetEnvelopeFromBlock(txEnvelopeBytes)
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
	var (
		rangeScans  []*rangeScan
		dbItrs      map[string]iterator.Iterator
		nextBlock   uint64
		keysInBlock []string
	)
	for _, key := range keys {
		rangeScan := constructRangeScan(namespace, key)
		dbItr, err := q.levelDB.GetIterator(rangeScan.startKey, rangeScan.endKey)
		if err != nil {
			return nil, err
		}
		if dbItr.Last() {
			indexVal := dbItr.Value()
			prev, _, _, err := decodeNewIndex(indexVal)
			if err != nil {
				return nil, err
			}
			if prev > nextBlock {
				nextBlock = prev
				keysInBlock = append([]string{}, key)
			} else if prev == nextBlock {
				keysInBlock = append(keysInBlock, key)
			}
		}
		rangeScans = append(rangeScans, rangeScan)
		dbItrs[key] = dbItr
	}
	return &parallelHistoryScanner{rangeScans, namespace, keys, dbItrs, q.blockStore, nextBlock, keysInBlock}, nil
}

// historyScanner implements ResultsIterator for iterating through history results
type parallelHistoryScanner struct {
	rangeScans      []*rangeScan
	namespace       string
	keys            []string
	dbItrs          map[string]iterator.Iterator
	blockStore      *blkstorage.BlockStore
	nextBlockToRead uint64
	keysInBlock     []string
}

// Next() will iterate over the histories for each key by block
// All key modifications from within the current block will be returned
func (scanner *parallelHistoryScanner) Next() (commonledger.QueryResult, error) {
	// No keys in next block indicates we have exhausted all iterators
	if len(scanner.keysInBlock) == 0 {
		return nil, nil
	}

	// Read the block once for all relevant keys
	blockNum := scanner.nextBlockToRead
	currentBlock, err := scanner.blockStore.RetrieveBlockByNumber(blockNum)
	if err != nil {
		return nil, err
	}

	var queryBatch []*pb.QueryResultBytes

	for _, key := range scanner.keysInBlock {
		currentIndexVal := scanner.dbItrs[key].Value()
		_, _, transactions, err := decodeNewIndex(currentIndexVal)
		if err != nil {
			return nil, err
		}
		for tranNum := range transactions {
			logger.Debugf("Found history record for namespace:%s key:%s at blockNumTranNum %v:%v\n",
				scanner.namespace, key, blockNum, tranNum)
			// Index into stored block & get the tranEnvelope
			txEnvelopeBytes := currentBlock.Data.Data[tranNum]
			// Get the transaction from block storage that is associated with this history record
			tranEnvelope, err := protoutil.GetEnvelopeFromBlock(txEnvelopeBytes)
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

			queryResultBytes, err := proto.Marshal(queryResult.(proto.Message))
			if err != nil {
				return nil, err
			}
			queryBatch = append(queryBatch, &pb.QueryResultBytes{ResultBytes: queryResultBytes})
		}
		scanner.dbItrs[key].Prev()
	}

	scanner.nextBlockToRead = 0
	for _, key := range scanner.keys {
		currentIndexVal := scanner.dbItrs[key].Value()
		if currentIndexVal != nil {
			prev, _, _, err := decodeNewIndex(currentIndexVal)
			if err != nil {
				return nil, err
			}
			if prev > scanner.nextBlockToRead {
				scanner.nextBlockToRead = prev
				scanner.keysInBlock = append([]string{}, key)
			} else if prev == scanner.nextBlockToRead {
				scanner.keysInBlock = append(scanner.keysInBlock, key)
			}
		}
	}

	return queryBatch, nil
}

func (scanner *parallelHistoryScanner) Close() {
	for _, dbItr := range scanner.dbItrs {
		dbItr.Release()
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
