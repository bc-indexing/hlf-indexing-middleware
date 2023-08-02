/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package history

import (
	"bytes"

	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/pkg/errors"
)

type dataKey []byte
type dataVal []byte
type rangeScan struct {
	startKey, endKey []byte
}

var (
	compositeKeySep = []byte{0x00} // used as a separator between different components of dataKey
	savePointKey    = []byte{'s'}  // a single key in db for persisting savepoint
)

// constructDataKey builds the key of the format namespace~len(key)~key~versionnum
// using an order preserving encoding so that history query results are ordered by height
// Note: this key format is different than the format in pre-v2.0 releases and requires
//
//	a historydb rebuild when upgrading an older version to v2.0.
func constructDataKey(ns string, key string, versionnum uint64) dataKey {
	k := append([]byte(ns), compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(uint64(len(key)))...)
	k = append(k, []byte(key)...)
	k = append(k, compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(versionnum)...)
	return dataKey(k)
}

func constructDataVal(blocknum uint64, trannum uint64) dataVal {
	v := append([]byte{}, util.EncodeOrderPreservingVarUint64(blocknum)...)
	v = append(v, util.EncodeOrderPreservingVarUint64(trannum)...)
	return dataVal(v)
}

// constructRangescanKeys returns start and endKey for performing a range scan
// that covers all the keys for <ns, key>.
// startKey = namespace~len(key)~key~
// endKey = namespace~len(key)~key~0xff
func constructRangeScan(ns string, key string) *rangeScan {
	k := append([]byte(ns), compositeKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(uint64(len(key)))...)
	k = append(k, []byte(key)...)
	k = append(k, compositeKeySep...)

	return &rangeScan{
		startKey: k,
		endKey:   append(k, 0xff),
	}
}

func (r *rangeScan) decodeVersionNum(dataKey dataKey) (uint64, error) {
	versionNumBytes := bytes.TrimPrefix(dataKey, r.startKey)
	versionNum, _, err := util.DecodeOrderPreservingVarUint64(versionNumBytes)
	if err != nil {
		return 0, err
	}
	return versionNum, nil
}

func (r *rangeScan) decodeBlockNumTranNum(dataVal dataVal) (uint64, uint64, error) {
	blockNum, blockBytesConsumed, err := util.DecodeOrderPreservingVarUint64(dataVal)
	if err != nil {
		return 0, 0, err
	}

	tranNum, tranBytesConsumed, err := util.DecodeOrderPreservingVarUint64(dataVal[blockBytesConsumed:])
	if err != nil {
		return 0, 0, err
	}

	// The following error should never happen. Keep the check just in case there is some unknown bug.
	if blockBytesConsumed+tranBytesConsumed != len(dataVal) {
		return 0, 0, errors.Errorf("number of decoded bytes (%d) is not equal to the length of blockNumTranNumBytes (%d)",
			blockBytesConsumed+tranBytesConsumed, len(dataVal))
	}
	return blockNum, tranNum, nil
}
