// Definition of transaction

package core

import (
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"time"
)

type Transaction struct {
	Sender    utils.Address
	Recipient utils.Address
	Nonce     uint64
	Signature []byte // not implemented now.
	Value     *big.Int
	TxHash    []byte
	GasUsed   uint64

	// GLASBaseGas is immutable input gas, shared by GLAS and the opt-in
	// unified execution layer. The original field names preserve stored/wire
	// compatibility. GasUsed is the priced physical stage, not the base.
	GLASBaseGas        uint64
	GLASCallGasPerCall uint64
	GLASCallRelations  []GLASCallRelation
	GLASCrossCallCount uint64
	GLASAttachedGas    uint64

	// SourceTxHash is the canonical Ethereum transaction hash from the
	// JiangZiya workload. TxHash remains BlockEmulator's execution identifier;
	// SourceTxHash survives relay and migration so the supervisor can release
	// the matching call trace only after the top-level transaction completes.
	SourceTxHash string
	TxCategory   string

	// Immutable dataset classification used by optional account planners.
	InputSenderIsContract    bool `json:",omitempty"`
	InputRecipientIsContract bool `json:",omitempty"`

	// Legacy GLAS-FREIGHT CALL fields describe an aggregated synthetic stage.
	// New attached-call transactions do not generate these records. Legacy
	// records consume gas but never update account state or enter Relay.
	IsGLASCallTask      bool
	GLASCallTaskID      string
	GLASCallOriginShard uint64
	GLASCallTargetShard uint64
	// GLASCallCallee makes the task owner explicit. Recipient carries the
	// same address so account migration can move the queued task with callee.
	GLASCallCallee string
	GLASCallCount  uint64
	// GLASStageEpoch records when a Relay2/call-task execution stage was
	// created. It is intentionally not rewritten during reconfiguration, which
	// lets block metrics distinguish carried stages from newly emitted ones.
	GLASStageEpoch    uint64
	GLASStageEpochSet bool

	ExecDelayMs float64

	Time time.Time // TimeStamp the tx proposed.

	// used in transaction relaying
	Relayed bool
	// used in broker_300k_BlockTransaction, if the tx is not a broker1 or broker2 tx, these values should be empty.
	HasBroker      bool
	SenderIsBroker bool
	OriginalSender utils.Address
	FinalRecipient utils.Address
	RawTxHash      []byte

	// Pool lifecycle timestamps (broker_300k_BlockTransaction mode)
	Broker1EnterPoolTime time.Time
	Broker1LeavePoolTime time.Time
	Broker2EnterPoolTime time.Time
	Broker2LeavePoolTime time.Time
}

func (tx *Transaction) PrintTx() string {
	vals := []interface{}{
		tx.Sender[:],
		tx.Recipient[:],
		tx.Value,
		string(tx.TxHash[:]),
	}
	res := fmt.Sprintf("%v\n", vals)
	return res
}

// Encode transaction for storing
func (tx *Transaction) Encode() []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(tx)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

// Decode transaction
func DecodeTx(to_decode []byte) *Transaction {
	var tx Transaction

	decoder := gob.NewDecoder(bytes.NewReader(to_decode))
	err := decoder.Decode(&tx)
	if err != nil {
		log.Panic(err)
	}

	return &tx
}

// new a transaction
func NewTransaction(sender, recipient string, value *big.Int, nonce uint64, proposeTime time.Time) *Transaction {
	tx := &Transaction{
		Sender:    sender,
		Recipient: recipient,
		Value:     value,
		Nonce:     nonce,
		Time:      proposeTime,
	}

	hash := sha256.Sum256(tx.Encode())
	tx.TxHash = hash[:]
	tx.GasUsed = 0
	tx.SourceTxHash = ""
	tx.TxCategory = ""
	tx.IsGLASCallTask = false
	tx.GLASCallTaskID = ""
	tx.GLASCallOriginShard = 0
	tx.GLASCallTargetShard = 0
	tx.GLASCallCallee = ""
	tx.GLASCallCount = 0
	tx.GLASStageEpoch = 0
	tx.GLASStageEpochSet = false
	tx.ExecDelayMs = 0
	tx.Relayed = false
	tx.FinalRecipient = ""
	tx.OriginalSender = ""
	tx.RawTxHash = nil
	tx.HasBroker = false
	tx.SenderIsBroker = false
	return tx
}
