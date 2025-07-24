package watchtower

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/stretchr/testify/assert"
)

const nodeID = "test-node-123"

var txHash = &chainhash.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

func TestBroadcastTransaction(t *testing.T) {
	ctx := context.Background()

	txBytes, wantTX := createTestTransaction(t)
	mockClient := &mockBitcoinClient{response: txHash}

	err := BroadcastTransaction(ctx, mockClient, nodeID, txBytes)
	require.NoError(t, err)

	if diff := cmp.Diff(wantTX, mockClient.seenTX, cmpopts.IgnoreUnexported(wire.MsgTx{}), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("BroadcastTransaction returned unexpected diff (-want +got):\n%s", diff)
	}
}

func TestBroadcastTransaction_AlreadyBroadcasted(t *testing.T) {
	ctx := context.Background()
	txBytes, _ := createTestTransaction(t)

	rpcErr := &btcjson.RPCError{
		Code:    btcjson.ErrRPCVerifyAlreadyInChain,
		Message: "transaction already in mempool",
	}

	mockClient := &mockBitcoinClient{err: rpcErr}

	err := BroadcastTransaction(ctx, mockClient, nodeID, txBytes)

	require.NoError(t, err, "should return nil when transaction is already broadcasted")
}

func TestBroadcastTransaction_InvalidTransactionBytes(t *testing.T) {
	ctx := context.Background()
	invalidTxBytes := []byte{0x01, 0x02, 0x03}

	mockClient := &mockBitcoinClient{}

	err := BroadcastTransaction(ctx, mockClient, nodeID, invalidTxBytes)

	require.ErrorContains(t, err, "failed to parse transaction")
}

func TestBroadcastTransaction_Errors(t *testing.T) {
	ctx := context.Background()
	txBytes, _ := createTestTransaction(t)

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "RPC error with code that's not ErrRPCVerifyAlreadyInChain",
			err: &btcjson.RPCError{
				Code:    btcjson.ErrRPCTxError,
				Message: "bad-txns-inputs-missingorspent",
			},
		},
		{
			name: "Non-RPC error",
			err:  fmt.Errorf("network connection failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockBitcoinClient{err: tt.err}

			err := BroadcastTransaction(ctx, mockClient, nodeID, txBytes)

			require.ErrorIs(t, err, tt.err)
		})
	}
}

func TestAlreadyBroadcasted(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "RPC error with code -27/ErrRPCVerifyAlreadyInChain",
			err: &btcjson.RPCError{
				Code:    btcjson.ErrRPCVerifyAlreadyInChain,
				Message: "transaction already in mempool",
			},
			want: true,
		},
		{
			name: "RPC error with different code",
			err: &btcjson.RPCError{
				Code:    -25,
				Message: "bad-txns-inputs-missingorspent",
			},
			want: false,
		},
		{
			name: "Non-RPC error",
			err:  fmt.Errorf("network error"),
			want: false,
		},
		{
			name: "Nil error",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := alreadyBroadcasted(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBroadcastTransaction_EmptyTransaction(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name  string
		value []byte
	}{
		{name: "nil", value: nil},
		{name: "empty", value: []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockBitcoinClient{}
			err := BroadcastTransaction(ctx, client, nodeID, tt.value)
			require.Error(t, err)
		})
	}
}

// mockBitcoinClient is a mock implementation of BitcoinClientInterface for testing.
type mockBitcoinClient struct {
	err      error
	response *chainhash.Hash
	seenTX   *wire.MsgTx
}

func (m *mockBitcoinClient) SendRawTransaction(tx *wire.MsgTx, _ bool) (*chainhash.Hash, error) {
	m.seenTX = tx
	return m.response, m.err
}

func createTestTransaction(t *testing.T) ([]byte, *wire.MsgTx) {
	tx := wire.NewMsgTx(2)
	prevHash, _ := chainhash.NewHashFromStr(strings.Repeat("0", 64))
	tx.AddTxIn(wire.NewTxIn(&wire.OutPoint{Hash: *prevHash, Index: 0}, nil, nil))
	tx.AddTxOut(wire.NewTxOut(1000000, []byte("test-pkscript")))

	var buf bytes.Buffer
	if err := tx.Serialize(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), tx
}
