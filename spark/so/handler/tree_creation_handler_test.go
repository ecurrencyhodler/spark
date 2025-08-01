package handler

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/btcsuite/btcd/wire"
	"github.com/google/uuid"
	"github.com/lightsparkdev/spark/common"
	"github.com/lightsparkdev/spark/common/keys"
	pb "github.com/lightsparkdev/spark/proto/spark"
	"github.com/lightsparkdev/spark/so"
	"github.com/lightsparkdev/spark/so/db"
	"github.com/lightsparkdev/spark/so/ent"
	st "github.com/lightsparkdev/spark/so/ent/schema/schematype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestHandler(t *testing.T) *TreeCreationHandler {
	config := &so.Config{
		BitcoindConfigs: map[string]so.BitcoindConfig{
			"regtest": {
				DepositConfirmationThreshold: 1,
			},
		},
	}
	return NewTreeCreationHandler(config)
}

func createTestTx(t *testing.T) *wire.MsgTx {
	tx := wire.NewMsgTx(wire.TxVersion)
	// Add a proper input
	tx.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: [32]byte{1, 2, 3}, Index: 0},
		Sequence:         wire.MaxTxInSequenceNum,
	})
	// Add a proper output with a valid P2PKH script
	pkScript := []byte{0x76, 0xa9, 0x14, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x88, 0xac}
	tx.AddTxOut(&wire.TxOut{Value: 100000, PkScript: pkScript})
	return tx
}

func createTestUTXO(t *testing.T, rawTx []byte, vout uint32) *pb.UTXO {
	return &pb.UTXO{
		RawTx:   rawTx,
		Vout:    vout,
		Network: pb.Network_REGTEST,
		Txid:    make([]byte, 32),
	}
}

func TestNewTreeCreationHandler(t *testing.T) {
	config := &so.Config{}
	handler := NewTreeCreationHandler(config)

	assert.NotNil(t, handler)
	assert.Equal(t, config, handler.config)
}

func TestFindParentOutputFromUtxo(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, dbCtx := db.NewTestSQLiteContext(t, ctx)
	defer dbCtx.Close()

	handler := createTestHandler(t)
	testTx := createTestTx(t)

	var txBuf []byte
	txBuf, err := common.SerializeTx(testTx)
	require.NoError(t, err)

	tests := []struct {
		name                  string
		utxo                  *pb.UTXO
		expectError           bool
		expectedErrorContains string
		setupTree             bool
	}{
		{
			name:        "valid utxo with single output",
			utxo:        createTestUTXO(t, txBuf, 0),
			expectError: false,
		},
		{
			name: "invalid raw transaction",
			utxo: &pb.UTXO{
				RawTx: []byte{0x01, 0x02}, // invalid tx
				Vout:  0,
			},
			expectError:           true,
			expectedErrorContains: "EOF", // The actual error from Bitcoin transaction parsing
		},
		{
			name:                  "vout out of bounds",
			utxo:                  createTestUTXO(t, txBuf, 5), // tx only has 1 output (index 0)
			expectError:           true,
			expectedErrorContains: "vout out of bounds",
		},
		{
			name:                  "tree already exists",
			utxo:                  createTestUTXO(t, txBuf, 0),
			expectError:           true,
			expectedErrorContains: "already exists",
			setupTree:             true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupTree {
				// Create a tree with the same base txid to trigger "already exists" error
				db, err := ent.GetDbFromContext(ctx)
				require.NoError(t, err)

				tx, err := common.TxFromRawTxBytes(tt.utxo.RawTx)
				require.NoError(t, err)

				txHash := tx.TxHash()
				_, err = db.Tree.Create().
					SetOwnerIdentityPubkey([]byte("test")).
					SetNetwork(st.NetworkRegtest).
					SetBaseTxid(txHash[:]).
					SetVout(0).
					SetStatus(st.TreeStatusPending).
					Save(ctx)
				require.NoError(t, err)
			}

			output, err := handler.findParentOutputFromUtxo(ctx, tt.utxo)

			if tt.expectError {
				require.ErrorContains(t, err, tt.expectedErrorContains)
				assert.Nil(t, output)
			} else {
				require.NoError(t, err)
				require.NotNil(t, output)
				assert.Equal(t, int64(100000), output.Value)
			}
		})
	}
}

func TestFindParentOutputFromNodeOutput(t *testing.T) {
	rng := rand.NewChaCha8([32]byte{1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, dbCtx := db.NewTestSQLiteContext(t, ctx)
	defer dbCtx.Close()

	handler := createTestHandler(t)

	// Setup test data
	db, err := ent.GetDbFromContext(ctx)
	require.NoError(t, err)

	keysharePrivkey := keys.MustGeneratePrivateKeyFromRand(rng)
	publicSharePrivkey := keys.MustGeneratePrivateKeyFromRand(rng)
	identityPrivkey := keys.MustGeneratePrivateKeyFromRand(rng)
	signingPrivkey := keys.MustGeneratePrivateKeyFromRand(rng)
	verifyingPrivkey := keys.MustGeneratePrivateKeyFromRand(rng)

	// Create a signing keyshare
	signingKeyshare, err := db.SigningKeyshare.Create().
		SetStatus(st.KeyshareStatusAvailable).
		SetSecretShare(keysharePrivkey.Serialize()).
		SetPublicShares(map[string][]byte{"test": publicSharePrivkey.Public().Serialize()}).
		SetPublicKey(keysharePrivkey.Public().Serialize()).
		SetMinSigners(2).
		SetCoordinatorIndex(0).
		Save(ctx)
	require.NoError(t, err)

	// Create a tree
	tree, err := db.Tree.Create().
		SetOwnerIdentityPubkey(identityPrivkey.Public().Serialize()).
		SetNetwork(st.NetworkRegtest).
		SetBaseTxid(make([]byte, 32)).
		SetVout(0).
		SetStatus(st.TreeStatusAvailable).
		Save(ctx)
	require.NoError(t, err)

	testTx := createTestTx(t)
	txBuf, err := common.SerializeTx(testTx)
	require.NoError(t, err)

	// Create a tree node
	node, err := db.TreeNode.Create().
		SetTree(tree).
		SetStatus(st.TreeNodeStatusAvailable).
		SetOwnerIdentityPubkey(identityPrivkey.Public().Serialize()).
		SetOwnerSigningPubkey(signingPrivkey.Public().Serialize()).
		SetValue(100000).
		SetVerifyingPubkey(verifyingPrivkey.Public().Serialize()).
		SetSigningKeyshare(signingKeyshare).
		SetRawTx(txBuf).
		SetVout(0).
		Save(ctx)
	require.NoError(t, err)

	tests := []struct {
		name                  string
		nodeOutput            *pb.NodeOutput
		expectError           bool
		expectedErrorContains string
		setupChild            bool
	}{
		{
			name: "valid node output",
			nodeOutput: &pb.NodeOutput{
				NodeId: node.ID.String(),
				Vout:   0,
			},
			expectError: false,
		},
		{
			name: "invalid node ID",
			nodeOutput: &pb.NodeOutput{
				NodeId: "invalid-uuid",
				Vout:   0,
			},
			expectError:           true,
			expectedErrorContains: "invalid UUID",
		},
		{
			name: "non-existent node",
			nodeOutput: &pb.NodeOutput{
				NodeId: uuid.New().String(),
				Vout:   0,
			},
			expectError:           true,
			expectedErrorContains: "not found",
		},
		{
			name: "vout out of bounds",
			nodeOutput: &pb.NodeOutput{
				NodeId: node.ID.String(),
				Vout:   5, // tx only has 1 output
			},
			expectError:           true,
			expectedErrorContains: "vout out of bounds",
		},
		{
			name: "child already exists",
			nodeOutput: &pb.NodeOutput{
				NodeId: node.ID.String(),
				Vout:   0,
			},
			expectError:           true,
			expectedErrorContains: "already exists",
			setupChild:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setupChild {
				// Create a child node to trigger "already exists" error
				_, err = db.TreeNode.Create().
					SetTree(tree).
					SetStatus(st.TreeNodeStatusAvailable).
					SetOwnerIdentityPubkey([]byte("test_identity")).
					SetOwnerSigningPubkey([]byte("test_signing")).
					SetValue(50000).
					SetVerifyingPubkey([]byte("test_verifying")).
					SetSigningKeyshare(signingKeyshare).
					SetRawTx(txBuf).
					SetParent(node).
					SetVout(0).
					Save(ctx)
				require.NoError(t, err)
			}

			output, err := handler.findParentOutputFromNodeOutput(ctx, tt.nodeOutput)

			if tt.expectError {
				require.ErrorContains(t, err, tt.expectedErrorContains)
				assert.Nil(t, output)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, output)
				assert.Equal(t, int64(100000), output.Value)
			}
		})
	}
}

func TestFindParentOutputFromPrepareTreeAddressRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, dbCtx := db.NewTestSQLiteContext(t, ctx)
	defer dbCtx.Close()

	handler := createTestHandler(t)
	testTx := createTestTx(t)
	txBuf, err := common.SerializeTx(testTx)
	require.NoError(t, err)

	tests := []struct {
		name        string
		req         *pb.PrepareTreeAddressRequest
		expectError bool
	}{
		{
			name: "parent node output source",
			req: &pb.PrepareTreeAddressRequest{
				Source: &pb.PrepareTreeAddressRequest_ParentNodeOutput{
					ParentNodeOutput: &pb.NodeOutput{
						NodeId: uuid.New().String(),
						Vout:   0,
					},
				},
			},
			expectError: true, // Will fail because node doesn't exist
		},
		{
			name: "on-chain utxo source",
			req: &pb.PrepareTreeAddressRequest{
				Source: &pb.PrepareTreeAddressRequest_OnChainUtxo{
					OnChainUtxo: createTestUTXO(t, txBuf, 0),
				},
			},
			expectError: false,
		},
		{
			name: "invalid source - nil",
			req: &pb.PrepareTreeAddressRequest{
				Source: nil,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := handler.findParentOutputFromPrepareTreeAddressRequest(ctx, tt.req)

			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, output)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, output)
			}
		})
	}
}

func TestFindParentOutputFromCreateTreeRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, dbCtx := db.NewTestSQLiteContext(t, ctx)
	defer dbCtx.Close()

	handler := createTestHandler(t)
	testTx := createTestTx(t)
	txBuf, err := common.SerializeTx(testTx)
	require.NoError(t, err)

	tests := []struct {
		name        string
		req         *pb.CreateTreeRequest
		expectError bool
	}{
		{
			name: "parent node output source",
			req: &pb.CreateTreeRequest{
				Source: &pb.CreateTreeRequest_ParentNodeOutput{
					ParentNodeOutput: &pb.NodeOutput{
						NodeId: uuid.New().String(),
						Vout:   0,
					},
				},
			},
			expectError: true, // Will fail because node doesn't exist
		},
		{
			name: "on-chain utxo source",
			req: &pb.CreateTreeRequest{
				Source: &pb.CreateTreeRequest_OnChainUtxo{
					OnChainUtxo: createTestUTXO(t, txBuf, 0),
				},
			},
			expectError: false,
		},
		{
			name: "invalid source - nil",
			req: &pb.CreateTreeRequest{
				Source: nil,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := handler.findParentOutputFromCreateTreeRequest(ctx, tt.req)

			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, output)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, output)
			}
		})
	}
}

func TestGetSigningKeyshareFromOutput(t *testing.T) {
	rng := rand.NewChaCha8([32]byte{1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, dbCtx := db.NewTestSQLiteContext(t, ctx)
	defer dbCtx.Close()

	handler := createTestHandler(t)

	// Setup test data
	db, err := ent.GetDbFromContext(ctx)
	require.NoError(t, err)

	keysharePrivkey := keys.MustGeneratePrivateKeyFromRand(rng)
	publicSharePrivkey := keys.MustGeneratePrivateKeyFromRand(rng)
	identityPrivkey := keys.MustGeneratePrivateKeyFromRand(rng)
	signingPrivkey := keys.MustGeneratePrivateKeyFromRand(rng)

	// Create a signing keyshare
	signingKeyshare, err := db.SigningKeyshare.Create().
		SetStatus(st.KeyshareStatusAvailable).
		SetSecretShare(keysharePrivkey.Serialize()).
		SetPublicShares(map[string][]byte{"test": publicSharePrivkey.Public().Serialize()}).
		SetPublicKey(keysharePrivkey.Public().Serialize()).
		SetMinSigners(2).
		SetCoordinatorIndex(0).
		Save(ctx)
	require.NoError(t, err)

	// Create a deposit address
	testAddress := "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx"
	depositAddress, err := db.DepositAddress.Create().
		SetAddress(testAddress).
		SetOwnerIdentityPubkey(identityPrivkey.Public().Serialize()).
		SetOwnerSigningPubkey(signingPrivkey.Public().Serialize()).
		SetSigningKeyshare(signingKeyshare).
		Save(ctx)
	require.NoError(t, err)

	tests := []struct {
		name        string
		output      *wire.TxOut
		network     common.Network
		expectError bool
	}{
		{
			name: "valid output with existing deposit address",
			output: &wire.TxOut{
				Value:    100000,
				PkScript: []byte{0x00, 0x14, 0x75, 0x1e, 0x76, 0xe8, 0x19, 0x91, 0x96, 0xd4, 0x54, 0x94, 0x1c, 0x45, 0xd1, 0xb3, 0xa3, 0x23, 0xf1, 0x43, 0x3b, 0xd6}, // P2WPKH script
			},
			network:     common.Regtest,
			expectError: true, // Will fail because P2TRAddressFromPkScript won't work with this script
		},
		{
			name: "invalid pkScript",
			output: &wire.TxOut{
				Value:    100000,
				PkScript: []byte{0x01, 0x02}, // invalid script
			},
			network:     common.Regtest,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			userPubKey, keyshare, err := handler.getSigningKeyshareFromOutput(ctx, tt.network, tt.output)

			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, userPubKey)
				assert.Nil(t, keyshare)
			} else {
				require.NoError(t, err)
				assert.Equal(t, depositAddress.OwnerSigningPubkey, userPubKey)
				assert.Equal(t, signingKeyshare.ID, keyshare.ID)
			}
		})
	}
}

func TestValidateAndCountTreeAddressNodes(t *testing.T) {
	rng := rand.NewChaCha8([32]byte{1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, dbCtx := db.NewTestSQLiteContext(t, ctx)
	defer dbCtx.Close()

	handler := createTestHandler(t)

	parentPrivkey := keys.MustGeneratePrivateKeyFromRand(rng)
	child1Privkey := keys.MustGeneratePrivateKeyFromRand(rng)
	child2Privkey := keys.MustGeneratePrivateKeyFromRand(rng)

	tests := []struct {
		name                string
		parentUserPublicKey []byte
		nodes               []*pb.AddressRequestNode
		expectedCount       int
		expectError         bool
	}{
		{
			name:                "empty nodes",
			parentUserPublicKey: parentPrivkey.Public().Serialize(),
			nodes:               []*pb.AddressRequestNode{},
			expectedCount:       0,
			expectError:         false,
		},
		{
			name:                "single leaf node",
			parentUserPublicKey: parentPrivkey.Public().Serialize(),
			nodes: []*pb.AddressRequestNode{
				{
					UserPublicKey: parentPrivkey.Public().Serialize(),
					Children:      nil,
				},
			},
			expectedCount: 0, // len(nodes) - 1 = 1 - 1 = 0
			expectError:   false,
		},
		{
			name:                "nodes with children - key mismatch",
			parentUserPublicKey: parentPrivkey.Public().Serialize(),
			nodes: []*pb.AddressRequestNode{
				{
					UserPublicKey: child1Privkey.Public().Serialize(), // This doesn't match parent
					Children:      nil,
				},
				{
					UserPublicKey: child2Privkey.Public().Serialize(), // This doesn't match parent
					Children:      nil,
				},
			},
			expectedCount: 0,
			expectError:   true, // Public key validation will fail
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := handler.validateAndCountTreeAddressNodes(ctx, tt.parentUserPublicKey, tt.nodes)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedCount, count)
			}
		})
	}
}

func TestCreatePrepareTreeAddressNodeFromAddressNode(t *testing.T) {
	rng := rand.NewChaCha8([32]byte{1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, dbCtx := db.NewTestSQLiteContext(t, ctx)
	defer dbCtx.Close()

	handler := createTestHandler(t)

	test_privkey := keys.MustGeneratePrivateKeyFromRand(rng)

	tests := []struct {
		name        string
		node        *pb.AddressRequestNode
		expectError bool
	}{
		{
			name: "leaf node",
			node: &pb.AddressRequestNode{
				UserPublicKey: test_privkey.Public().Serialize(),
				Children:      nil,
			},
			expectError: false,
		},
		{
			name: "node with children",
			node: &pb.AddressRequestNode{
				UserPublicKey: test_privkey.Public().Serialize(),
				Children: []*pb.AddressRequestNode{
					{
						UserPublicKey: test_privkey.Public().Serialize(),
						Children:      nil,
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := handler.createPrepareTreeAddressNodeFromAddressNode(ctx, tt.node)

			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, tt.node.UserPublicKey, result.UserPublicKey)
				assert.Len(t, result.Children, len(tt.node.Children))
			}
		})
	}
}

func TestUpdateParentNodeStatus(t *testing.T) {
	rng := rand.NewChaCha8([32]byte{1})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, dbCtx := db.NewTestSQLiteContext(t, ctx)
	defer dbCtx.Close()

	handler := createTestHandler(t)

	// Setup test data
	db, err := ent.GetDbFromContext(ctx)
	require.NoError(t, err)

	keyshare_privkey := keys.MustGeneratePrivateKeyFromRand(rng)
	public_share_privkey := keys.MustGeneratePrivateKeyFromRand(rng)
	identity_privkey := keys.MustGeneratePrivateKeyFromRand(rng)
	signing_privkey := keys.MustGeneratePrivateKeyFromRand(rng)
	verifying_privkey := keys.MustGeneratePrivateKeyFromRand(rng)

	// Create a signing keyshare
	signingKeyshare, err := db.SigningKeyshare.Create().
		SetStatus(st.KeyshareStatusAvailable).
		SetSecretShare(keyshare_privkey.Serialize()).
		SetPublicShares(map[string][]byte{"test": public_share_privkey.Public().Serialize()}).
		SetPublicKey(keyshare_privkey.Public().Serialize()).
		SetMinSigners(2).
		SetCoordinatorIndex(0).
		Save(ctx)
	require.NoError(t, err)

	// Create a tree
	tree, err := db.Tree.Create().
		SetOwnerIdentityPubkey(identity_privkey.Public().Serialize()).
		SetNetwork(st.NetworkRegtest).
		SetBaseTxid(make([]byte, 32)).
		SetVout(0).
		SetStatus(st.TreeStatusAvailable).
		Save(ctx)
	require.NoError(t, err)

	// Create a tree node with Available status
	availableNode, err := db.TreeNode.Create().
		SetTree(tree).
		SetStatus(st.TreeNodeStatusAvailable).
		SetOwnerIdentityPubkey(identity_privkey.Public().Serialize()).
		SetOwnerSigningPubkey(signing_privkey.Public().Serialize()).
		SetValue(100000).
		SetVerifyingPubkey(verifying_privkey.Public().Serialize()).
		SetSigningKeyshare(signingKeyshare).
		SetRawTx([]byte("test_tx")).
		SetVout(0).
		Save(ctx)
	require.NoError(t, err)

	// Create a tree node with different status
	creatingNode, err := db.TreeNode.Create().
		SetTree(tree).
		SetStatus(st.TreeNodeStatusCreating).
		SetOwnerIdentityPubkey(identity_privkey.Public().Serialize()).
		SetOwnerSigningPubkey(signing_privkey.Public().Serialize()).
		SetValue(100000).
		SetVerifyingPubkey(verifying_privkey.Public().Serialize()).
		SetSigningKeyshare(signingKeyshare).
		SetRawTx([]byte("test_tx")).
		SetVout(1).
		Save(ctx)
	require.NoError(t, err)

	tests := []struct {
		name                string
		parentNodeOutput    *pb.NodeOutput
		expectError         bool
		expectedFinalStatus st.TreeNodeStatus
	}{
		{
			name:             "nil parent node output",
			parentNodeOutput: nil,
			expectError:      false,
		},
		{
			name: "invalid node ID",
			parentNodeOutput: &pb.NodeOutput{
				NodeId: "invalid-uuid",
				Vout:   0,
			},
			expectError: true,
		},
		{
			name: "non-existent node",
			parentNodeOutput: &pb.NodeOutput{
				NodeId: uuid.New().String(),
				Vout:   0,
			},
			expectError: true,
		},
		{
			name: "available node - should be updated to splitted",
			parentNodeOutput: &pb.NodeOutput{
				NodeId: availableNode.ID.String(),
				Vout:   0,
			},
			expectError:         false,
			expectedFinalStatus: st.TreeNodeStatusSplitted,
		},
		{
			name: "creating node - should remain unchanged",
			parentNodeOutput: &pb.NodeOutput{
				NodeId: creatingNode.ID.String(),
				Vout:   1,
			},
			expectError:         false,
			expectedFinalStatus: st.TreeNodeStatusCreating, // Should remain unchanged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := handler.updateParentNodeStatus(ctx, tt.parentNodeOutput)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				if tt.parentNodeOutput != nil {
					// Verify the status was updated correctly
					nodeID, err := uuid.Parse(tt.parentNodeOutput.NodeId)
					require.NoError(t, err)

					updatedNode, err := db.TreeNode.Get(ctx, nodeID)
					require.NoError(t, err)
					assert.Equal(t, tt.expectedFinalStatus, updatedNode.Status)
				}
			}
		})
	}
}

func TestCreateTestHelpers(t *testing.T) {
	t.Run("createTestHandler", func(t *testing.T) {
		handler := createTestHandler(t)
		assert.NotNil(t, handler)
		assert.NotNil(t, handler.config)
	})

	t.Run("createTestTx", func(t *testing.T) {
		tx := createTestTx(t)
		assert.NotNil(t, tx)
		assert.Len(t, tx.TxOut, 1)
		assert.Equal(t, int64(100000), tx.TxOut[0].Value)
	})

	t.Run("createTestUTXO", func(t *testing.T) {
		rawTx := []byte("test_tx")
		vout := uint32(1)
		utxo := createTestUTXO(t, rawTx, vout)

		assert.NotNil(t, utxo)
		assert.Equal(t, rawTx, utxo.RawTx)
		assert.Equal(t, vout, utxo.Vout)
		assert.Equal(t, pb.Network_REGTEST, utxo.Network)
	})
}

func TestEdgeCases(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, dbCtx := db.NewTestSQLiteContext(t, ctx)
	defer dbCtx.Close()

	handler := createTestHandler(t)

	t.Run("findParentOutputFromUtxo with malformed transaction", func(t *testing.T) {
		utxo := &pb.UTXO{
			Vout: 0,
		}

		output, err := handler.findParentOutputFromUtxo(ctx, utxo)
		require.Error(t, err)
		assert.Nil(t, output)
	})

	t.Run("validateAndCountTreeAddressNodes with nil parent key", func(t *testing.T) {
		rng := rand.NewChaCha8([32]byte{1})
		userPrivkey := keys.MustGeneratePrivateKeyFromRand(rng)
		nodes := []*pb.AddressRequestNode{
			{
				UserPublicKey: userPrivkey.Public().Serialize(),
				Children:      nil,
			},
		}

		count, err := handler.validateAndCountTreeAddressNodes(ctx, nil, nodes)
		require.Error(t, err) // Should fail due to nil parent key
		assert.Equal(t, 0, count)
	})
}
