package grpctest

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/lightsparkdev/spark/common/keys"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightsparkdev/spark/common"
	"github.com/lightsparkdev/spark/proto/spark"
	pb "github.com/lightsparkdev/spark/proto/spark"
	"github.com/lightsparkdev/spark/so/handler"
	testutil "github.com/lightsparkdev/spark/test_util"
	"github.com/lightsparkdev/spark/wallet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setupUsers(t *testing.T, amountSats int64) (*wallet.Config, *wallet.Config, wallet.LeafKeyTweak) {
	config, err := testutil.TestWalletConfig()
	require.NoError(t, err)
	sspConfig, err := testutil.TestWalletConfig()
	require.NoError(t, err)

	leafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)

	rootNode, err := testutil.CreateNewTree(config, faucet, leafPrivKey.ToBTCEC(), amountSats)
	require.NoError(t, err)

	transferNode := wallet.LeafKeyTweak{
		Leaf:              rootNode,
		SigningPrivKey:    leafPrivKey.Serialize(),
		NewSigningPrivKey: sspConfig.IdentityPrivateKey.Serialize(),
	}

	return config, sspConfig, transferNode
}

func createTestCoopExitAndConnectorOutputs(
	t *testing.T,
	config *wallet.Config,
	leafCount int,
	outPoint *wire.OutPoint,
	userPubKey keys.Public, userAmountSats int64,
) (*wire.MsgTx, []*wire.OutPoint) {
	// Get arbitrary SSP address, using identity for convenience
	identityPubKey, err := keys.ParsePublicKey(config.IdentityPublicKey())
	require.NoError(t, err)
	sspIntermediateAddress, err := common.P2TRAddressFromPublicKey(identityPubKey, config.Network)
	require.NoError(t, err)

	withdrawAddress, err := common.P2TRAddressFromPublicKey(userPubKey, config.Network)
	require.NoError(t, err)

	dustAmountSats := 354
	intermediateAmountSats := int64((leafCount + 1) * dustAmountSats)

	exitTx, err := testutil.CreateTestCoopExitTransaction(outPoint, withdrawAddress, userAmountSats, sspIntermediateAddress, intermediateAmountSats)
	require.NoError(t, err)

	exitTxHash := exitTx.TxHash()
	intermediateOutPoint := wire.NewOutPoint(&exitTxHash, 1)
	connectorP2trAddrs := make([]string, 0)
	for range leafCount + 1 {
		connectorPrivKey, err := keys.GeneratePrivateKey()
		require.NoError(t, err)
		connectorAddress, err := common.P2TRAddressFromPublicKey(connectorPrivKey.Public(), config.Network)
		require.NoError(t, err)
		connectorP2trAddrs = append(connectorP2trAddrs, connectorAddress)
	}
	feeBumpAddr := connectorP2trAddrs[len(connectorP2trAddrs)-1]
	connectorP2trAddrs = connectorP2trAddrs[:len(connectorP2trAddrs)-1]
	connectorTx, err := testutil.CreateTestConnectorTransaction(intermediateOutPoint, intermediateAmountSats, connectorP2trAddrs, feeBumpAddr)
	require.NoError(t, err)

	connectorOutputs := make([]*wire.OutPoint, 0)
	for i := range connectorTx.TxOut[:len(connectorTx.TxOut)-1] {
		txHash := connectorTx.TxHash()
		connectorOutputs = append(connectorOutputs, wire.NewOutPoint(&txHash, uint32(i)))
	}
	return exitTx, connectorOutputs
}

func waitForPendingTransferToConfirm(
	ctx context.Context,
	t *testing.T,
	config *wallet.Config,
) *spark.Transfer {
	pendingTransfer, err := wallet.QueryPendingTransfers(ctx, config)
	require.NoError(t, err)
	startTime := time.Now()
	for len(pendingTransfer.Transfers) == 0 {
		if time.Since(startTime) > 10*time.Second {
			t.Fatalf("timed out waiting for key to be tweaked from tx confirmation")
		}
		time.Sleep(100 * time.Millisecond)
		pendingTransfer, err = wallet.QueryPendingTransfers(ctx, config)
		require.NoError(t, err)
	}
	return pendingTransfer.Transfers[0]
}

func TestCoopExitBasic(t *testing.T) {
	client := testutil.GetBitcoinClient()

	coin, err := faucet.Fund()
	require.NoError(t, err)

	amountSats := int64(100_000)
	config, sspConfig, transferNode := setupUsers(t, amountSats)

	// SSP creates transactions
	withdrawPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	exitTx, connectorOutputs := createTestCoopExitAndConnectorOutputs(
		t, sspConfig, 1, coin.OutPoint, withdrawPrivKey.Public(), amountSats,
	)

	// User creates transfer to SSP on the condition that the tx is confirmed
	exitTxID, err := hex.DecodeString(exitTx.TxID())
	require.NoError(t, err)
	senderTransfer, _, err := wallet.GetConnectorRefundSignatures(
		context.Background(),
		config,
		[]wallet.LeafKeyTweak{transferNode},
		exitTxID,
		connectorOutputs,
		sspConfig.IdentityPrivateKey.PubKey(),
		time.Now().Add(24*time.Hour),
	)
	require.NoError(t, err)
	assert.Equal(t, spark.TransferStatus_TRANSFER_STATUS_SENDER_KEY_TWEAK_PENDING, senderTransfer.Status)

	// SSP signs exit tx and broadcasts
	signedExitTx, err := testutil.SignFaucetCoin(exitTx, coin.TxOut, coin.Key)
	require.NoError(t, err)

	_, err = client.SendRawTransaction(signedExitTx, true)
	require.NoError(t, err)

	// Make sure the exit tx gets enough confirmations
	randomKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	randomAddress, err := common.P2TRRawAddressFromPublicKey(randomKey.Public(), common.Regtest)
	require.NoError(t, err)
	// Confirm extra buffer to scan more blocks than needed
	// So that we don't race the chain watcher in this test
	_, err = client.GenerateToAddress(handler.CoopExitConfirmationThreshold+6, randomAddress, nil)
	require.NoError(t, err)

	// Wait until tx is confirmed and picked up by SO
	sspToken, err := wallet.AuthenticateWithServer(context.Background(), sspConfig)
	require.NoError(t, err)
	sspCtx := wallet.ContextWithToken(context.Background(), sspToken)

	receiverTransfer := waitForPendingTransferToConfirm(sspCtx, t, sspConfig)
	assert.Equal(t, senderTransfer.Id, receiverTransfer.Id)
	assert.Equal(t, spark.TransferStatus_TRANSFER_STATUS_SENDER_KEY_TWEAKED, receiverTransfer.Status)
	assert.Equal(t, spark.TransferType_COOPERATIVE_EXIT, receiverTransfer.Type)

	leafPrivKeyMap, err := wallet.VerifyPendingTransfer(context.Background(), sspConfig, receiverTransfer)
	require.NoError(t, err)
	assert.Len(t, leafPrivKeyMap, 1)
	assert.Equal(t, leafPrivKeyMap[transferNode.Leaf.Id], sspConfig.IdentityPrivateKey.Serialize())

	// Claim leaf. This requires a loop because sometimes there are
	// delays in processing blocks, and after the tx initially confirms,
	// the SO will still reject a claim until the tx has enough confirmations.
	finalLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	claimingNode := wallet.LeafKeyTweak{
		Leaf:              senderTransfer.Leaves[0].Leaf,
		SigningPrivKey:    sspConfig.IdentityPrivateKey.Serialize(),
		NewSigningPrivKey: finalLeafPrivKey.Serialize(),
	}
	leavesToClaim := [1]wallet.LeafKeyTweak{claimingNode}
	startTime := time.Now()
	for {
		_, err = wallet.ClaimTransfer(
			sspCtx,
			receiverTransfer,
			sspConfig,
			leavesToClaim[:],
		)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
		if time.Since(startTime) > 15*time.Second {
			t.Fatalf("timed out waiting for tx to confirm")
		}
	}
}

func TestCoopExitV2Basic(t *testing.T) {
	client, err := testutil.NewRegtestClient()
	require.NoError(t, err)

	coin, err := faucet.Fund()
	require.NoError(t, err)

	amountSats := int64(100_000)
	config, sspConfig, transferNode := setupUsers(t, amountSats)

	// Create gRPC client for V2 function
	conn, err := common.NewGRPCConnectionWithTestTLS(config.CoodinatorAddress(), nil)
	require.NoError(t, err, "failed to create grpc connection")
	defer conn.Close()

	authToken, err := wallet.AuthenticateWithServer(context.Background(), config)
	require.NoError(t, err, "failed to authenticate sender")
	tmpCtx := wallet.ContextWithToken(context.Background(), authToken)

	sparkClient := pb.NewSparkServiceClient(conn)

	// SSP creates transactions
	withdrawPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	exitTx, connectorOutputs := createTestCoopExitAndConnectorOutputs(
		t, sspConfig, 1, coin.OutPoint, withdrawPrivKey.Public(), amountSats,
	)

	// User creates transfer to SSP on the condition that the tx is confirmed
	exitTxID, err := hex.DecodeString(exitTx.TxID())
	require.NoError(t, err)
	senderTransfer, _, err := wallet.GetConnectorRefundSignaturesV2(
		tmpCtx,
		config,
		sparkClient,
		[]wallet.LeafKeyTweak{transferNode},
		exitTxID,
		connectorOutputs,
		sspConfig.IdentityPrivateKey.PubKey(),
		time.Now().Add(24*time.Hour),
	)
	require.NoError(t, err)
	assert.Equal(t, spark.TransferStatus_TRANSFER_STATUS_SENDER_KEY_TWEAK_PENDING, senderTransfer.Status)

	// SSP signs exit tx and broadcasts
	signedExitTx, err := testutil.SignFaucetCoin(exitTx, coin.TxOut, coin.Key)
	require.NoError(t, err)

	_, err = client.SendRawTransaction(signedExitTx, true)
	require.NoError(t, err)

	// Make sure the exit tx gets enough confirmations
	randomKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	randomAddress, err := common.P2TRRawAddressFromPublicKey(randomKey.Public(), common.Regtest)
	require.NoError(t, err)
	// Confirm extra buffer to scan more blocks than needed
	// So that we don't race the chain watcher in this test
	_, err = client.GenerateToAddress(handler.CoopExitConfirmationThreshold+6, randomAddress, nil)
	require.NoError(t, err)

	// Wait until tx is confirmed and picked up by SO
	sspToken, err := wallet.AuthenticateWithServer(context.Background(), sspConfig)
	require.NoError(t, err)
	sspCtx := wallet.ContextWithToken(context.Background(), sspToken)

	receiverTransfer := waitForPendingTransferToConfirm(sspCtx, t, sspConfig)
	assert.Equal(t, receiverTransfer.Id, senderTransfer.Id)
	assert.Equal(t, spark.TransferStatus_TRANSFER_STATUS_SENDER_KEY_TWEAKED, receiverTransfer.Status)
	assert.Equal(t, spark.TransferType_COOPERATIVE_EXIT, receiverTransfer.Type)

	leafPrivKeyMap, err := wallet.VerifyPendingTransfer(context.Background(), sspConfig, receiverTransfer)
	require.NoError(t, err)
	assert.Len(t, leafPrivKeyMap, 1)
	assert.Equal(t, leafPrivKeyMap[transferNode.Leaf.Id], sspConfig.IdentityPrivateKey.Serialize())

	// Claim leaf. This requires a loop because sometimes there are
	// delays in processing blocks, and after the tx initially confirms,
	// the SO will still reject a claim until the tx has enough confirmations.
	finalLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	claimingNode := wallet.LeafKeyTweak{
		Leaf:              senderTransfer.Leaves[0].Leaf,
		SigningPrivKey:    sspConfig.IdentityPrivateKey.Serialize(),
		NewSigningPrivKey: finalLeafPrivKey.Serialize(),
	}
	leavesToClaim := [1]wallet.LeafKeyTweak{claimingNode}
	startTime := time.Now()
	for {
		_, err = wallet.ClaimTransfer(
			sspCtx,
			receiverTransfer,
			sspConfig,
			leavesToClaim[:],
		)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
		if time.Since(startTime) > 15*time.Second {
			t.Fatalf("timed out waiting for tx to confirm")
		}
	}
}

func TestCoopExitCannotClaimBeforeEnoughConfirmations(t *testing.T) {
	client := testutil.GetBitcoinClient()

	coin, err := faucet.Fund()
	require.NoError(t, err)

	amountSats := int64(100_000)
	config, sspConfig, transferNode := setupUsers(t, amountSats)

	// SSP creates transactions
	withdrawPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	exitTx, connectorOutputs := createTestCoopExitAndConnectorOutputs(
		t, sspConfig, 1, coin.OutPoint, withdrawPrivKey.Public(), amountSats,
	)

	// User creates transfer to SSP on the condition that the tx is confirmed
	exitTxID, err := hex.DecodeString(exitTx.TxID())
	require.NoError(t, err)
	_, _, err = wallet.GetConnectorRefundSignatures(
		context.Background(),
		config,
		[]wallet.LeafKeyTweak{transferNode},
		exitTxID,
		connectorOutputs,
		sspConfig.IdentityPrivateKey.PubKey(),
		time.Now().Add(24*time.Hour),
	)
	require.NoError(t, err)

	// SSP signs exit tx and broadcasts
	signedExitTx, err := testutil.SignFaucetCoin(exitTx, coin.TxOut, coin.Key)
	require.NoError(t, err)

	_, err = client.SendRawTransaction(signedExitTx, true)
	require.NoError(t, err)

	randomKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	randomAddress, err := common.P2TRRawAddressFromPublicKey(randomKey.Public(), common.Regtest)
	require.NoError(t, err)
	// Confirm half the threshold
	_, err = client.GenerateToAddress(handler.CoopExitConfirmationThreshold/2, randomAddress, nil)
	require.NoError(t, err)

	// Wait until tx is confirmed and picked up by SO
	sspToken, err := wallet.AuthenticateWithServer(context.Background(), sspConfig)
	require.NoError(t, err)
	sspCtx := wallet.ContextWithToken(context.Background(), sspToken)

	receiverTransfer := waitForPendingTransferToConfirm(sspCtx, t, sspConfig)

	// Try to claim leaf before exit tx confirms -> should fail
	finalLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	claimingNode := wallet.LeafKeyTweak{
		Leaf:              receiverTransfer.Leaves[0].Leaf,
		SigningPrivKey:    sspConfig.IdentityPrivateKey.Serialize(),
		NewSigningPrivKey: finalLeafPrivKey.Serialize(),
	}
	leavesToClaim := [1]wallet.LeafKeyTweak{claimingNode}
	_, err = wallet.ClaimTransfer(
		sspCtx,
		receiverTransfer,
		sspConfig,
		leavesToClaim[:],
	)
	require.Error(t, err, "expected error claiming transfer before exit tx confirms")
	stat, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, stat.Code())
}

func TestCoopExitCannotClaimBeforeConfirm(t *testing.T) {
	coin, err := faucet.Fund()
	require.NoError(t, err)

	amountSats := int64(100_000)
	config, sspConfig, transferNode := setupUsers(t, amountSats)

	// SSP creates transactions
	withdrawPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	exitTx, connectorOutputs := createTestCoopExitAndConnectorOutputs(
		t, sspConfig, 1, coin.OutPoint, withdrawPrivKey.Public(), amountSats,
	)

	// User creates transfer to SSP on the condition that the tx is confirmed
	exitTxID, err := hex.DecodeString(exitTx.TxID())
	require.NoError(t, err)
	senderTransfer, _, err := wallet.GetConnectorRefundSignatures(
		context.Background(),
		config,
		[]wallet.LeafKeyTweak{transferNode},
		exitTxID,
		connectorOutputs,
		sspConfig.IdentityPrivateKey.PubKey(),
		time.Now().Add(24*time.Hour),
	)
	require.NoError(t, err)
	assert.Equal(t, spark.TransferStatus_TRANSFER_STATUS_SENDER_KEY_TWEAK_PENDING, senderTransfer.Status)

	// Prepare for claim
	finalLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	claimingNode := wallet.LeafKeyTweak{
		Leaf:              senderTransfer.Leaves[0].Leaf,
		SigningPrivKey:    sspConfig.IdentityPrivateKey.Serialize(),
		NewSigningPrivKey: finalLeafPrivKey.Serialize(),
	}

	// Try to claim leaf before exit tx confirms -> should fail
	sspToken, err := wallet.AuthenticateWithServer(context.Background(), sspConfig)
	require.NoError(t, err)
	sspCtx := wallet.ContextWithToken(context.Background(), sspToken)
	leavesToClaim := [1]wallet.LeafKeyTweak{claimingNode}
	_, err = wallet.ClaimTransferTweakKeys(
		sspCtx,
		senderTransfer,
		sspConfig,
		leavesToClaim[:],
	)
	require.Error(t, err, "expected error claiming transfer before exit tx confirms")
	stat, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, stat.Code())
}

// Start coop exit, SSP doesn't broadcast, should be able to cancel after expiry
func TestCoopExitCancelNoBroadcast(t *testing.T) {
	coin, err := faucet.Fund()
	require.NoError(t, err)

	amountSats := int64(100_000)
	config, sspConfig, transferNode := setupUsers(t, amountSats)

	withdrawPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	exitTx, connectorOutputs := createTestCoopExitAndConnectorOutputs(
		t, sspConfig, 1, coin.OutPoint, withdrawPrivKey.Public(), amountSats,
	)

	exitTxID, err := hex.DecodeString(exitTx.TxID())
	require.NoError(t, err)
	expiryDelta := 1 * time.Second
	senderTransfer, _, err := wallet.GetConnectorRefundSignatures(
		context.Background(),
		config,
		[]wallet.LeafKeyTweak{transferNode},
		exitTxID,
		connectorOutputs,
		sspConfig.IdentityPrivateKey.PubKey(),
		time.Now().Add(expiryDelta),
	)
	require.NoError(t, err)

	time.Sleep(expiryDelta)

	_, err = wallet.CancelTransfer(context.Background(), config, senderTransfer)
	require.NoError(t, err)
}

// Start coop exit, SSP broadcasts, should not be able to cancel after expiry
func TestCoopExitCannotCancelAfterBroadcast(t *testing.T) {
	client := testutil.GetBitcoinClient()

	coin, err := faucet.Fund()
	require.NoError(t, err)

	amountSats := int64(100_000)
	config, sspConfig, transferNode := setupUsers(t, amountSats)

	withdrawPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	exitTx, connectorOutputs := createTestCoopExitAndConnectorOutputs(
		t, sspConfig, 1, coin.OutPoint, withdrawPrivKey.Public(), amountSats,
	)

	exitTxID, err := hex.DecodeString(exitTx.TxID())
	require.NoError(t, err)
	expiryDelta := 1 * time.Second
	senderTransfer, _, err := wallet.GetConnectorRefundSignatures(
		context.Background(),
		config,
		[]wallet.LeafKeyTweak{transferNode},
		exitTxID,
		connectorOutputs,
		sspConfig.IdentityPrivateKey.PubKey(),
		time.Now().Add(expiryDelta),
	)
	require.NoError(t, err)

	time.Sleep(expiryDelta)

	// Broadcast and make sure 1. we can't cancel, and 2. we can claim
	signedExitTx, err := testutil.SignFaucetCoin(exitTx, coin.TxOut, coin.Key)
	require.NoError(t, err)

	_, err = client.SendRawTransaction(signedExitTx, true)
	require.NoError(t, err)

	randomKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	randomPubKey := randomKey.Public()
	randomAddress, err := common.P2TRRawAddressFromPublicKey(randomPubKey, common.Regtest)
	require.NoError(t, err)

	_, err = client.GenerateToAddress(handler.CoopExitConfirmationThreshold+6, randomAddress, nil)
	require.NoError(t, err)

	sspToken, err := wallet.AuthenticateWithServer(context.Background(), sspConfig)
	require.NoError(t, err)
	sspCtx := wallet.ContextWithToken(context.Background(), sspToken)

	pendingTransfer, err := wallet.QueryPendingTransfers(sspCtx, sspConfig)
	require.NoError(t, err)
	startTime := time.Now()
	for len(pendingTransfer.Transfers) == 0 {
		if time.Since(startTime) > 10*time.Second {
			t.Fatalf("timed out waiting for key to be tweaked from tx confirmation")
		}
		time.Sleep(100 * time.Millisecond)
		pendingTransfer, err = wallet.QueryPendingTransfers(sspCtx, sspConfig)
		require.NoError(t, err)
	}
	receiverTransfer := pendingTransfer.Transfers[0]
	assert.Equal(t, receiverTransfer.Id, senderTransfer.Id)
	assert.Equal(t, spark.TransferStatus_TRANSFER_STATUS_SENDER_KEY_TWEAKED, receiverTransfer.Status)
	assert.Equal(t, spark.TransferType_COOPERATIVE_EXIT, receiverTransfer.Type)

	leafPrivKeyMap, err := wallet.VerifyPendingTransfer(context.Background(), sspConfig, receiverTransfer)
	require.NoError(t, err)
	assert.Len(t, leafPrivKeyMap, 1)
	assert.Equal(t, leafPrivKeyMap[transferNode.Leaf.Id], sspConfig.IdentityPrivateKey.Serialize())

	// Fail to cancel
	_, err = wallet.CancelTransfer(context.Background(), config, senderTransfer)
	assert.Error(t, err, "expected error cancelling transfer after exit tx confirmed")

	// Succeed in claiming
	finalLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)
	claimingNode := wallet.LeafKeyTweak{
		Leaf:              senderTransfer.Leaves[0].Leaf,
		SigningPrivKey:    sspConfig.IdentityPrivateKey.Serialize(),
		NewSigningPrivKey: finalLeafPrivKey.Serialize(),
	}
	leavesToClaim := [1]wallet.LeafKeyTweak{claimingNode}
	startTime = time.Now()
	for {
		_, err = wallet.ClaimTransfer(
			sspCtx,
			receiverTransfer,
			sspConfig,
			leavesToClaim[:],
		)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
		if time.Since(startTime) > 15*time.Second {
			t.Fatalf("timed out waiting for tx to confirm")
		}
	}
}
