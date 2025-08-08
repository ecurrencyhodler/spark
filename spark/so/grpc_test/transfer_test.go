package grpctest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lightsparkdev/spark/common/keys"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/txscript"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/lightsparkdev/spark/common"
	pbmock "github.com/lightsparkdev/spark/proto/mock"
	"github.com/lightsparkdev/spark/proto/spark"
	testutil "github.com/lightsparkdev/spark/test_util"
	"github.com/lightsparkdev/spark/wallet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTransfer(t *testing.T) {
	// Sender initiates transfer
	senderConfig, err := testutil.TestWalletConfig()
	require.NoError(t, err, "failed to create sender wallet config")

	leafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create node signing private key")
	rootNode, err := testutil.CreateNewTree(senderConfig, faucet, leafPrivKey, 100_000)
	require.NoError(t, err, "failed to create new tree")

	newLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")

	receiverPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create receiver private key")

	transferNode := wallet.LeafKeyTweak{
		Leaf:              rootNode,
		SigningPrivKey:    leafPrivKey,
		NewSigningPrivKey: newLeafPrivKey,
	}
	leavesToTransfer := [1]wallet.LeafKeyTweak{transferNode}

	conn, err := common.NewGRPCConnectionWithTestTLS(senderConfig.CoodinatorAddress(), nil)
	require.NoError(t, err, "failed to create grpc connection")
	defer conn.Close()

	authToken, err := wallet.AuthenticateWithServer(context.Background(), senderConfig)
	require.NoError(t, err, "failed to authenticate sender")
	senderCtx := wallet.ContextWithToken(context.Background(), authToken)

	senderTransfer, err := wallet.SendTransferWithKeyTweaks(
		senderCtx,
		senderConfig,
		leavesToTransfer[:],
		receiverPrivKey.Public(),
		time.Now().Add(10*time.Minute),
	)
	require.NoError(t, err, "failed to transfer tree node")

	// Receiver queries pending transfer
	receiverConfig, err := testutil.TestWalletConfigWithIdentityKey(*receiverPrivKey.ToBTCEC())
	require.NoError(t, err, "failed to create wallet config")
	receiverToken, err := wallet.AuthenticateWithServer(context.Background(), receiverConfig)
	require.NoError(t, err, "failed to authenticate receiver")
	receiverCtx := wallet.ContextWithToken(context.Background(), receiverToken)
	pendingTransfer, err := wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)
	receiverTransfer := pendingTransfer.Transfers[0]
	require.Equal(t, senderTransfer.Id, receiverTransfer.Id)
	require.Equal(t, spark.TransferType_TRANSFER, receiverTransfer.Type)

	leafPrivKeyMap, err := wallet.VerifyPendingTransfer(context.Background(), receiverConfig, receiverTransfer)
	assertVerifiedPendingTransfer(t, err, leafPrivKeyMap, rootNode, newLeafPrivKey)

	finalLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")
	claimingNode := wallet.LeafKeyTweak{
		Leaf:              receiverTransfer.Leaves[0].Leaf,
		SigningPrivKey:    newLeafPrivKey,
		NewSigningPrivKey: finalLeafPrivKey,
	}
	leavesToClaim := [1]wallet.LeafKeyTweak{claimingNode}
	res, err := wallet.ClaimTransfer(
		receiverCtx,
		receiverTransfer,
		receiverConfig,
		leavesToClaim[:],
	)
	require.NoError(t, err, "failed to ClaimTransfer")
	require.Equal(t, res[0].Id, claimingNode.Leaf.Id)
}

func TestQueryPendingTransferByNetwork(t *testing.T) {
	senderConfig, err := testutil.TestWalletConfig()
	require.NoError(t, err, "failed to create sender wallet config")

	leafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create node signing private key")
	rootNode, err := testutil.CreateNewTree(senderConfig, faucet, leafPrivKey, 100_000)
	require.NoError(t, err, "failed to create new tree")

	newLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")

	receiverPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create receiver private key")

	transferNode := wallet.LeafKeyTweak{
		Leaf:              rootNode,
		SigningPrivKey:    leafPrivKey,
		NewSigningPrivKey: newLeafPrivKey,
	}
	leavesToTransfer := [1]wallet.LeafKeyTweak{transferNode}

	conn, err := common.NewGRPCConnectionWithTestTLS(senderConfig.CoodinatorAddress(), nil)
	require.NoError(t, err, "failed to create grpc connection")
	defer conn.Close()

	authToken, err := wallet.AuthenticateWithServer(context.Background(), senderConfig)
	require.NoError(t, err, "failed to authenticate sender")
	senderCtx := wallet.ContextWithToken(context.Background(), authToken)

	_, err = wallet.SendTransferWithKeyTweaks(
		senderCtx,
		senderConfig,
		leavesToTransfer[:],
		receiverPrivKey.Public(),
		time.Now().Add(10*time.Minute),
	)
	require.NoError(t, err, "failed to transfer tree node")

	receiverConfig, err := testutil.TestWalletConfigWithIdentityKey(*receiverPrivKey.ToBTCEC())
	require.NoError(t, err, "failed to create wallet config")
	receiverToken, err := wallet.AuthenticateWithServer(context.Background(), receiverConfig)
	require.NoError(t, err, "failed to authenticate receiver")
	receiverCtx := wallet.ContextWithToken(context.Background(), receiverToken)
	pendingTransfer, err := wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)

	incorrectNetworkReceiverConfig := receiverConfig
	incorrectNetworkReceiverConfig.Network = common.Mainnet
	incorrectNetworkReceiverToken, err := wallet.AuthenticateWithServer(context.Background(), incorrectNetworkReceiverConfig)
	require.NoError(t, err, "failed to authenticate receiver")
	incorrectNetworkReceiverCtx := wallet.ContextWithToken(context.Background(), incorrectNetworkReceiverToken)
	pendingTransfer, err = wallet.QueryPendingTransfers(incorrectNetworkReceiverCtx, incorrectNetworkReceiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Empty(t, pendingTransfer.Transfers)
}

func TestTransferInterrupt(t *testing.T) {
	// Sender initiates transfer
	senderConfig, err := testutil.TestWalletConfig()
	require.NoError(t, err, "failed to create sender wallet config")

	leafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create node signing private key")
	rootNode, err := testutil.CreateNewTree(senderConfig, faucet, leafPrivKey, 100_000)
	require.NoError(t, err, "failed to create new tree")

	newLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")

	receiverPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create receiver private key")

	transferNode := wallet.LeafKeyTweak{
		Leaf:              rootNode,
		SigningPrivKey:    leafPrivKey,
		NewSigningPrivKey: newLeafPrivKey,
	}
	leavesToTransfer := [1]wallet.LeafKeyTweak{transferNode}

	conn, err := common.NewGRPCConnectionWithTestTLS(senderConfig.CoodinatorAddress(), nil)
	require.NoError(t, err, "failed to create grpc connection")
	defer conn.Close()

	authToken, err := wallet.AuthenticateWithServer(context.Background(), senderConfig)
	require.NoError(t, err, "failed to authenticate sender")
	senderCtx := wallet.ContextWithToken(context.Background(), authToken)

	senderTransfer, err := wallet.SendTransferWithKeyTweaks(
		senderCtx,
		senderConfig,
		leavesToTransfer[:],
		receiverPrivKey.Public(),
		time.Now().Add(10*time.Minute),
	)
	require.NoError(t, err, "failed to transfer tree node")

	// Receiver queries pending transfer
	receiverConfig, err := testutil.TestWalletConfigWithIdentityKey(*receiverPrivKey.ToBTCEC())
	require.NoError(t, err, "failed to create wallet config")
	receiverToken, err := wallet.AuthenticateWithServer(context.Background(), receiverConfig)
	require.NoError(t, err, "failed to authenticate receiver")
	receiverCtx := wallet.ContextWithToken(context.Background(), receiverToken)
	pendingTransfer, err := wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)
	receiverTransfer := pendingTransfer.Transfers[0]
	require.Equal(t, senderTransfer.Id, receiverTransfer.Id)
	require.Equal(t, spark.TransferType_TRANSFER, receiverTransfer.Type)

	leafPrivKeyMap, err := wallet.VerifyPendingTransfer(context.Background(), receiverConfig, receiverTransfer)
	assertVerifiedPendingTransfer(t, err, leafPrivKeyMap, rootNode, newLeafPrivKey)

	mockClient := pbmock.NewMockServiceClient(conn)
	_, err = mockClient.InterruptTransfer(context.Background(), &pbmock.InterruptTransferRequest{
		Action: pbmock.InterruptTransferRequest_INTERRUPT,
	})
	require.NoError(t, err, "failed to interrupt transfer")

	finalLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")
	claimingNode := wallet.LeafKeyTweak{
		Leaf:              receiverTransfer.Leaves[0].Leaf,
		SigningPrivKey:    newLeafPrivKey,
		NewSigningPrivKey: finalLeafPrivKey,
	}
	leavesToClaim := [1]wallet.LeafKeyTweak{claimingNode}
	_, err = wallet.ClaimTransfer(
		receiverCtx,
		receiverTransfer,
		receiverConfig,
		leavesToClaim[:],
	)
	require.Error(t, err, "expected error when claiming transfer")

	_, err = mockClient.InterruptTransfer(context.Background(), &pbmock.InterruptTransferRequest{
		Action: pbmock.InterruptTransferRequest_RESUME,
	})
	require.NoError(t, err, "failed to resume transfer")

	pendingTransfer, err = wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)
	receiverTransfer = pendingTransfer.Transfers[0]
	require.Equal(t, senderTransfer.Id, receiverTransfer.Id)
	require.Equal(t, spark.TransferType_TRANSFER, receiverTransfer.Type)

	res, err := wallet.ClaimTransfer(receiverCtx, receiverTransfer, receiverConfig, leavesToClaim[:])
	require.NoError(t, err, "failed to ClaimTransfer")
	require.Equal(t, claimingNode.Leaf.Id, res[0].Id)
}

func TestTransferRecoverFinalizeSignatures(t *testing.T) {
	// Sender initiates transfer
	senderConfig, err := testutil.TestWalletConfig()
	require.NoError(t, err, "failed to create sender wallet config")

	leafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create node signing private key")
	rootNode, err := testutil.CreateNewTree(senderConfig, faucet, leafPrivKey, 100_000)
	require.NoError(t, err, "failed to create new tree")

	newLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")

	receiverPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create receiver private key")

	transferNode := wallet.LeafKeyTweak{
		Leaf:              rootNode,
		SigningPrivKey:    leafPrivKey,
		NewSigningPrivKey: newLeafPrivKey,
	}
	leavesToTransfer := [1]wallet.LeafKeyTweak{transferNode}
	senderTransfer, err := wallet.SendTransfer(
		context.Background(),
		senderConfig,
		leavesToTransfer[:],
		receiverPrivKey.Public(),
		time.Now().Add(10*time.Minute),
	)
	require.NoError(t, err, "failed to transfer tree node")

	// Receiver queries pending transfer
	receiverConfig, err := testutil.TestWalletConfigWithIdentityKey(*receiverPrivKey.ToBTCEC())
	require.NoError(t, err, "failed to create wallet config")
	receiverToken, err := wallet.AuthenticateWithServer(context.Background(), receiverConfig)
	require.NoError(t, err, "failed to authenticate receiver")
	receiverCtx := wallet.ContextWithToken(context.Background(), receiverToken)
	pendingTransfer, err := wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)
	receiverTransfer := pendingTransfer.Transfers[0]
	require.Equal(t, senderTransfer.Id, receiverTransfer.Id)
	require.Equal(t, spark.TransferType_TRANSFER, receiverTransfer.Type)

	leafPrivKeyMap, err := wallet.VerifyPendingTransfer(context.Background(), receiverConfig, receiverTransfer)
	assertVerifiedPendingTransfer(t, err, leafPrivKeyMap, rootNode, newLeafPrivKey)

	finalLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")
	claimingNode := wallet.LeafKeyTweak{
		Leaf:              receiverTransfer.Leaves[0].Leaf,
		SigningPrivKey:    newLeafPrivKey,
		NewSigningPrivKey: finalLeafPrivKey,
	}
	leavesToClaim := [1]wallet.LeafKeyTweak{claimingNode}
	_, err = wallet.ClaimTransferWithoutFinalizeSignatures(
		receiverCtx,
		receiverTransfer,
		receiverConfig,
		leavesToClaim[:],
	)
	require.NoError(t, err, "failed to ClaimTransfer")

	pendingTransfer, err = wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)
	receiverTransfer = pendingTransfer.Transfers[0]
	require.Equal(t, senderTransfer.Id, receiverTransfer.Id)

	res, err := wallet.ClaimTransfer(
		receiverCtx,
		receiverTransfer,
		receiverConfig,
		leavesToClaim[:],
	)
	require.NoError(t, err, "failed to ClaimTransfer")
	require.Equal(t, res[0].Id, claimingNode.Leaf.Id)
}

func TestTransferZeroLeaves(t *testing.T) {
	senderConfig, err := testutil.TestWalletConfig()
	require.NoError(t, err, "failed to create sender wallet config: %v", err)

	receiverPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create receiver private key: %v", err)

	var leavesToTransfer []wallet.LeafKeyTweak
	_, err = wallet.SendTransfer(
		context.Background(),
		senderConfig,
		leavesToTransfer[:],
		receiverPrivKey.Public(),
		time.Now().Add(10*time.Minute),
	)
	require.Error(t, err, "expected error when transferring zero leaves")
	stat, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, stat.Code())
}

func TestTransferWithSeparateSteps(t *testing.T) {
	// Sender initiates transfer
	senderConfig, err := testutil.TestWalletConfig()
	require.NoError(t, err, "failed to create sender wallet config")

	leafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create node signing private key")
	rootNode, err := testutil.CreateNewTree(senderConfig, faucet, leafPrivKey, 100_000)
	require.NoError(t, err, "failed to create new tree")

	newLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")

	receiverPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create receiver private key")

	transferNode := wallet.LeafKeyTweak{
		Leaf:              rootNode,
		SigningPrivKey:    leafPrivKey,
		NewSigningPrivKey: newLeafPrivKey,
	}
	leavesToTransfer := [1]wallet.LeafKeyTweak{transferNode}
	senderTransfer, err := wallet.SendTransfer(
		context.Background(),
		senderConfig,
		leavesToTransfer[:],
		receiverPrivKey.Public(),
		time.Now().Add(10*time.Minute),
	)
	require.NoError(t, err, "failed to transfer tree node")

	// Receiver queries pending transfer
	receiverConfig, err := testutil.TestWalletConfigWithIdentityKey(*receiverPrivKey.ToBTCEC())
	require.NoError(t, err, "failed to create wallet config")
	receiverToken, err := wallet.AuthenticateWithServer(context.Background(), receiverConfig)
	require.NoError(t, err, "failed to authenticate receiver")
	receiverCtx := wallet.ContextWithToken(context.Background(), receiverToken)
	pendingTransfer, err := wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)
	receiverTransfer := pendingTransfer.Transfers[0]
	require.Equal(t, senderTransfer.Id, receiverTransfer.Id)

	leafPrivKeyMap, err := wallet.VerifyPendingTransfer(context.Background(), receiverConfig, receiverTransfer)
	assertVerifiedPendingTransfer(t, err, leafPrivKeyMap, rootNode, newLeafPrivKey)

	finalLeafPrivKey, err := keys.GeneratePrivateKey()
	if err != nil {
		t.Fatalf("failed to create new node signing private key: %v", err)
	}
	claimingNode := wallet.LeafKeyTweak{
		Leaf:              receiverTransfer.Leaves[0].Leaf,
		SigningPrivKey:    newLeafPrivKey,
		NewSigningPrivKey: finalLeafPrivKey,
	}
	leavesToClaim := [1]wallet.LeafKeyTweak{claimingNode}

	_, err = wallet.ClaimTransferTweakKeys(
		receiverCtx,
		receiverTransfer,
		receiverConfig,
		leavesToClaim[:],
	)
	require.NoError(t, err, "failed to ClaimTransferTweakKeys")

	pendingTransfer, err = wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)
	receiverTransfer = pendingTransfer.Transfers[0]
	require.Equal(t, senderTransfer.Id, receiverTransfer.Id)

	leafPrivKeyMap, err = wallet.VerifyPendingTransfer(context.Background(), receiverConfig, receiverTransfer)
	assertVerifiedPendingTransfer(t, err, leafPrivKeyMap, rootNode, newLeafPrivKey)

	_, err = wallet.ClaimTransferSignRefunds(
		receiverCtx,
		receiverTransfer,
		receiverConfig,
		leavesToClaim[:],
		nil,
	)
	require.NoError(t, err, "failed to ClaimTransferSignRefunds")

	pendingTransfer, err = wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)

	_, err = wallet.ClaimTransfer(
		receiverCtx,
		receiverTransfer,
		receiverConfig,
		leavesToClaim[:],
	)
	require.NoError(t, err, "failed to ClaimTransfer")
}

func TestCancelTransfer(t *testing.T) {
	// Sender initiates transfer
	senderConfig, err := testutil.TestWalletConfig()
	require.NoError(t, err, "failed to create sender wallet config")

	leafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create node signing private key")
	rootNode, err := testutil.CreateNewTree(senderConfig, faucet, leafPrivKey, 100_000)
	require.NoError(t, err, "failed to create new tree")

	newLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")

	receiverPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create receiver private key")

	transferNode := wallet.LeafKeyTweak{
		Leaf:              rootNode,
		SigningPrivKey:    leafPrivKey,
		NewSigningPrivKey: newLeafPrivKey,
	}
	leavesToTransfer := [1]wallet.LeafKeyTweak{transferNode}
	expiryDelta := 2 * time.Second
	senderTransfer, _, _, err := wallet.SendTransferSignRefund(
		context.Background(),
		senderConfig,
		leavesToTransfer[:],
		receiverPrivKey.Public(),
		time.Now().Add(expiryDelta),
	)
	require.NoError(t, err, "failed to transfer tree node")

	// We don't need to wait for the expiry because we haven't
	// tweaked our key yet.
	_, err = wallet.CancelTransfer(context.Background(), senderConfig, senderTransfer)
	require.NoError(t, err, "failed to cancel transfer")

	for operator := range senderConfig.SigningOperators {
		senderConfig.CoodinatorIdentifier = operator
		transfers, _, err := wallet.QueryAllTransfers(context.Background(), senderConfig, 1, 0)
		require.NoError(t, err)
		require.Len(t, transfers, 1)
	}

	senderTransfer, err = wallet.SendTransfer(
		context.Background(),
		senderConfig,
		leavesToTransfer[:],
		receiverPrivKey.Public(),
		time.Now().Add(10*time.Minute),
	)
	require.NoError(t, err, "failed to transfer tree node")

	receiverConfig, err := testutil.TestWalletConfigWithIdentityKey(*receiverPrivKey.ToBTCEC())
	require.NoError(t, err, "failed to create wallet config")
	receiverToken, err := wallet.AuthenticateWithServer(context.Background(), receiverConfig)
	require.NoError(t, err, "failed to authenticate receiver")
	receiverCtx := wallet.ContextWithToken(context.Background(), receiverToken)
	pendingTransfer, err := wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)
	receiverTransfer := pendingTransfer.Transfers[0]
	require.Equal(t, senderTransfer.Id, receiverTransfer.Id)

	leafPrivKeyMap, err := wallet.VerifyPendingTransfer(context.Background(), receiverConfig, receiverTransfer)
	assertVerifiedPendingTransfer(t, err, leafPrivKeyMap, rootNode, newLeafPrivKey)

	finalLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")
	claimingNode := wallet.LeafKeyTweak{
		Leaf:              receiverTransfer.Leaves[0].Leaf,
		SigningPrivKey:    newLeafPrivKey,
		NewSigningPrivKey: finalLeafPrivKey,
	}
	leavesToClaim := [1]wallet.LeafKeyTweak{claimingNode}
	_, err = wallet.ClaimTransfer(
		receiverCtx,
		receiverTransfer,
		receiverConfig,
		leavesToClaim[:],
	)
	require.NoError(t, err, "failed to ClaimTransfer")
}

func TestCancelTransferAfterTweak(t *testing.T) {
	// Sender initiates transfer
	senderConfig, err := testutil.TestWalletConfig()
	require.NoError(t, err, "failed to create sender wallet config")

	leafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create node signing private key")
	rootNode, err := testutil.CreateNewTree(senderConfig, faucet, leafPrivKey, 100_000)
	require.NoError(t, err, "failed to create new tree")

	newLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")

	receiverPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create receiver private key")

	transferNode := wallet.LeafKeyTweak{
		Leaf:              rootNode,
		SigningPrivKey:    leafPrivKey,
		NewSigningPrivKey: newLeafPrivKey,
	}
	leavesToTransfer := [1]wallet.LeafKeyTweak{transferNode}
	expiryDuration := 1 * time.Second
	senderTransfer, err := wallet.SendTransfer(
		context.Background(),
		senderConfig,
		leavesToTransfer[:],
		receiverPrivKey.Public(),
		time.Now().Add(expiryDuration),
	)
	require.NoError(t, err, "failed to transfer tree node")

	// Make sure transfers can't be cancelled after key tweak even after
	// expiration
	time.Sleep(expiryDuration)

	_, err = wallet.CancelTransfer(context.Background(), senderConfig, senderTransfer)
	require.Error(t, err, "expected to fail but didn't")
}

func TestQueryTransfers(t *testing.T) {
	// Initiate sender
	senderConfig, err := testutil.TestWalletConfig()
	require.NoError(t, err, "failed to create sender wallet config")

	senderLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create node signing private key")
	senderRootNode, err := testutil.CreateNewTree(senderConfig, faucet, senderLeafPrivKey, 100_000)
	require.NoError(t, err, "failed to create new tree")

	// Initiate receiver
	receiverConfig, err := testutil.TestWalletConfig()
	require.NoError(t, err, "failed to create receiver wallet config")

	receiverLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create node signing private key")
	receiverRootNode, err := testutil.CreateNewTree(receiverConfig, faucet, receiverLeafPrivKey, 100_000)
	require.NoError(t, err, "failed to create new tree")

	// Sender initiates transfer
	senderNewLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")

	senderTransferNode := wallet.LeafKeyTweak{
		Leaf:              senderRootNode,
		SigningPrivKey:    senderLeafPrivKey,
		NewSigningPrivKey: senderNewLeafPrivKey,
	}
	senderLeavesToTransfer := [1]wallet.LeafKeyTweak{senderTransferNode}

	// Get signature for refunds (normal flow)
	senderTransfer, senderRefundSignatureMap, leafDataMap, err := wallet.SendTransferSignRefund(
		context.Background(),
		senderConfig,
		senderLeavesToTransfer[:],
		receiverConfig.IdentityPublicKey(),
		time.Now().Add(10*time.Minute),
	)
	require.NoError(t, err)
	assert.Len(t, senderRefundSignatureMap, 1)
	signature := senderRefundSignatureMap[senderRootNode.Id]
	assert.NotNil(t, signature, "expected refund signature for root node")
	leafData := leafDataMap[senderRootNode.Id]
	require.NotNil(t, leafData, "expected leaf data for root node")
	require.NotNil(t, leafData.RefundTx, "expected refund tx")
	require.NotNil(t, leafData.Tx, "expected tx")
	require.NotNil(t, leafData.Tx.TxOut, "expected tx out")
	require.NotNil(t, leafData.Vout, "expected Vout")

	sighash, err := common.SigHashFromTx(leafData.RefundTx, 0, leafData.Tx.TxOut[leafData.Vout])
	require.NoError(t, err)

	// Create adaptor from that signature
	adaptorAddedSignature, adaptorPrivKey, err := common.GenerateAdaptorFromSignature(signature)
	require.NoError(t, err)
	_, adaptorPub := btcec.PrivKeyFromBytes(adaptorPrivKey)

	// Alice sends adaptor and signature to Bob, Bob validates the adaptor
	nodeVerifyingPubkey, err := secp256k1.ParsePubKey(senderRootNode.VerifyingPublicKey)
	require.NoError(t, err)
	taprootKey := txscript.ComputeTaprootKeyNoScript(nodeVerifyingPubkey)
	err = common.ValidateOutboundAdaptorSignature(taprootKey, sighash, adaptorAddedSignature, adaptorPub.SerializeCompressed())
	require.NoError(t, err)

	// Bob signs refunds with adaptor
	receiverNewLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err)

	receiverTransferNode := wallet.LeafKeyTweak{
		Leaf:              receiverRootNode,
		SigningPrivKey:    receiverLeafPrivKey,
		NewSigningPrivKey: receiverNewLeafPrivKey,
	}
	receiverLeavesToTransfer := [1]wallet.LeafKeyTweak{receiverTransferNode}
	receiverTransfer, receiverRefundSignatureMap, leafDataMap, operatorSigningResults, err := wallet.CounterSwapSignRefund(
		context.Background(),
		receiverConfig,
		receiverLeavesToTransfer[:],
		senderConfig.IdentityPublicKey(),
		time.Now().Add(10*time.Minute),
		adaptorPub,
	)
	require.NoError(t, err)

	// Alice verifies Bob's signatures
	receiverSighash, err := common.SigHashFromTx(leafDataMap[receiverLeavesToTransfer[0].Leaf.Id].RefundTx, 0, leafDataMap[receiverLeavesToTransfer[0].Leaf.Id].Tx.TxOut[leafDataMap[receiverLeavesToTransfer[0].Leaf.Id].Vout])
	require.NoError(t, err)

	receiverKey, err := secp256k1.ParsePubKey(receiverLeavesToTransfer[0].Leaf.VerifyingPublicKey)
	require.NoError(t, err)
	receiverTaprootKey := txscript.ComputeTaprootKeyNoScript(receiverKey)

	_, err = common.ApplyAdaptorToSignature(receiverTaprootKey, receiverSighash, receiverRefundSignatureMap[receiverLeavesToTransfer[0].Leaf.Id], adaptorPrivKey)
	require.NoError(t, err)

	// Alice reveals adaptor secret to Bob, Bob combines with existing adaptor signatures to get valid signatures
	newReceiverRefundSignatureMap := make(map[string][]byte)
	for nodeID, signature := range receiverRefundSignatureMap {
		leafData := leafDataMap[nodeID]
		sighash, _ := common.SigHashFromTx(leafData.RefundTx, 0, leafData.Tx.TxOut[leafData.Vout])
		var verifyingPubkey *secp256k1.PublicKey
		for _, signingResult := range operatorSigningResults {
			if signingResult.LeafId == nodeID {
				verifyingPubkey, err = secp256k1.ParsePubKey(signingResult.VerifyingKey)
				require.NoError(t, err)
			}
		}
		assert.NotNil(t, verifyingPubkey, "expected signing result for leaf %s", nodeID)
		taprootKey := txscript.ComputeTaprootKeyNoScript(verifyingPubkey)
		adaptorSig, err := common.ApplyAdaptorToSignature(taprootKey, sighash, signature, adaptorPrivKey)
		require.NoError(t, err)
		newReceiverRefundSignatureMap[nodeID] = adaptorSig
	}

	// Alice provides key tweak, Bob claims alice's leaves
	senderTransfer, err = wallet.SendTransferTweakKey(
		context.Background(),
		senderConfig,
		senderTransfer,
		senderLeavesToTransfer[:],
		senderRefundSignatureMap,
	)
	require.NoError(t, err, "failed to send transfer tweak key")

	receiverToken, err := wallet.AuthenticateWithServer(context.Background(), receiverConfig)
	require.NoError(t, err, "failed to authenticate receiver")
	receiverCtx := wallet.ContextWithToken(context.Background(), receiverToken)
	pendingTransfer, err := wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)
	receiverPendingTransfer := pendingTransfer.Transfers[0]
	require.Equal(t, senderTransfer.Id, receiverPendingTransfer.Id)

	leafPrivKeyMap, err := wallet.VerifyPendingTransfer(context.Background(), receiverConfig, receiverPendingTransfer)
	assertVerifiedPendingTransfer(t, err, leafPrivKeyMap, senderRootNode, senderNewLeafPrivKey)

	finalLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")
	claimingNode := wallet.LeafKeyTweak{
		Leaf:              receiverPendingTransfer.Leaves[0].Leaf,
		SigningPrivKey:    senderNewLeafPrivKey,
		NewSigningPrivKey: finalLeafPrivKey,
	}
	leavesToClaim := [1]wallet.LeafKeyTweak{claimingNode}
	_, err = wallet.ClaimTransfer(
		receiverCtx,
		receiverPendingTransfer,
		receiverConfig,
		leavesToClaim[:],
	)
	require.NoError(t, err, "failed to ClaimTransfer")

	// Bob provides key tweak, Alice claims bob's leaves
	_, err = wallet.SendTransferTweakKey(
		context.Background(),
		receiverConfig,
		receiverTransfer,
		receiverLeavesToTransfer[:],
		newReceiverRefundSignatureMap,
	)
	require.NoError(t, err, "failed to send transfer tweak key")

	senderToken, err := wallet.AuthenticateWithServer(context.Background(), senderConfig)
	require.NoError(t, err, "failed to authenticate sender")
	senderCtx := wallet.ContextWithToken(context.Background(), senderToken)
	pendingTransfer, err = wallet.QueryPendingTransfers(senderCtx, senderConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)
	senderPendingTransfer := pendingTransfer.Transfers[0]
	require.Equal(t, senderTransfer.Id, receiverPendingTransfer.Id)

	leafPrivKeyMap, err = wallet.VerifyPendingTransfer(context.Background(), senderConfig, senderPendingTransfer)
	assertVerifiedPendingTransfer(t, err, leafPrivKeyMap, receiverRootNode, receiverNewLeafPrivKey)

	finalLeafPrivKey, err = keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")
	claimingNode = wallet.LeafKeyTweak{
		Leaf:              senderPendingTransfer.Leaves[0].Leaf,
		SigningPrivKey:    receiverNewLeafPrivKey,
		NewSigningPrivKey: finalLeafPrivKey,
	}
	leavesToClaim = [1]wallet.LeafKeyTweak{claimingNode}
	_, err = wallet.ClaimTransfer(
		senderCtx,
		senderPendingTransfer,
		senderConfig,
		leavesToClaim[:],
	)
	require.NoError(t, err, "failed to ClaimTransfer")

	transfers, offset, err := wallet.QueryAllTransfers(context.Background(), senderConfig, 1, 0)
	require.NoError(t, err, "failed to QueryAllTransfers")
	require.Len(t, transfers, 1)
	require.EqualValues(t, 1, offset)

	transfers, offset, err = wallet.QueryAllTransfers(context.Background(), senderConfig, 1, offset)
	require.NoError(t, err, "failed to QueryAllTransfers")
	require.Len(t, transfers, 1)
	require.EqualValues(t, 2, offset)

	transfers, _, err = wallet.QueryAllTransfers(context.Background(), senderConfig, 100, 0)
	require.NoError(t, err, "failed to QueryAllTransfers")
	require.Len(t, transfers, 2)

	typeCounts := make(map[spark.TransferType]int)
	for _, transfer := range transfers {
		typeCounts[transfer.Type]++
	}
	assert.Equal(t, 1, typeCounts[spark.TransferType_TRANSFER], "expected 1 transfer")
	assert.Equal(t, 1, typeCounts[spark.TransferType_COUNTER_SWAP], "expected 1 counter swap transfer")

	transfers, _, err = wallet.QueryAllTransfersWithTypes(context.Background(), senderConfig, 2, 0, []spark.TransferType{spark.TransferType_TRANSFER})
	require.NoError(t, err)
	assert.Len(t, transfers, 1)

	transfers, _, err = wallet.QueryAllTransfersWithTypes(context.Background(), senderConfig, 2, 0, []spark.TransferType{spark.TransferType_COUNTER_SWAP})
	require.NoError(t, err)
	assert.Len(t, transfers, 1)

	transfers, _, err = wallet.QueryAllTransfersWithTypes(context.Background(), senderConfig, 3, 0, []spark.TransferType{spark.TransferType_TRANSFER, spark.TransferType_COUNTER_SWAP})
	require.NoError(t, err)
	assert.Len(t, transfers, 2)
}

func TestDoubleClaimTransfer(t *testing.T) {
	// Sender initiates transfer
	senderConfig, err := testutil.TestWalletConfig()
	require.NoError(t, err, "failed to create sender wallet config")

	leafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create node signing private key")
	rootNode, err := testutil.CreateNewTree(senderConfig, faucet, leafPrivKey, 100_000)
	require.NoError(t, err, "failed to create new tree")

	newLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")

	receiverPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create receiver private key")

	transferNode := wallet.LeafKeyTweak{
		Leaf:              rootNode,
		SigningPrivKey:    leafPrivKey,
		NewSigningPrivKey: newLeafPrivKey,
	}
	leavesToTransfer := [1]wallet.LeafKeyTweak{transferNode}
	senderTransfer, err := wallet.SendTransfer(
		context.Background(),
		senderConfig,
		leavesToTransfer[:],
		receiverPrivKey.Public(),
		time.Now().Add(10*time.Minute),
	)
	require.NoError(t, err, "failed to transfer tree node")

	// Receiver queries pending transfer
	receiverConfig, err := testutil.TestWalletConfigWithIdentityKey(*receiverPrivKey.ToBTCEC())
	require.NoError(t, err, "failed to create wallet config")
	receiverToken, err := wallet.AuthenticateWithServer(context.Background(), receiverConfig)
	require.NoError(t, err, "failed to authenticate receiver")
	receiverCtx := wallet.ContextWithToken(context.Background(), receiverToken)
	pendingTransfer, err := wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
	require.NoError(t, err, "failed to query pending transfers")
	require.Len(t, pendingTransfer.Transfers, 1)
	receiverTransfer := pendingTransfer.Transfers[0]
	require.Equal(t, senderTransfer.Id, receiverTransfer.Id)

	leafPrivKeyMap, err := wallet.VerifyPendingTransfer(context.Background(), receiverConfig, receiverTransfer)
	assertVerifiedPendingTransfer(t, err, leafPrivKeyMap, rootNode, newLeafPrivKey)

	finalLeafPrivKey, err := keys.GeneratePrivateKey()
	require.NoError(t, err, "failed to create new node signing private key")
	claimingNode := wallet.LeafKeyTweak{
		Leaf:              receiverTransfer.Leaves[0].Leaf,
		SigningPrivKey:    newLeafPrivKey,
		NewSigningPrivKey: finalLeafPrivKey,
	}
	leavesToClaim := [1]wallet.LeafKeyTweak{claimingNode}

	errCount := 0
	wg := sync.WaitGroup{}
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err = wallet.ClaimTransfer(receiverCtx, receiverTransfer, receiverConfig, leavesToClaim[:])
			if err != nil {
				errCount++
			}
		}()
	}
	wg.Wait()

	if errCount == 5 {
		pendingTransfer, err = wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
		require.NoError(t, err, "failed to query pending transfers")
		require.Len(t, pendingTransfer.Transfers, 1)
		receiverTransfer = pendingTransfer.Transfers[0]
		require.Equal(t, senderTransfer.Id, receiverTransfer.Id)

		res, err := wallet.ClaimTransfer(
			receiverCtx,
			receiverTransfer,
			receiverConfig,
			leavesToClaim[:],
		)
		if err != nil {
			// if the claim failed, the transfer should revert back to sender key tweaked status
			pendingTransfer, err = wallet.QueryPendingTransfers(receiverCtx, receiverConfig)
			require.NoError(t, err, "failed to query pending transfers")
			require.Len(t, pendingTransfer.Transfers, 1)
			receiverTransfer = pendingTransfer.Transfers[0]
			require.Equal(t, senderTransfer.Id, receiverTransfer.Id)

			res, err = wallet.ClaimTransfer(
				receiverCtx,
				receiverTransfer,
				receiverConfig,
				leavesToClaim[:],
			)
			require.NoError(t, err, "failed to ClaimTransfer")
		}

		require.Equal(t, res[0].Id, claimingNode.Leaf.Id)
	}
}
