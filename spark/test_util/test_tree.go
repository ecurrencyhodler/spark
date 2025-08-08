package testutil

import (
	"context"
	"fmt"
	"time"

	"github.com/lightsparkdev/spark/common/keys"

	"github.com/google/uuid"
	"github.com/lightsparkdev/spark/common"
	pb "github.com/lightsparkdev/spark/proto/spark"
	st "github.com/lightsparkdev/spark/so/ent/schema/schematype"
	"github.com/lightsparkdev/spark/wallet"
)

const (
	DepositTimeout      = 30 * time.Second
	DepositPollInterval = 100 * time.Millisecond
)

func WaitForPendingDepositNode(ctx context.Context, sparkClient pb.SparkServiceClient, node *pb.TreeNode) (*pb.TreeNode, error) {
	startTime := time.Now()
	for node.Status != string(st.TreeNodeStatusAvailable) {
		if time.Since(startTime) >= DepositTimeout {
			return nil, fmt.Errorf("timed out waiting for node to be available")
		}
		time.Sleep(DepositPollInterval)
		nodesResp, err := sparkClient.QueryNodes(ctx, &pb.QueryNodesRequest{
			Source: &pb.QueryNodesRequest_NodeIds{NodeIds: &pb.TreeNodeIds{NodeIds: []string{node.Id}}},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to query nodes: %w", err)
		}
		if len(nodesResp.Nodes) != 1 {
			return nil, fmt.Errorf("expected 1 node, got %d", len(nodesResp.Nodes))
		}
		node = nodesResp.Nodes[node.Id]
	}
	return node, nil
}

// CreateNewTree creates a new Tree
func CreateNewTree(config *wallet.Config, faucet *Faucet, privKey keys.Private, amountSats int64) (*pb.TreeNode, error) {
	coin, err := faucet.Fund()
	if err != nil {
		return nil, fmt.Errorf("failed to fund faucet: %w", err)
	}

	conn, err := common.NewGRPCConnectionWithTestTLS(config.CoodinatorAddress(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to operator: %w", err)
	}
	defer conn.Close()

	token, err := wallet.AuthenticateWithConnection(context.Background(), config, conn)
	if err != nil {
		return nil, fmt.Errorf("failed to authenticate: %w", err)
	}
	ctx := wallet.ContextWithToken(context.Background(), token)

	leafID := uuid.New().String()
	depositResp, err := wallet.GenerateDepositAddress(ctx, config, privKey.Public(), &leafID, false)
	if err != nil {
		return nil, fmt.Errorf("failed to generate deposit address: %w", err)
	}

	depositTx, err := CreateTestDepositTransaction(coin.OutPoint, depositResp.DepositAddress.Address, amountSats)
	if err != nil {
		return nil, fmt.Errorf("failed to create deposit tx: %w", err)
	}
	vout := 0

	resp, err := wallet.CreateTreeRoot(ctx, config, privKey, depositResp.DepositAddress.VerifyingKey, depositTx, vout, false)
	if err != nil {
		return nil, fmt.Errorf("failed to create tree: %w", err)
	}
	if len(resp.Nodes) == 0 {
		return nil, fmt.Errorf("no nodes found after creating tree")
	}

	// Sign, broadcast, mine deposit tx
	signedExitTx, err := SignFaucetCoin(depositTx, coin.TxOut, coin.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to sign deposit tx: %w", err)
	}

	client := GetBitcoinClient()
	_, err = client.SendRawTransaction(signedExitTx, true)
	if err != nil {
		return nil, fmt.Errorf("failed to broadcast deposit tx: %w", err)
	}
	randomKey, err := keys.GeneratePrivateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate random key: %w", err)
	}
	randomAddress, err := common.P2TRRawAddressFromPublicKey(randomKey.Public(), common.Regtest)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random address: %w", err)
	}
	_, err = client.GenerateToAddress(1, randomAddress, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to mine deposit tx: %w", err)
	}

	// Wait until the deposited leaf is available
	sparkClient := pb.NewSparkServiceClient(conn)
	return WaitForPendingDepositNode(ctx, sparkClient, resp.Nodes[0])
}

// CreateNewTreeWithLevels creates a new Tree
func CreateNewTreeWithLevels(config *wallet.Config, faucet *Faucet, privKey keys.Private, amountSats int64, levels uint32) (*wallet.DepositAddressTree, []*pb.TreeNode, error) {
	coin, err := faucet.Fund()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fund faucet: %w", err)
	}

	conn, err := common.NewGRPCConnectionWithTestTLS(config.CoodinatorAddress(), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to operator: %w", err)
	}
	defer conn.Close()

	token, err := wallet.AuthenticateWithConnection(context.Background(), config, conn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to authenticate: %w", err)
	}
	ctx := wallet.ContextWithToken(context.Background(), token)

	leafID := uuid.New().String()
	depositResp, err := wallet.GenerateDepositAddress(ctx, config, privKey.Public(), &leafID, false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate deposit address: %w", err)
	}

	depositTx, err := CreateTestDepositTransaction(coin.OutPoint, depositResp.DepositAddress.Address, amountSats)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create deposit tx: %w", err)
	}
	vout := 0

	tree, err := wallet.GenerateDepositAddressesForTree(ctx, config, depositTx, nil, uint32(vout), privKey.Serialize(), levels)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create tree: %w", err)
	}

	treeNodes, err := wallet.CreateTree(ctx, config, depositTx, nil, uint32(vout), tree, true)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create tree: %w", err)
	}

	// Sign, broadcast, mine deposit tx
	signedExitTx, err := SignFaucetCoin(depositTx, coin.TxOut, coin.Key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to sign deposit tx: %w", err)
	}

	client := GetBitcoinClient()
	_, err = client.SendRawTransaction(signedExitTx, true)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to broadcast deposit tx: %w", err)
	}
	randomKey, err := keys.GeneratePrivateKey()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate random key: %w", err)
	}
	randomPubKey := randomKey.Public()
	randomAddress, err := common.P2TRRawAddressFromPublicKey(randomPubKey, common.Regtest)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate random address: %w", err)
	}
	_, err = client.GenerateToAddress(1, randomAddress, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to mine deposit tx: %w", err)
	}

	leafNode := treeNodes.Nodes[len(treeNodes.Nodes)-1]
	_, err = WaitForPendingDepositNode(ctx, pb.NewSparkServiceClient(conn), leafNode)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to wait for pending deposit node: %w", err)
	}

	return tree, treeNodes.Nodes, nil
}
