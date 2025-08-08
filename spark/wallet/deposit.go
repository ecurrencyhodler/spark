package wallet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/lightsparkdev/spark/common/keys"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"github.com/google/uuid"
	"github.com/lightsparkdev/spark"
	"github.com/lightsparkdev/spark/common"
	pbcommon "github.com/lightsparkdev/spark/proto/common"
	pbfrost "github.com/lightsparkdev/spark/proto/frost"
	pb "github.com/lightsparkdev/spark/proto/spark"
	pbssp "github.com/lightsparkdev/spark/proto/spark_ssp_internal"
	"github.com/lightsparkdev/spark/so/objects"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func validateDepositAddress(config *Config, address *pb.Address, userPubkey []byte) error {
	if address.DepositAddressProof.ProofOfPossessionSignature == nil {
		return fmt.Errorf("proof of possession signature is nil")
	}

	operatorPubkey, err := common.SubtractPublicKeys(address.VerifyingKey, userPubkey)
	if err != nil {
		return err
	}
	msg := common.ProofOfPossessionMessageHashForDepositAddress(config.IdentityPublicKey().Serialize(), operatorPubkey, []byte(address.Address))
	sig, err := schnorr.ParseSignature(address.DepositAddressProof.ProofOfPossessionSignature)
	if err != nil {
		return err
	}

	pubKey, err := btcec.ParsePubKey(operatorPubkey)
	if err != nil {
		return err
	}
	taprootKey := txscript.ComputeTaprootKeyNoScript(pubKey)

	verified := sig.Verify(msg[:], taprootKey)
	if !verified {
		return fmt.Errorf("signature verification failed")
	}

	if address.DepositAddressProof.AddressSignatures == nil {
		return fmt.Errorf("address signatures is nil")
	}

	addrHash := sha256.Sum256([]byte(address.Address))
	for _, operator := range config.SigningOperators {
		if operator.Identifier == config.CoodinatorIdentifier {
			continue
		}
		operatorSig, ok := address.DepositAddressProof.AddressSignatures[operator.Identifier]
		if !ok {
			return fmt.Errorf("address signature for operator %s is nil", operator.Identifier)
		}

		sig, err := ecdsa.ParseDERSignature(operatorSig)
		if err != nil {
			return err
		}

		if !sig.Verify(addrHash[:], operator.IdentityPublicKey.ToBTCEC()) {
			return fmt.Errorf("signature verification failed for operator %s", operator.Identifier)
		}
	}
	return nil
}

// GenerateDepositAddress generates a deposit address for a given identity and signing public key.
func GenerateDepositAddress(
	ctx context.Context,
	config *Config,
	signingPubkey []byte,
	// Signing pub key should be generated in a deterministic way from this leaf ID.
	// This will be used as the leaf ID for the leaf node.
	customLeafID *string,
	isStatic bool,
) (*pb.GenerateDepositAddressResponse, error) {
	sparkConn, err := common.NewGRPCConnectionWithTestTLS(config.CoodinatorAddress(), nil)
	if err != nil {
		return nil, err
	}
	defer sparkConn.Close()
	sparkClient := pb.NewSparkServiceClient(sparkConn)
	depositResp, err := sparkClient.GenerateDepositAddress(ctx, &pb.GenerateDepositAddressRequest{
		SigningPublicKey:  signingPubkey,
		IdentityPublicKey: config.IdentityPublicKey().Serialize(),
		Network:           config.ProtoNetwork(),
		LeafId:            customLeafID,
		IsStatic:          &isStatic,
	})
	if err != nil {
		return nil, err
	}
	if err := validateDepositAddress(config, depositResp.DepositAddress, signingPubkey); err != nil {
		return nil, err
	}
	return depositResp, nil
}

// GenerateStaticDepositAddress generates a static deposit address for a given identity and signing public key.
func GenerateStaticDepositAddress(
	ctx context.Context,
	config *Config,
	signingPubkey []byte,
) (*pb.GenerateDepositAddressResponse, error) {
	sparkConn, err := common.NewGRPCConnectionWithTestTLS(config.CoodinatorAddress(), nil)
	if err != nil {
		return nil, err
	}
	defer sparkConn.Close()
	sparkClient := pb.NewSparkServiceClient(sparkConn)
	isStatic := true
	depositResp, err := sparkClient.GenerateDepositAddress(ctx, &pb.GenerateDepositAddressRequest{
		SigningPublicKey:  signingPubkey,
		IdentityPublicKey: config.IdentityPublicKey().Serialize(),
		Network:           config.ProtoNetwork(),
		IsStatic:          &isStatic,
	})
	if err != nil {
		return nil, err
	}
	if err := validateDepositAddress(config, depositResp.DepositAddress, signingPubkey); err != nil {
		return nil, err
	}
	return depositResp, nil
}

func QueryUnusedDepositAddresses(
	ctx context.Context,
	config *Config,
) (*pb.QueryUnusedDepositAddressesResponse, error) {
	sparkConn, err := common.NewGRPCConnectionWithTestTLS(config.CoodinatorAddress(), nil)
	if err != nil {
		return nil, err
	}
	defer sparkConn.Close()
	sparkClient := pb.NewSparkServiceClient(sparkConn)
	network, err := common.ProtoNetworkFromNetwork(config.Network)
	if err != nil {
		return nil, fmt.Errorf("failed to get proto network: %w", err)
	}

	var allAddresses []*pb.DepositAddressQueryResult
	offset := int64(0)
	limit := int64(100) // Use reasonable batch size

	for {
		response, err := sparkClient.QueryUnusedDepositAddresses(ctx, &pb.QueryUnusedDepositAddressesRequest{
			IdentityPublicKey: config.IdentityPublicKey().Serialize(),
			Network:           network,
			Limit:             limit,
			Offset:            offset,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to query unused deposit addresses at offset %d: %w", offset, err)
		}

		// Collect results from this page
		allAddresses = append(allAddresses, response.DepositAddresses...)

		// Check if there are more results
		if response.Offset == -1 {
			break // No more results
		}

		offset = response.Offset
	}

	return &pb.QueryUnusedDepositAddressesResponse{
		DepositAddresses: allAddresses,
		Offset:           offset,
	}, nil
}

func QueryStaticDepositAddresses(
	ctx context.Context,
	config *Config,
) (*pb.QueryStaticDepositAddressesResponse, error) {
	sparkConn, err := common.NewGRPCConnectionWithTestTLS(config.CoodinatorAddress(), nil)
	if err != nil {
		return nil, err
	}
	defer sparkConn.Close()
	sparkClient := pb.NewSparkServiceClient(sparkConn)
	network, err := common.ProtoNetworkFromNetwork(config.Network)
	if err != nil {
		return nil, fmt.Errorf("failed to get proto network: %w", err)
	}
	return sparkClient.QueryStaticDepositAddresses(ctx, &pb.QueryStaticDepositAddressesRequest{
		IdentityPublicKey: config.IdentityPublicKey().Serialize(),
		Network:           network,
	})
}

// DepositTreeCreationData holds the data needed to finalize deposit tree creation
// CreateTreeRoot creates a tree root for a given deposit transaction.
func CreateTreeRoot(
	ctx context.Context,
	config *Config,
	signingPrivKey keys.Private,
	verifyingKey []byte,
	depositTx *wire.MsgTx,
	vout int,
	skipFinalizeSignatures bool,
) (*pb.FinalizeNodeSignaturesResponse, error) {
	signingPubKey := signingPrivKey.Public()
	signingPubkeyBytes := signingPubKey.Serialize()
	// Creat root tx
	depositOutPoint := &wire.OutPoint{Hash: depositTx.TxHash(), Index: uint32(vout)}
	rootTx := createRootTx(depositOutPoint, depositTx.TxOut[0])
	var rootBuf bytes.Buffer
	if err := rootTx.Serialize(&rootBuf); err != nil {
		return nil, err
	}
	rootNonce, err := objects.RandomSigningNonce()
	if err != nil {
		return nil, err
	}
	rootNonceProto, err := rootNonce.MarshalProto()
	if err != nil {
		return nil, err
	}
	rootNonceCommitmentProto, err := rootNonce.SigningCommitment().MarshalProto()
	if err != nil {
		return nil, err
	}
	rootTxSighash, err := common.SigHashFromTx(rootTx, 0, depositTx.TxOut[0])
	if err != nil {
		return nil, err
	}
	var depositBuf bytes.Buffer
	err = depositTx.Serialize(&depositBuf)
	if err != nil {
		return nil, err
	}

	// Create refund tx
	_, cpfpRefundTx, err := createRefundTxs(
		spark.InitialSequence(),
		&wire.OutPoint{Hash: rootTx.TxHash(), Index: 0},
		rootTx.TxOut[0].Value,
		signingPubKey,
		true,
	)
	if err != nil {
		return nil, err
	}
	var refundBuf bytes.Buffer
	err = cpfpRefundTx.Serialize(&refundBuf)
	if err != nil {
		return nil, err
	}
	refundNonce, err := objects.RandomSigningNonce()
	if err != nil {
		return nil, err
	}
	refundNonceProto, err := refundNonce.MarshalProto()
	if err != nil {
		return nil, err
	}
	refundNonceCommitmentProto, err := refundNonce.SigningCommitment().MarshalProto()
	if err != nil {
		return nil, err
	}
	refundTxSighash, err := common.SigHashFromTx(cpfpRefundTx, 0, rootTx.TxOut[0])
	if err != nil {
		return nil, err
	}

	sparkConn, err := common.NewGRPCConnectionWithTestTLS(config.CoodinatorAddress(), nil)
	if err != nil {
		return nil, err
	}
	defer sparkConn.Close()
	sparkClient := pb.NewSparkServiceClient(sparkConn)

	treeResponse, err := sparkClient.StartDepositTreeCreation(ctx, &pb.StartDepositTreeCreationRequest{
		IdentityPublicKey: config.IdentityPublicKey().Serialize(),
		OnChainUtxo: &pb.UTXO{
			Vout:    uint32(vout),
			RawTx:   depositBuf.Bytes(),
			Network: config.ProtoNetwork(),
		},
		RootTxSigningJob: &pb.SigningJob{
			RawTx:                  rootBuf.Bytes(),
			SigningPublicKey:       signingPubkeyBytes,
			SigningNonceCommitment: rootNonceCommitmentProto,
		},
		RefundTxSigningJob: &pb.SigningJob{
			RawTx:                  refundBuf.Bytes(),
			SigningPublicKey:       signingPubkeyBytes,
			SigningNonceCommitment: refundNonceCommitmentProto,
		},
	})
	if err != nil {
		return nil, err
	}

	if skipFinalizeSignatures {
		return nil, nil
	}

	if !bytes.Equal(treeResponse.RootNodeSignatureShares.VerifyingKey, verifyingKey) {
		return nil, fmt.Errorf("verifying key does not match")
	}

	userKeyPackage := CreateUserKeyPackage(signingPrivKey.Serialize())

	nodeJobID := uuid.NewString()
	refundJobID := uuid.NewString()
	userSigningJobs := []*pbfrost.FrostSigningJob{
		{
			JobId:           nodeJobID,
			Message:         rootTxSighash,
			KeyPackage:      userKeyPackage,
			VerifyingKey:    verifyingKey,
			Nonce:           rootNonceProto,
			Commitments:     treeResponse.RootNodeSignatureShares.NodeTxSigningResult.SigningNonceCommitments,
			UserCommitments: rootNonceCommitmentProto,
		},
		{
			JobId:           refundJobID,
			Message:         refundTxSighash,
			KeyPackage:      userKeyPackage,
			VerifyingKey:    treeResponse.RootNodeSignatureShares.VerifyingKey,
			Nonce:           refundNonceProto,
			Commitments:     treeResponse.RootNodeSignatureShares.RefundTxSigningResult.SigningNonceCommitments,
			UserCommitments: refundNonceCommitmentProto,
		}}

	frostConn, err := common.NewGRPCConnectionWithoutTLS(config.FrostSignerAddress, nil)
	if err != nil {
		return nil, err
	}
	defer frostConn.Close()

	frostClient := pbfrost.NewFrostServiceClient(frostConn)

	userSignatures, err := frostClient.SignFrost(context.Background(), &pbfrost.SignFrostRequest{
		SigningJobs: userSigningJobs,
		Role:        pbfrost.SigningRole_USER,
	})
	if err != nil {
		return nil, err
	}

	rootSignature, err := frostClient.AggregateFrost(context.Background(), &pbfrost.AggregateFrostRequest{
		Message:            rootTxSighash,
		SignatureShares:    treeResponse.RootNodeSignatureShares.NodeTxSigningResult.SignatureShares,
		PublicShares:       treeResponse.RootNodeSignatureShares.NodeTxSigningResult.PublicKeys,
		VerifyingKey:       verifyingKey,
		Commitments:        treeResponse.RootNodeSignatureShares.NodeTxSigningResult.SigningNonceCommitments,
		UserCommitments:    rootNonceCommitmentProto,
		UserPublicKey:      signingPubkeyBytes,
		UserSignatureShare: userSignatures.Results[nodeJobID].SignatureShare,
	})
	if err != nil {
		return nil, err
	}

	refundSignature, err := frostClient.AggregateFrost(context.Background(), &pbfrost.AggregateFrostRequest{
		Message:            refundTxSighash,
		SignatureShares:    treeResponse.RootNodeSignatureShares.RefundTxSigningResult.SignatureShares,
		PublicShares:       treeResponse.RootNodeSignatureShares.RefundTxSigningResult.PublicKeys,
		VerifyingKey:       verifyingKey,
		Commitments:        treeResponse.RootNodeSignatureShares.RefundTxSigningResult.SigningNonceCommitments,
		UserCommitments:    refundNonceCommitmentProto,
		UserPublicKey:      signingPubkeyBytes,
		UserSignatureShare: userSignatures.Results[refundJobID].SignatureShare,
	})
	if err != nil {
		return nil, err
	}

	return sparkClient.FinalizeNodeSignatures(context.Background(), &pb.FinalizeNodeSignaturesRequest{
		Intent: pbcommon.SignatureIntent_CREATION,
		NodeSignatures: []*pb.NodeSignatures{
			{
				NodeId:            treeResponse.RootNodeSignatureShares.NodeId,
				NodeTxSignature:   rootSignature.Signature,
				RefundTxSignature: refundSignature.Signature,
			},
		},
	})
}

// ClaimStaticDepositLegacy claims a static deposit.
func ClaimStaticDepositLegacy(
	ctx context.Context,
	config *Config,
	network common.Network,
	leavesToTransfer []LeafKeyTweak,
	spendTx *wire.MsgTx,
	requestType pb.UtxoSwapRequestType,
	depositAddressSecretKey keys.Private,
	userSignature []byte,
	sspSignature []byte,
	userIdentityPubKey keys.Public,
	sspConn *grpc.ClientConn,
	prevTxOut *wire.TxOut,
) (*wire.MsgTx, *pb.Transfer, error) {
	var spendTxBytes bytes.Buffer
	err := spendTx.Serialize(&spendTxBytes)
	if err != nil {
		return nil, nil, err
	}
	spendTxSighash, err := common.SigHashFromTx(
		spendTx,
		0,
		prevTxOut,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get sighash: %w", err)
	}

	hidingPriv, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		return nil, nil, err
	}
	bindingPriv, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		return nil, nil, err
	}
	hidingPubBytes := hidingPriv.PubKey().SerializeCompressed()
	bindingPubBytes := bindingPriv.PubKey().SerializeCompressed()
	spendTxNonceCommitment, err := objects.NewSigningCommitment(bindingPubBytes, hidingPubBytes)
	if err != nil {
		return nil, nil, err
	}
	spendTxNonceCommitmentProto, err := spendTxNonceCommitment.MarshalProto()
	if err != nil {
		return nil, nil, err
	}

	spendTxSigningJob := &pb.SigningJob{
		RawTx:                  spendTxBytes.Bytes(),
		SigningPublicKey:       depositAddressSecretKey.Public().Serialize(),
		SigningNonceCommitment: spendTxNonceCommitmentProto,
	}

	sparkClient := pb.NewSparkServiceClient(sspConn)

	creditAmountSats := uint64(0)
	for _, leaf := range leavesToTransfer {
		creditAmountSats += leaf.Leaf.Value
	}
	transferID, err := uuid.NewV7()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate transfer id: %w", err)
	}
	keyTweakInputMap, err := prepareSendTransferKeyTweaks(config, transferID.String(), userIdentityPubKey, leavesToTransfer, map[string][]byte{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare transfer data: %w", err)
	}
	transferPackage, err := prepareTransferPackage(ctx, config, sparkClient, transferID, keyTweakInputMap, leavesToTransfer, userIdentityPubKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare transfer data: %w", err)
	}

	conn, err := common.NewGRPCConnectionWithoutTLS(config.FrostSignerAddress, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to frost signer: %w", err)
	}
	defer conn.Close()
	protoNetwork, err := common.ProtoNetworkFromNetwork(network)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get proto network: %w", err)
	}
	depositTxID, err := hex.DecodeString(spendTx.TxIn[0].PreviousOutPoint.Hash.String())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode deposit txid: %w", err)
	}
	swapResponse, err := sparkClient.InitiateUtxoSwap(ctx, &pb.InitiateUtxoSwapRequest{
		OnChainUtxo: &pb.UTXO{
			Txid:    depositTxID,
			Vout:    spendTx.TxIn[0].PreviousOutPoint.Index,
			Network: protoNetwork,
		},
		RequestType:   requestType,
		Amount:        &pb.InitiateUtxoSwapRequest_CreditAmountSats{CreditAmountSats: creditAmountSats},
		UserSignature: userSignature,
		SspSignature:  sspSignature,
		Transfer: &pb.StartTransferRequest{
			TransferId:                transferID.String(),
			OwnerIdentityPublicKey:    config.IdentityPublicKey().Serialize(),
			ReceiverIdentityPublicKey: userIdentityPubKey.Serialize(),
			ExpiryTime:                timestamppb.New(time.Now().Add(2 * time.Minute)),
			TransferPackage:           transferPackage,
		},
		SpendTxSigningJob: spendTxSigningJob,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initiate utxo swap: %w", err)
	}
	// Similar to CreateUserKeyPackage(depositAddressSecretKey.Serialize())
	frostUserIdentifier := "0000000000000000000000000000000000000000000000000000000000000063"
	userKeyPackage := pbfrost.KeyPackage{
		Identifier:  frostUserIdentifier,
		SecretShare: depositAddressSecretKey.Serialize(),
		PublicShares: map[string][]byte{
			frostUserIdentifier: depositAddressSecretKey.Public().Serialize(),
		},
		PublicKey:  swapResponse.DepositAddress.VerifyingPublicKey,
		MinSigners: 1,
	}
	userNonce, err := objects.NewSigningNonce(bindingPriv.Serialize(), hidingPriv.Serialize())
	if err != nil {
		return nil, nil, err
	}
	userNonceProto, err := userNonce.MarshalProto()
	if err != nil {
		return nil, nil, err
	}
	userCommitmentProto, err := userNonce.SigningCommitment().MarshalProto()
	if err != nil {
		return nil, nil, err
	}
	operatorCommitments := swapResponse.SpendTxSigningResult.SigningNonceCommitments

	userSigningJobs := make([]*pbfrost.FrostSigningJob, 0)
	userJobID := uuid.NewString()
	userSigningJobs = append(userSigningJobs, &pbfrost.FrostSigningJob{
		JobId:           userJobID,
		Message:         spendTxSighash,
		KeyPackage:      &userKeyPackage,
		VerifyingKey:    swapResponse.DepositAddress.VerifyingPublicKey,
		Nonce:           userNonceProto,
		Commitments:     operatorCommitments,
		UserCommitments: userCommitmentProto,
	})

	frostConn, err := common.NewGRPCConnectionWithoutTLS(config.FrostSignerAddress, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to frost signer: %w", err)
	}
	defer frostConn.Close()

	frostClient := pbfrost.NewFrostServiceClient(frostConn)

	userSignatures, err := frostClient.SignFrost(context.Background(), &pbfrost.SignFrostRequest{
		SigningJobs: userSigningJobs,
		Role:        pbfrost.SigningRole_USER,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to sign frost: %w", err)
	}

	signatureResult, err := frostClient.AggregateFrost(ctx, &pbfrost.AggregateFrostRequest{
		Message:            spendTxSighash,
		SignatureShares:    swapResponse.SpendTxSigningResult.SignatureShares,
		PublicShares:       swapResponse.SpendTxSigningResult.PublicKeys,
		VerifyingKey:       swapResponse.DepositAddress.VerifyingPublicKey,
		Commitments:        operatorCommitments,
		UserCommitments:    userCommitmentProto,
		UserPublicKey:      depositAddressSecretKey.Public().Serialize(),
		UserSignatureShare: userSignatures.Results[userJobID].SignatureShare,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to aggregate frost: %w", err)
	}

	// Verify signature using go lib.
	sig, err := schnorr.ParseSignature(signatureResult.Signature)
	if err != nil {
		return nil, nil, err
	}

	pubKey, err := btcec.ParsePubKey(swapResponse.DepositAddress.VerifyingPublicKey)
	if err != nil {
		return nil, nil, err
	}
	taprootKey := txscript.ComputeTaprootKeyNoScript(pubKey)

	verified := sig.Verify(spendTxSighash[:], taprootKey)
	if !verified {
		return nil, nil, fmt.Errorf("signature verification failed")
	}
	spendTx.TxIn[0].Witness = wire.TxWitness{signatureResult.Signature}
	return spendTx, swapResponse.Transfer, nil
}

func RefundStaticDepositLegacy(
	ctx context.Context,
	config *Config,
	network common.Network,
	spendTx *wire.MsgTx,
	depositAddressSecretKey *secp256k1.PrivateKey,
	userSignature []byte,
	userIdentityPubkey *secp256k1.PublicKey,
	prevTxOut *wire.TxOut,
	aliceConn *grpc.ClientConn,
) (*wire.MsgTx, error) {
	var spendTxBytes bytes.Buffer
	err := spendTx.Serialize(&spendTxBytes)
	if err != nil {
		return nil, err
	}
	spendTxSighash, err := common.SigHashFromTx(
		spendTx,
		0,
		prevTxOut,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get sighash: %w", err)
	}

	hidingPriv, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		return nil, err
	}
	bindingPriv, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		return nil, err
	}
	hidingPubBytes := hidingPriv.PubKey().SerializeCompressed()
	bindingPubBytes := bindingPriv.PubKey().SerializeCompressed()
	spendTxNonceCommitment, err := objects.NewSigningCommitment(bindingPubBytes, hidingPubBytes)
	if err != nil {
		return nil, err
	}
	spendTxNonceCommitmentProto, err := spendTxNonceCommitment.MarshalProto()
	if err != nil {
		return nil, err
	}

	signingJob := &pb.SigningJob{
		RawTx:                  spendTxBytes.Bytes(),
		SigningPublicKey:       depositAddressSecretKey.PubKey().SerializeCompressed(),
		SigningNonceCommitment: spendTxNonceCommitmentProto,
	}

	protoNetwork, err := common.ProtoNetworkFromNetwork(network)
	if err != nil {
		return nil, fmt.Errorf("failed to get proto network: %w", err)
	}
	depositTxID, err := hex.DecodeString(spendTx.TxIn[0].PreviousOutPoint.Hash.String())
	if err != nil {
		return nil, fmt.Errorf("failed to decode deposit txid: %w", err)
	}

	// *********************************************************************************
	// Initiate Utxo Swap
	// *********************************************************************************
	sparkClient := pb.NewSparkServiceClient(aliceConn)
	transferID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("failed to generate transfer id: %w", err)
	}
	swapResponse, err := sparkClient.InitiateUtxoSwap(ctx, &pb.InitiateUtxoSwapRequest{
		OnChainUtxo: &pb.UTXO{
			Txid:    depositTxID,
			Vout:    spendTx.TxIn[0].PreviousOutPoint.Index,
			Network: protoNetwork,
		},
		RequestType:   pb.UtxoSwapRequestType_Refund,
		Amount:        &pb.InitiateUtxoSwapRequest_CreditAmountSats{CreditAmountSats: 0},
		UserSignature: userSignature,
		SspSignature:  []byte{},
		Transfer: &pb.StartTransferRequest{
			TransferId:                transferID.String(),
			OwnerIdentityPublicKey:    config.IdentityPublicKey().Serialize(),
			ReceiverIdentityPublicKey: userIdentityPubkey.SerializeCompressed(),
			ExpiryTime:                nil,
			TransferPackage:           nil,
		},
		SpendTxSigningJob: signingJob,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initiate utxo swap: %w", err)
	}

	// *********************************************************************************
	// Sign the spend tx
	// *********************************************************************************
	frostUserIdentifier := "0000000000000000000000000000000000000000000000000000000000000063"
	userKeyPackage := pbfrost.KeyPackage{
		Identifier:  frostUserIdentifier,
		SecretShare: depositAddressSecretKey.Serialize(),
		PublicShares: map[string][]byte{
			frostUserIdentifier: depositAddressSecretKey.PubKey().SerializeCompressed(),
		},
		PublicKey:  swapResponse.DepositAddress.VerifyingPublicKey,
		MinSigners: 1,
	}
	userNonce, err := objects.NewSigningNonce(bindingPriv.Serialize(), hidingPriv.Serialize())
	if err != nil {
		return nil, err
	}
	userNonceProto, err := userNonce.MarshalProto()
	if err != nil {
		return nil, err
	}
	userCommitmentProto, err := userNonce.SigningCommitment().MarshalProto()
	if err != nil {
		return nil, err
	}
	operatorCommitments := swapResponse.SpendTxSigningResult.SigningNonceCommitments

	userSigningJobs := make([]*pbfrost.FrostSigningJob, 0)
	userJobID := uuid.NewString()
	userSigningJobs = append(userSigningJobs, &pbfrost.FrostSigningJob{
		JobId:           userJobID,
		Message:         spendTxSighash,
		KeyPackage:      &userKeyPackage,
		VerifyingKey:    swapResponse.DepositAddress.VerifyingPublicKey,
		Nonce:           userNonceProto,
		Commitments:     operatorCommitments,
		UserCommitments: userCommitmentProto,
	})

	frostConn, err := common.NewGRPCConnectionWithoutTLS(config.FrostSignerAddress, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to frost signer: %w", err)
	}
	defer frostConn.Close()

	frostClient := pbfrost.NewFrostServiceClient(frostConn)

	userSignatures, err := frostClient.SignFrost(context.Background(), &pbfrost.SignFrostRequest{
		SigningJobs: userSigningJobs,
		Role:        pbfrost.SigningRole_USER,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to sign frost: %w", err)
	}

	signatureResult, err := frostClient.AggregateFrost(ctx, &pbfrost.AggregateFrostRequest{
		Message:            spendTxSighash,
		SignatureShares:    swapResponse.SpendTxSigningResult.SignatureShares,
		PublicShares:       swapResponse.SpendTxSigningResult.PublicKeys,
		VerifyingKey:       swapResponse.DepositAddress.VerifyingPublicKey,
		Commitments:        operatorCommitments,
		UserCommitments:    userCommitmentProto,
		UserPublicKey:      depositAddressSecretKey.PubKey().SerializeCompressed(),
		UserSignatureShare: userSignatures.Results[userJobID].SignatureShare,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate frost: %w", err)
	}

	// Verify signature using go lib.
	sig, err := schnorr.ParseSignature(signatureResult.Signature)
	if err != nil {
		return nil, err
	}

	pubKey, err := btcec.ParsePubKey(swapResponse.DepositAddress.VerifyingPublicKey)
	if err != nil {
		return nil, err
	}
	taprootKey := txscript.ComputeTaprootKeyNoScript(pubKey)

	verified := sig.Verify(spendTxSighash[:], taprootKey)
	if !verified {
		return nil, fmt.Errorf("signature verification failed")
	}
	spendTx.TxIn[0].Witness = wire.TxWitness{signatureResult.Signature}

	return spendTx, nil
}

// ClaimStaticDeposit claims a static deposit.
func ClaimStaticDeposit(
	ctx context.Context,
	config *Config,
	network common.Network,
	leavesToTransfer []LeafKeyTweak,
	spendTx *wire.MsgTx,
	requestType pb.UtxoSwapRequestType,
	depositAddressSecretKey *secp256k1.PrivateKey,
	userSignature []byte,
	sspSignature []byte,
	userIdentityPubkey *secp256k1.PublicKey,
	sspConn *grpc.ClientConn,
	prevTxOut *wire.TxOut,
) (*wire.MsgTx, *pb.Transfer, error) {
	var spendTxBytes bytes.Buffer
	err := spendTx.Serialize(&spendTxBytes)
	if err != nil {
		return nil, nil, err
	}
	spendTxSighash, err := common.SigHashFromTx(
		spendTx,
		0,
		prevTxOut,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get sighash: %w", err)
	}

	hidingPriv, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		return nil, nil, err
	}
	bindingPriv, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		return nil, nil, err
	}
	hidingPubBytes := hidingPriv.PubKey().SerializeCompressed()
	bindingPubBytes := bindingPriv.PubKey().SerializeCompressed()
	spendTxNonceCommitment, err := objects.NewSigningCommitment(bindingPubBytes, hidingPubBytes)
	if err != nil {
		return nil, nil, err
	}
	spendTxNonceCommitmentProto, err := spendTxNonceCommitment.MarshalProto()
	if err != nil {
		return nil, nil, err
	}

	spendTxSigningJob := &pb.SigningJob{
		RawTx:                  spendTxBytes.Bytes(),
		SigningPublicKey:       depositAddressSecretKey.PubKey().SerializeCompressed(),
		SigningNonceCommitment: spendTxNonceCommitmentProto,
	}

	sparkClient := pb.NewSparkServiceClient(sspConn)
	sparkSspInternalClient := pbssp.NewSparkSspInternalServiceClient(sspConn)

	creditAmountSats := uint64(0)
	for _, leaf := range leavesToTransfer {
		creditAmountSats += leaf.Leaf.Value
	}
	transferID, err := uuid.NewV7()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate transfer id: %w", err)
	}
	keyTweakInputMap, err := prepareSendTransferKeyTweaks(config, transferID.String(), keys.PublicKeyFromKey(*userIdentityPubkey), leavesToTransfer, map[string][]byte{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare transfer data: %w", err)
	}
	transferPackage, err := prepareTransferPackage(ctx, config, sparkClient, transferID, keyTweakInputMap, leavesToTransfer, keys.PublicKeyFromKey(*userIdentityPubkey))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare transfer data: %w", err)
	}

	conn, err := common.NewGRPCConnectionWithoutTLS(config.FrostSignerAddress, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to frost signer: %w", err)
	}
	defer conn.Close()
	protoNetwork, err := common.ProtoNetworkFromNetwork(network)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get proto network: %w", err)
	}
	depositTxID, err := hex.DecodeString(spendTx.TxIn[0].PreviousOutPoint.Hash.String())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode deposit txid: %w", err)
	}
	swapResponse, err := sparkSspInternalClient.InitiateStaticDepositUtxoSwap(ctx, &pbssp.InitiateStaticDepositUtxoSwapRequest{
		OnChainUtxo: &pb.UTXO{
			Txid:    depositTxID,
			Vout:    spendTx.TxIn[0].PreviousOutPoint.Index,
			Network: protoNetwork,
		},
		UserSignature: userSignature,
		SspSignature:  sspSignature,
		Transfer: &pb.StartTransferRequest{
			TransferId:                transferID.String(),
			OwnerIdentityPublicKey:    config.IdentityPublicKey().Serialize(),
			ReceiverIdentityPublicKey: userIdentityPubkey.SerializeCompressed(),
			ExpiryTime:                timestamppb.New(time.Now().Add(2 * time.Minute)),
			TransferPackage:           transferPackage,
		},
		SpendTxSigningJob: spendTxSigningJob,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initiate utxo swap: %w", err)
	}
	// Similar to CreateUserKeyPackage(depositAddressSecretKey.Serialize())
	frostUserIdentifier := "0000000000000000000000000000000000000000000000000000000000000063"
	userKeyPackage := pbfrost.KeyPackage{
		Identifier:  frostUserIdentifier,
		SecretShare: depositAddressSecretKey.Serialize(),
		PublicShares: map[string][]byte{
			frostUserIdentifier: depositAddressSecretKey.PubKey().SerializeCompressed(),
		},
		PublicKey:  swapResponse.DepositAddress.VerifyingPublicKey,
		MinSigners: 1,
	}
	userNonce, err := objects.NewSigningNonce(bindingPriv.Serialize(), hidingPriv.Serialize())
	if err != nil {
		return nil, nil, err
	}
	userNonceProto, err := userNonce.MarshalProto()
	if err != nil {
		return nil, nil, err
	}
	userCommitmentProto, err := userNonce.SigningCommitment().MarshalProto()
	if err != nil {
		return nil, nil, err
	}
	operatorCommitments := swapResponse.SpendTxSigningResult.SigningNonceCommitments

	userSigningJobs := make([]*pbfrost.FrostSigningJob, 0)
	userJobID := uuid.NewString()
	userSigningJobs = append(userSigningJobs, &pbfrost.FrostSigningJob{
		JobId:           userJobID,
		Message:         spendTxSighash,
		KeyPackage:      &userKeyPackage,
		VerifyingKey:    swapResponse.DepositAddress.VerifyingPublicKey,
		Nonce:           userNonceProto,
		Commitments:     operatorCommitments,
		UserCommitments: userCommitmentProto,
	})

	frostConn, err := common.NewGRPCConnectionWithoutTLS(config.FrostSignerAddress, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to frost signer: %w", err)
	}
	defer frostConn.Close()

	frostClient := pbfrost.NewFrostServiceClient(frostConn)

	userSignatures, err := frostClient.SignFrost(context.Background(), &pbfrost.SignFrostRequest{
		SigningJobs: userSigningJobs,
		Role:        pbfrost.SigningRole_USER,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to sign frost: %w", err)
	}

	signatureResult, err := frostClient.AggregateFrost(ctx, &pbfrost.AggregateFrostRequest{
		Message:            spendTxSighash,
		SignatureShares:    swapResponse.SpendTxSigningResult.SignatureShares,
		PublicShares:       swapResponse.SpendTxSigningResult.PublicKeys,
		VerifyingKey:       swapResponse.DepositAddress.VerifyingPublicKey,
		Commitments:        operatorCommitments,
		UserCommitments:    userCommitmentProto,
		UserPublicKey:      depositAddressSecretKey.PubKey().SerializeCompressed(),
		UserSignatureShare: userSignatures.Results[userJobID].SignatureShare,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to aggregate frost: %w", err)
	}

	// Verify signature using go lib.
	sig, err := schnorr.ParseSignature(signatureResult.Signature)
	if err != nil {
		return nil, nil, err
	}

	pubKey, err := btcec.ParsePubKey(swapResponse.DepositAddress.VerifyingPublicKey)
	if err != nil {
		return nil, nil, err
	}
	taprootKey := txscript.ComputeTaprootKeyNoScript(pubKey)

	verified := sig.Verify(spendTxSighash[:], taprootKey)
	if !verified {
		return nil, nil, fmt.Errorf("signature verification failed")
	}
	spendTx.TxIn[0].Witness = wire.TxWitness{signatureResult.Signature}
	return spendTx, swapResponse.Transfer, nil
}

func RefundStaticDeposit(
	ctx context.Context,
	config *Config,
	network common.Network,
	spendTx *wire.MsgTx,
	depositAddressSecretKey *secp256k1.PrivateKey,
	userSignature []byte,
	userIdentityPubkey *secp256k1.PublicKey,
	prevTxOut *wire.TxOut,
	aliceConn *grpc.ClientConn,
) (*wire.MsgTx, error) {
	var spendTxBytes bytes.Buffer
	err := spendTx.Serialize(&spendTxBytes)
	if err != nil {
		return nil, err
	}
	spendTxSighash, err := common.SigHashFromTx(
		spendTx,
		0,
		prevTxOut,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get sighash: %w", err)
	}

	hidingPriv, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		return nil, err
	}
	bindingPriv, err := secp256k1.GeneratePrivateKey()
	if err != nil {
		return nil, err
	}
	hidingPubBytes := hidingPriv.PubKey().SerializeCompressed()
	bindingPubBytes := bindingPriv.PubKey().SerializeCompressed()
	spendTxNonceCommitment, err := objects.NewSigningCommitment(bindingPubBytes, hidingPubBytes)
	if err != nil {
		return nil, err
	}
	spendTxNonceCommitmentProto, err := spendTxNonceCommitment.MarshalProto()
	if err != nil {
		return nil, err
	}

	signingJob := &pb.SigningJob{
		RawTx:                  spendTxBytes.Bytes(),
		SigningPublicKey:       depositAddressSecretKey.PubKey().SerializeCompressed(),
		SigningNonceCommitment: spendTxNonceCommitmentProto,
	}

	protoNetwork, err := common.ProtoNetworkFromNetwork(network)
	if err != nil {
		return nil, fmt.Errorf("failed to get proto network: %w", err)
	}
	depositTxID, err := hex.DecodeString(spendTx.TxIn[0].PreviousOutPoint.Hash.String())
	if err != nil {
		return nil, fmt.Errorf("failed to decode deposit txid: %w", err)
	}

	// *********************************************************************************
	// Initiate Utxo Swap
	// *********************************************************************************
	sparkClient := pb.NewSparkServiceClient(aliceConn)
	swapResponse, err := sparkClient.InitiateStaticDepositUtxoRefund(ctx, &pb.InitiateStaticDepositUtxoRefundRequest{
		OnChainUtxo: &pb.UTXO{
			Txid:    depositTxID,
			Vout:    spendTx.TxIn[0].PreviousOutPoint.Index,
			Network: protoNetwork,
		},
		RefundTxSigningJob: signingJob,
		UserSignature:      userSignature,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initiate utxo swap: %w", err)
	}

	// *********************************************************************************
	// Sign the spend tx
	// *********************************************************************************
	frostUserIdentifier := "0000000000000000000000000000000000000000000000000000000000000063"
	userKeyPackage := pbfrost.KeyPackage{
		Identifier:  frostUserIdentifier,
		SecretShare: depositAddressSecretKey.Serialize(),
		PublicShares: map[string][]byte{
			frostUserIdentifier: depositAddressSecretKey.PubKey().SerializeCompressed(),
		},
		PublicKey:  swapResponse.DepositAddress.VerifyingPublicKey,
		MinSigners: 1,
	}
	userNonce, err := objects.NewSigningNonce(bindingPriv.Serialize(), hidingPriv.Serialize())
	if err != nil {
		return nil, err
	}
	userNonceProto, err := userNonce.MarshalProto()
	if err != nil {
		return nil, err
	}
	userCommitmentProto, err := userNonce.SigningCommitment().MarshalProto()
	if err != nil {
		return nil, err
	}
	operatorCommitments := swapResponse.RefundTxSigningResult.SigningNonceCommitments

	userSigningJobs := make([]*pbfrost.FrostSigningJob, 0)
	userJobID := uuid.NewString()
	userSigningJobs = append(userSigningJobs, &pbfrost.FrostSigningJob{
		JobId:           userJobID,
		Message:         spendTxSighash,
		KeyPackage:      &userKeyPackage,
		VerifyingKey:    swapResponse.DepositAddress.VerifyingPublicKey,
		Nonce:           userNonceProto,
		Commitments:     operatorCommitments,
		UserCommitments: userCommitmentProto,
	})

	frostConn, err := common.NewGRPCConnectionWithoutTLS(config.FrostSignerAddress, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to frost signer: %w", err)
	}
	defer frostConn.Close()

	frostClient := pbfrost.NewFrostServiceClient(frostConn)

	userSignatures, err := frostClient.SignFrost(context.Background(), &pbfrost.SignFrostRequest{
		SigningJobs: userSigningJobs,
		Role:        pbfrost.SigningRole_USER,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to sign frost: %w", err)
	}

	signatureResult, err := frostClient.AggregateFrost(ctx, &pbfrost.AggregateFrostRequest{
		Message:            spendTxSighash,
		SignatureShares:    swapResponse.RefundTxSigningResult.SignatureShares,
		PublicShares:       swapResponse.RefundTxSigningResult.PublicKeys,
		VerifyingKey:       swapResponse.DepositAddress.VerifyingPublicKey,
		Commitments:        operatorCommitments,
		UserCommitments:    userCommitmentProto,
		UserPublicKey:      depositAddressSecretKey.PubKey().SerializeCompressed(),
		UserSignatureShare: userSignatures.Results[userJobID].SignatureShare,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate frost: %w", err)
	}

	// Verify signature using go lib.
	sig, err := schnorr.ParseSignature(signatureResult.Signature)
	if err != nil {
		return nil, err
	}

	pubKey, err := btcec.ParsePubKey(swapResponse.DepositAddress.VerifyingPublicKey)
	if err != nil {
		return nil, err
	}
	taprootKey := txscript.ComputeTaprootKeyNoScript(pubKey)

	verified := sig.Verify(spendTxSighash[:], taprootKey)
	if !verified {
		return nil, fmt.Errorf("signature verification failed")
	}
	spendTx.TxIn[0].Witness = wire.TxWitness{signatureResult.Signature}

	return spendTx, nil
}
