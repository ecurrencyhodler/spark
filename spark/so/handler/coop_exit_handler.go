package handler

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/lightsparkdev/spark/common/logging"
	pb "github.com/lightsparkdev/spark/proto/spark"
	pbinternal "github.com/lightsparkdev/spark/proto/spark_internal"
	"github.com/lightsparkdev/spark/so"
	"github.com/lightsparkdev/spark/so/authz"
	"github.com/lightsparkdev/spark/so/ent"
	st "github.com/lightsparkdev/spark/so/ent/schema/schematype"
	"github.com/lightsparkdev/spark/so/helper"
)

// CooperativeExitHandler tracks transfers
// and on-chain txs events for cooperative exits.
type CooperativeExitHandler struct {
	config *so.Config
}

// NewCooperativeExitHandler creates a new CooperativeExitHandler.
func NewCooperativeExitHandler(config *so.Config) *CooperativeExitHandler {
	return &CooperativeExitHandler{
		config: config,
	}
}

// CooperativeExit signs refund transactions for leaves, spending connector outputs.
// It will lock the transferred leaves based on seeing a txid confirming on-chain.
func (h *CooperativeExitHandler) CooperativeExit(ctx context.Context, req *pb.CooperativeExitRequest) (*pb.CooperativeExitResponse, error) {
	return h.cooperativeExit(ctx, req, false)
}

// CooperativeExitV2 is the same as above, but it enforces the use of direct
// transactions for unilateral exits.
func (h *CooperativeExitHandler) CooperativeExitV2(ctx context.Context, req *pb.CooperativeExitRequest) (*pb.CooperativeExitResponse, error) {
	return h.cooperativeExit(ctx, req, true)
}

func (h *CooperativeExitHandler) cooperativeExit(ctx context.Context, req *pb.CooperativeExitRequest, requireDirectTx bool) (*pb.CooperativeExitResponse, error) {
	if err := authz.EnforceSessionIdentityPublicKeyMatches(ctx, h.config, req.Transfer.OwnerIdentityPublicKey); err != nil {
		return nil, err
	}

	transferHandler := NewTransferHandler(h.config)
	cpfpLeafRefundMap := make(map[string][]byte)
	directLeafRefundMap := make(map[string][]byte)
	directFromCpfpLeafRefundMap := make(map[string][]byte)
	for _, job := range req.Transfer.LeavesToSend {
		cpfpLeafRefundMap[job.LeafId] = job.RefundTxSigningJob.RawTx
		if job.DirectRefundTxSigningJob != nil && job.DirectFromCpfpRefundTxSigningJob != nil {
			directLeafRefundMap[job.LeafId] = job.DirectRefundTxSigningJob.RawTx
			directFromCpfpLeafRefundMap[job.LeafId] = job.DirectFromCpfpRefundTxSigningJob.RawTx
		} else if requireDirectTx {
			return nil, fmt.Errorf("DirectRefundTxSigningJob and DirectFromCpfpRefundTxSigningJob are required. Please upgrade to the latest SDK version")
		}
	}

	transfer, leafMap, err := transferHandler.createTransfer(
		ctx,
		req.Transfer.TransferId,
		st.TransferTypeCooperativeExit,
		req.Transfer.ExpiryTime.AsTime(),
		req.Transfer.OwnerIdentityPublicKey,
		req.Transfer.ReceiverIdentityPublicKey,
		cpfpLeafRefundMap,
		directLeafRefundMap,
		directFromCpfpLeafRefundMap,
		nil,
		TransferRoleCoordinator,
		requireDirectTx,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create transfer for request %s: %w", logging.FormatProto("cooperative_exit_request", req), err)
	}

	exitUUID, err := uuid.Parse(req.ExitId)
	if err != nil {
		return nil, fmt.Errorf("unable to parse exit_id %s in request %s: %w", req.ExitId, logging.FormatProto("cooperative_exit_request", req), err)
	}

	if len(req.ExitTxid) != 32 {
		return nil, fmt.Errorf("exit_txid is not 32 bytes in request %s: %x", logging.FormatProto("cooperative_exit_request", req), req.ExitTxid)
	}

	db, err := ent.GetDbFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create current tx for request %s: %w", logging.FormatProto("cooperative_exit_request", req), err)
	}

	_, err = db.CooperativeExit.Create().
		SetID(exitUUID).
		SetTransfer(transfer).
		SetExitTxid(req.ExitTxid).
		// ConfirmationHeight is nil since the transaction is not confirmed yet.
		Save(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create cooperative exit for request %s: %w", logging.FormatProto("cooperative_exit_request", req), err)
	}

	transferProto, err := transfer.MarshalProto(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal transfer for request %s: %w", logging.FormatProto("cooperative_exit_request", req), err)
	}

	signingResults, err := signRefunds(ctx, h.config, req.Transfer, leafMap, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to sign refund transactions for request %s: %w", logging.FormatProto("cooperative_exit_request", req), err)
	}

	err = transferHandler.syncCoopExitInit(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to sync transfer init for request %s: %w", logging.FormatProto("cooperative_exit_request", req), err)
	}

	response := &pb.CooperativeExitResponse{
		Transfer:       transferProto,
		SigningResults: signingResults,
	}
	return response, nil
}

func (h *TransferHandler) syncCoopExitInit(ctx context.Context, req *pb.CooperativeExitRequest) error {
	transfer := req.Transfer
	leaves := make([]*pbinternal.InitiateTransferLeaf, 0)
	for _, leaf := range transfer.LeavesToSend {
		directRefundTx := []byte{}
		directFromCpfpRefundTx := []byte{}
		if leaf.DirectRefundTxSigningJob != nil {
			directRefundTx = leaf.DirectRefundTxSigningJob.RawTx
		}
		if leaf.DirectFromCpfpRefundTxSigningJob != nil {
			directFromCpfpRefundTx = leaf.DirectFromCpfpRefundTxSigningJob.RawTx
		}
		leaves = append(leaves, &pbinternal.InitiateTransferLeaf{
			LeafId:                 leaf.LeafId,
			RawRefundTx:            leaf.RefundTxSigningJob.RawTx,
			DirectRefundTx:         directRefundTx,
			DirectFromCpfpRefundTx: directFromCpfpRefundTx,
		})
	}
	initTransferRequest := &pbinternal.InitiateTransferRequest{
		TransferId:                transfer.TransferId,
		SenderIdentityPublicKey:   transfer.OwnerIdentityPublicKey,
		ReceiverIdentityPublicKey: transfer.ReceiverIdentityPublicKey,
		ExpiryTime:                transfer.ExpiryTime,
		Leaves:                    leaves,
	}
	coopExitRequest := &pbinternal.InitiateCooperativeExitRequest{
		Transfer: initTransferRequest,
		ExitId:   req.ExitId,
		ExitTxid: req.ExitTxid,
	}
	selection := helper.OperatorSelection{
		Option: helper.OperatorSelectionOptionExcludeSelf,
	}
	_, err := helper.ExecuteTaskWithAllOperators(ctx, h.config, &selection, func(ctx context.Context, operator *so.SigningOperator) (any, error) {
		logger := logging.GetLoggerFromContext(ctx)

		conn, err := operator.NewGRPCConnection()
		if err != nil {
			logger.Error("Failed to connect to operator", "error", err)
			return nil, err
		}
		defer conn.Close()

		client := pbinternal.NewSparkInternalServiceClient(conn)
		return client.InitiateCooperativeExit(ctx, coopExitRequest)
	})
	return err
}
