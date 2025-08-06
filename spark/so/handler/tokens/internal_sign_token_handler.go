package tokens

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strconv"
	"strings"

	"github.com/lightsparkdev/spark/common/keys"

	"entgo.io/ent/dialect/sql"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"github.com/google/uuid"
	"github.com/lightsparkdev/spark/common"
	"github.com/lightsparkdev/spark/common/logging"
	secretsharing "github.com/lightsparkdev/spark/common/secret_sharing"
	pb "github.com/lightsparkdev/spark/proto/spark"
	pbtkinternal "github.com/lightsparkdev/spark/proto/spark_token_internal"
	"github.com/lightsparkdev/spark/so"
	"github.com/lightsparkdev/spark/so/ent"
	"github.com/lightsparkdev/spark/so/ent/predicate"
	st "github.com/lightsparkdev/spark/so/ent/schema/schematype"
	"github.com/lightsparkdev/spark/so/ent/signingkeyshare"
	"github.com/lightsparkdev/spark/so/ent/tokenoutput"
	"github.com/lightsparkdev/spark/so/ent/tokenpartialrevocationsecretshare"
	"github.com/lightsparkdev/spark/so/ent/tokentransaction"
	"github.com/lightsparkdev/spark/so/ent/tokentransactionpeersignature"
	"github.com/lightsparkdev/spark/so/tokens"
	"github.com/lightsparkdev/spark/so/utils"
)

type InternalSignTokenHandler struct {
	config *so.Config
}

// NewInternalSignTokenHandler creates a new InternalSignTokenHandler.
func NewInternalSignTokenHandler(config *so.Config) *InternalSignTokenHandler {
	return &InternalSignTokenHandler{
		config: config,
	}
}

// getRequiredParticipatingOperatorsCount returns the number of operators required to
// sign/reveal to consider a transaction valid. By default, signatures from all operators are
// required. If the Token.RequireThresholdOperators flag is enabled, we fall back
// to the configured threshold value instead.
func (h *InternalSignTokenHandler) getRequiredParticipatingOperatorsCount() int {
	if h.config.Token.RequireThresholdOperators {
		return int(h.config.Threshold)
	}
	return len(h.config.SigningOperatorMap)
}

// SignAndPersistTokenTransaction performs the core logic for signing a token transaction from coordination.
// It validates the transaction, input signatures, signs the hash, updates the DB, and returns the signature bytes.
func (h *InternalSignTokenHandler) SignAndPersistTokenTransaction(
	ctx context.Context,
	tokenTransaction *ent.TokenTransaction,
	finalTokenTransactionHash []byte,
	operatorSpecificSignatures []*pb.OperatorSpecificOwnerSignature,
) ([]byte, error) {
	ctx, span := tracer.Start(ctx, "InternalSignTokenHandler.SignAndPersistTokenTransaction", getTokenTransactionAttributesFromEnt(tokenTransaction, h.config))
	defer span.End()

	logger := logging.GetLoggerFromContext(ctx)

	if err := validateTokenTransactionForSigning(h.config, tokenTransaction); err != nil {
		return nil, tokens.FormatErrorWithTransactionEnt(err.Error(), tokenTransaction, err)
	}

	if err := validateOperatorSpecificSignatures(h.config.IdentityPublicKey(), operatorSpecificSignatures, tokenTransaction); err != nil {
		return nil, err
	}

	if tokenTransaction.Status == st.TokenTransactionStatusSigned {
		signature, err := h.regenerateOperatorSignatureForDuplicateRequest(ctx, h.config, tokenTransaction, finalTokenTransactionHash)
		if err != nil {
			return nil, err
		}
		return signature, nil
	}

	invalidOutputs := validateOutputs(tokenTransaction.Edges.CreatedOutput, st.TokenOutputStatusCreatedStarted)
	if len(invalidOutputs) > 0 {
		return nil, tokens.FormatErrorWithTransactionEnt(fmt.Sprintf("%s: %s", tokens.ErrInvalidOutputs, strings.Join(invalidOutputs, "; ")), tokenTransaction, nil)
	}
	txType, err := tokenTransaction.InferTokenTransactionTypeEnt()
	if err != nil {
		return nil, fmt.Errorf("failed to check token transaction type: %w", err)
	}
	switch txType {
	case utils.TokenTransactionTypeCreate:
		if tokenTransaction.Edges.Create == nil {
			return nil, tokens.FormatErrorWithTransactionEnt("create input ent not found when attempting to sign create transaction", tokenTransaction, nil)
		}
	case utils.TokenTransactionTypeMint:
		// For mint transactions, validate that the mint does not exceed the max supply.
		// This is also checked during the Start() step, but we check before signing as well
		// in case two transactions are started at once.
		err = tokens.ValidateMintDoesNotExceedMaxSupplyEnt(ctx, tokenTransaction)
		if err != nil {
			return nil, err
		}
	case utils.TokenTransactionTypeTransfer:
		// If token outputs are being spent, verify the expected status of inputs and check for active freezes.
		if len(tokenTransaction.Edges.SpentOutput) == 0 {
			return nil, tokens.FormatErrorWithTransactionEnt("no spent outputs found when attempting to validate transfer transaction", tokenTransaction, nil)
		}
		invalidInputs := validateInputs(tokenTransaction.Edges.SpentOutput, st.TokenOutputStatusSpentStarted)
		if len(invalidInputs) > 0 {
			return nil, tokens.FormatErrorWithTransactionEnt(fmt.Sprintf("%s: %s", tokens.ErrInvalidInputs, strings.Join(invalidInputs, "; ")), tokenTransaction, nil)
		}
		// Collect owner public keys for freeze check.
		ownerPublicKeys := make([][]byte, len(tokenTransaction.Edges.SpentOutput))
		tokenCreateId := tokenTransaction.Edges.SpentOutput[0].TokenCreateID
		if tokenCreateId == uuid.Nil {
			return nil, tokens.FormatErrorWithTransactionEnt("no created token found when attempting to validate transfer transaction", tokenTransaction, nil)
		}
		for i, output := range tokenTransaction.Edges.SpentOutput {
			ownerPublicKeys[i] = output.OwnerPublicKey
		}

		// Bulk query all input ids to ensure none of them are frozen.
		activeFreezes, err := ent.GetActiveFreezes(ctx, ownerPublicKeys, tokenCreateId)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", tokens.ErrFailedToQueryTokenFreezeStatus, err)
		}

		if len(activeFreezes) > 0 {
			for _, freeze := range activeFreezes {
				logger.Info("Found active freeze", "owner", freeze.OwnerPublicKey, "token", freeze.TokenPublicKey, "freeze_timestamp", freeze.WalletProvidedFreezeTimestamp)
			}
			return nil, fmt.Errorf("at least one input is frozen. Cannot proceed with transaction")
		}
	case utils.TokenTransactionTypeUnknown:
		return nil, fmt.Errorf("token transaction type unknown")
	}

	operatorSignature := ecdsa.Sign(h.config.IdentityPrivateKey.ToBTCEC(), finalTokenTransactionHash)

	// Order the signatures according to their index before updating the DB.
	operatorSpecificSignatureMap := make(map[int][]byte, len(operatorSpecificSignatures))
	for _, sig := range operatorSpecificSignatures {
		inputIndex := int(sig.OwnerSignature.InputIndex)
		operatorSpecificSignatureMap[inputIndex] = sig.OwnerSignature.Signature
	}
	operatorSpecificSignaturesArr := make([][]byte, len(operatorSpecificSignatureMap))
	for i := 0; i < len(operatorSpecificSignatureMap); i++ {
		operatorSpecificSignaturesArr[i] = operatorSpecificSignatureMap[i]
	}
	err = ent.UpdateSignedTransaction(ctx, tokenTransaction, operatorSpecificSignaturesArr, operatorSignature.Serialize())
	if err != nil {
		return nil, tokens.FormatErrorWithTransactionEnt("failed to update outputs after signing", tokenTransaction, err)
	}

	return operatorSignature.Serialize(), nil
}

// regenerateOperatorSignatureForDuplicateRequest handles the case where a transaction has already been signed.
// This allows for simpler wallet SDK logic such that if a Sign() call to one of the SOs failed,
// the wallet SDK can retry with all SOs and get successful responses.
func (h *InternalSignTokenHandler) regenerateOperatorSignatureForDuplicateRequest(
	ctx context.Context,
	config *so.Config,
	tokenTransaction *ent.TokenTransaction,
	finalTokenTransactionHash []byte,
) ([]byte, error) {
	tokens.LogWithTransactionEnt(ctx, "Regenerating response for a duplicate SignTokenTransaction() Call", tokenTransaction, slog.LevelDebug)

	var invalidOutputs []string
	isMint := tokenTransaction.Edges.Mint != nil
	expectedCreatedOutputStatus := st.TokenOutputStatusCreatedSigned
	if isMint {
		expectedCreatedOutputStatus = st.TokenOutputStatusCreatedFinalized
	}

	invalidOutputs = validateOutputs(tokenTransaction.Edges.CreatedOutput, expectedCreatedOutputStatus)
	if len(tokenTransaction.Edges.SpentOutput) > 0 {
		invalidOutputs = append(invalidOutputs, validateInputs(tokenTransaction.Edges.SpentOutput, st.TokenOutputStatusSpentSigned)...)
	}
	if len(invalidOutputs) > 0 {
		return nil, tokens.FormatErrorWithTransactionEnt(
			fmt.Sprintf("%s: %s",
				tokens.ErrInvalidOutputs,
				strings.Join(invalidOutputs, "; ")),
			tokenTransaction, nil)
	}

	if err := utils.ValidateOwnershipSignature(
		tokenTransaction.OperatorSignature,
		finalTokenTransactionHash,
		config.IdentityPublicKey().Serialize(),
	); err != nil {
		return nil, tokens.FormatErrorWithTransactionEnt(tokens.ErrStoredOperatorSignatureInvalid, tokenTransaction, err)
	}

	tokens.LogWithTransactionEnt(ctx, "Returning stored signature in response to repeat Sign() call", tokenTransaction, slog.LevelDebug)
	return tokenTransaction.OperatorSignature, nil
}

// === Revocation Secret Exchange ===
type ShareKey struct {
	TokenOutputID             uuid.UUID
	OperatorIdentityPublicKey keys.Public
}
type ShareValue struct {
	SecretShare               []byte
	OperatorIdentityPublicKey keys.Public
}

type operatorSharesMap map[keys.Public][]*pbtkinternal.RevocationSecretShare

func (h *InternalSignTokenHandler) ExchangeRevocationSecretsShares(ctx context.Context, req *pbtkinternal.ExchangeRevocationSecretsSharesRequest) (*pbtkinternal.ExchangeRevocationSecretsSharesResponse, error) {
	if len(req.OperatorShares) == 0 {
		return nil, fmt.Errorf("no operator shares provided in request")
	}
	logger := logging.GetLoggerFromContext(ctx)
	reqPubKey, err := keys.ParsePublicKey(req.OperatorIdentityPublicKey)
	if err != nil {
		return nil, fmt.Errorf("unable to parse request operator identity public key: %w", err)
	}
	reqOperatorIdentifier := h.config.GetOperatorIdentifierFromIdentityPublicKey(reqPubKey)
	logger.Info("exchanging revocation secret shares with operator",
		"operator_identity_pubkey", req.OperatorIdentityPublicKey,
		"operator_identifier", reqOperatorIdentifier)

	// Verify the incoming operator signatures package
	operatorSignatures := make(operatorSignaturesMap)
	for _, sig := range req.OperatorTransactionSignatures {
		sigOperatorIdentityPublicKey, err := keys.ParsePublicKey(sig.OperatorIdentityPublicKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse signature operator identity public key: %w", err)
		}
		identifier := h.config.GetOperatorIdentifierFromIdentityPublicKey(sigOperatorIdentityPublicKey)
		operatorSignatures[identifier] = sig.GetSignature()
	}

	db, err := ent.GetDbFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create current tx for request: %w", err)
	}
	tokenTransaction, err := db.TokenTransaction.Query().
		Where(tokentransaction.FinalizedTokenTransactionHashEQ(req.FinalTokenTransactionHash)).
		Only(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load token transaction with txHash (%x) in ExchangeRevocationSecretsShares: %w", req.FinalTokenTransactionHash, err)
	}
	if err := h.validateSignaturesPackageAndPersistPeerSignatures(ctx, operatorSignatures, tokenTransaction); err != nil {
		return nil, tokens.FormatErrorWithTransactionEnt("failed to validate signature package and persist peer signatures", tokenTransaction, err)
	}

	inputOperatorShareMap, err := buildInputOperatorShareMap(req.OperatorShares)
	if err != nil {
		return nil, tokens.FormatErrorWithTransactionEnt("failed to build input operator share map", tokenTransaction, err)
	}
	finalized, err := h.persistPartialRevocationSecretShares(ctx, inputOperatorShareMap, req.FinalTokenTransactionHash)
	if err != nil {
		return nil, tokens.FormatErrorWithTransactionEnt("failed to persist partial revocation secret shares", tokenTransaction, err)
	}

	response, err := h.prepareResponseForExchangeRevocationSecretsShare(ctx, inputOperatorShareMap)
	if err != nil {
		return nil, tokens.FormatErrorWithTransactionEnt("failed to prepare response for exchange revocation secrets share", tokenTransaction, err)
	}

	// No actions take place after this point so we don't have to worry about commiting the revealed status.
	// It is possible for us to finalize in the exchange step above.
	// If that happens, the status will go directly from Signed to Finalized.
	if !finalized &&
		tokenTransaction.Status != st.TokenTransactionStatusRevealed &&
		tokenTransaction.Status != st.TokenTransactionStatusFinalized {
		_, err = tokenTransaction.Update().
			Where(
				tokentransaction.IDEQ(tokenTransaction.ID),
				tokentransaction.StatusNotIn(
					st.TokenTransactionStatusFinalized,
					st.TokenTransactionStatusRevealed,
				),
			).
			SetStatus(st.TokenTransactionStatusRevealed).
			Save(ctx)
		if ent.IsNotFound(err) {
			// We know the row exists, but it's either Finalized or Revealed. Ignore.
			err = nil
		}
		if err != nil {
			return nil, tokens.FormatErrorWithTransactionEnt("failed to update token transaction status", tokenTransaction, err)
		}
	}
	return response, nil
}

func (h *InternalSignTokenHandler) prepareResponseForExchangeRevocationSecretsShare(ctx context.Context, inputOperatorShareMap map[ShareKey]ShareValue) (*pbtkinternal.ExchangeRevocationSecretsSharesResponse, error) {
	operatorSharesMap, err := h.getSecretSharesNotInInput(ctx, inputOperatorShareMap)
	if err != nil {
		return nil, fmt.Errorf("failed to get token outputs with shares: %w", err)
	}
	secretSharesToReturn := make([]*pbtkinternal.OperatorRevocationShares, 0, len(operatorSharesMap))
	for operatorIdentity, shares := range operatorSharesMap {
		secretSharesToReturn = append(secretSharesToReturn, &pbtkinternal.OperatorRevocationShares{
			OperatorIdentityPublicKey: operatorIdentity.Serialize(),
			Shares:                    shares,
		})
	}

	return &pbtkinternal.ExchangeRevocationSecretsSharesResponse{
		ReceivedOperatorShares: secretSharesToReturn,
	}, nil
}

func (h *InternalSignTokenHandler) getSecretSharesNotInInput(ctx context.Context, inputOperatorShareMap map[ShareKey]ShareValue) (operatorSharesMap, error) {
	if len(inputOperatorShareMap) == 0 {
		return nil, fmt.Errorf("no input operator shares provided")
	}
	db, err := ent.GetDbFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create current tx for request: %w", err)
	}
	thisOperatorIdentityPubkey := h.config.IdentityPublicKey()

	uniqueTokenOutputIDs := make([]uuid.UUID, 0, len(inputOperatorShareMap))
	seen := make(map[uuid.UUID]bool)
	for shareKey := range inputOperatorShareMap {
		if !seen[shareKey.TokenOutputID] {
			uniqueTokenOutputIDs = append(uniqueTokenOutputIDs, shareKey.TokenOutputID)
			seen[shareKey.TokenOutputID] = true
		}
	}

	const batchSize = queryTokenOutputsWithPartialRevocationSecretSharesBatchSize
	var outputsWithKeyShares []*ent.TokenOutput

	for i := 0; i < len(uniqueTokenOutputIDs); i += batchSize {
		end := i + batchSize
		if end > len(uniqueTokenOutputIDs) {
			end = len(uniqueTokenOutputIDs)
		}

		batchOutputIDs := uniqueTokenOutputIDs[i:end]

		var excludeKeyshareTokenOutputIDs []any
		var excludePartialShareConditions []predicate.TokenPartialRevocationSecretShare

		for shareKey, shareValue := range inputOperatorShareMap {
			for _, outputID := range batchOutputIDs {
				if shareKey.TokenOutputID == outputID {
					if shareKey.OperatorIdentityPublicKey.Equals(thisOperatorIdentityPubkey) {
						excludeKeyshareTokenOutputIDs = append(excludeKeyshareTokenOutputIDs, shareKey.TokenOutputID)
					}

					excludePartialShareConditions = append(excludePartialShareConditions,
						tokenpartialrevocationsecretshare.And(
							tokenpartialrevocationsecretshare.HasTokenOutputWith(tokenoutput.IDEQ(shareKey.TokenOutputID)),
							tokenpartialrevocationsecretshare.OperatorIdentityPublicKeyEQ(shareValue.OperatorIdentityPublicKey.Serialize()),
						),
					)
					break
				}
			}
		}
		batchOutputs, err := db.TokenOutput.Query().Where(tokenoutput.IDIn(batchOutputIDs...)).
			WithRevocationKeyshare(func(q *ent.SigningKeyshareQuery) {
				if len(excludeKeyshareTokenOutputIDs) > 0 {
					q.Where(func(s *sql.Selector) {
						subquery := sql.Select(tokenoutput.RevocationKeyshareColumn).
							From(sql.Table(tokenoutput.Table)).
							Where(sql.In(tokenoutput.FieldID, excludeKeyshareTokenOutputIDs...))
						s.Where(sql.NotIn(signingkeyshare.FieldID, subquery))
					})
				}
			}).
			WithTokenPartialRevocationSecretShares(func(q *ent.TokenPartialRevocationSecretShareQuery) {
				if len(excludePartialShareConditions) > 0 {
					q.Where(tokenpartialrevocationsecretshare.Not(
						tokenpartialrevocationsecretshare.Or(excludePartialShareConditions...),
					))
				}
			}).
			All(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get token outputs with shares batch %d-%d: %w", i, end-1, err)
		}

		outputsWithKeyShares = append(outputsWithKeyShares, batchOutputs...)
	}

	operatorShares, err := h.buildOperatorPubkeyToRevocationSecretShareMap(outputsWithKeyShares)
	if err != nil {
		return nil, fmt.Errorf("failed to build operator pubkey to revocation secret share map: %w", err)
	}
	return operatorShares, nil
}

func (h *InternalSignTokenHandler) buildOperatorPubkeyToRevocationSecretShareMap(tokenOutputs []*ent.TokenOutput) (operatorSharesMap, error) {
	operatorShares := make(operatorSharesMap)
	for _, to := range tokenOutputs {
		if share := to.Edges.RevocationKeyshare; share != nil {
			operatorIdentityPubkey := h.config.IdentityPublicKey()
			operatorShares[operatorIdentityPubkey] = append(
				operatorShares[operatorIdentityPubkey],
				&pbtkinternal.RevocationSecretShare{
					InputTtxoId: to.ID.String(),
					SecretShare: share.SecretShare,
				},
			)
		}
		for _, partialShare := range to.Edges.TokenPartialRevocationSecretShares {
			partialShareOperatorIdentityPubkey, err := keys.ParsePublicKey(partialShare.OperatorIdentityPublicKey)
			if err != nil {
				return nil, fmt.Errorf("failed to create operator identity pubkey: %w", err)
			}
			operatorShares[partialShareOperatorIdentityPubkey] = append(
				operatorShares[partialShareOperatorIdentityPubkey],
				&pbtkinternal.RevocationSecretShare{
					InputTtxoId: to.ID.String(),
					SecretShare: partialShare.SecretShare,
				},
			)
		}
	}
	return operatorShares, nil
}

func (h *InternalSignTokenHandler) persistPartialRevocationSecretShares(
	ctx context.Context,
	inputOperatorShareMap map[ShareKey]ShareValue,
	transactionHash []byte,
) (finalized bool, err error) {
	if len(inputOperatorShareMap) == 0 {
		return false, nil
	}
	db, err := ent.GetDbFromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get or create current tx for request: %w", err)
	}

	inputTokenOutputIDMap := make(map[uuid.UUID]struct{}, len(inputOperatorShareMap))
	for k := range inputOperatorShareMap {
		inputTokenOutputIDMap[k.TokenOutputID] = struct{}{}
	}
	uniqueInputTokenOutputIDs := make([]uuid.UUID, 0, len(inputTokenOutputIDMap))
	for id := range inputTokenOutputIDMap {
		uniqueInputTokenOutputIDs = append(uniqueInputTokenOutputIDs, id)
	}

	tx, err := db.TokenTransaction.
		Query().
		Where(tokentransaction.FinalizedTokenTransactionHash(transactionHash)).
		WithSpentOutput(func(q *ent.TokenOutputQuery) {
			q.WithRevocationKeyshare()
		}).
		Only(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to load token transaction with txHash in persistPartialRevocationSecretShares: %x: %w", transactionHash, err)
	}

	err = validateInputTokenOutputsMatchSpentTokenOutputs(uniqueInputTokenOutputIDs, tx.Edges.SpentOutput)
	if err != nil {
		return false, tokens.FormatErrorWithTransactionEnt("input token outputs do not match spent token outputs", tx, err)
	}

	revocationKeyshares := make(map[uuid.UUID]*ent.SigningKeyshare)
	for _, spentOutput := range tx.Edges.SpentOutput {
		if revocationKeyshare := spentOutput.Edges.RevocationKeyshare; revocationKeyshare != nil {
			revocationKeyshares[spentOutput.ID] = revocationKeyshare
		}
	}

	var newShares []*ent.TokenPartialRevocationSecretShareCreate
	for sk, sv := range inputOperatorShareMap {
		if sv.OperatorIdentityPublicKey == (keys.Public{}) {
			return false, fmt.Errorf("nil operator identity public key bytes found in input operator share map")
		}
		if sv.SecretShare == nil {
			return false, fmt.Errorf("nil secret share found in input operator share map")
		}
		// Do not write shares that belong to this server to the TokenPartialRevocationSecretShare table.
		if sv.OperatorIdentityPublicKey.Equals(h.config.IdentityPublicKey()) {
			continue
		}
		newShares = append(newShares, db.TokenPartialRevocationSecretShare.Create().
			SetOperatorIdentityPublicKey(sv.OperatorIdentityPublicKey.Serialize()).
			SetSecretShare(sv.SecretShare).
			SetTokenOutputID(sk.TokenOutputID))
	}

	if len(newShares) > 0 {
		// Insert the new secret shares: if an operator already has a secret share from a specific
		// peer operator (same operator identity pubkey + token-output edge), ignore the conflict and move on.
		err := db.TokenPartialRevocationSecretShare.
			CreateBulk(newShares...).
			OnConflictColumns(
				tokenpartialrevocationsecretshare.FieldOperatorIdentityPublicKey,
				tokenpartialrevocationsecretshare.TokenOutputColumn,
			).
			DoNothing().
			Exec(ctx)
		if err != nil {
			return false, tokens.FormatErrorWithTransactionEnt("failed to save new secret shares", tx, err)
		}
	}
	finalized, err = h.recoverFullRevocationSecretsAndFinalize(ctx, transactionHash)
	if err != nil {
		return false, fmt.Errorf("failed to finalize token transaction: %w", err)
	}
	return finalized, nil
}

func (h *InternalSignTokenHandler) recoverFullRevocationSecretsAndFinalize(ctx context.Context, tokenTransactionHash []byte) (finalized bool, err error) {
	db, err := ent.GetDbFromContext(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get or create current tx for request: %w", err)
	}

	tokenTransaction, err := db.TokenTransaction.Query().
		Where(tokentransaction.FinalizedTokenTransactionHashEQ(tokenTransactionHash)).
		Where(tokentransaction.StatusIn(
			st.TokenTransactionStatusSigned,
			st.TokenTransactionStatusRevealed,
			st.TokenTransactionStatusFinalized,
		)).
		WithSpentOutput().
		Only(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to load token transaction with txHash in recoverFullRevocationSecretsAndFinalize: %x: %w", tokenTransactionHash, err)
	}
	// Token transaction is already finalized, so we can return early.
	if tokenTransaction.Status == st.TokenTransactionStatusFinalized {
		return true, nil
	}
	if len(tokenTransaction.Edges.SpentOutput) == 0 {
		return false, fmt.Errorf("transaction %x has no spent outputs loaded", tokenTransactionHash)
	}

	outputIDs := make([]uuid.UUID, len(tokenTransaction.Edges.SpentOutput))
	for i, output := range tokenTransaction.Edges.SpentOutput {
		outputIDs[i] = output.ID
	}

	const batchSize = queryTokenOutputsWithPartialRevocationSecretSharesBatchSize
	outputsWithShares := make(map[uuid.UUID]*ent.TokenOutput)

	for i := 0; i < len(outputIDs); i += batchSize {
		end := i + batchSize
		if end > len(outputIDs) {
			end = len(outputIDs)
		}

		batchOutputIDs := outputIDs[i:end]
		batchOutputs, err := db.TokenOutput.Query().
			Where(tokenoutput.IDIn(batchOutputIDs...)).
			WithTokenPartialRevocationSecretShares().
			WithRevocationKeyshare().
			All(ctx)
		if err != nil {
			return false, tokens.FormatErrorWithTransactionEnt(fmt.Sprintf("failed to load shares for outputs batch (%d-%d)", i, end-1), tokenTransaction, err)
		}

		for _, output := range batchOutputs {
			outputsWithShares[output.ID] = output
		}
	}

	// Replace the spent outputs with the ones that have shares loaded
	for i, spentOutput := range tokenTransaction.Edges.SpentOutput {
		if outputWithShares, exists := outputsWithShares[spentOutput.ID]; exists {
			tokenTransaction.Edges.SpentOutput[i] = outputWithShares
		}
	}

	minCountOutputPartialRevocationSecretSharesForAllOutputs := len(h.config.SigningOperatorMap)
	for _, output := range tokenTransaction.Edges.SpentOutput {
		if output.Edges.RevocationKeyshare == nil {
			return false, tokens.FormatErrorWithTransactionEnt(
				"missing revocation key-share on output", tokenTransaction, nil)
		}
		if output.Edges.RevocationKeyshare.SecretShare == nil {
			return false, tokens.FormatErrorWithTransactionEnt(
				"nil revocation secret share on output", tokenTransaction, nil)
		}
		minCountOutputPartialRevocationSecretSharesForAllOutputs = min(
			minCountOutputPartialRevocationSecretSharesForAllOutputs,
			len(output.Edges.TokenPartialRevocationSecretShares),
		)
	}
	// min count of partial revocation secret shares + this server's share >= threshold, for all outputs
	requiredOperators := h.getRequiredParticipatingOperatorsCount()
	if minCountOutputPartialRevocationSecretSharesForAllOutputs+1 >= requiredOperators {
		outputRecoveredSecrets, outputToSpendRevocationCommitments, err := h.recoverFullRevocationSecrets(tokenTransaction)
		if err != nil {
			return false, tokens.FormatErrorWithTransactionEnt("failed to recover full revocation secrets", tokenTransaction, err)
		}

		parsedCommitments := make([]keys.Public, len(outputToSpendRevocationCommitments))
		for i, commitment := range outputToSpendRevocationCommitments {
			parsed, err := keys.ParsePublicKey(commitment)
			if err != nil {
				return false, fmt.Errorf("failed to parse commitment for output %d: %w", i, err)
			}
			parsedCommitments[i] = parsed
		}
		recoveredSecretsToValidate := make([]keys.Private, len(outputRecoveredSecrets))
		for i, secret := range outputRecoveredSecrets {
			recoveredSecretsToValidate[i] = keys.PrivateFromKey(*secret.RevocationSecret)
		}
		if err := utils.ValidateRevocationKeys(recoveredSecretsToValidate, parsedCommitments); err != nil {
			return false, tokens.FormatErrorWithTransactionEnt("invalid revocation keys found", tokenTransaction, err)
		}

		internalFinalizeHandler := NewInternalFinalizeTokenHandler(h.config)
		err = internalFinalizeHandler.FinalizeCoordinatedTokenTransactionInternal(ctx, tokenTransactionHash, outputRecoveredSecrets)
		if err != nil {
			return false, tokens.FormatErrorWithTransactionEnt("failed to finalize token transaction", tokenTransaction, err)
		}
		return true, nil
	}
	return false, nil
}

func (h *InternalSignTokenHandler) recoverFullRevocationSecrets(tokenTransaction *ent.TokenTransaction) (outputRecoveredSecrets []*ent.RecoveredRevocationSecret, outputToSpendRevocationCommitments [][]byte, err error) {
	outputRecoveredSecrets = make([]*ent.RecoveredRevocationSecret, 0, len(tokenTransaction.Edges.SpentOutput))
	outputToSpendRevocationCommitments = make([][]byte, 0, len(tokenTransaction.Edges.SpentOutput))

	for _, output := range tokenTransaction.Edges.SpentOutput {
		outputToSpendRevocationCommitments = append(outputToSpendRevocationCommitments, output.WithdrawRevocationCommitment)
		outputShares := make([]*secretsharing.SecretShare, 0, len(output.Edges.TokenPartialRevocationSecretShares)+1)
		for _, share := range output.Edges.TokenPartialRevocationSecretShares {
			identityPubKey, err := keys.ParsePublicKey(share.OperatorIdentityPublicKey)
			if err != nil {
				return nil, nil, err
			}
			operatorIndex, err := strconv.ParseInt(h.config.GetOperatorIdentifierFromIdentityPublicKey(identityPubKey), 10, 64)
			if err != nil {
				return nil, nil, tokens.FormatErrorWithTransactionEnt("failed to parse operator index", tokenTransaction, err)
			}
			outputShares = append(outputShares, &secretsharing.SecretShare{
				FieldModulus: secp256k1.S256().N,
				Threshold:    int(h.config.Threshold),
				Index:        big.NewInt(operatorIndex),
				Share:        new(big.Int).SetBytes(share.SecretShare),
			})
		}
		coordinatorIndex, err := strconv.ParseInt(h.config.GetOperatorIdentifierFromIdentityPublicKey(h.config.IdentityPublicKey()), 10, 64)
		if err != nil {
			return nil, nil, tokens.FormatErrorWithTransactionEnt("failed to parse coordinator index", tokenTransaction, err)
		}
		outputShares = append(outputShares, &secretsharing.SecretShare{
			FieldModulus: secp256k1.S256().N,
			Threshold:    int(h.config.Threshold),
			Index:        big.NewInt(coordinatorIndex),
			Share:        new(big.Int).SetBytes(output.Edges.RevocationKeyshare.SecretShare),
		})
		recoveredSecret, err := secretsharing.RecoverSecret(outputShares)
		if err != nil {
			return nil, nil, tokens.FormatErrorWithTransactionEnt("failed to recover secret", tokenTransaction, err)
		}
		privKey, err := common.PrivateKeyFromBigInt(recoveredSecret)
		if err != nil {
			return nil, nil, tokens.FormatErrorWithTransactionEnt("failed to convert recovered keyshare to private key", tokenTransaction, err)
		}
		outputRecoveredSecrets = append(outputRecoveredSecrets, &ent.RecoveredRevocationSecret{
			OutputIndex:      uint32(output.SpentTransactionInputVout),
			RevocationSecret: privKey,
		})
	}
	return outputRecoveredSecrets, outputToSpendRevocationCommitments, nil
}

func buildInputOperatorShareMap(operatorShares []*pbtkinternal.OperatorRevocationShares) (map[ShareKey]ShareValue, error) {
	inputOperatorShareMap := make(map[ShareKey]ShareValue)
	for _, operatorShare := range operatorShares {
		if operatorShare == nil {
			return nil, fmt.Errorf("nil operator share found in buildInputOperatorShareMap")
		}
		for _, share := range operatorShare.Shares {
			if share == nil {
				return nil, fmt.Errorf("nil share found on operator share in buildInputOperatorShareMap")
			}
			tokenOutputID, err := uuid.Parse(share.GetInputTtxoId())
			if err != nil {
				return nil, fmt.Errorf("failed to parse token output id: %w", err)
			}
			opIDPubKey, err := keys.ParsePublicKey(operatorShare.OperatorIdentityPublicKey)
			if err != nil {
				return nil, fmt.Errorf("failed to parse operator identity public key: %w", err)
			}
			inputOperatorShareMap[ShareKey{
				TokenOutputID:             tokenOutputID,
				OperatorIdentityPublicKey: opIDPubKey,
			}] = ShareValue{
				SecretShare:               share.SecretShare,
				OperatorIdentityPublicKey: opIDPubKey,
			}
		}
	}
	return inputOperatorShareMap, nil
}

func (h *InternalSignTokenHandler) validateSignaturesPackageAndPersistPeerSignatures(ctx context.Context, signatures operatorSignaturesMap, tokenTransaction *ent.TokenTransaction) error {
	expectedSignatures := h.getRequiredParticipatingOperatorsCount()
	if len(signatures) < expectedSignatures {
		return tokens.FormatErrorWithTransactionEnt("less than required operators have signed this transaction", tokenTransaction, fmt.Errorf("expected %d signatures, got %d", expectedSignatures, len(signatures)))
	}

	if err := verifyOperatorSignatures(signatures, h.config.SigningOperatorMap, tokenTransaction.FinalizedTokenTransactionHash); err != nil {
		return tokens.FormatErrorWithTransactionEnt("failed to verify operator signatures", tokenTransaction, err)
	}

	db, err := ent.GetDbFromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to get or create current tx for request: %w", err)
	}
	peerSignatures := make([]*ent.TokenTransactionPeerSignatureCreate, 0, len(h.config.SigningOperatorMap)-1)
	for identifier, sig := range signatures {
		// DO NOT WRITE this operator's signature to the peer signatures table
		if identifier != h.config.Identifier {
			operatorIdentityPubkey := h.config.SigningOperatorMap[identifier].IdentityPublicKey
			peerSignatures = append(peerSignatures, db.TokenTransactionPeerSignature.Create().
				SetTokenTransactionID(tokenTransaction.ID).
				SetOperatorIdentityPublicKey(operatorIdentityPubkey.Serialize()).
				SetSignature(sig))
		}
	}

	if len(peerSignatures) > 0 {
		// Insert the new peer signature: if an operator already has a signature from a specific
		// peer operator (same operator identity pubkey + token-transaction edge), ignore the conflict and move on.
		err := db.TokenTransactionPeerSignature.
			CreateBulk(peerSignatures...).
			OnConflictColumns(
				tokentransactionpeersignature.FieldOperatorIdentityPublicKey,
				tokentransactionpeersignature.TokenTransactionColumn,
			).
			DoNothing().
			Exec(ctx)
		if err != nil {
			return tokens.FormatErrorWithTransactionEnt("failed to bulk create peer signatures", tokenTransaction, err)
		}
	}
	return nil
}

func validateInputTokenOutputsMatchSpentTokenOutputs(tokenOutputIDs []uuid.UUID, spentOutputs []*ent.TokenOutput) error {
	spentOutputMap := make(map[uuid.UUID]*ent.TokenOutput)
	for _, spentOutput := range spentOutputs {
		spentOutputMap[spentOutput.ID] = &ent.TokenOutput{}
	}
	if len(spentOutputMap) != len(tokenOutputIDs) {
		return fmt.Errorf("length of spent token outputs does not match length of token output ids: num spent output in DB (%d) != num input token output ids (%d)", len(spentOutputMap), len(tokenOutputIDs))
	}
	for _, tokenOutputID := range tokenOutputIDs {
		if _, ok := spentOutputMap[tokenOutputID]; !ok {
			return fmt.Errorf("input token output id: %s not spent in transaction", tokenOutputID)
		}
	}
	return nil
}

func validateSecretShareMatchesPublicKey(secretShareBytes []byte, publicKeyBytes []byte) error {
	if len(secretShareBytes) != 32 {
		// validate the secret share length from other operator
		return fmt.Errorf("secret share must be 32 bytes")
	}
	secretSharePrivKey := secp256k1.PrivKeyFromBytes(secretShareBytes)
	derivedPubKey := secretSharePrivKey.PubKey()

	pubkey, err := secp256k1.ParsePubKey(publicKeyBytes)
	if err != nil {
		return fmt.Errorf("failed to parse public key: %w", err)
	}
	if !derivedPubKey.IsEqual(pubkey) {
		return fmt.Errorf("secret share: (%x) does not match public key: (%x)", secretShareBytes, publicKeyBytes)
	}
	return nil
}
