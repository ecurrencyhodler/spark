package ent

import (
	"fmt"

	"github.com/lightsparkdev/spark/common"
)

func (r *L1TokenCreate) ToTokenMetadata() (*common.TokenMetadata, error) {
	network, err := common.NetworkFromSchemaNetwork(r.Network)
	if err != nil {
		return nil, fmt.Errorf("failed to convert network: %w", err)
	}

	return &common.TokenMetadata{
		IssuerPublicKey:         r.IssuerPublicKey,
		TokenName:               r.TokenName,
		TokenTicker:             r.TokenTicker,
		Decimals:                r.Decimals,
		MaxSupply:               r.MaxSupply,
		IsFreezable:             r.IsFreezable,
		CreationEntityPublicKey: common.L1CreationEntityPublicKey,
		Network:                 network,
	}, nil
}
