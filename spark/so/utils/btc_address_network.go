package utils

import (
	"strings"

	"github.com/lightsparkdev/spark/common"
)

// IsBitcoinAddressForNetwork checks if the given Bitcoin address matches the expected prefix for the specified network.
// It uses simple prefix matching for common address types (legacy, P2SH, SegWit, Taproot) for each Bitcoin network.
// TODO: Investigate using btcutil for this instead of using our own.
func IsBitcoinAddressForNetwork(address string, network common.Network) bool {
	switch network {
	case common.Mainnet:
		return hasAnyPrefix(address, "bc1", "3", "1")
	case common.Regtest:
		return hasAnyPrefix(address, "bcrt", "2", "m", "n")
	case common.Testnet:
		return hasAnyPrefix(address, "tb1", "2", "m", "n")
	case common.Signet:
		return hasAnyPrefix(address, "tb1", "sb1", "2", "m", "n")
	default:
		return false
	}
}

func hasAnyPrefix(address string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(address, prefix) {
			return true
		}
	}
	return false
}
