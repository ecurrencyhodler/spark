import { secp256k1 } from "@noble/curves/secp256k1";
import { Address, OutScript, Transaction } from "@scure/btc-signer";
import { RPCError } from "../errors/types.js";
import { KeyDerivationType } from "../index.js";
import { TreeNode } from "../proto/spark.js";
import { WalletConfigService } from "../services/config.js";
import { ConnectionManager } from "../services/connection.js";
import { DepositService } from "../services/deposit.js";
import { ConfigOptions, WalletConfig } from "../services/wallet-config.js";
import { getP2TRAddressFromPublicKey } from "../utils/bitcoin.js";
import { getNetwork, Network } from "../utils/network.js";
import { SparkWalletTesting } from "./utils/spark-testing-wallet.js";
import { BitcoinFaucet } from "./utils/test-faucet.js";

export { BitcoinFaucet };

export function getTestWalletConfig() {
  const identityPrivateKey = secp256k1.utils.randomPrivateKey();
  return getTestWalletConfigWithIdentityKey(identityPrivateKey);
}

export function getTestWalletConfigWithIdentityKey(
  identityPrivateKey: Uint8Array,
) {
  return {
    ...WalletConfig.LOCAL,
    identityPrivateKey,
  } as ConfigOptions;
}

export async function createNewTree(
  wallet: SparkWalletTesting,
  leafId: string,
  faucet: BitcoinFaucet,
  amountSats: bigint = 100_000n,
): Promise<TreeNode> {
  const faucetCoin = await faucet.fund();

  const configService = new WalletConfigService(
    {
      network: "LOCAL",
    },
    wallet.getSigner(),
  );
  const connectionManager = new ConnectionManager(configService);
  const depositService = new DepositService(configService, connectionManager);

  const pubKey = await wallet.getSigner().getPublicKeyFromDerivation({
    type: KeyDerivationType.LEAF,
    path: leafId,
  });
  const depositResp = await depositService.generateDepositAddress({
    signingPubkey: pubKey,
    leafId,
  });

  if (!depositResp.depositAddress) {
    throw new RPCError("Deposit address not found", {
      method: "generateDepositAddress",
      params: { signingPubkey: pubKey, leafId },
    });
  }

  const depositTx = new Transaction();
  depositTx.addInput(faucetCoin!.outpoint);

  // Add the main output
  const addr = Address(getNetwork(Network.LOCAL)).decode(
    depositResp.depositAddress.address,
  );
  const script = OutScript.encode(addr);
  depositTx.addOutput({ script, amount: amountSats });

  const treeResp = await depositService.createTreeRoot({
    keyDerivation: {
      type: KeyDerivationType.LEAF,
      path: leafId,
    },
    verifyingKey: depositResp.depositAddress.verifyingKey,
    depositTx,
    vout: 0,
  });

  const signedDepositTx = await faucet.signFaucetCoin(
    depositTx,
    faucetCoin!.txout,
    faucetCoin!.key,
  );

  await faucet.broadcastTx(signedDepositTx.hex);

  // Mine just 1 block instead of waiting for many confirmations
  const randomKey = secp256k1.utils.randomPrivateKey();
  const randomPubKey = secp256k1.getPublicKey(randomKey);
  const randomAddress = getP2TRAddressFromPublicKey(
    randomPubKey,
    Network.LOCAL,
  );

  await faucet.generateToAddress(1, randomAddress);

  await new Promise((resolve) => setTimeout(resolve, 100));
  return treeResp.nodes[0]!;
}
