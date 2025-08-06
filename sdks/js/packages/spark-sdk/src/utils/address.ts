import { secp256k1 } from "@noble/curves/secp256k1";
import { bytesToHex, hexToBytes } from "@noble/hashes/utils";
import { bech32m } from "@scure/base";
import { SparkInvoiceFields, SparkAddress } from "../proto/spark.js";
import { NetworkType } from "./network.js";
import { ValidationError } from "../errors/index.js";

import { UUID } from "uuidv7";
import { bytesToNumberBE } from "@noble/curves/abstract/utils";

const AddressNetwork: Record<NetworkType, string> = {
  MAINNET: "sp",
  TESTNET: "spt",
  REGTEST: "sprt",
  SIGNET: "sps",
  LOCAL: "spl",
} as const;

export type SparkAddressFormat =
  `${(typeof AddressNetwork)[keyof typeof AddressNetwork]}1${string}`;

export interface SparkAddressData {
  identityPublicKey: string;
  network: NetworkType;
  sparkInvoiceFields?: SparkInvoiceFields;
}

export interface DecodedSparkAddressData {
  identityPublicKey: string;
  network: NetworkType;
  sparkInvoiceFields?: {
    version: number;
    id: string;
    paymentType?:
      | { type: "tokens"; tokenIdentifier?: string; amount?: bigint }
      | { type: "sats"; amount?: number };
    memo?: string;
    senderPublicKey?: string;
    expiryTime?: Date;
  };
  signature?: string;
}

export function encodeSparkAddress(
  payload: SparkAddressData,
): SparkAddressFormat {
  try {
    isValidPublicKey(payload.identityPublicKey);

    let sparkInvoiceFields: SparkInvoiceFields | undefined;
    if (payload.sparkInvoiceFields) {
      validateSparkInvoiceFields(payload.sparkInvoiceFields);
      sparkInvoiceFields = payload.sparkInvoiceFields;
    }

    const sparkAddressProto = SparkAddress.create({
      identityPublicKey: hexToBytes(payload.identityPublicKey),
      sparkInvoiceFields,
    });

    const serializedPayload = SparkAddress.encode(sparkAddressProto).finish();
    const words = bech32m.toWords(serializedPayload);

    return bech32m.encode(
      AddressNetwork[payload.network],
      words,
      500,
    ) as SparkAddressFormat;
  } catch (error) {
    throw new ValidationError(
      "Failed to encode Spark address",
      {
        field: "publicKey",
        value: payload.identityPublicKey,
      },
      error as Error,
    );
  }
}

export function decodeSparkAddress(
  address: string,
  network: NetworkType,
): DecodedSparkAddressData {
  try {
    const decoded = bech32m.decode(address as SparkAddressFormat, 500);

    if (decoded.prefix !== AddressNetwork[network]) {
      throw new ValidationError("Invalid Spark address prefix", {
        field: "address",
        value: address,
        expected: `prefix='${AddressNetwork[network]}'`,
      });
    }

    const payload = SparkAddress.decode(bech32m.fromWords(decoded.words));

    const { identityPublicKey, sparkInvoiceFields, signature } = payload;

    const identityPubkeyHex = bytesToHex(identityPublicKey);
    const signatureHex = signature ? bytesToHex(signature) : undefined;
    isValidPublicKey(identityPubkeyHex);

    return {
      identityPublicKey: identityPubkeyHex,
      network,
      sparkInvoiceFields: sparkInvoiceFields && {
        version: sparkInvoiceFields.version,
        id: UUID.ofInner(sparkInvoiceFields.id).toString(),
        paymentType: sparkInvoiceFields.paymentType
          ? sparkInvoiceFields.paymentType.$case === "tokensPayment"
            ? {
                type: "tokens" as const,
                tokenIdentifier: sparkInvoiceFields.paymentType.tokensPayment
                  .tokenIdentifier
                  ? bytesToHex(
                      sparkInvoiceFields.paymentType.tokensPayment
                        .tokenIdentifier,
                    )
                  : undefined,
                amount: sparkInvoiceFields.paymentType.tokensPayment.amount
                  ? bytesToNumberBE(
                      sparkInvoiceFields.paymentType.tokensPayment.amount,
                    )
                  : undefined,
              }
            : sparkInvoiceFields.paymentType.$case === "satsPayment"
              ? {
                  type: "sats" as const,
                  amount: sparkInvoiceFields.paymentType.satsPayment.amount,
                }
              : undefined
          : undefined,
        memo: sparkInvoiceFields.memo,
        senderPublicKey: sparkInvoiceFields.senderPublicKey
          ? bytesToHex(sparkInvoiceFields.senderPublicKey)
          : undefined,
        expiryTime: sparkInvoiceFields.expiryTime,
      },
      signature: signatureHex,
    };
  } catch (error) {
    if (error instanceof ValidationError) {
      throw error;
    }
    throw new ValidationError(
      "Failed to decode Spark address",
      {
        field: "address",
        value: address,
      },
      error as Error,
    );
  }
}

export function isValidSparkAddress(address: string) {
  try {
    const network = Object.entries(AddressNetwork).find(([_, prefix]) =>
      address.startsWith(prefix),
    )?.[0] as NetworkType | undefined;

    if (!network) {
      throw new ValidationError("Invalid Spark address network", {
        field: "network",
        value: address,
        expected: Object.values(AddressNetwork),
      });
    }

    decodeSparkAddress(address, network);
    return true;
  } catch (error) {
    if (error instanceof ValidationError) {
      throw error;
    }
    throw new ValidationError(
      "Invalid Spark address",
      {
        field: "address",
        value: address,
      },
      error as Error,
    );
  }
}

export function isValidPublicKey(publicKey: string) {
  try {
    const point = secp256k1.ProjectivePoint.fromHex(publicKey);
    point.assertValidity();
  } catch (error) {
    throw new ValidationError(
      "Invalid public key",
      {
        field: "publicKey",
        value: publicKey,
      },
      error as Error,
    );
  }
}

export function validateSparkInvoiceFields(
  sparkInvoiceFields: SparkInvoiceFields,
) {
  const { version, id, paymentType, memo, senderPublicKey } =
    sparkInvoiceFields;
  if (version !== 1) {
    throw new ValidationError("Version must be 1", {
      field: "version",
      value: version,
    });
  }
  // ID is required and must be a valid UUID
  try {
    UUID.ofInner(id);
  } catch (error) {
    throw new ValidationError(
      "Invalid id",
      {
        field: "id",
        value: id,
      },
      error as Error,
    );
  }
  if (senderPublicKey) {
    try {
      isValidPublicKey(bytesToHex(senderPublicKey));
    } catch (error) {
      throw new ValidationError(
        "Invalid sender public key",
        {
          field: "senderPublicKey",
          value: senderPublicKey,
        },
        error as Error,
      );
    }
  }
  if (memo) {
    const encoder = new TextEncoder();
    const memoBytes = encoder.encode(memo);
    if (memoBytes.length > 120) {
      throw new ValidationError(
        "Memo exceeds the maximum allowed byte length of 120.",
        {
          field: "memo",
          value: memo,
          expected: "less than 120 bytes",
        },
      );
    }
  }
  if (paymentType) {
    if (paymentType.$case === "tokensPayment") {
      const MAX_UINT128 = BigInt(2 ** 128 - 1);
      const { amount: tokensAmount, tokenIdentifier } =
        paymentType.tokensPayment;
      if (tokenIdentifier) {
        if (!(tokenIdentifier instanceof Uint8Array)) {
          throw new ValidationError("Token identifier must be Uint8Array", {
            field: "paymentType.tokensPayment.tokenIdentifier",
            value: tokenIdentifier,
          });
        }
        if (tokenIdentifier.length !== 32) {
          throw new ValidationError("Token identifier must be 32 bytes", {
            field: "paymentType.tokensPayment.tokenIdentifier",
            value: tokenIdentifier,
          });
        }
      }
      if (tokensAmount) {
        if (tokensAmount.length > 16) {
          throw new ValidationError("Amount must be less than 16 bytes", {
            field: "paymentType.tokensPayment.amount",
            value: tokensAmount,
          });
        }
        const tokensAmountBigInt = bytesToNumberBE(tokensAmount);
        if (tokensAmountBigInt < 0 || tokensAmountBigInt > MAX_UINT128) {
          throw new ValidationError(
            "Asset amount must be between 0 and MAX_UINT128",
            {
              field: "amount",
              value: tokensAmount,
            },
          );
        }
      }
    } else if (paymentType.$case === "satsPayment") {
      const { amount } = paymentType.satsPayment;
      if (amount) {
        const MAX_SATS_AMOUNT = 2_100_000_000_000_000; // 21_000_000 BTC * 100_000_000 sats/BTC
        if (amount < 0) {
          throw new ValidationError(
            "Amount must be greater than or equal to 0",
            {
              field: "paymentType.satsPayment.amount",
              value: amount,
            },
          );
        }
        if (amount > MAX_SATS_AMOUNT) {
          throw new ValidationError(
            `Amount must be less than ${MAX_SATS_AMOUNT} sats`,
          );
        }
      }
    } else {
      throw new ValidationError("Invalid payment type", {
        field: "paymentType",
        value: paymentType,
      });
    }
  }
}
