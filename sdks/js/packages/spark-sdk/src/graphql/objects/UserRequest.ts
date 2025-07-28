// Copyright ©, 2023-present, Lightspark Group, Inc. - All Rights Reserved

import { LightsparkException, Query, isObject } from "@lightsparkdev/core";
import BitcoinNetwork from "./BitcoinNetwork.js";
import ClaimStaticDeposit from "./ClaimStaticDeposit.js";
import ClaimStaticDepositStatus from "./ClaimStaticDepositStatus.js";
import CoopExitRequest from "./CoopExitRequest.js";
import {
  CurrencyAmountFromJson,
  CurrencyAmountToJson,
} from "./CurrencyAmount.js";
import ExitSpeed from "./ExitSpeed.js";
import { InvoiceFromJson, InvoiceToJson } from "./Invoice.js";
import LeavesSwapRequest from "./LeavesSwapRequest.js";
import LightningReceiveRequest from "./LightningReceiveRequest.js";
import LightningReceiveRequestStatus from "./LightningReceiveRequestStatus.js";
import LightningSendRequest from "./LightningSendRequest.js";
import LightningSendRequestStatus from "./LightningSendRequestStatus.js";
import SparkCoopExitRequestStatus from "./SparkCoopExitRequestStatus.js";
import SparkLeavesSwapRequestStatus from "./SparkLeavesSwapRequestStatus.js";
import { SwapLeafFromJson, SwapLeafToJson } from "./SwapLeaf.js";
import { TransferFromJson } from "./Transfer.js";

interface UserRequest {
  /**
   * The unique identifier of this entity across all Lightspark systems. Should be treated as an opaque
   * string.
   **/
  id: string;

  /** The date and time when the entity was first created. **/
  createdAt: string;

  /** The date and time when the entity was last updated. **/
  updatedAt: string;

  /** The network the lightning send request is on. **/
  network: BitcoinNetwork;

  /** The typename of the object **/
  typename: string;
}

export const UserRequestFromJson = (obj: any): UserRequest => {
  if (obj["__typename"] == "ClaimStaticDeposit") {
    return {
      id: obj["claim_static_deposit_id"],
      createdAt: obj["claim_static_deposit_created_at"],
      updatedAt: obj["claim_static_deposit_updated_at"],
      network:
        BitcoinNetwork[obj["claim_static_deposit_network"]] ??
        BitcoinNetwork.FUTURE_VALUE,
      creditAmount: CurrencyAmountFromJson(
        obj["claim_static_deposit_credit_amount"],
      ),
      maxFee: CurrencyAmountFromJson(obj["claim_static_deposit_max_fee"]),
      status:
        ClaimStaticDepositStatus[obj["claim_static_deposit_status"]] ??
        ClaimStaticDepositStatus.FUTURE_VALUE,
      transactionId: obj["claim_static_deposit_transaction_id"],
      outputIndex: obj["claim_static_deposit_output_index"],
      bitcoinNetwork:
        BitcoinNetwork[obj["claim_static_deposit_bitcoin_network"]] ??
        BitcoinNetwork.FUTURE_VALUE,
      typename: "ClaimStaticDeposit",
      transferSparkId: obj["claim_static_deposit_transfer_spark_id"],
    } as ClaimStaticDeposit;
  }
  if (obj["__typename"] == "CoopExitRequest") {
    return {
      id: obj["coop_exit_request_id"],
      createdAt: obj["coop_exit_request_created_at"],
      updatedAt: obj["coop_exit_request_updated_at"],
      network:
        BitcoinNetwork[obj["coop_exit_request_network"]] ??
        BitcoinNetwork.FUTURE_VALUE,
      fee: CurrencyAmountFromJson(obj["coop_exit_request_fee"]),
      l1BroadcastFee: CurrencyAmountFromJson(
        obj["coop_exit_request_l1_broadcast_fee"],
      ),
      status:
        SparkCoopExitRequestStatus[obj["coop_exit_request_status"]] ??
        SparkCoopExitRequestStatus.FUTURE_VALUE,
      expiresAt: obj["coop_exit_request_expires_at"],
      rawConnectorTransaction:
        obj["coop_exit_request_raw_connector_transaction"],
      rawCoopExitTransaction:
        obj["coop_exit_request_raw_coop_exit_transaction"],
      coopExitTxid: obj["coop_exit_request_coop_exit_txid"],
      typename: "CoopExitRequest",
      feeQuoteId: obj["coop_exit_request_fee_quote"]?.id ?? undefined,
      exitSpeed: !!obj["coop_exit_request_exit_speed"]
        ? (ExitSpeed[obj["coop_exit_request_exit_speed"]] ??
          ExitSpeed.FUTURE_VALUE)
        : null,
      transfer: !!obj["coop_exit_request_transfer"]
        ? TransferFromJson(obj["coop_exit_request_transfer"])
        : undefined,
    } as CoopExitRequest;
  }
  if (obj["__typename"] == "LeavesSwapRequest") {
    return {
      id: obj["leaves_swap_request_id"],
      createdAt: obj["leaves_swap_request_created_at"],
      updatedAt: obj["leaves_swap_request_updated_at"],
      network:
        BitcoinNetwork[obj["leaves_swap_request_network"]] ??
        BitcoinNetwork.FUTURE_VALUE,
      status:
        SparkLeavesSwapRequestStatus[obj["leaves_swap_request_status"]] ??
        SparkLeavesSwapRequestStatus.FUTURE_VALUE,
      totalAmount: CurrencyAmountFromJson(
        obj["leaves_swap_request_total_amount"],
      ),
      targetAmount: CurrencyAmountFromJson(
        obj["leaves_swap_request_target_amount"],
      ),
      fee: CurrencyAmountFromJson(obj["leaves_swap_request_fee"]),
      inboundTransfer: TransferFromJson(
        obj["leaves_swap_request_inbound_transfer"],
      ),
      swapLeaves: obj["leaves_swap_request_swap_leaves"].map((e) =>
        SwapLeafFromJson(e),
      ),
      typename: "LeavesSwapRequest",
      outboundTransfer: !!obj["leaves_swap_request_outbound_transfer"]
        ? TransferFromJson(obj["leaves_swap_request_outbound_transfer"])
        : undefined,
      expiresAt: obj["leaves_swap_request_expires_at"],
    } as LeavesSwapRequest;
  }
  if (obj["__typename"] == "LightningReceiveRequest") {
    return {
      id: obj["lightning_receive_request_id"],
      createdAt: obj["lightning_receive_request_created_at"],
      updatedAt: obj["lightning_receive_request_updated_at"],
      network:
        BitcoinNetwork[obj["lightning_receive_request_network"]] ??
        BitcoinNetwork.FUTURE_VALUE,
      invoice: InvoiceFromJson(obj["lightning_receive_request_invoice"]),
      status:
        LightningReceiveRequestStatus[
          obj["lightning_receive_request_status"]
        ] ?? LightningReceiveRequestStatus.FUTURE_VALUE,
      typename: "LightningReceiveRequest",
      transfer: !!obj["lightning_receive_request_transfer"]
        ? TransferFromJson(obj["lightning_receive_request_transfer"])
        : undefined,
      paymentPreimage: obj["lightning_receive_request_payment_preimage"],
      receiverIdentityPublicKey:
        obj["lightning_receive_request_receiver_identity_public_key"],
    } as LightningReceiveRequest;
  }
  if (obj["__typename"] == "LightningSendRequest") {
    return {
      id: obj["lightning_send_request_id"],
      createdAt: obj["lightning_send_request_created_at"],
      updatedAt: obj["lightning_send_request_updated_at"],
      network:
        BitcoinNetwork[obj["lightning_send_request_network"]] ??
        BitcoinNetwork.FUTURE_VALUE,
      encodedInvoice: obj["lightning_send_request_encoded_invoice"],
      fee: CurrencyAmountFromJson(obj["lightning_send_request_fee"]),
      idempotencyKey: obj["lightning_send_request_idempotency_key"],
      status:
        LightningSendRequestStatus[obj["lightning_send_request_status"]] ??
        LightningSendRequestStatus.FUTURE_VALUE,
      typename: "LightningSendRequest",
      transfer: !!obj["lightning_send_request_transfer"]
        ? TransferFromJson(obj["lightning_send_request_transfer"])
        : undefined,
      paymentPreimage: obj["lightning_send_request_payment_preimage"],
    } as LightningSendRequest;
  }
  throw new LightsparkException(
    "DeserializationError",
    `Couldn't find a concrete type for interface UserRequest corresponding to the typename=${obj["__typename"]}`,
  );
};
export const UserRequestToJson = (obj: UserRequest): any => {
  if (obj.typename == "ClaimStaticDeposit") {
    const claimStaticDeposit = obj as ClaimStaticDeposit;
    return {
      __typename: "ClaimStaticDeposit",
      claim_static_deposit_id: claimStaticDeposit.id,
      claim_static_deposit_created_at: claimStaticDeposit.createdAt,
      claim_static_deposit_updated_at: claimStaticDeposit.updatedAt,
      claim_static_deposit_network: claimStaticDeposit.network,
      claim_static_deposit_credit_amount: CurrencyAmountToJson(
        claimStaticDeposit.creditAmount,
      ),
      claim_static_deposit_max_fee: CurrencyAmountToJson(
        claimStaticDeposit.maxFee,
      ),
      claim_static_deposit_status: claimStaticDeposit.status,
      claim_static_deposit_transaction_id: claimStaticDeposit.transactionId,
      claim_static_deposit_output_index: claimStaticDeposit.outputIndex,
      claim_static_deposit_bitcoin_network: claimStaticDeposit.bitcoinNetwork,
      claim_static_deposit_transfer_spark_id:
        claimStaticDeposit.transferSparkId,
    };
  }
  if (obj.typename == "CoopExitRequest") {
    const coopExitRequest = obj as CoopExitRequest;
    return {
      __typename: "CoopExitRequest",
      coop_exit_request_id: coopExitRequest.id,
      coop_exit_request_created_at: coopExitRequest.createdAt,
      coop_exit_request_updated_at: coopExitRequest.updatedAt,
      coop_exit_request_network: coopExitRequest.network,
      coop_exit_request_fee: CurrencyAmountToJson(coopExitRequest.fee),
      coop_exit_request_l1_broadcast_fee: CurrencyAmountToJson(
        coopExitRequest.l1BroadcastFee,
      ),
      coop_exit_request_fee_quote: { id: coopExitRequest.feeQuoteId },
      coop_exit_request_exit_speed: coopExitRequest.exitSpeed,
      coop_exit_request_status: coopExitRequest.status,
      coop_exit_request_expires_at: coopExitRequest.expiresAt,
      coop_exit_request_raw_connector_transaction:
        coopExitRequest.rawConnectorTransaction,
      coop_exit_request_raw_coop_exit_transaction:
        coopExitRequest.rawCoopExitTransaction,
      coop_exit_request_coop_exit_txid: coopExitRequest.coopExitTxid,
      coop_exit_request_transfer: coopExitRequest.transfer
        ? coopExitRequest.transfer.toJson()
        : undefined,
    };
  }
  if (obj.typename == "LeavesSwapRequest") {
    const leavesSwapRequest = obj as LeavesSwapRequest;
    return {
      __typename: "LeavesSwapRequest",
      leaves_swap_request_id: leavesSwapRequest.id,
      leaves_swap_request_created_at: leavesSwapRequest.createdAt,
      leaves_swap_request_updated_at: leavesSwapRequest.updatedAt,
      leaves_swap_request_network: leavesSwapRequest.network,
      leaves_swap_request_status: leavesSwapRequest.status,
      leaves_swap_request_total_amount: CurrencyAmountToJson(
        leavesSwapRequest.totalAmount,
      ),
      leaves_swap_request_target_amount: CurrencyAmountToJson(
        leavesSwapRequest.targetAmount,
      ),
      leaves_swap_request_fee: CurrencyAmountToJson(leavesSwapRequest.fee),
      leaves_swap_request_inbound_transfer:
        leavesSwapRequest.inboundTransfer.toJson(),
      leaves_swap_request_outbound_transfer: leavesSwapRequest.outboundTransfer
        ? leavesSwapRequest.outboundTransfer.toJson()
        : undefined,
      leaves_swap_request_expires_at: leavesSwapRequest.expiresAt,
      leaves_swap_request_swap_leaves: leavesSwapRequest.swapLeaves.map((e) =>
        SwapLeafToJson(e),
      ),
    };
  }
  if (obj.typename == "LightningReceiveRequest") {
    const lightningReceiveRequest = obj as LightningReceiveRequest;
    return {
      __typename: "LightningReceiveRequest",
      lightning_receive_request_id: lightningReceiveRequest.id,
      lightning_receive_request_created_at: lightningReceiveRequest.createdAt,
      lightning_receive_request_updated_at: lightningReceiveRequest.updatedAt,
      lightning_receive_request_network: lightningReceiveRequest.network,
      lightning_receive_request_invoice: InvoiceToJson(
        lightningReceiveRequest.invoice,
      ),
      lightning_receive_request_status: lightningReceiveRequest.status,
      lightning_receive_request_transfer: lightningReceiveRequest.transfer
        ? lightningReceiveRequest.transfer.toJson()
        : undefined,
      lightning_receive_request_payment_preimage:
        lightningReceiveRequest.paymentPreimage,
      lightning_receive_request_receiver_identity_public_key:
        lightningReceiveRequest.receiverIdentityPublicKey,
    };
  }
  if (obj.typename == "LightningSendRequest") {
    const lightningSendRequest = obj as LightningSendRequest;
    return {
      __typename: "LightningSendRequest",
      lightning_send_request_id: lightningSendRequest.id,
      lightning_send_request_created_at: lightningSendRequest.createdAt,
      lightning_send_request_updated_at: lightningSendRequest.updatedAt,
      lightning_send_request_network: lightningSendRequest.network,
      lightning_send_request_encoded_invoice:
        lightningSendRequest.encodedInvoice,
      lightning_send_request_fee: CurrencyAmountToJson(
        lightningSendRequest.fee,
      ),
      lightning_send_request_idempotency_key:
        lightningSendRequest.idempotencyKey,
      lightning_send_request_status: lightningSendRequest.status,
      lightning_send_request_transfer: lightningSendRequest.transfer
        ? lightningSendRequest.transfer.toJson()
        : undefined,
      lightning_send_request_payment_preimage:
        lightningSendRequest.paymentPreimage,
    };
  }
  throw new LightsparkException(
    "DeserializationError",
    `Couldn't find a concrete type for interface UserRequest corresponding to the typename=${obj.typename}`,
  );
};

export const FRAGMENT = `
fragment UserRequestFragment on UserRequest {
    __typename
    ... on ClaimStaticDeposit {
        __typename
        claim_static_deposit_id: id
        claim_static_deposit_created_at: created_at
        claim_static_deposit_updated_at: updated_at
        claim_static_deposit_network: network
        claim_static_deposit_credit_amount: credit_amount {
            __typename
            currency_amount_original_value: original_value
            currency_amount_original_unit: original_unit
            currency_amount_preferred_currency_unit: preferred_currency_unit
            currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
            currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
        }
        claim_static_deposit_max_fee: max_fee {
            __typename
            currency_amount_original_value: original_value
            currency_amount_original_unit: original_unit
            currency_amount_preferred_currency_unit: preferred_currency_unit
            currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
            currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
        }
        claim_static_deposit_status: status
        claim_static_deposit_transaction_id: transaction_id
        claim_static_deposit_output_index: output_index
        claim_static_deposit_bitcoin_network: bitcoin_network
        claim_static_deposit_transfer_spark_id: transfer_spark_id
    }
    ... on CoopExitRequest {
        __typename
        coop_exit_request_id: id
        coop_exit_request_created_at: created_at
        coop_exit_request_updated_at: updated_at
        coop_exit_request_network: network
        coop_exit_request_fee: fee {
            __typename
            currency_amount_original_value: original_value
            currency_amount_original_unit: original_unit
            currency_amount_preferred_currency_unit: preferred_currency_unit
            currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
            currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
        }
        coop_exit_request_l1_broadcast_fee: l1_broadcast_fee {
            __typename
            currency_amount_original_value: original_value
            currency_amount_original_unit: original_unit
            currency_amount_preferred_currency_unit: preferred_currency_unit
            currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
            currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
        }
        coop_exit_request_fee_quote: fee_quote {
            id
        }
        coop_exit_request_exit_speed: exit_speed
        coop_exit_request_status: status
        coop_exit_request_expires_at: expires_at
        coop_exit_request_raw_connector_transaction: raw_connector_transaction
        coop_exit_request_raw_coop_exit_transaction: raw_coop_exit_transaction
        coop_exit_request_coop_exit_txid: coop_exit_txid
        coop_exit_request_transfer: transfer {
            __typename
            transfer_total_amount: total_amount {
                __typename
                currency_amount_original_value: original_value
                currency_amount_original_unit: original_unit
                currency_amount_preferred_currency_unit: preferred_currency_unit
                currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
                currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
            }
            transfer_spark_id: spark_id
            transfer_user_request: user_request {
                id
            }
        }
    }
    ... on LeavesSwapRequest {
        __typename
        leaves_swap_request_id: id
        leaves_swap_request_created_at: created_at
        leaves_swap_request_updated_at: updated_at
        leaves_swap_request_network: network
        leaves_swap_request_status: status
        leaves_swap_request_total_amount: total_amount {
            __typename
            currency_amount_original_value: original_value
            currency_amount_original_unit: original_unit
            currency_amount_preferred_currency_unit: preferred_currency_unit
            currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
            currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
        }
        leaves_swap_request_target_amount: target_amount {
            __typename
            currency_amount_original_value: original_value
            currency_amount_original_unit: original_unit
            currency_amount_preferred_currency_unit: preferred_currency_unit
            currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
            currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
        }
        leaves_swap_request_fee: fee {
            __typename
            currency_amount_original_value: original_value
            currency_amount_original_unit: original_unit
            currency_amount_preferred_currency_unit: preferred_currency_unit
            currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
            currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
        }
        leaves_swap_request_inbound_transfer: inbound_transfer {
            __typename
            transfer_total_amount: total_amount {
                __typename
                currency_amount_original_value: original_value
                currency_amount_original_unit: original_unit
                currency_amount_preferred_currency_unit: preferred_currency_unit
                currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
                currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
            }
            transfer_spark_id: spark_id
            transfer_user_request: user_request {
                id
            }
        }
        leaves_swap_request_outbound_transfer: outbound_transfer {
            __typename
            transfer_total_amount: total_amount {
                __typename
                currency_amount_original_value: original_value
                currency_amount_original_unit: original_unit
                currency_amount_preferred_currency_unit: preferred_currency_unit
                currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
                currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
            }
            transfer_spark_id: spark_id
            transfer_user_request: user_request {
                id
            }
        }
        leaves_swap_request_expires_at: expires_at
        leaves_swap_request_swap_leaves: swap_leaves {
            __typename
            swap_leaf_leaf_id: leaf_id
            swap_leaf_raw_unsigned_refund_transaction: raw_unsigned_refund_transaction
            swap_leaf_adaptor_signed_signature: adaptor_signed_signature
            swap_leaf_direct_raw_unsigned_refund_transaction: direct_raw_unsigned_refund_transaction
            swap_leaf_direct_adaptor_signed_signature: direct_adaptor_signed_signature
            swap_leaf_direct_from_cpfp_raw_unsigned_refund_transaction: direct_from_cpfp_raw_unsigned_refund_transaction
            swap_leaf_direct_from_cpfp_adaptor_signed_signature: direct_from_cpfp_adaptor_signed_signature
        }
    }
    ... on LightningReceiveRequest {
        __typename
        lightning_receive_request_id: id
        lightning_receive_request_created_at: created_at
        lightning_receive_request_updated_at: updated_at
        lightning_receive_request_network: network
        lightning_receive_request_invoice: invoice {
            __typename
            invoice_encoded_invoice: encoded_invoice
            invoice_bitcoin_network: bitcoin_network
            invoice_payment_hash: payment_hash
            invoice_amount: amount {
                __typename
                currency_amount_original_value: original_value
                currency_amount_original_unit: original_unit
                currency_amount_preferred_currency_unit: preferred_currency_unit
                currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
                currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
            }
            invoice_created_at: created_at
            invoice_expires_at: expires_at
            invoice_memo: memo
        }
        lightning_receive_request_status: status
        lightning_receive_request_transfer: transfer {
            __typename
            transfer_total_amount: total_amount {
                __typename
                currency_amount_original_value: original_value
                currency_amount_original_unit: original_unit
                currency_amount_preferred_currency_unit: preferred_currency_unit
                currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
                currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
            }
            transfer_spark_id: spark_id
            transfer_user_request: user_request {
                id
            }
        }
        lightning_receive_request_payment_preimage: payment_preimage
        lightning_receive_request_receiver_identity_public_key: receiver_identity_public_key
    }
    ... on LightningSendRequest {
        __typename
        lightning_send_request_id: id
        lightning_send_request_created_at: created_at
        lightning_send_request_updated_at: updated_at
        lightning_send_request_network: network
        lightning_send_request_encoded_invoice: encoded_invoice
        lightning_send_request_fee: fee {
            __typename
            currency_amount_original_value: original_value
            currency_amount_original_unit: original_unit
            currency_amount_preferred_currency_unit: preferred_currency_unit
            currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
            currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
        }
        lightning_send_request_idempotency_key: idempotency_key
        lightning_send_request_status: status
        lightning_send_request_transfer: transfer {
            __typename
            transfer_total_amount: total_amount {
                __typename
                currency_amount_original_value: original_value
                currency_amount_original_unit: original_unit
                currency_amount_preferred_currency_unit: preferred_currency_unit
                currency_amount_preferred_currency_value_rounded: preferred_currency_value_rounded
                currency_amount_preferred_currency_value_approx: preferred_currency_value_approx
            }
            transfer_spark_id: spark_id
            transfer_user_request: user_request {
                id
            }
        }
        lightning_send_request_payment_preimage: payment_preimage
    }
}`;

export const getUserRequestQuery = (id: string): Query<UserRequest> => {
  return {
    queryPayload: `
query GetUserRequest($id: ID!) {
    entity(id: $id) {
        ... on UserRequest {
            ...UserRequestFragment
        }
    }
}

${FRAGMENT}    
`,
    variables: { id },
    constructObject: (data: unknown) =>
      isObject(data) && "entity" in data && isObject(data.entity)
        ? UserRequestFromJson(data.entity)
        : null,
  };
};

export default UserRequest;
