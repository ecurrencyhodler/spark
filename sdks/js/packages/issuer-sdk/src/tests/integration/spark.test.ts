import {
  WalletConfig,
  ConfigOptions,
  filterTokenBalanceForTokenPublicKey,
} from "@buildonspark/spark-sdk";
import { jest } from "@jest/globals";
import { IssuerSparkWalletTesting } from "../utils/issuer-test-wallet.js";
import { SparkWalletTesting } from "../utils/spark-testing-wallet.js";
import { IssuerSparkWallet } from "../../index.js";

export const TOKENS_V0_SCHNORR_CONFIG: Required<ConfigOptions> = {
  ...WalletConfig.LOCAL,
  tokenTransactionVersion: "V0",
  tokenSignatures: "SCHNORR",
};

export const TOKENS_V1_SCHNORR_CONFIG: Required<ConfigOptions> = {
  ...WalletConfig.LOCAL,
  tokenTransactionVersion: "V1",
  tokenSignatures: "SCHNORR",
};

export const TOKENS_V0_ECDSA_CONFIG: Required<ConfigOptions> = {
  ...WalletConfig.LOCAL,
  tokenSignatures: "ECDSA",
  tokenTransactionVersion: "V0",
};

export const TOKENS_V1_ECDSA_CONFIG: Required<ConfigOptions> = {
  ...WalletConfig.LOCAL,
  tokenSignatures: "ECDSA",
  tokenTransactionVersion: "V1",
};

const TEST_CONFIGS = [
  { name: "TV0E", config: TOKENS_V0_ECDSA_CONFIG },
  { name: "TV0S", config: TOKENS_V0_SCHNORR_CONFIG },
  { name: "TV1E", config: TOKENS_V1_ECDSA_CONFIG },
  { name: "TV1S", config: TOKENS_V1_SCHNORR_CONFIG },
];

const brokenTestFn = process.env.GITHUB_ACTIONS ? it.skip : it;

describe.each(TEST_CONFIGS)(
  "token integration tests - $name",
  ({ name, config }) => {
    jest.setTimeout(80000);

    let sharedIssuerWallet: IssuerSparkWallet;
    let sharedUserWallet: any;
    let sharedTokenPublicKey: string;

    afterEach(async () => {
      await new Promise((resolve) => setTimeout(resolve, 1000));
    });

    it("should fail when minting tokens without creation", async () => {
      const tokenAmount: bigint = 1000n;
      const { wallet } = await IssuerSparkWalletTesting.initialize({
        options: config,
      });

      await expect(wallet.mintTokens(tokenAmount)).rejects.toThrow();
    });

    it("should fail when creation decimal is greater than js MAX_SAFE_INTEGER", async () => {
      const tokenAmount: bigint = 1000n;
      const { wallet } = await IssuerSparkWalletTesting.initialize({
        options: config,
      });

      await expect(
        wallet.createToken({
          tokenName: "2Pow53Decimal",
          tokenTicker: "2P53D",
          decimals: 2 ** 53,
          isFreezable: false,
          maxSupply: tokenAmount,
        }),
      ).rejects.toThrow();
    });

    it("should fail when minting more than max supply", async () => {
      const tokenAmount: bigint = 1000n;
      const { wallet } = await IssuerSparkWalletTesting.initialize({
        options: config,
      });

      await wallet.createToken({
        tokenName: "MST",
        tokenTicker: "MST",
        decimals: 0,
        isFreezable: false,
        maxSupply: 2n,
      });
      await expect(wallet.mintTokens(tokenAmount)).rejects.toThrow();
    });

    it("should mint tokens successfully", async () => {
      const tokenAmount: bigint = 1000n;

      const { wallet: issuerWallet } =
        await IssuerSparkWalletTesting.initialize({
          options: config,
        });
      await issuerWallet.createToken({
        tokenName: `${name}M`,
        tokenTicker: "MIN",
        decimals: 0,
        isFreezable: false,
        maxSupply: 1_000_000n,
      });
      sharedIssuerWallet = issuerWallet;
      sharedTokenPublicKey = await issuerWallet.getIdentityPublicKey();

      const tokenMetadata = await sharedIssuerWallet.getIssuerTokenMetadata();

      const identityPublicKey = await sharedIssuerWallet.getIdentityPublicKey();
      expect(tokenMetadata?.tokenName).toEqual(`${name}M`);
      expect(tokenMetadata?.tokenTicker).toEqual("MIN");
      expect(tokenMetadata?.decimals).toEqual(0);
      expect(tokenMetadata?.maxSupply).toEqual(1000000n);
      expect(tokenMetadata?.isFreezable).toEqual(false);

      // Compare the public key using bytesToHex
      const metadataPubkey = tokenMetadata?.tokenPublicKey;
      expect(metadataPubkey).toEqual(identityPublicKey);

      await sharedIssuerWallet.mintTokens(tokenAmount);

      const tokenBalance = await sharedIssuerWallet.getIssuerTokenBalance();
      expect(tokenBalance.balance).toBeGreaterThanOrEqual(tokenAmount);
    });

    it("should mint and transfer tokens", async () => {
      const tokenAmount: bigint = 1000n;

      const { wallet: issuerWallet } =
        await IssuerSparkWalletTesting.initialize({
          options: config,
        });
      const { wallet: userWallet } = await SparkWalletTesting.initialize({
        options: config,
      });
      await issuerWallet.createToken({
        tokenName: `${name}MTR`,
        tokenTicker: "MTR",
        decimals: 0,
        isFreezable: false,
        maxSupply: 1_000_000n,
      });
      sharedIssuerWallet = issuerWallet;
      sharedUserWallet = userWallet;
      sharedTokenPublicKey = await issuerWallet.getIdentityPublicKey();

      await sharedIssuerWallet.mintTokens(tokenAmount);

      const sharedIssuerBalance =
        await sharedIssuerWallet.getIssuerTokenBalance();
      expect(sharedIssuerBalance).toBeDefined();
      expect(sharedIssuerBalance.tokenIdentifier).toBeDefined();
      await sharedIssuerWallet.transferTokens({
        tokenAmount,
        tokenIdentifier: sharedIssuerBalance.tokenIdentifier!,
        receiverSparkAddress: await sharedUserWallet.getSparkAddress(),
      });

      const balanceObj = await sharedUserWallet.getBalance();
      const destinationBalance = filterTokenBalanceForTokenPublicKey(
        balanceObj?.tokenBalances,
        sharedTokenPublicKey,
      );
      expect(destinationBalance.balance).toBeGreaterThanOrEqual(tokenAmount);
    });

    // it("should announce, mint, get list all transactions, and transfer tokens multiple times, get list all transactions again and check difference", async () => {
    //   const tokenAmount: bigint = 100n;

    //   const { wallet: issuerWallet } =
    //     await IssuerSparkWalletTesting.initialize({
    //       options: config,
    //     });

    //   const { wallet: destinationWallet } = await SparkWalletTesting.initialize(
    //     {
    //       options: config,
    //     },
    //   );

    //   await fundAndAnnounce(issuerWallet, 100000n, 0, `${name}Transfer`, "TTO");

    //   {
    //     const transactions = await issuerWallet.getIssuerTokenActivity();
    //     const amount_of_transactions = transactions.transactions.length;
    //     expect(amount_of_transactions).toEqual(0);
    //   }

    //   await issuerWallet.mintTokens(tokenAmount);

    //   {
    //     const transactions = await issuerWallet.getIssuerTokenActivity();
    //     const amount_of_transactions = transactions.transactions.length;
    //     expect(amount_of_transactions).toEqual(1);
    //   }

    //   await issuerWallet.transferTokens({
    //     tokenAmount,
    //     tokenPublicKey: await issuerWallet.getIdentityPublicKey(),
    //     receiverSparkAddress: await destinationWallet.getSparkAddress(),
    //   });

    //   {
    //     const transactions = await issuerWallet.getIssuerTokenActivity();
    //     const amount_of_transactions = transactions.transactions.length;
    //     expect(amount_of_transactions).toEqual(2);
    //   }

    //   for (let index = 0; index < 100; ++index) {
    //     await issuerWallet.mintTokens(tokenAmount);
    //     await issuerWallet.transferTokens({
    //       tokenAmount,
    //       tokenPublicKey: await issuerWallet.getIdentityPublicKey(),
    //       receiverSparkAddress: await destinationWallet.getSparkAddress(),
    //     });
    //   } // 202 in total

    //   let all_transactions = await issuerWallet.getIssuerTokenActivity(250);
    //   const amount_of_transactions = all_transactions.transactions.length;
    //   expect(amount_of_transactions).toEqual(202);

    //   {
    //     const transactions = await issuerWallet.getIssuerTokenActivity(10);
    //     const amount_of_transactions = transactions.transactions.length;
    //     expect(amount_of_transactions).toEqual(10);
    //   }

    //   {
    //     let hashset_of_all_transactions: Set<String> = new Set();

    //     let transactions = await issuerWallet.getIssuerTokenActivity(10);
    //     let amount_of_transactions = transactions.transactions.length;
    //     expect(amount_of_transactions).toEqual(10);
    //     let page_num = 0;
    //     for (let index = 0; index < transactions.transactions.length; ++index) {
    //       const element = transactions.transactions[index];
    //       if (!(element.transaction === undefined)) {
    //         let hash: String = "";
    //         if (element.transaction.$case === "spark") {
    //           hash = element.transaction.spark.transactionHash;
    //         } else if (element.transaction.$case === "onChain") {
    //           hash = element.transaction.onChain.transactionHash;
    //         }
    //         if (hashset_of_all_transactions.has(hash)) {
    //           expect(
    //             `Dublicate found. Pagination is broken? Index of transaction: ${index} ; page №: ${page_num} ; page size: 10 ; hash_dublicate: ${hash}`,
    //           ).toEqual("");
    //         } else {
    //           hashset_of_all_transactions.add(hash);
    //         }
    //       } else {
    //         expect(
    //           `Transaction is undefined. Something is really wrong. Index of transaction: ${index} ; page №: ${page_num} ; page size: 10`,
    //         ).toEqual("");
    //       }
    //     }

    //     while (!(undefined === transactions.nextCursor)) {
    //       let transactions_2 = await issuerWallet.getIssuerTokenActivity(10, {
    //         lastTransactionHash: hexToBytes(
    //           transactions.nextCursor.lastTransactionHash,
    //         ),
    //         layer: transactions.nextCursor.layer,
    //       });

    //       ++page_num;

    //       for (
    //         let index = 0;
    //         index < transactions_2.transactions.length;
    //         ++index
    //       ) {
    //         const element = transactions_2.transactions[index];
    //         if (!(element.transaction === undefined)) {
    //           let hash: String = "";
    //           if (element.transaction.$case === "spark") {
    //             hash = element.transaction.spark.transactionHash;
    //           } else if (element.transaction.$case === "onChain") {
    //             hash = element.transaction.onChain.transactionHash;
    //           }
    //           if (hashset_of_all_transactions.has(hash)) {
    //             expect(
    //               `Dublicate found. Pagination is broken? Index of transaction: ${index} ; page №: ${page_num} ; page size: 10 ; hash_dublicate: ${hash}`,
    //             ).toEqual("");
    //           } else {
    //             hashset_of_all_transactions.add(hash);
    //           }
    //         } else {
    //           expect(
    //             `Transaction is undefined. Something is really wrong. Index of transaction: ${index} ; page №: ${page_num} ; page size: 10`,
    //           ).toEqual("");
    //         }
    //       }

    //       transactions = transactions_2;
    //     }

    //     expect(hashset_of_all_transactions.size == 202);
    //   }
    // });

    it("should mint and batchtransfer tokens", async () => {
      const tokenAmount: bigint = 999n;

      const { wallet: issuerWallet } =
        await IssuerSparkWalletTesting.initialize({
          options: config,
        });
      await issuerWallet.createToken({
        tokenName: `${name}MBN`,
        tokenTicker: "MBN",
        decimals: 0,
        isFreezable: false,
        maxSupply: 1_000_000n,
      });
      sharedIssuerWallet = issuerWallet;
      sharedTokenPublicKey = await issuerWallet.getIdentityPublicKey();

      const { wallet: destinationWallet } = await SparkWalletTesting.initialize(
        {
          options: config,
        },
      );

      const { wallet: destinationWallet2 } =
        await SparkWalletTesting.initialize({
          options: config,
        });

      const { wallet: destinationWallet3 } =
        await SparkWalletTesting.initialize({
          options: config,
        });

      await sharedIssuerWallet.mintTokens(tokenAmount);
      const sharedIssuerBalance =
        await sharedIssuerWallet.getIssuerTokenBalance();
      expect(sharedIssuerBalance).toBeDefined();
      expect(sharedIssuerBalance.tokenIdentifier).toBeDefined();

      const tokenIdentifier = sharedIssuerBalance.tokenIdentifier!;
      const sourceBalanceBefore = sharedIssuerBalance.balance;

      await sharedIssuerWallet.batchTransferTokens([
        {
          tokenAmount: tokenAmount / 3n,
          tokenIdentifier,
          receiverSparkAddress: await destinationWallet.getSparkAddress(),
        },
        {
          tokenAmount: tokenAmount / 3n,
          tokenIdentifier,
          receiverSparkAddress: await destinationWallet2.getSparkAddress(),
        },
        {
          tokenAmount: tokenAmount / 3n,
          tokenIdentifier,
          receiverSparkAddress: await destinationWallet3.getSparkAddress(),
        },
      ]);

      const sourceBalanceAfter = (
        await sharedIssuerWallet.getIssuerTokenBalance()
      ).balance;
      expect(sourceBalanceAfter).toEqual(sourceBalanceBefore - tokenAmount);

      const balanceObj = await destinationWallet.getBalance();
      const destinationBalance = filterTokenBalanceForTokenPublicKey(
        balanceObj?.tokenBalances,
        sharedTokenPublicKey,
      );
      expect(destinationBalance.balance).toEqual(tokenAmount / 3n);
      const balanceObj2 = await destinationWallet2.getBalance();
      const destinationBalance2 = filterTokenBalanceForTokenPublicKey(
        balanceObj2?.tokenBalances,
        sharedTokenPublicKey,
      );
      expect(destinationBalance2.balance).toEqual(tokenAmount / 3n);
      const balanceObj3 = await destinationWallet3.getBalance();
      const destinationBalance3 = filterTokenBalanceForTokenPublicKey(
        balanceObj3?.tokenBalances,
        sharedTokenPublicKey,
      );
      expect(destinationBalance3.balance).toEqual(tokenAmount / 3n);
    });

    // it("should track token operations in monitoring", async () => {
    //   const tokenAmount: bigint = 1000n;

    //   await sharedIssuerWallet.mintTokens(tokenAmount);
    //   await sharedIssuerWallet.transferTokens({
    //     tokenAmount,
    //     tokenPublicKey: sharedTokenPublicKey,
    //     receiverSparkAddress: await sharedUserWallet.getSparkAddress(),
    //   });

    //   const balanceObj = await sharedUserWallet.getBalance();
    //   const destinationBalance = filterTokenBalanceForTokenPublicKey(
    //     balanceObj?.tokenBalances,
    //     sharedTokenPublicKey,
    //   );
    //   expect(destinationBalance.balance).toBeGreaterThanOrEqual(tokenAmount);

    //   const issuerOperations =
    //     await sharedIssuerWallet.getIssuerTokenActivity();
    //   expect(issuerOperations.transactions.length).toBeGreaterThanOrEqual(2);

    //   let mint_operation = 0;
    //   let transfer_operation = 0;
    //   issuerOperations.transactions.forEach((transaction) => {
    //     if (transaction.transaction?.$case === "spark") {
    //       if (transaction.transaction.spark.operationType === "ISSUER_MINT") {
    //         mint_operation++;
    //       } else if (
    //         transaction.transaction.spark.operationType === "ISSUER_TRANSFER"
    //       ) {
    //         transfer_operation++;
    //       }
    //     }
    //   });
    //   expect(mint_operation).toBeGreaterThanOrEqual(1);
    //   expect(transfer_operation).toBeGreaterThanOrEqual(1);
    // });

    it("it should mint token with 1 max supply without issue", async () => {
      const tokenAmount: bigint = 1n;
      const { wallet: issuerWallet } =
        await IssuerSparkWalletTesting.initialize({
          options: config,
        });

      await issuerWallet.createToken({
        tokenName: "MST",
        tokenTicker: "MST",
        decimals: 0,
        isFreezable: false,
        maxSupply: 1n,
      });
      await issuerWallet.mintTokens(tokenAmount);

      const tokenBalance = await issuerWallet.getIssuerTokenBalance();
      expect(tokenBalance.balance).toEqual(tokenAmount);
    });

    it("it should be able to create a token with name of size equal to MAX_SYMBOL_SIZE", async () => {
      const { wallet: issuerWallet } =
        await IssuerSparkWalletTesting.initialize({
          options: config,
        });

      await issuerWallet.createToken({
        tokenName: "MST",
        tokenTicker: "TESTAA",
        decimals: 0,
        isFreezable: false,
        maxSupply: 1n,
      });
    });

    it("it should be able to create a token with symbol of size equal to MAX_NAME_SIZE", async () => {
      const { wallet: issuerWallet } =
        await IssuerSparkWalletTesting.initialize({
          options: config,
        });

      await issuerWallet.createToken({
        tokenName: "ABCDEFGHIJKLMNOPQ",
        tokenTicker: "MQS",
        decimals: 0,
        isFreezable: false,
        maxSupply: 1n,
      });
    });

    it("should create, mint, freeze and unfreeze tokens", async () => {
      const tokenAmount: bigint = 1000n;
      const { wallet: issuerWallet } =
        await IssuerSparkWalletTesting.initialize({
          options: config,
        });

      await issuerWallet.createToken({
        tokenName: `${name}FRZ`,
        tokenTicker: "FRZ",
        decimals: 0,
        isFreezable: true,
        maxSupply: 100000n,
      });
      await issuerWallet.mintTokens(tokenAmount);

      // Check issuer balance after minting
      const issuerBalanceObjAfterMint =
        await issuerWallet.getIssuerTokenBalance();
      expect(issuerBalanceObjAfterMint).toBeDefined();
      expect(issuerBalanceObjAfterMint.tokenIdentifier).toBeDefined();

      const issuerBalanceAfterMint = issuerBalanceObjAfterMint.balance;
      const tokenIdentifier = issuerBalanceObjAfterMint.tokenIdentifier!;

      expect(issuerBalanceAfterMint).toEqual(tokenAmount);

      const { wallet: userWallet } = await SparkWalletTesting.initialize({
        options: config,
      });
      const userSparkAddress = await userWallet.getSparkAddress();

      await issuerWallet.transferTokens({
        tokenAmount,
        tokenIdentifier,
        receiverSparkAddress: userSparkAddress,
      });
      const issuerBalanceAfterTransfer = (
        await issuerWallet.getIssuerTokenBalance()
      ).balance;
      expect(issuerBalanceAfterTransfer).toEqual(0n);

      const tokenPublicKey = await issuerWallet.getIdentityPublicKey();
      const userBalanceObj = await userWallet.getBalance();
      const userBalanceAfterTransfer = filterTokenBalanceForTokenPublicKey(
        userBalanceObj?.tokenBalances,
        tokenPublicKey,
      );
      expect(userBalanceAfterTransfer.balance).toEqual(tokenAmount);

      // Freeze tokens
      const freezeResponse = await issuerWallet.freezeTokens(userSparkAddress);
      expect(freezeResponse.impactedOutputIds.length).toBeGreaterThan(0);
      expect(freezeResponse.impactedTokenAmount).toEqual(tokenAmount);

      // Unfreeze tokens
      const unfreezeResponse =
        await issuerWallet.unfreezeTokens(userSparkAddress);
      expect(unfreezeResponse.impactedOutputIds.length).toBeGreaterThan(0);
      expect(unfreezeResponse.impactedTokenAmount).toEqual(tokenAmount);
    });

    it("should mint and burn tokens", async () => {
      const tokenAmount: bigint = 200n;

      const { wallet: issuerWallet } =
        await IssuerSparkWalletTesting.initialize({
          options: config,
        });
      await issuerWallet.createToken({
        tokenName: `${name}MBN`,
        tokenTicker: "MBN",
        decimals: 0,
        isFreezable: false,
        maxSupply: 1_000_000n,
      });
      sharedIssuerWallet = issuerWallet;
      sharedTokenPublicKey = await issuerWallet.getIdentityPublicKey();

      await sharedIssuerWallet.mintTokens(tokenAmount);
      const issuerTokenBalance = (
        await sharedIssuerWallet.getIssuerTokenBalance()
      ).balance;
      expect(issuerTokenBalance).toBeGreaterThanOrEqual(tokenAmount);

      await sharedIssuerWallet.burnTokens(tokenAmount);

      const issuerTokenBalanceAfterBurn = (
        await sharedIssuerWallet.getIssuerTokenBalance()
      ).balance;
      expect(issuerTokenBalanceAfterBurn).toEqual(
        issuerTokenBalance - tokenAmount,
      );
    });

    it("should complete full token lifecycle: create, mint, transfer, return, burn", async () => {
      const tokenAmount: bigint = 1000n;

      const { wallet: issuerWallet } =
        await IssuerSparkWalletTesting.initialize({
          options: config,
        });
      await issuerWallet.createToken({
        tokenName: `${name}LFC`,
        tokenTicker: "LFC",
        decimals: 0,
        isFreezable: false,
        maxSupply: 1_000_000n,
      });
      sharedIssuerWallet = issuerWallet;
      sharedTokenPublicKey = await issuerWallet.getIdentityPublicKey();

      const { wallet: userWallet } = await SparkWalletTesting.initialize({
        options: config,
      });

      const initialBalance = (await sharedIssuerWallet.getIssuerTokenBalance())
        .balance;

      await sharedIssuerWallet.mintTokens(tokenAmount);
      const issuerBalanceObjAfterMint =
        await sharedIssuerWallet.getIssuerTokenBalance();
      expect(issuerBalanceObjAfterMint).toBeDefined();
      const issuerBalanceAfterMint = issuerBalanceObjAfterMint.balance;
      expect(issuerBalanceAfterMint).toEqual(initialBalance + tokenAmount);
      expect(issuerBalanceObjAfterMint.tokenIdentifier).toBeDefined();
      const tokenIdentifier = issuerBalanceObjAfterMint.tokenIdentifier!;
      const userSparkAddress = await userWallet.getSparkAddress();

      await sharedIssuerWallet.transferTokens({
        tokenAmount,
        tokenIdentifier,
        receiverSparkAddress: userSparkAddress,
      });

      const issuerBalanceAfterTransfer = (
        await sharedIssuerWallet.getIssuerTokenBalance()
      ).balance;
      expect(issuerBalanceAfterTransfer).toEqual(initialBalance);

      const userBalanceObj = await userWallet.getBalance();
      const userBalanceAfterTransfer = filterTokenBalanceForTokenPublicKey(
        userBalanceObj?.tokenBalances,
        sharedTokenPublicKey,
      );
      expect(userBalanceAfterTransfer.balance).toEqual(tokenAmount);

      await userWallet.transferTokens({
        tokenIdentifier,
        tokenAmount,
        receiverSparkAddress: await sharedIssuerWallet.getSparkAddress(),
      });

      const userBalanceObjAfterTransferBack = await userWallet.getBalance();
      const userBalanceAfterTransferBack = filterTokenBalanceForTokenPublicKey(
        userBalanceObjAfterTransferBack?.tokenBalances,
        sharedTokenPublicKey,
      );

      expect(userBalanceAfterTransferBack.balance).toEqual(0n);

      const issuerTokenBalance = (
        await sharedIssuerWallet.getIssuerTokenBalance()
      ).balance;
      expect(issuerTokenBalance).toEqual(initialBalance + tokenAmount);

      await sharedIssuerWallet.burnTokens(tokenAmount);

      const issuerTokenBalanceAfterBurn = (
        await sharedIssuerWallet.getIssuerTokenBalance()
      ).balance;
      expect(issuerTokenBalanceAfterBurn).toEqual(initialBalance);
    });

    // it("should correctly assign operation types for complete token lifecycle operations", async () => {
    //   const { wallet: userWallet } = await SparkWalletTesting.initialize({
    //     options: config,
    //   });

    //   const tokenAmount = 1000n;

    //   await sharedIssuerWallet.mintTokens(tokenAmount);

    //   await sharedIssuerWallet.transferTokens({
    //     tokenAmount: 500n,
    //     tokenPublicKey: sharedTokenPublicKey,
    //     receiverSparkAddress: await userWallet.getSparkAddress(),
    //   });

    //   await userWallet.transferTokens({
    //     tokenPublicKey: sharedTokenPublicKey,
    //     tokenAmount: 250n,
    //     receiverSparkAddress: await sharedIssuerWallet.getSparkAddress(),
    //   });

    //   // as in userWallet we didn't have burnTokens method, we need to transfer tokens to burn address manually
    //   const BURN_ADDRESS = "02".repeat(33);
    //   const burnAddress = encodeSparkAddress({
    //     identityPublicKey: BURN_ADDRESS,
    //     network: "LOCAL",
    //   });

    //   await userWallet.transferTokens({
    //     tokenPublicKey: sharedTokenPublicKey,
    //     tokenAmount: 250n,
    //     receiverSparkAddress: burnAddress,
    //   });

    //   await sharedIssuerWallet.burnTokens(250n);

    //   const activity = await sharedIssuerWallet.getIssuerTokenActivity();

    //   const mintTransaction = activity.transactions.find(
    //     (tx) =>
    //       tx.transaction?.$case === "spark" &&
    //       tx.transaction.spark.operationType === "ISSUER_MINT",
    //   );

    //   const transferTransaction = activity.transactions.find(
    //     (tx) =>
    //       tx.transaction?.$case === "spark" &&
    //       tx.transaction.spark.operationType === "ISSUER_TRANSFER",
    //   );

    //   const burnTransaction = activity.transactions.find(
    //     (tx) =>
    //       tx.transaction?.$case === "spark" &&
    //       tx.transaction.spark.operationType === "ISSUER_BURN",
    //   );

    //   const transferBackTransaction = activity.transactions.find(
    //     (tx) =>
    //       tx.transaction?.$case === "spark" &&
    //       tx.transaction.spark.operationType === "USER_TRANSFER",
    //   );

    //   const userBurnTransaction = activity.transactions.find(
    //     (tx) =>
    //       tx.transaction?.$case === "spark" &&
    //       tx.transaction.spark.operationType === "USER_BURN",
    //   );

    //   expect(mintTransaction).toBeDefined();
    //   expect(transferTransaction).toBeDefined();
    //   expect(burnTransaction).toBeDefined();
    //   expect(transferBackTransaction).toBeDefined();
    //   expect(userBurnTransaction).toBeDefined();
    // });

    (config.tokenTransactionVersion === "V0" ? it.skip : it)(
      "should create a token using createToken API",
      async () => {
        const { wallet: issuerWallet } =
          await IssuerSparkWalletTesting.initialize({
            options: config,
          });

        const tokenName = `${name}Creatable`;
        const tokenTicker = "CRT";
        const maxSupply = 5000n;
        const decimals = 0;
        const txId = await issuerWallet.createToken({
          tokenName,
          tokenTicker,
          decimals,
          isFreezable: false,
          maxSupply,
        });

        expect(typeof txId).toBe("string");
        expect(txId.length).toBeGreaterThan(0);

        const metadata = await issuerWallet.getIssuerTokenMetadata();
        expect(metadata.tokenName).toEqual(tokenName);
        expect(metadata.tokenTicker).toEqual(tokenTicker);
        expect(metadata.maxSupply).toEqual(maxSupply);
        expect(metadata.decimals).toEqual(decimals);
      },
    );
  },
);
