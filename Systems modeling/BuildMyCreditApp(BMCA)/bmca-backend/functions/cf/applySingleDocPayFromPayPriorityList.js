// functions/cf/applySingleDocPayFromPayPriorityList.js
const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');

// ---------- helpers ----------
const asNum = (v, d = 0) => {
  if (typeof v === 'number' && isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '' && !isNaN(Number(v))) return Number(v);
  return d;
};

const clamp = (val, min, max) => Math.min(Math.max(val, min), max);

function getMinPayment(payDocData) {
  const ab = payDocData.allocationBreakdown || {};
  const stockType = payDocData.stockType;

  switch (stockType) {
    case 'user_credit_cards':
    case 'user_loans':
      return asNum(ab.minPayment, asNum(payDocData.minPayment, 0));

    case 'user_credit_cards_late_payments':
      return asNum(ab.collections, asNum(payDocData.amount, 0));

    case 'user_loans_late_payments':
      return asNum(ab.lateSeverityPayment, asNum(payDocData.amount, 0));

    case 'user_collections_3rd_party':
      return asNum(ab.collections, asNum(payDocData.amount, 0));

    default:
      return 0;
  }
}

function resolveOrigin(payDocData) {
  const stockType = payDocData.stockType;
  const origin = payDocData.originDocRef || {};

  switch (stockType) {
    case 'user_credit_cards':
      if (!origin.CardsDocRef) return null;
      return {
        accountRef: origin.CardsDocRef,
        balanceField: 'totalBalance',
        deltaLogCollection: 'user_deltaLog_CC',
        deltaLogRefField: 'ccRef',
      };

    case 'user_loans':
      if (!origin.LoansDocRef) return null;
      return {
        accountRef: origin.LoansDocRef,
        balanceField: 'balance',
        deltaLogCollection: 'user_deltaLog_Loans',
        deltaLogRefField: 'loanRef',
      };

    case 'user_credit_cards_late_payments':
      if (!origin.CardLatesOrCollectionsDocRef) return null;
      return {
        accountRef: origin.CardLatesOrCollectionsDocRef,
        balanceField: 'amount',
        deltaLogCollection: 'user_deltaLog_CClates',
        deltaLogRefField: 'ccLateRef',
      };

    case 'user_loans_late_payments':
      if (!origin.LoanLatesOrCollectionsDocRef) return null;
      return {
        accountRef: origin.LoanLatesOrCollectionsDocRef,
        balanceField: 'amount',
        deltaLogCollection: 'user_deltaLog_LoansLates',
        deltaLogRefField: 'loanLateRef',
      };

    case 'user_collections_3rd_party':
      if (!origin.Collections3rdPartDocRef) return null;
      return {
        accountRef: origin.Collections3rdPartDocRef,
        balanceField: 'amount',
        deltaLogCollection: 'user_deltaLog_3rdPartyCollections',
        deltaLogRefField: 'thirdPartyCollectionRef',
      };

    default:
      return null;
  }
}

// ---------- MAIN CF ----------
const applySingleDocPayFromPayPriorityList = functions
  .region('us-central1')
  .https.onCall(async (data, context) => {
    if (!context.auth || !context.auth.uid) {
      throw new functions.https.HttpsError(
        'unauthenticated',
        'Must be signed in.'
      );
    }

    const db = admin.firestore();
    const uid = context.auth.uid;
    const userRef = db.doc(`users/${uid}`);

    const payPriorityDocPath = data.docRef;
    if (typeof payPriorityDocPath !== 'string' || !payPriorityDocPath) {
      throw new functions.https.HttpsError(
        'invalid-argument',
        'docRef (path string to user_pay_priority_list doc) is required.'
      );
    }

    try {
      const payPriorityDocRef = db.doc(payPriorityDocPath);
      const paySnap = await payPriorityDocRef.get();

      if (!paySnap.exists) {
        throw new functions.https.HttpsError('not-found', 'Pay-priority document not found.');
      }

      const payData = paySnap.data() || {};

      // Permission check
      if (!payData.userRef || payData.userRef.path !== userRef.path) {
        throw new functions.https.HttpsError('permission-denied', 'This document is not yours.');
      }

      const cycleId = typeof payData.cycleId === 'string' ? payData.cycleId : null;
      const actionScope = "OAOA"; // one document, one account (single pay)

      const minPayRaw = getMinPayment(payData);
      if (minPayRaw <= 0) {
        return {
          payPriorityDocPath,
          paymentApplied: false,
          reason: 'No positive payment amount for this item.',
        };
      }

      const originInfo = resolveOrigin(payData);
      if (!originInfo) {
        return {
          payPriorityDocPath,
          paymentApplied: false,
          reason: 'Origin account not wired for this stockType.',
        };
      }

      const {
        accountRef,
        balanceField,
        deltaLogCollection,
        deltaLogRefField,
      } = originInfo;

      if (!accountRef || typeof accountRef.path !== 'string') {
        return {
          payPriorityDocPath,
          paymentApplied: false,
          reason: 'Origin account reference is invalid.',
        };
      }

      let result = {
        payPriorityDocPath,
        paymentApplied: false,
        amountApplied: 0,
        previousBalance: null,
        newBalance: null,
      };

      await db.runTransaction(async (tx) => {
        const accSnap = await tx.get(accountRef);
        if (!accSnap.exists) {
          result.reason = 'Origin account does not exist.';
          return;
        }

        const accData = accSnap.data() || {};
        const oldBalance = asNum(accData[balanceField], 0);
        if (oldBalance <= 0) {
          result.reason = 'Origin account balance is zero or negative.';
          return;
        }

        const payment = clamp(minPayRaw, 0, oldBalance);
        if (payment <= 0) {
          result.reason = 'Computed payment is not positive.';
          return;
        }

        const newBalance = oldBalance - payment;
        const oldVersion = asNum(accData.liveVersion, 0);
        const newVersion = oldVersion + 1;

        if (newBalance === oldBalance) {
          result.reason = 'No net change to balance.';
          return;
        }

        // Update account
        tx.update(accountRef, {
          [balanceField]: newBalance,
          liveVersion: newVersion,
        });

        // Delta log
        const deltaRef = db.collection(deltaLogCollection).doc();
        const deltaData = {
          prev_Version: oldVersion,
          subs_Version: newVersion,
          field_Changed: balanceField,
          previous_Value: oldBalance,
          amountChanged: payment,
          cycleId,
          actionScope,
          actionName: "single minimum payment",
          userRef,
          IsChange: true,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        };
        deltaData[deltaLogRefField] = accountRef;
        tx.set(deltaRef, deltaData);

        result.paymentApplied = true;
        result.amountApplied = payment;
        result.previousBalance = oldBalance;
        result.newBalance = newBalance;
        result.deltaLogDocPath = deltaRef.path;
      });

      return result;
    } catch (err) {
      console.error('applySingleDocPayFromPayPriorityList error:', err);
      throw new functions.https.HttpsError(
        'internal',
        'Failed to apply payment for single pay-priority item.'
      );
    }
  });

// ---------- EXPORT ----------
module.exports = applySingleDocPayFromPayPriorityList;
