// functions/cf/applyMinPaymentsFromPayPriorityList.js
const functions = require('firebase-functions/v1');
const admin     = require('firebase-admin');
// admin.initializeApp() is called once in index.js

// ---------- helpers ----------
const asNum = (v, d = 0) => {
  if (typeof v === 'number' && isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '' && !isNaN(Number(v))) {
    return Number(v);
  }
  return d;
};

const clamp = (val, min, max) => Math.min(Math.max(val, min), max);

/**
 * Extract the minimum-payment-style amount for this pay-priority item.
 */
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

/**
 * Map pay-priority doc → origin account reference + deltaLog metadata.
 */
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
      if (!origin.ThirdPartyCollectionRef) return null;
      return {
        accountRef: origin.ThirdPartyCollectionRef,
        balanceField: 'amount',
        deltaLogCollection: 'user_deltaLog_3rdPartyCollections',
        deltaLogRefField: 'thirdPartyCollectionRef',
      };

    default:
      return null;
  }
}

module.exports = functions
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

    // batching model: cycleId + actionScope
    const actionScope = 'OAAA'; // One Action, All Accounts – PayMin
    const actionName  = 'all accounts minimum payment';

    try {
      const ppSnap = await db
        .collection('user_pay_priority_list')
        .where('userRef', '==', userRef)
        .get();

      if (ppSnap.empty) {
        return {
          cycleId: null,
          totalPriorityItems: 0,
          processedItems: 0,
          updatedAccounts: 0,
        };
      }

      let processedItems = 0;
      let updatedAccounts = 0;

      for (const doc of ppSnap.docs) {
        processedItems++;

        const payData = doc.data() || {};
        const minPayRaw = getMinPayment(payData);
        if (minPayRaw <= 0) continue;

        const docCycleId = payData.cycleId || null;

        const originInfo = resolveOrigin(payData);
        if (!originInfo) continue;

        const {
          accountRef,
          balanceField,
          deltaLogCollection,
          deltaLogRefField,
        } = originInfo;

        if (!accountRef || typeof accountRef.path !== 'string') continue;

        await db.runTransaction(async (tx) => {
          const accSnap = await tx.get(accountRef);
          if (!accSnap.exists) return;

          const accData = accSnap.data() || {};
          const oldBalance = asNum(accData[balanceField], 0);
          if (oldBalance <= 0) return;

          const payment = clamp(minPayRaw, 0, oldBalance);
          if (payment <= 0) return;

          const newBalance = oldBalance - payment;
          const oldVersion = asNum(accData.liveVersion, 0);
          const newVersion = oldVersion + 1;

          if (newBalance === oldBalance) return;

          tx.update(accountRef, {
            [balanceField]: newBalance,
            liveVersion: newVersion,
          });

          const deltaRef = db.collection(deltaLogCollection).doc();
          const deltaData = {
            prev_Version: oldVersion,
            subs_Version: newVersion,
            field_Changed: balanceField,
            previous_Value: oldBalance,
            amountChanged: newBalance - oldBalance, // signed delta

            userRef,
            IsChange: true,
            IsUndo: false,

            cycleId: docCycleId,
            actionScope,
            actionName, // <-- NEW FIELD

            createdAt: admin.firestore.FieldValue.serverTimestamp(),
          };

          // attach origin reference
          deltaData[deltaLogRefField] = accountRef;

          tx.set(deltaRef, deltaData);

          updatedAccounts++;
        });
      }

      return {
        cycleId: null,
        totalPriorityItems: ppSnap.size,
        processedItems,
        updatedAccounts,
      };
    } catch (err) {
      console.error(
        'applyMinPaymentsFromPayPriorityList error:',
        err
      );
      throw new functions.https.HttpsError(
        'internal',
        'Failed to apply minimum payments.'
      );
    }
  });
