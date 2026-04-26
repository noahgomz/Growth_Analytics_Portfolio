// functions/cf/applyTotalBudgetFromPayPriorityList.js
const functions = require('firebase-functions/v1');
const admin     = require('firebase-admin');
// admin.initializeApp() is called once in index.js

// ---------- helpers ----------
const asNum = (v, d = 0) => {
  if (typeof v === 'number' && isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '' && !isNaN(Number(v))) return Number(v);
  return d;
};

const clamp = (val, min, max) => Math.min(Math.max(val, min), max);

/**
 * Sum ALL positive numeric allocations in allocationBreakdown.
 * If empty/zero, fall back to sensible fields so it still does “something.”
 */
function getTotalAllocation(payDocData) {
  const ab = payDocData.allocationBreakdown || {};
  let total = 0;

  for (const [k, v] of Object.entries(ab)) {
    const n = asNum(v, 0);
    if (n > 0) total += n;
  }

  if (total <= 0) {
    const stockType = payDocData.stockType;
    switch (stockType) {
      case 'user_credit_cards':
      case 'user_loans':
        total = asNum(payDocData.minPayment, 0);
        break;
      case 'user_credit_cards_late_payments':
      case 'user_loans_late_payments':
      case 'user_collections_3rd_party':
        total = asNum(payDocData.amount, 0);
        break;
      default:
        total = 0;
    }
  }
  return total;
}

/**
 * Resolve origin doc + metadata for updates & delta logs.
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

    // Same batching model as min-pay CF:
    // cycleId from the pay-priority doc, plus actionScope for OAAA-style apply
    const actionScope = 'OAAA'; // One Action, All Accounts – PayTotal
    const actionName  = 'all accounts total allocation payment';

    try {
      // Pull all pay-priority items for this user
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
          totalApplied: 0,
        };
      }

      let processedItems = 0;
      let updatedAccounts = 0;
      let totalApplied = 0;

      // Sequential processing to keep transactions tame
      for (const doc of ppSnap.docs) {
        const payData = doc.data() || {};
        processedItems += 1;

        const allocation = getTotalAllocation(payData);
        if (allocation <= 0) continue;

        const originInfo = resolveOrigin(payData);
        if (!originInfo) continue;

        const {
          accountRef,
          balanceField,
          deltaLogCollection,
          deltaLogRefField,
        } = originInfo;

        if (!accountRef || typeof accountRef.path !== 'string') continue;

        // Cycle is carried on each pay-priority doc
        const docCycleId = payData.cycleId || null;

        await db.runTransaction(async (tx) => {
          const accountSnap = await tx.get(accountRef);
          if (!accountSnap.exists) return;

          const accData = accountSnap.data() || {};
          const oldBalance = asNum(accData[balanceField], 0);
          if (oldBalance <= 0) return;

          const payment = clamp(allocation, 0, oldBalance);
          if (payment <= 0) return;

          const newBalance = oldBalance - payment;
          const oldVersion = asNum(accData.liveVersion, 0);
          const newVersion = oldVersion + 1;

          // No-op protection
          if (newBalance === oldBalance && newVersion === oldVersion) return;

          // Update origin account
          const updateData = {};
          updateData[balanceField] = newBalance;
          updateData.liveVersion = newVersion;
          tx.update(accountRef, updateData);

          // Delta log – match min-pay CF structure + actionName
          const deltaRef = db.collection(deltaLogCollection).doc();
          const deltaData = {
            prev_Version: oldVersion,
            subs_Version: newVersion,
            field_Changed: balanceField,
            previous_Value: oldBalance,

            // signed delta (usually negative)
            amountChanged: newBalance - oldBalance,

            userRef,
            IsChange: true,
            IsUndo: false,

            cycleId: docCycleId,
            actionScope,
            actionName, // <-- NEW FIELD

            createdAt: admin.firestore.FieldValue.serverTimestamp(),
          };
          deltaData[deltaLogRefField] = accountRef;

          tx.set(deltaRef, deltaData);

          totalApplied += payment;
          updatedAccounts += 1;
        });
      }

      // FlutterFlow-safe return (no DocRefs)
      return {
        cycleId: null, // multiple cycles may be present in the list
        totalPriorityItems: ppSnap.size,
        processedItems,
        updatedAccounts,
        totalApplied,
      };
    } catch (err) {
      console.error('applyTotalBudgetFromPayPriorityList error:', err);
      throw new functions.https.HttpsError(
        'internal',
        'Failed to apply total budget allocations.'
      );
    }
  });
