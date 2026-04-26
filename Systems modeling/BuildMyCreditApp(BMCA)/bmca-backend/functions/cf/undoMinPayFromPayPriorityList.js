// functions/cf/undoMinPayFromPayPriorityList.js
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

// Same origin resolver used in apply CF
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
    if (!context.auth?.uid) {
      throw new functions.https.HttpsError(
        'unauthenticated',
        'Must be signed in.'
      );
    }

    const db = admin.firestore();
    const uid = context.auth.uid;
    const userRef = db.doc(`users/${uid}`);

    try {
      // Fetch user pay priority list
      const ppSnap = await db
        .collection('user_pay_priority_list')
        .where('userRef', '==', userRef)
        .get();

      if (ppSnap.empty) {
        return {
          totalPriorityItems: 0,
          processedItems: 0,
          undoneAccounts: 0,
        };
      }

      let processedItems = 0;
      let undoneAccounts = 0;

      for (const doc of ppSnap.docs) {
        processedItems++;

        const payData = doc.data() || {};
        const originInfo = resolveOrigin(payData);
        if (!originInfo) continue;

        const {
          accountRef,
          balanceField,
          deltaLogCollection,
          deltaLogRefField,
        } = originInfo;

        if (!accountRef || typeof accountRef.path !== 'string') continue;

        // 1️⃣ Find the most recent OAAA delta for this account/field
        const latestOAAASnap = await db
          .collection(deltaLogCollection)
          .where('userRef', '==', userRef)
          .where(deltaLogRefField, '==', accountRef)
          .where('field_Changed', '==', balanceField)
          .where('IsChange', '==', true)
          .where('IsUndo', '==', false)
          .where('actionScope', '==', 'OAAA')
          .orderBy('subs_Version', 'desc')
          .limit(1)
          .get();

        if (latestOAAASnap.empty) continue;

        const oaaaDeltaDoc = latestOAAASnap.docs[0];
        const oaaaData = oaaaDeltaDoc.data() || {};

        const prevVersionBeforeOAAA = asNum(oaaaData.prev_Version, 0);
        const subsVersionOAAA = asNum(
          oaaaData.subs_Version,
          prevVersionBeforeOAAA + 1
        );
        const valueBeforeOAAA = asNum(oaaaData.previous_Value, NaN);
        const cycleId = oaaaData.cycleId || null;
        const actionScope = oaaaData.actionScope || 'OAAA';

        if (!Number.isFinite(valueBeforeOAAA)) continue;

        // 2️⃣ Find the first delta AFTER OAAA whose prev_Version == subs_Version(OAAA)
        const nextSnap = await db
          .collection(deltaLogCollection)
          .where('userRef', '==', userRef)
          .where(deltaLogRefField, '==', accountRef)
          .where('field_Changed', '==', balanceField)
          .where('IsChange', '==', true)
          .where('IsUndo', '==', false)
          .where('prev_Version', '==', subsVersionOAAA)
          .orderBy('subs_Version', 'asc')
          .limit(1)
          .get();

        let newValueAfterOAAA = null;
        if (!nextSnap.empty) {
          const nextData = nextSnap.docs[0].data() || {};
          newValueAfterOAAA = asNum(nextData.previous_Value, NaN);
          if (!Number.isFinite(newValueAfterOAAA)) {
            newValueAfterOAAA = null;
          }
        }

        await db.runTransaction(async (tx) => {
          const accSnap = await tx.get(accountRef);
          if (!accSnap.exists) return;

          const accData = accSnap.data() || {};
          const currentBalance = asNum(accData[balanceField], 0);
          const currentVersion = asNum(accData.liveVersion, 0);

          // If we couldn't infer the "after OAAA" value from logs,
          // assume the account's current balance is the value after OAAA.
          const inferredAfterOAAA =
            newValueAfterOAAA != null ? newValueAfterOAAA : currentBalance;

          // Effect of OAAA at the time it was applied:
          // effect = value_after_OAAA - value_before_OAAA
          const effectOfOAAA = inferredAfterOAAA - valueBeforeOAAA;

          // If effect is zero, nothing to undo
          if (effectOfOAAA === 0) return;

          // Remove only OAAA’s contribution:
          // newBalance = currentBalance - effectOfOAAA
          const newBalance = currentBalance - effectOfOAAA;

          if (newBalance === currentBalance) return;

          const newVersion = currentVersion + 1;

          // Update origin doc
          const updateData = {};
          updateData[balanceField] = newBalance;
          updateData['liveVersion'] = newVersion;
          tx.update(accountRef, updateData);

          // 3️⃣ Write an UNDO delta with lineage + cycleId + actionScope + actionName
          const undoDeltaRef = db.collection(deltaLogCollection).doc();
          const undoDeltaData = {
            prev_Version: currentVersion,
            subs_Version: newVersion,
            field_Changed: balanceField,

            previous_Value: currentBalance,   // before undo
            revertedTo_Value: newBalance,     // after undo

            userRef,
            IsChange: true,
            IsUndo: true,

            // link this undo to the original OAAA delta
            originalDeltaLogRef: oaaaDeltaDoc.ref,

            // keep cycle + scope for lineage
            cycleId,
            actionScope,

            // keep consistent with applyMinPaymentsFromPayPriorityList
            actionName: 'all accounts minimum payment',

            createdAt: admin.firestore.FieldValue.serverTimestamp(),
          };
          undoDeltaData[deltaLogRefField] = accountRef;

          tx.set(undoDeltaRef, undoDeltaData);

          undoneAccounts++;
        });
      }

      return {
        totalPriorityItems: ppSnap.size,
        processedItems,
        undoneAccounts,
      };
    } catch (err) {
      console.error('undoMinPayFromPayPriorityList error:', err);
      throw new functions.https.HttpsError(
        'internal',
        'Failed to undo minimum payments.'
      );
    }
  });
