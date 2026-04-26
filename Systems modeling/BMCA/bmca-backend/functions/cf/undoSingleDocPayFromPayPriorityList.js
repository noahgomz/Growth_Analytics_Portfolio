// functions/cf/undoSingleDocPayFromPayPriorityList.js
const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// admin.initializeApp() is called once in index.js

// ---------- helpers ----------
const asNum = (v, d = 0) => {
  if (typeof v === 'number' && isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '' && !isNaN(Number(v))) return Number(v);
  return d;
};

/**
 * Fully reconstructs resolveOrigin() inline
 * so this CF can work independently.
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

const undoSingleDocPayFromPayPriorityList = functions
  .region('us-central1')
  .https.onCall(async (data, context) => {
    if (!context.auth || !context.auth.uid) {
      throw new functions.https.HttpsError(
        'unauthenticated',
        'Must be signed in.'
      );
    }

    const docRefPath = data.docRef;
    if (!docRefPath || typeof docRefPath !== 'string') {
      throw new functions.https.HttpsError(
        'invalid-argument',
        'docRef (path to pay-priority doc) is required.'
      );
    }

    const db = admin.firestore();
    const userRef = db.doc(`users/${context.auth.uid}`);

    try {
      const payPriorityDocRef = db.doc(docRefPath);
      const paySnap = await payPriorityDocRef.get();

      if (!paySnap.exists) {
        throw new functions.https.HttpsError(
          'not-found',
          'Pay-priority document not found.'
        );
      }

      const payData = paySnap.data() || {};

      // Security: ensure belongs to user
      if (!payData.userRef || payData.userRef.path !== userRef.path) {
        throw new functions.https.HttpsError(
          'permission-denied',
          'You are not allowed to modify this document.'
        );
      }

      const originInfo = resolveOrigin(payData);
      if (!originInfo) {
        return {
          undoApplied: false,
          reason: 'Origin resolution failed for this stockType.',
        };
      }

      const {
        accountRef,
        balanceField,
        deltaLogCollection,
        deltaLogRefField,
      } = originInfo;

      let result = {
        undoApplied: false,
        previousBalance: null,
        restoredBalance: null,
        undoneAmount: 0,
        deltaLogDocPath: null,
        cycleId: null,
        actionScope: null,
        actionName: null,
      };

      await db.runTransaction(async (tx) => {
        const accSnap = await tx.get(accountRef);
        if (!accSnap.exists) {
          result.reason = 'Origin account not found.';
          return;
        }

        const accData = accSnap.data() || {};
        const currentBalance = asNum(accData[balanceField], 0);
        const currentVersion = asNum(accData.liveVersion, 0);

        if (currentVersion <= 0) {
          result.reason = 'No versioned changes exist to undo.';
          return;
        }

        // Find the forward delta that produced currentVersion for this field
        const dlQuery = db
          .collection(deltaLogCollection)
          .where(deltaLogRefField, '==', accountRef)
          .where('subs_Version', '==', currentVersion)
          .where('field_Changed', '==', balanceField)
          .limit(1);

        const dlSnap = await tx.get(dlQuery);
        if (dlSnap.empty) {
          result.reason = 'No deltaLog entry found for this version.';
          return;
        }

        const deltaDoc = dlSnap.docs[0];
        const deltaData = deltaDoc.data();

        const previousBalance = asNum(deltaData.previous_Value, currentBalance);

        // Undo amount = how much was applied: previous - current
        const undoneAmount = previousBalance - currentBalance;
        if (undoneAmount <= 0) {
          result.reason = 'Undo amount is not positive.';
          return;
        }

        // cycle / scope / actionName from original delta
        const cycleId = deltaData.cycleId || null;
        const actionScope = deltaData.actionScope || null;
        const actionName = deltaData.actionName || null;

        const newVersion = currentVersion + 1;

        // Update origin account back to old value, bumping version forward
        const updateData = {};
        updateData[balanceField] = previousBalance;
        updateData.liveVersion = newVersion;
        tx.update(accountRef, updateData);

        // Write undo delta (first-class event, same cycle/scope/actionName)
        const undoDeltaRef = db.collection(deltaLogCollection).doc();
        const now = admin.firestore.Timestamp.now();

        const undoDeltaData = {
          // version lineage
          prev_Version: currentVersion,
          subs_Version: newVersion,
          field_Changed: balanceField,
          previous_Value: currentBalance,
          revertedTo_Value: previousBalance,
          originalDeltaLogRef: deltaDoc.ref,

          // origin linkage
          [deltaLogRefField]: accountRef,

          // cycle + scope + actionName (copied from original delta)
          cycleId,
          actionScope,
          actionName,

          // identity + flags
          userRef,
          IsChange: true,
          IsUndo: true,

          // timestamp
          createdAt: now,
        };

        tx.set(undoDeltaRef, undoDeltaData);

        result.undoApplied = true;
        result.previousBalance = currentBalance;
        result.restoredBalance = previousBalance;
        result.undoneAmount = undoneAmount;
        result.deltaLogDocPath = undoDeltaRef.path;
        result.cycleId = cycleId;
        result.actionScope = actionScope;
        result.actionName = actionName;
      });

      return result;
    } catch (err) {
      console.error('undoSingleDocPayFromPayPriorityList error:', err);
      throw new functions.https.HttpsError(
        'internal',
        'Failed to undo payment.'
      );
    }
  });

// Export the function itself (not an object) so the name stays clean.
module.exports = undoSingleDocPayFromPayPriorityList;
