// functions/cf/applyUseEstimateSpendFromUseActions.js
const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// admin.initializeApp() is called once in index.js

// ---------- helpers ----------
const asNum = (v, d = 0) => {
  if (typeof v === 'number' && isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '' && !isNaN(Number(v))) {
    return Number(v);
  }
  return d;
};

const applyUseEstimateSpendFromUseActions = functions
  .region('us-central1')
  .https.onCall(async (data, context) => {
    if (!context.auth || !context.auth.uid) return;

    const db = admin.firestore();
    const uid = context.auth.uid;
    const userRef = db.doc(`users/${uid}`);

    const rawDocRef = data.docRef;   // string path OR { path: string }
    const amountRaw = data.amount;   // numeric or string
    const actionScope = 'OAOA';      // One Action, One Account (Use)

    // --- normalize docRef from client (string or object) ---
    let docPath = null;
    if (typeof rawDocRef === 'string') {
      docPath = rawDocRef;
    } else if (rawDocRef && typeof rawDocRef.path === 'string') {
      docPath = rawDocRef.path;
    }

    if (!docPath || typeof docPath !== 'string') {
      throw new functions.https.HttpsError(
        'invalid-argument',
        'docRef must be a valid path string to user_use_cards_list.'
      );
    }

    const useDocRef = db.doc(docPath);

    const spendRequested = asNum(amountRaw, NaN);
    if (!Number.isFinite(spendRequested) || spendRequested <= 0) {
      throw new functions.https.HttpsError(
        'invalid-argument',
        'amount must be a positive number.'
      );
    }

    try {
      // 1) Read the use-actions doc to get origin card + cycleId
      const useSnap = await useDocRef.get();
      if (!useSnap.exists) {
        throw new functions.https.HttpsError(
          'not-found',
          'Use actions document not found.'
        );
      }

      const useData = useSnap.data() || {};

      // Ownership check
      if (useData.userRef && useData.userRef.path !== userRef.path) {
        throw new functions.https.HttpsError(
          'permission-denied',
          'Use actions document does not belong to this user.'
        );
      }

      const cardRef = useData.DocRefCard;
      const cycleId = useData.cycleId || null;

      if (!cardRef || typeof cardRef.path !== 'string') {
        throw new functions.https.HttpsError(
          'failed-precondition',
          'DocRefCard is missing or invalid on use actions document.'
        );
      }

      let result = null;

      // 2) Apply spend to the underlying credit card + deltaLog
      await db.runTransaction(async (tx) => {
        const cardSnap = await tx.get(cardRef);
        if (!cardSnap.exists) {
          throw new functions.https.HttpsError(
            'not-found',
            'Credit card document not found.'
          );
        }

        const cardData = cardSnap.data() || {};

        // Ownership check on the card as well
        if (cardData.userRef && cardData.userRef.path !== userRef.path) {
          throw new functions.https.HttpsError(
            'permission-denied',
            'Card does not belong to this user.'
          );
        }

        const oldBalance = asNum(cardData.totalBalance, 0);
        const oldVersion = asNum(cardData.liveVersion, 0);

        // Use = adding spend → increase totalBalance
        const spend = spendRequested;
        const newBalance = oldBalance + spend;
        const newVersion = oldVersion + 1;

        // If nothing actually changes, no-op
        if (newBalance === oldBalance) {
          result = {
            applied: 0,
            previousBalance: oldBalance,
            newBalance,
            liveVersion: oldVersion,
          };
          return;
        }

        // Update card
        tx.update(cardRef, {
          totalBalance: newBalance,
          liveVersion: newVersion,
        });

        // Delta log in user_deltaLog_CC
        const deltaRef = db.collection('user_deltaLog_CC').doc();
        const deltaData = {
          prev_Version: oldVersion,
          subs_Version: newVersion,
          field_Changed: 'totalBalance',
          previous_Value: oldBalance,

          // Signed change: new - old (positive for spend)
          amountChanged: newBalance - oldBalance,

          userRef,
          IsChange: true,
          IsUndo: false,

          cycleId,
          actionScope,
          actionName: 'single estimated spend from use actions',
          createdAt: admin.firestore.FieldValue.serverTimestamp(),

          ccRef: cardRef,
        };

        tx.set(deltaRef, deltaData);

        result = {
          applied: spend,
          previousBalance: oldBalance,
          newBalance,
          liveVersion: newVersion,
        };
      });

      return result;
    } catch (err) {
      console.error('applyUseEstimateSpendFromUseActions error:', err);
      if (err instanceof functions.https.HttpsError) throw err;

      throw new functions.https.HttpsError(
        'internal',
        'Failed to apply estimated spend from use actions.'
      );
    }
  });

module.exports = {
  applyUseEstimateSpendFromUseActions,
};
