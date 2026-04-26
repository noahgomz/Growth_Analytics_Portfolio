// functions/cf/deselectAllCardsAndLoans.js
const functions = require('firebase-functions/v1');
const admin     = require('firebase-admin');
// admin.initializeApp() is called once in index.js

module.exports = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth || !context.auth.uid) return;

    const db = admin.firestore();
    const userRef = db.doc(`users/${context.auth.uid}`);

    async function deselectInCollection(colName) {
      const snap = await db
        .collection(colName)
        .where('userRef', '==', userRef)
        .get();

      let updated = 0;
      let scanned = 0;
      let batch = db.batch();
      let ops = 0;
      const MAX_OPS = 450; // stay under 500/commit

      snap.forEach((doc) => {
        scanned++;
        const curr = doc.get('isSelected');
        // Write if missing or not already false
        if (curr !== false) {
          batch.update(doc.ref, { isSelected: false });
          ops++;
          updated++;
          if (ops >= MAX_OPS) {
            // flush & start a new batch
            batch.commit();
            batch = db.batch();
            ops = 0;
          }
        }
      });

      if (ops > 0) {
        await batch.commit();
      }

      return { scanned, updated };
    }

    try {
      const cards = await deselectInCollection('user_card_recommendations');
      const loans = await deselectInCollection('user_loan_recommendations');

      // Return only primitives to avoid recursive encoding issues
      return {
        ok: true,
        cardsScanned: cards.scanned,
        cardsUpdated: cards.updated,
        loansScanned: loans.scanned,
        loansUpdated: loans.updated,
      };
    } catch (err) {
      throw new functions.https.HttpsError(
        'internal',
        err.message || 'Failed to deselect all.'
      );
    }
  }
);
