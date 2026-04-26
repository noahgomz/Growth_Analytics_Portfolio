const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// To avoid deployment errors, do not call admin.initializeApp() in your code

exports.conservativeRiskParameters = functions
  .region('us-central1')
  .https.onCall(async (data, context) => {
    if (!context.auth || !context.auth.uid) {
      return;
    }

    const db = admin.firestore();
    const uid = context.auth.uid;

    const userRef = db.collection('users').doc(uid);
    const collRef = db.collection('user_openAndCloseAdjustments');

    const PAGE_SIZE = 500;
    let updatedCount = 0;
    let lastDocId = null;

    while (true) {
      let q = collRef
        .where('userRef', '==', userRef)
        .orderBy(admin.firestore.FieldPath.documentId())
        .limit(PAGE_SIZE);

      if (lastDocId) q = q.startAfter(lastDocId);

      const snap = await q.get();
      if (snap.empty) break;

      let batch = db.batch();
      let ops = 0;

      for (const docSnap of snap.docs) {
        const d = docSnap.data();

        if (
          Object.prototype.hasOwnProperty.call(d, 'Default_Value') &&
          d.User_Value !== d.Default_Value
        ) {
          batch.update(docSnap.ref, {
            User_Value: d.Default_Value,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
          ops += 1;
          updatedCount += 1;
        }

        if (ops >= 450) {
          await batch.commit();
          batch = db.batch();
          ops = 0;
        }
      }

      if (ops > 0) await batch.commit();

      lastDocId = snap.docs[snap.docs.length - 1].id;
      if (snap.size < PAGE_SIZE) break;
    }

    return { updatedCount };
  });
