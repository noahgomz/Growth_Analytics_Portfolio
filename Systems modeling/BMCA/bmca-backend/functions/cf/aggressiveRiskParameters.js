const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// Do NOT call admin.initializeApp()

exports.aggressiveRiskParameters = functions
  .region('us-central1')
  .https.onCall(async (data, context) => {
    if (!context.auth || !context.auth.uid) {
      return { updatedCount: 0, skippedCount: 0, unmatchedCount: 0 };
    }

    const db = admin.firestore();
    const userRef = db.collection('users').doc(context.auth.uid);

    // Aggressive values keyed by Unique_Name
    const aggressiveMap = {
      'Current accounts / lates': 4,
      'Yearly opens allowable': 8,
      'Min interval between opens in LTM': 3,
      'Half-Yearly requests allowable': 6,
      'Min interval between requests in LTM': 3,
      'At least 1 loan': true,
      'Revolving % of total': 70,
      'Max loans allowed': 1,
      'Utilization': 30,
      'Yearly closes allowable': 8,
      'Min interval between closes in LTM': 3,
    };

    let updatedCount = 0;
    let skippedCount = 0;
    let unmatchedCount = 0;

    const pageSize = 500;
    const batchSoftLimit = 450;

    let lastDoc = null;
    let more = true;

    while (more) {
      let q = db
        .collection('user_openAndCloseAdjustments')
        .where('userRef', '==', userRef)
        .orderBy(admin.firestore.FieldPath.documentId())
        .limit(pageSize);

      if (lastDoc) {
        q = q.startAfter(lastDoc);
      }

      const snap = await q.get();
      if (snap.empty) {
        break;
      }

      let batch = db.batch();
      let ops = 0;

      for (const doc of snap.docs) {
        const d = doc.data() || {};
        const uniqueName = d.Unique_Name;

        // Skip if identifier missing or not mapped
        if (!uniqueName || !(uniqueName in aggressiveMap)) {
          unmatchedCount += 1;
          continue;
        }

        const aggressiveValue = aggressiveMap[uniqueName];

        // Skip null / undefined mappings
        if (aggressiveValue === null || aggressiveValue === undefined) {
          unmatchedCount += 1;
          continue;
        }

        const currentUserValue = d.User_Value;

        // Idempotent guard
        if (currentUserValue === aggressiveValue) {
          skippedCount += 1;
          continue;
        }

        batch.update(doc.ref, {
          User_Value: aggressiveValue,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        });

        updatedCount += 1;
        ops += 1;

        if (ops >= batchSoftLimit) {
          await batch.commit();
          batch = db.batch();
          ops = 0;
        }
      }

      if (ops > 0) {
        await batch.commit();
      }

      lastDoc = snap.docs[snap.docs.length - 1];
      more = snap.size === pageSize;
    }

    return { updatedCount, skippedCount, unmatchedCount };
  });
