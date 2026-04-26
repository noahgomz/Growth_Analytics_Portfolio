// functions/cf/orchestrateActionRecommendations.js
const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// admin.initializeApp() is called once in index.js

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
    const userRef = db.collection('users').doc(uid);

    // -------------------------------------------------
    // Step 1: Read all existing cycle meta docs
    // -------------------------------------------------
    const planMetaSnap = await userRef.collection('plan_meta').get();

    // Determine highest existing cycleID
    let highestCycleId = -1;
    planMetaSnap.forEach((doc) => {
      const d = doc.data() || {};
      const idNum = d.cycleID ? Number(d.cycleID) : NaN;
      if (!isNaN(idNum)) {
        highestCycleId = Math.max(highestCycleId, idNum);
      }
    });

    const nextCycleId = highestCycleId + 1;
    const cycleIdStr = nextCycleId.toString();

    // -------------------------------------------------
    // Step 2: Write new cycle meta doc
    // -------------------------------------------------
    const now = new Date();
    const monthKey = `${now.getFullYear()}-${String(
      now.getMonth() + 1
    ).padStart(2, '0')}`;

    const docId = `CycleID-${cycleIdStr}`;
    const metaRef = userRef.collection('plan_meta').doc(docId);

    await metaRef.set(
      {
        cycleID: cycleIdStr,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        monthKey,
        actionGroupsIncluded: [], // intentionally empty for now
      },
      { merge: true }
    );

    // -------------------------------------------------
    // Step 3: Return JUST the cycleId string
    // -------------------------------------------------
    return cycleIdStr;
  });
