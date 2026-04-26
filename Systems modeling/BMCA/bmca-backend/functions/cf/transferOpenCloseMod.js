// functions/cf/transferOpenCloseMod.js
const functions = require('firebase-functions/v1');
const admin     = require('firebase-admin');
// admin.initializeApp() runs once in index.js

module.exports = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth || !context.auth.uid) {
      return; // or throw new HttpsError('unauthenticated', ...)
    }

    try {
      const uid = context.auth.uid;
      const db = admin.firestore();

      const srcSnap = await db.collection('openAndCloseAdjustments').get();
      if (srcSnap.empty) {
        return { written: 0, skipped: 0, skippedByUnique: 0 };
      }

      const userRef = db.doc(`users/${uid}`);

      // Pull existing user docs (Uniqueness check)
      const existingSnap = await db.collection('user_openAndCloseAdjustments')
        .where('userRef', '==', userRef)
        .select('Unique_Name')
        .get();

      const existingNames = new Set(
        existingSnap.docs
          .map(d => d.data()?.Unique_Name)
          .filter(v => v != null)
      );

      let batch = db.batch();
      let opCount = 0;
      let skipped = 0;
      let skippedByUnique = 0;

      const now = admin.firestore.FieldValue.serverTimestamp();

      for (const doc of srcSnap.docs) {
        const src = doc.data() || {};
        const uniqueName = src.Unique_Name;

        // Enforce uniqueness per user
        if (uniqueName !== undefined && existingNames.has(uniqueName)) {
          skippedByUnique++;
          continue;
        }

        // Build base object to compare + write
        const targetBase = {};

        if (src.Action_Type !== undefined) targetBase.Action_Type = src.Action_Type;
        if (src.Sub_Action_Type !== undefined) targetBase.Sub_Action_Type = src.Sub_Action_Type;
        if (src.Type !== undefined) targetBase.Type = src.Type;
        if (src.Unique_Name !== undefined) targetBase.Unique_Name = src.Unique_Name;
        if (src.modType !== undefined) targetBase.modType = src.modType;
        if (src.Default_Interval !== undefined) targetBase.Default_Interval = src.Default_Interval;
        if (src.Default_Interval_Unit !== undefined) targetBase.Default_Interval_Unit = src.Default_Interval_Unit;
        if (src.Default_Value !== undefined) targetBase.Default_Value = src.Default_Value;

        // initial user override values == defaults
        if (src.Default_Value !== undefined) targetBase.User_Value = src.Default_Value;
        if (src.Default_Interval !== undefined) targetBase.User_Interval = src.Default_Interval;
        if (src.Default_Interval_Unit !== undefined) targetBase.User_Interval_Unit = src.Default_Interval_Unit;

        targetBase.userRef = userRef;
        targetBase.srcId = doc.id;

        const destId = `${uid}_${doc.id}`;
        const destRef = db.collection('user_openAndCloseAdjustments').doc(destId);

        // If already exists, check whether equal → skip
        const existingDest = await destRef.get();
        if (existingDest.exists) {
          const existing = existingDest.data() || {};

          let equal = true;
          for (const [k, v] of Object.entries(targetBase)) {
            const cur = existing[k];

            if (k === 'userRef') {
              const curPath = cur?.path || null;
              const tgtPath = targetBase.userRef.path;
              if (curPath !== tgtPath) { equal = false; break; }
              continue;
            }

            if (typeof v === 'object' && v !== null) {
              if (JSON.stringify(cur) !== JSON.stringify(v)) {
                equal = false;
                break;
              }
            } else {
              if (cur !== v) {
                equal = false;
                break;
              }
            }
          }

          if (equal) {
            skipped++;
            continue;
          }
        }

        // Only if writing → stamp
        const target = {
          ...targetBase,
          updated_time: now
        };

        if (data?.addCreatedTime === true && !existingDest.exists) {
          target.created_time = now;
        }

        batch.set(destRef, target, { merge: true });
        opCount++;

        // Prevent duplicates later in this same run
        if (uniqueName !== undefined) existingNames.add(uniqueName);

        if (opCount % 450 === 0) {
          await batch.commit();
          batch = db.batch();
        }
      }

      if (opCount % 450 !== 0) {
        await batch.commit();
      }

      return { written: opCount, skipped, skippedByUnique };

    } catch (err) {
      return { error: true, message: err?.message || String(err) };
    }
  }
);
