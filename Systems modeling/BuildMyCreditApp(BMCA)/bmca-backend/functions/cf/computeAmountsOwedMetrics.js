// functions/cf/computeAmountsOwedMetrics.js
const functions = require('firebase-functions/v1');
const admin     = require('firebase-admin');
// admin.initializeApp() is called once in index.js

module.exports = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth || !context.auth.uid) return;

    const uid = context.auth.uid;
    const db = admin.firestore();
    const userRef = db.doc(`users/${uid}`);

    // helper: normalize numbers (finite + rounding to 6 dp)
    const n = (x) => {
      const v = Number(x ?? 0);
      if (!Number.isFinite(v)) return 0;
      return Math.round(v * 1e6) / 1e6;
    };

    // 1) Read source
    const snap = await db
      .collection('user_stocks_conso')
      .where('userRef', '==', userRef)
      .get();

    // 2) Aggregate (open accounts only; lateness doesn’t matter)
    let cardBal = 0,
      cardLimit = 0,
      loanBal = 0,
      loanLimit = 0;

    snap.forEach((doc) => {
      const d = doc.data();
      if (!d?.isOpen) return;

      if (d.stock === 'user_credit_cards') {
        cardBal += Number(d.amountsOwed ?? 0);
        cardLimit += Number(d.creditLimit ?? 0);
      } else if (d.stock === 'user_loans') {
        loanBal += Number(d.amountsOwed ?? 0);
        loanLimit += Number(d.creditLimit ?? 0);
      }
    });

    // 3) Compute utilizations (safe divide)
    const revolvingUtilization =
      cardLimit > 0 ? cardBal / cardLimit : 0;
    const installmentUtilization =
      loanLimit > 0 ? loanBal / loanLimit : 0;
    const totalDenom = cardLimit + loanLimit;
    const totalUtilization =
      totalDenom > 0 ? (cardBal + loanBal) / totalDenom : 0;

    // 4) Normalize
    const nextMetrics = {
      cardBal: n(cardBal),
      cardLimit: n(cardLimit),
      revolvingUtilization: n(revolvingUtilization),
      loanBal: n(loanBal),
      loanLimit: n(loanLimit),
      installmentUtilization: n(installmentUtilization),
      totalUtilization: n(totalUtilization),
    };

    // 5) Compare with existing; only write if changed
    const docRef = db.collection('user_metricsAmountsOwed').doc(uid);
    const existingSnap = await docRef.get();
    const prev = existingSnap.exists ? existingSnap.data() : null;

    const changed =
      !prev ||
      nextMetrics.cardBal !== n(prev.cardBal) ||
      nextMetrics.cardLimit !== n(prev.cardLimit) ||
      nextMetrics.revolvingUtilization !==
        n(prev.revolvingUtilization) ||
      nextMetrics.loanBal !== n(prev.loanBal) ||
      nextMetrics.loanLimit !== n(prev.loanLimit) ||
      nextMetrics.installmentUtilization !==
        n(prev.installmentUtilization) ||
      nextMetrics.totalUtilization !== n(prev.totalUtilization);

    if (changed) {
      await docRef.set(
        {
          ...nextMetrics,
          userRef, // ok to store DocumentReference in Firestore
          updated_time: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    }

    // Return only plain JSON (no DocumentReference/sentinels)
    return {
      success: true,
      wrote: changed,
      metrics: { ...nextMetrics, userPath: `users/${uid}` },
    };
  }
);
