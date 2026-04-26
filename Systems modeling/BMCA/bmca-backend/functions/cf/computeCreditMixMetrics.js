// functions/cf/computeCreditMixMetrics.js
const functions = require('firebase-functions/v1');
const admin     = require('firebase-admin');
// admin.initializeApp() is called once in index.js

module.exports = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth || !context.auth.uid) {
      return;
    }

    const db = admin.firestore();
    const uid = context.auth.uid;
    const userRef = db.doc(`users/${uid}`);

    // 1) Pull consolidated stocks for this user
    const consoSnap = await db
      .collection('user_stocks_conso')
      .where('userRef', '==', userRef)
      .get();

    // 2) Count credit-mix buckets (open + closed)
    let numRevolving = 0;
    let numInstallment = 0;

    consoSnap.forEach((doc) => {
      const d = doc.data();
      const stock = d?.stock;
      if (!stock) return;

      // Exclude non-account stocks
      if (
        stock === 'user_hard_pulls' ||
        stock === 'user_collections_3rd_party' ||
        stock.endsWith('_late_payments')
      ) {
        return;
      }

      if (stock === 'user_credit_cards' || stock === 'user_HELOCs') {
        // Revolving
        numRevolving += 1;
      } else if (stock === 'user_loans') {
        // Installment
        numInstallment += 1;
      } else {
        // Unknown type -> ignore
      }
    });

    const totalAccounts = numRevolving + numInstallment;
    const toPct4 = (v, tot) =>
      tot > 0 ? Math.round((v / tot) * 1e4) / 1e4 : 0;

    const writeBody = {
      userRef,
      totalAccounts,
      numRevolving,
      numInstallment,
      pctRevolving: toPct4(numRevolving, totalAccounts),
      pctInstallment: toPct4(numInstallment, totalAccounts),
      updated_time: admin.firestore.FieldValue.serverTimestamp(),
      source: 'user_stocks_conso@allAccounts',
    };

    // 3) Idempotent write
    const outRef = db.collection('user_metricsCreditMix').doc(uid);
    const prevSnap = await outRef.get();
    let changed = true;

    if (prevSnap.exists) {
      const p = prevSnap.data() || {};
      changed =
        p.totalAccounts !== writeBody.totalAccounts ||
        p.numRevolving !== writeBody.numRevolving ||
        p.numInstallment !== writeBody.numInstallment ||
        p.pctRevolving !== writeBody.pctRevolving ||
        p.pctInstallment !== writeBody.pctInstallment;
    }

    if (!changed) {
      return {
        status: 'noop',
        totalAccounts,
        numRevolving,
        numInstallment,
        pctRevolving: writeBody.pctRevolving,
        pctInstallment: writeBody.pctInstallment,
      };
    }

    await outRef.set(writeBody, { merge: true });

    return {
      status: 'ok',
      totalAccounts,
      numRevolving,
      numInstallment,
      pctRevolving: writeBody.pctRevolving,
      pctInstallment: writeBody.pctInstallment,
    };
  }
);
