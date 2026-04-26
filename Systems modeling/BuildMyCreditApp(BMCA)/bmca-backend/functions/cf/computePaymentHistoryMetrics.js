// functions/cf/computePaymentHistoryMetrics.js
const functions = require('firebase-functions/v1');
const admin     = require('firebase-admin');
// admin.initializeApp() is called once in index.js

module.exports = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth || !context.auth.uid) return;

    const uid = context.auth.uid;
    const db = admin.firestore();
    const userRef = db.doc(`users/${uid}`);

    // ---------- Helpers ----------
    const nowMs = Date.now();

    const asDate = (val) => {
      if (!val) return null;
      if (typeof val.toDate === 'function') return val.toDate();
      if (typeof val === 'object' && val.__time__) return new Date(val.__time__);
      if (typeof val === 'string' || typeof val === 'number') return new Date(val);
      return null;
    };

    const daysBetween = (d) =>
      d ? (nowMs - d.getTime()) / 86400000 : null;

    const sevScore = (severity) => {
      if (!severity) return 0;
      const s = String(severity).trim().toLowerCase();
      if (s === 'collection') return 10000;
      const n = parseInt(s, 10);
      return Number.isFinite(n) ? n : 0;
    };

    const round2 = (x) =>
      x == null ? null : Number(x.toFixed(2));

    const stableJSON = (obj) =>
      JSON.stringify(obj, Object.keys(obj).sort());

    const deepEqual = (a, b) => stableJSON(a) === stableJSON(b);

    const isLateOrCollectionStock = (stock) =>
      typeof stock === 'string' &&
      (stock.endsWith('_late_payments') || stock === 'user_collections_3rd_party');

    const isAccountStock = (stock) =>
      typeof stock === 'string' &&
      (stock === 'user_credit_cards' || stock === 'user_loans');

    const computeGroupMetrics = (arr) => {
      const out = {
        count: 0,
        totalAmountOwed: 0,
        averageAgeDays: null,
        mostRecentDate: null,
        oldestDate: null,
        mostSevere_label: null,
        mostSevere_score: null,
      };
      if (!arr.length) return out;

      out.count = arr.length;
      let totalAmt = 0;
      let mostSevere = { label: null, score: -1 };
      const ages = [];
      let mostRecent = null;
      let oldest = null;

      for (const d of arr) {
        const amt = Number(d.amountsOwed ?? 0);
        if (Number.isFinite(amt)) totalAmt += amt;

        const label = d.severity ?? null;
        const score = sevScore(d.severity);
        if (score > mostSevere.score) {
          mostSevere = { label: label ?? 'Unknown', score };
        }

        const dt = asDate(d.DOFRecord);
        if (dt && !isNaN(dt)) {
          ages.push(daysBetween(dt));
          if (!mostRecent || dt > mostRecent) mostRecent = dt;
          if (!oldest || dt < oldest) oldest = dt;
        }
      }

      const validAges = ages.filter((x) => Number.isFinite(x));
      const avgAge = validAges.length
        ? validAges.reduce((a, b) => a + b, 0) / validAges.length
        : null;

      out.totalAmountOwed = round2(totalAmt);
      out.averageAgeDays = avgAge != null ? round2(avgAge) : null;
      out.mostRecentDate = mostRecent || null;
      out.oldestDate = oldest || null;
      out.mostSevere_label = mostSevere.label;
      out.mostSevere_score = mostSevere.score >= 0 ? mostSevere.score : null;

      return out;
    };

    // ---------- Load source ----------
    const consoSnap = await db
      .collection('user_stocks_conso')
      .where('userRef', '==', userRef)
      .get();

    const accountDocs = [];
    const paidDocs = [];
    const unpaidDocs = [];

    consoSnap.forEach((doc) => {
      const d = doc.data() || {};
      const stock = d.stock || '';
      if (isAccountStock(stock)) {
        accountDocs.push(d);
      } else if (isLateOrCollectionStock(stock)) {
        (d.isPaid ? paidDocs : unpaidDocs).push(d);
      }
    });

    // ---------- Compute ----------
    const currentAccountsCount = accountDocs.filter(
      (d) => d.isCurrent === true
    ).length;

    const unpaid = computeGroupMetrics(unpaidDocs);
    const paid = computeGroupMetrics(paidDocs);

    const payload = {
      userRef,
      currentAccountsCount,

      unpaid_count: unpaid.count,
      unpaid_totalAmountOwed: unpaid.totalAmountOwed,
      unpaid_averageAgeDays: unpaid.averageAgeDays,
      unpaid_mostRecentDate: unpaid.mostRecentDate,
      unpaid_oldestDate: unpaid.oldestDate,
      unpaid_mostSevere_label: unpaid.mostSevere_label,
      unpaid_mostSevere_score: unpaid.mostSevere_score,

      paid_count: paid.count,
      paid_totalAmountOwed: paid.totalAmountOwed,
      paid_averageAgeDays: paid.averageAgeDays,
      paid_mostRecentDate: paid.mostRecentDate,
      paid_oldestDate: paid.oldestDate,
      paid_mostSevere_label: paid.mostSevere_label,
      paid_mostSevere_score: paid.mostSevere_score,
    };

    // ---------- Compare & write ----------
    const existingSnap = await db
      .collection('user_metricsPayHistory')
      .where('userRef', '==', userRef)
      .limit(1)
      .get();

    let destRef;

    if (!existingSnap.empty) {
      const prevDoc = existingSnap.docs[0];
      const prev = prevDoc.data() || {};

      const prevComparable = { ...prev };
      delete prevComparable.created_time;
      delete prevComparable.updated_time;

      if (deepEqual(prevComparable, payload)) {
        return { ok: true, changed: false, reason: 'no_update_needed' };
      }

      destRef = prevDoc.ref;
      await destRef.set(
        {
          ...payload,
          created_time:
            prev.created_time ||
            admin.firestore.FieldValue.serverTimestamp(),
          updated_time: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    } else {
      destRef = db.collection('user_metricsPayHistory').doc();
      await destRef.set({
        ...payload,
        created_time: admin.firestore.FieldValue.serverTimestamp(),
        updated_time: admin.firestore.FieldValue.serverTimestamp(),
      });
    }

    return { ok: true, changed: true, docPath: destRef.path };
  }
);
