// functions/cf/computeLengthOfHistoryMetrics.js
const functions = require('firebase-functions/v1');
const admin     = require('firebase-admin');
// admin.initializeApp() is called once in index.js

module.exports = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth || !context.auth.uid) {
      // Silent no-op per your pattern
      return;
    }

    const uid = context.auth.uid;
    const db = admin.firestore();
    const userRef = db.doc(`users/${uid}`);

    // --- Config: which stocks count as real tradelines ---
    const ACCOUNT_STOCKS = new Set([
      'user_credit_cards',
      'user_loans',
      'user_helocs',
      'user_mortgages',
    ]);

    // --- Helper: coerce DOFRecord to JS Date ---
    const toDate = (val) => {
      if (!val) return null;
      if (typeof val === 'object' && typeof val.toDate === 'function') {
        return val.toDate(); // Firestore Timestamp
      }
      if (typeof val === 'number') {
        const d = new Date(val);
        return isNaN(d.getTime()) ? null : d;
      }
      if (typeof val === 'string') {
        const d = new Date(val);
        return isNaN(d.getTime()) ? null : d;
      }
      return null;
    };

    // --- Helper: precise calendar-month difference (open -> now) ---
    const monthDiff = (fromDate, toDateObj) => {
      let months =
        (toDateObj.getFullYear() - fromDate.getFullYear()) * 12 +
        (toDateObj.getMonth() - fromDate.getMonth());
      if (toDateObj.getDate() < fromDate.getDate()) months -= 1;
      return Math.max(0, months);
    };

    // --- Pull user_stocks_conso for this user ---
    const consoSnap = await db
      .collection('user_stocks_conso')
      .where('userRef', '==', userRef)
      .get();

    const now = new Date();

    let totalAccountsCount = 0;
    let accountsIncludedCount = 0;
    let accountsExcludedCount = 0;
    let excludedNoDOFRecord = 0;
    let excludedNonAccountStock = 0;

    const agesMonths = [];
    let oldestOpenDate = null;
    let newestOpenDate = null;

    consoSnap.forEach((doc) => {
      totalAccountsCount += 1;
      const d = doc.data();
      const stock = d?.stock;

      if (!ACCOUNT_STOCKS.has(stock)) {
        accountsExcludedCount += 1;
        excludedNonAccountStock += 1;
        return;
      }

      const openDate = toDate(d?.DOFRecord);
      if (!openDate) {
        accountsExcludedCount += 1;
        excludedNoDOFRecord += 1;
        return;
      }

      const m = monthDiff(openDate, now);
      agesMonths.push(m);
      accountsIncludedCount += 1;

      if (!oldestOpenDate || openDate < oldestOpenDate) {
        oldestOpenDate = openDate;
      }
      if (!newestOpenDate || openDate > newestOpenDate) {
        newestOpenDate = openDate;
      }
    });

    let oldestAgeMonths = 0;
    let newestAgeMonths = 0;
    let averageAgeMonths = 0;

    if (agesMonths.length > 0) {
      oldestAgeMonths = Math.max(...agesMonths);
      newestAgeMonths = Math.min(...agesMonths);
      averageAgeMonths =
        agesMonths.reduce((a, b) => a + b, 0) / agesMonths.length;
    }

    const toYears = (months) => months / 12.0;

    const payload = {
      userRef,

      totalAccountsCount: Number(totalAccountsCount),
      accountsIncludedCount: Number(accountsIncludedCount),
      accountsExcludedCount: Number(accountsExcludedCount),
      excludedNoDOFRecord: Number(excludedNoDOFRecord),
      excludedNonAccountStock: Number(excludedNonAccountStock),

      oldestOpenDate: oldestOpenDate
        ? admin.firestore.Timestamp.fromDate(oldestOpenDate)
        : null,
      newestOpenDate: newestOpenDate
        ? admin.firestore.Timestamp.fromDate(newestOpenDate)
        : null,

      oldestAgeMonths: Number(oldestAgeMonths),
      newestAgeMonths: Number(newestAgeMonths),
      averageAgeMonths: Number(Number(averageAgeMonths.toFixed(6))),

      oldestAgeYears: Number(toYears(oldestAgeMonths).toFixed(6)),
      newestAgeYears: Number(toYears(newestAgeMonths).toFixed(6)),
      averageAgeYears: Number(toYears(averageAgeMonths).toFixed(6)),

      hasNoAccounts: accountsIncludedCount === 0,
      computedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    // --- Idempotent write to top-level: user_metricsLengthOfHistory/{uid} ---
    const outRef = db.collection('user_metricsLengthOfHistory').doc(uid);
    const existing = await outRef.get();

    const stableSubset = (obj) => {
      const { computedAt, userRef: _ignoredUserRef, ...rest } = obj;
      return rest;
    };

    let shouldWrite = true;
    if (existing.exists) {
      const current = existing.data() || {};
      const a = stableSubset(payload);
      const b = stableSubset(current);

      const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
      shouldWrite = false;

      for (const k of keys) {
        const va = a[k];
        const vb = b[k];

        const isTimestamp =
          (va && typeof va === 'object' && typeof va.toMillis === 'function') ||
          (vb && typeof vb === 'object' && typeof vb.toMillis === 'function');

        if (isTimestamp) {
          const ma = va ? va.toMillis() : null;
          const mb = vb ? vb.toMillis() : null;
          if (ma !== mb) {
            shouldWrite = true;
            break;
          }
        } else if (va !== vb) {
          shouldWrite = true;
          break;
        }
      }
    }

    if (shouldWrite) {
      await outRef.set(payload, { merge: true });
    }

    return; // silent success
  }
);
