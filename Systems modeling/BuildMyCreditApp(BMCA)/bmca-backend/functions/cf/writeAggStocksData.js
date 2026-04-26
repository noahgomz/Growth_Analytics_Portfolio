// functions/cf/writeAggStocksData.js
const functions = require('firebase-functions/v1');
const admin     = require('firebase-admin');
// admin.initializeApp() is called once in index.js

const MS_PER_DAY = 1000 * 60 * 60 * 24;
const AVG_DAYS_PER_MONTH = 30.4375;

function toMillis(v) {
  if (v == null) return null;
  if (typeof v === 'number') return v;
  if (v.toMillis) return v.toMillis();
  if (v instanceof Date) return v.getTime();
  return null;
}

function monthsFromWholeDays(nowMs, thenMs) {
  if (nowMs == null || thenMs == null) return 0;
  const diffMs = Math.max(0, nowMs - thenMs);
  const wholeDays = Math.floor(diffMs / MS_PER_DAY);
  const months = wholeDays / AVG_DAYS_PER_MONTH;
  return Math.round(months * 10) / 10;
}

function num(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function shallowEqualPayload(a, b) {
  if (!a || !b) return false;
  const aUserPath = a.user && typeof a.user.path === 'string' ? a.user.path : null;
  const bUserPath = b.user && typeof b.user.path === 'string' ? b.user.path : null;
  return (
    a.amountAllowed === b.amountAllowed &&
    a.amountOwed === b.amountOwed &&
    a.averageAge === b.averageAge &&
    a.count === b.count &&
    a.newest === b.newest &&
    a.oldest === b.oldest &&
    a.stockType === b.stockType &&
    aUserPath === bUserPath
  );
}

async function safeQuery(q) {
  try {
    const snap = await q.get();
    if (!snap || snap.empty) return [];
    return snap.docs.map(d => ({ id: d.id, ...d.data() }));
  } catch (e) {
    console.warn('safeQuery error:', e?.message || e);
    return [];
  }
}

const STOCKS = [
  {
    id: 'creditCards',
    collection: 'user_credit_cards',
    stockType: 'Credit Card',
    where: [{ f: 'isOpen', op: '==', v: true }],
    amountAllowedField: 'creditLimit',
    amountOwedField: 'totalBalance',
    dateField: 'dateIssued',
    sumOwedOnlyIfUnpaid: false,
  },
  {
    id: 'loans',
    collection: 'user_loans',
    stockType: 'Loan',
    where: [{ f: 'isOpen', op: '==', v: true }],
    amountAllowedField: 'principalOriginal',
    amountOwedField: 'balance',
    dateField: 'dateIssued',
    sumOwedOnlyIfUnpaid: false,
  },
  {
    id: 'cardLates',
    collection: 'user_credit_cards_late_payments',
    stockType: 'Credit Card Late',
    where: [{ f: 'sentToCollections', op: '==', v: false }],
    amountAllowedField: null,
    amountOwedField: 'amount',
    dateField: 'DOFD',
    sumOwedOnlyIfUnpaid: true,
  },
  {
    id: 'loanLates',
    collection: 'user_loans_late_payments',
    stockType: 'Loan Late',
    where: [{ f: 'sentToCollections', op: '==', v: false }],
    amountAllowedField: null,
    amountOwedField: 'amount',
    dateField: 'DOFD',
    sumOwedOnlyIfUnpaid: true,
  },
  {
    id: 'cardCollections',
    collection: 'user_credit_cards_late_payments',
    stockType: 'cardCollections',
    where: [{ f: 'sentToCollections', op: '==', v: true }],
    amountAllowedField: null,
    amountOwedField: 'amount',
    dateField: 'DOFD',
    sumOwedOnlyIfUnpaid: true,
  },
  {
    id: 'loanCollections',
    collection: 'user_loans_late_payments',
    stockType: 'loanCollections',
    where: [{ f: 'sentToCollections', op: '==', v: true }],
    amountAllowedField: null,
    amountOwedField: 'amount',
    dateField: 'DOFD',
    sumOwedOnlyIfUnpaid: true,
  },
  {
    id: 'collections3p',
    collection: 'user_collections_3rd_party',
    stockType: '3rd Party Collection',
    where: [],
    amountAllowedField: null,
    amountOwedField: 'amount',
    dateField: 'DOFD',
    sumOwedOnlyIfUnpaid: true,
  },
];

module.exports = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth?.uid) return; // no auth

    const db = admin.firestore();
    const uid = context.auth.uid;
    const userRef = db.doc(`users/${uid}`);
    const aggCol = db.collection('user_stocks_aggregate'); // top-level
    const nowMs = Date.now();
    const actions = [];

    for (const cfg of STOCKS) {
      // query top-level collection filtered by userRef
      let q = db.collection(cfg.collection).where('userRef', '==', userRef);
      for (const w of (cfg.where || [])) {
        q = q.where(w.f, w.op, w.v);
      }

      const docs = await safeQuery(q);

      let count = 0;
      let amountAllowed = 0;
      let amountOwed = 0;
      let newestMs = null;
      let oldestMs = null;
      let ageSumMonths = 0;
      let ageN = 0;

      for (const d of docs) {
        count += 1;
        if (cfg.amountAllowedField) {
          amountAllowed += num(d[cfg.amountAllowedField], 0);
        }
        if (cfg.amountOwedField) {
          const include = cfg.sumOwedOnlyIfUnpaid ? (d.isPaid !== true) : true;
          if (include) {
            amountOwed += num(d[cfg.amountOwedField], 0);
          }
        }
        const dMs = toMillis(d[cfg.dateField]);
        if (dMs != null) {
          newestMs = newestMs == null ? dMs : Math.max(newestMs, dMs);
          oldestMs = oldestMs == null ? dMs : Math.min(oldestMs, dMs);
          ageSumMonths += monthsFromWholeDays(nowMs, dMs);
          ageN += 1;
        }
      }

      const averageAge = ageN ? Math.round((ageSumMonths / ageN) * 10) / 10 : 0;

      // new ID format
      const docId = `${cfg.id}__${uid}`;
      const docRef = aggCol.doc(docId);
      const existingSnap = await docRef.get();
      const exists = existingSnap.exists;

      if (count === 0) {
        if (exists) {
          await docRef.delete();
          actions.push({ id: cfg.id, action: 'deleted-empty' });
        } else {
          actions.push({ id: cfg.id, action: 'noop-empty' });
        }
        continue;
      }

      const payload = {
        amountAllowed: cfg.amountAllowedField ? amountAllowed : 0,
        amountOwed,
        averageAge,
        count,
        newest: newestMs,
        oldest: oldestMs,
        stockType: cfg.stockType,
        user: userRef,
      };

      const shouldWrite =
        !exists || !shallowEqualPayload(payload, existingSnap.data());

      if (shouldWrite) {
        await docRef.set(payload, { merge: true });
        actions.push({ id: cfg.id, action: exists ? 'updated' : 'created' });
      } else {
        actions.push({ id: cfg.id, action: 'noop-unchanged' });
      }
    }

    return { ok: true, actions };
  }
);
