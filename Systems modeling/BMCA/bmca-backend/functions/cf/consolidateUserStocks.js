const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');

// NOTE: Do NOT call admin.initializeApp() here
// It's already called once in index.js

module.exports = functions
  .region('us-central1')
  .https.onCall(async (data, context) => {
    const uid = context.auth && context.auth.uid;
    if (!uid) {
      throw new functions.https.HttpsError(
        'unauthenticated',
        'Must be called while authenticated.'
      );
    }

    const db = admin.firestore();
    const userDocRef = db.doc(`users/${uid}`);

    // 1) Hard pulls
    const pullsSnap = await db.collection('user_hard_pulls')
      .where('userRef', '==', userDocRef).get();
    await mapRequests(pullsSnap, db, userDocRef);

    // 2) Accounts
    await mapAccounts('user_credit_cards', db, userDocRef);
    await mapAccounts('user_loans', db, userDocRef);

    // 3) Late payments
    await mapLates('user_credit_cards_late_payments', db, userDocRef);
    await mapLates('user_loans_late_payments', db, userDocRef);

    // 4) Collections (3rd party)
    await mapCollections('user_collections_3rd_party', db, userDocRef);

    return { success: true };
  });

// strip only undefined (null/false/0 are kept)
function clean(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) if (v !== undefined) out[k] = v;
  return out;
}

async function mapRequests(snap, db, userDocRef) {
  const oldSnap = await db.collection('user_stocks_conso')
    .where('stock', '==', 'user_hard_pulls')
    .where('userRef', '==', userDocRef).get();

  const keepIds = new Set(snap.docs.map(d => d.id));
  const batch = db.batch();

  oldSnap.docs.forEach(d => { if (!keepIds.has(d.id)) batch.delete(d.ref); });

  snap.docs.forEach(doc => {
    const d = doc.data();
    const consoRef = db.collection('user_stocks_conso').doc(doc.id);
    const flatData = clean({
      DOFRecord: d.dateOfRequest,
      userRef:   d.userRef,
      stock:     'user_hard_pulls',
      subStock:  d.debtType,
      lender:    d.lender,
      name:      d.productName
    });
    batch.set(consoRef, flatData, { merge: true });
  });

  await batch.commit();
}

async function mapAccounts(collectionName, db, userDocRef) {
  const snap = await db.collection(collectionName)
    .where('userRef', '==', userDocRef).get();

  const oldSnap = await db.collection('user_stocks_conso')
    .where('stock', '==', collectionName)
    .where('userRef', '==', userDocRef).get();

  const keepIds = new Set(snap.docs.map(d => d.id));
  const batch = db.batch();

  oldSnap.docs.forEach(d => { if (!keepIds.has(d.id)) batch.delete(d.ref); });

  snap.docs.forEach(doc => {
    const d = doc.data();
    const consoRef = db.collection('user_stocks_conso').doc(doc.id);
    const isCards = collectionName === 'user_credit_cards';

    // isOpen handling (boolean, null, or missing)
    let isOpenField;
    let isOpenIsNullFlag;
    let isOpenIsMissingFlag;
    if (d.hasOwnProperty('isOpen')) {
      if (d.isOpen === null) {
        isOpenIsNullFlag = true;
      } else if (typeof d.isOpen === 'boolean') {
        isOpenField = d.isOpen;
      }
    } else {
      isOpenIsMissingFlag = true;
    }

    const flatData = clean({
      DOFRecord:   d.dateIssued,
      userRef:     d.userRef,
      stock:       collectionName,
      subStock:    isCards ? 'Revolving' : 'Installment',
      lender:      d.lender,
      name:        d.commercialName,
      isCurrent:   d.isCurrent,
      isOpen:      isOpenField,
      isOpenIsNull: isOpenIsNullFlag,
      isOpenIsMissing: isOpenIsMissingFlag,
      isCFA:       d.isCFA,
      isAnnualFee: isCards ? d.isAnnualFee : undefined,
      creditLimit: isCards ? d.creditLimit : d.principalOriginal,
      amountsOwed: isCards ? d.totalBalance : d.balance
    });

    batch.set(consoRef, flatData, { merge: true });
  });

  await batch.commit();
}

async function mapLates(collectionName, db, userDocRef) {
  const snap = await db.collection(collectionName)
    .where('userRef', '==', userDocRef).get();

  const oldSnap = await db.collection('user_stocks_conso')
    .where('stock', '==', collectionName)
    .where('userRef', '==', userDocRef).get();

  const keepIds = new Set(snap.docs.map(d => d.id));
  const batch = db.batch();

  oldSnap.docs.forEach(d => { if (!keepIds.has(d.id)) batch.delete(d.ref); });

  const MS_PER_DAY = 1000 * 60 * 60 * 24;

  for (const doc of snap.docs) {
    const d = doc.data();
    const consoRef = db.collection('user_stocks_conso').doc(doc.id);

    const accountRef = d.cardRef || d.loanRef;
    let refData = {};
    if (accountRef && typeof accountRef.get === 'function') {
      const refSnap = await accountRef.get();
      refData = refSnap.exists ? (refSnap.data() || {}) : {};
    }

    let severity;
    if (d.sentToCollections) {
      severity = 'Collection';
    } else if (d.chargedOff) {
      severity = 'Charge Off';
    } else if (d.DOFD && typeof d.DOFD.toDate === 'function') {
      const firstMissTs = d.DOFD.toDate().getTime();
      const paid = d.isPaid && d.datePaid && typeof d.datePaid.toDate === 'function';
      let deltaMs = (paid ? d.datePaid.toDate().getTime() : Date.now()) - firstMissTs;
      deltaMs = Math.max(0, deltaMs);
      const daysRounded = Math.floor((deltaMs / MS_PER_DAY) / 30) * 30;
      severity = String(daysRounded);
    }

    const isCardsLate = collectionName === 'user_credit_cards_late_payments';

    // isPaid handling (boolean, null, or missing)
    let isPaidField;
    let isPaidIsNullFlag;
    let isPaidIsMissingFlag;
    if (d.hasOwnProperty('isPaid')) {
      if (d.isPaid === null) {
        isPaidIsNullFlag = true;
      } else if (typeof d.isPaid === 'boolean') {
        isPaidField = d.isPaid;
      }
    } else {
      isPaidIsMissingFlag = true;
    }

    const flatData = clean({
      userRef:     d.userRef,
      stock:       collectionName,
      subStock:    isCardsLate ? 'Revolving' : 'Installment',
      lender:      refData.lender,
      name:        refData.commercialName,
      isPaid:      isPaidField,
      isPaidIsNull: isPaidIsNullFlag,
      isPaidIsMissing: isPaidIsMissingFlag,
      DOFRecord:   d.DOFD,
      severity,
      amountsOwed: d.amount
    });

    batch.set(consoRef, flatData, { merge: true });
  }

  await batch.commit();
}

async function mapCollections(collectionName, db, userDocRef) {
  const snap = await db.collection(collectionName)
    .where('userRef', '==', userDocRef).get();

  const oldSnap = await db.collection('user_stocks_conso')
    .where('stock', '==', collectionName)
    .where('userRef', '==', userDocRef).get();

  const keepIds = new Set(snap.docs.map(d => d.id));
  const batch = db.batch();

  oldSnap.docs.forEach(d => { if (!keepIds.has(d.id)) batch.delete(d.ref); });

  snap.docs.forEach(doc => {
    const d = doc.data();
    const consoRef = db.collection('user_stocks_conso').doc(doc.id);

    // isPaid handling (boolean, null, or missing)
    let isPaidField;
    let isPaidIsNullFlag;
    let isPaidIsMissingFlag;
    if (d.hasOwnProperty('isPaid')) {
      if (d.isPaid === null) {
        isPaidIsNullFlag = true;
      } else if (typeof d.isPaid === 'boolean') {
        isPaidField = d.isPaid;
      }
    } else {
      isPaidIsMissingFlag = true;
    }

    const flatData = clean({
      userRef:     d.userRef,
      stock:       collectionName,
      subStock:    'Collection',
      lender:      d.originalProvider,
      name:        d.collectionsAgency,
      isPaid:      isPaidField,
      isPaidIsNull: isPaidIsNullFlag,
      isPaidIsMissing: isPaidIsMissingFlag,
      DOFRecord:   d.DOFD,
      severity:    'Collection',
      amountsOwed: d.amount
    });

    batch.set(consoRef, flatData, { merge: true });
  });

  await batch.commit();
}
