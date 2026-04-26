// functions/lib/creditEngine/1 - loaders/loadSimulationInputs.js

const MS_PER_DAY = 1000 * 60 * 60 * 24;

const asNum = (v, d = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};

// strip only undefined (null/false/0 are kept)
function clean(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v !== undefined) out[k] = v;
  }
  return out;
}

// ---------- MAIN LOADER ----------

async function loadSimulationInputs(db, userRef, rawMonths) {
  // ---- normalize months (default 12 if null/invalid) ----
  let monthsInSim = asNum(rawMonths, NaN);
  if (!Number.isFinite(monthsInSim) || monthsInSim <= 0) {
    monthsInSim = 12;
  } else {
    monthsInSim = Math.floor(monthsInSim);
  }

  // ---- load user doc (budget, permissions, etc.) ----
  const userSnap = await userRef.get();
  const userData = userSnap.exists ? userSnap.data() : {};
  const user = { id: userSnap.id, ref: userRef, ...userData };

  // ---- load origins for stocks_conso-shaped dataset + offers/adjustments/targets ----
  const [
    pullsSnap,
    cardsSnap,
    loansSnap,
    cardLatesSnap,
    loanLatesSnap,
    tpcSnap,
    offersLoansSnap,
    offersCardsSnap,
    openCloseAdjSnap,
    chaSnap,         // 🔹 CHA account-number targets (GLOBAL — no user filter)
    chaMetricsSnap,  // 🔹 CHA metrics (GLOBAL — no user filter)
  ] = await Promise.all([
    db.collection('user_hard_pulls').where('userRef', '==', userRef).get(),
    db.collection('user_credit_cards').where('userRef', '==', userRef).get(),
    db.collection('user_loans').where('userRef', '==', userRef).get(),
    db.collection('user_credit_cards_late_payments').where('userRef', '==', userRef).get(),
    db.collection('user_loans_late_payments').where('userRef', '==', userRef).get(),
    db.collection('user_collections_3rd_party').where('userRef', '==', userRef).get(),

    // 🔹 Global offers, no user filter
    db.collection('offers_loans').get(),
    db.collection('offers_cards').get(),

    // 🔹 User-specific adjustments
    db.collection('user_openAndCloseAdjustments')
      .where('userRef', '==', userRef)
      .get(),

    // 🔹 High achiever targets (same as writeOpenActionsListORCH)
    db.collection('credit_high_achiever_account_numbers').get(),

    // 🔹 High achiever metrics (for age target, etc.)
    db.collection('credit_high_achiever_metrics').get(),
  ]);

  const stocksConso = [];

  // ---------- mapRequests (hard pulls) ----------
  pullsSnap.docs.forEach(doc => {
    const d = doc.data();
    const flatData = clean({
      id:        doc.id,
      userRef,
      stock:     'user_hard_pulls',
      subStock:  d.debtType,
      lender:    d.lender,
      name:      d.productName,
      DOFRecord: d.dateOfRequest,
    });
    stocksConso.push(flatData);
  });

  // ---------- mapAccounts (cards + loans) ----------
  const mapAccountsInMemory = (collectionName, snap) => {
    const isCards = collectionName === 'user_credit_cards';

    snap.docs.forEach(doc => {
      const d = doc.data();

      // isOpen handling (boolean, null, or missing)
      let isOpenField;
      let isOpenIsNullFlag;
      let isOpenIsMissingFlag;
      if (Object.prototype.hasOwnProperty.call(d, 'isOpen')) {
        if (d.isOpen === null) {
          isOpenIsNullFlag = true;
        } else if (typeof d.isOpen === 'boolean') {
          isOpenField = d.isOpen;
        }
      } else {
        isOpenIsMissingFlag = true;
      }

      const flatData = clean({
        id:              doc.id,
        userRef,
        stock:           collectionName,
        subStock:        isCards ? 'Revolving' : 'Installment',
        lender:          d.lender,
        name:            d.commercialName,
        isCurrent:       d.isCurrent,
        isOpen:          isOpenField,
        isOpenIsNull:    isOpenIsNullFlag,
        isOpenIsMissing: isOpenIsMissingFlag,
        isCFA:           d.isCFA,
        isAnnualFee:     isCards ? d.isAnnualFee : undefined,
        creditLimit:     isCards ? d.creditLimit : d.principalOriginal,
        amountsOwed:     isCards ? d.totalBalance : d.balance,
        DOFRecord:       d.dateIssued,

        // 🔹 bring live last-used date into sim for Use logic
        dateLastUsed:    isCards ? d.dateLastUsed : undefined,

        // 🔹 New fields for Pay / ranking logic
        apr:             asNum(d.apr, null),
        minPayment:      isCards
                          ? asNum(d.minimumPayment, null)
                          : asNum(d.monthlyPayment, null),
        monthlyPayment:  isCards
                          ? null
                          : asNum(d.monthlyPayment, null),
        dayOfMonthDue:   asNum(d.dayOfMonthDue, null),
      });

      stocksConso.push(flatData);
    });
  };

  mapAccountsInMemory('user_credit_cards', cardsSnap);
  mapAccountsInMemory('user_loans', loansSnap);

  // ---------- mapLates (card + loan lates) ----------
  const mapLatesInMemory = async (collectionName, snap) => {
    const isCardsLate = collectionName === 'user_credit_cards_late_payments';

    for (const doc of snap.docs) {
      const d = doc.data();

      // fetch origin account for lender/name (cardRef or loanRef)
      const accountRef = d.cardRef || d.loanRef;

      // NEW: derive sim-only originAccountStock / originAccountId
      let originAccountStock;
      if (d.cardRef) {
        originAccountStock = 'user_credit_cards';
      } else if (d.loanRef) {
        originAccountStock = 'user_loans';
      }

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

      // isPaid handling
      let isPaidField;
      let isPaidIsNullFlag;
      let isPaidIsMissingFlag;
      if (Object.prototype.hasOwnProperty.call(d, 'isPaid')) {
        if (d.isPaid === null) {
          isPaidIsNullFlag = true;
        } else if (typeof d.isPaid === 'boolean') {
          isPaidField = d.isPaid;
        }
      } else {
        isPaidIsMissingFlag = true;
      }

      const flatData = clean({
        id:                  doc.id,
        userRef,
        stock:               collectionName,
        subStock:            isCardsLate ? 'Revolving' : 'Installment',
        lender:              refData.lender,
        name:                refData.commercialName,
        isPaid:              isPaidField,
        isPaidIsNull:        isPaidIsNullFlag,
        isPaidIsMissing:     isPaidIsMissingFlag,
        DOFRecord:           d.DOFD,
        severity,
        amountsOwed:         d.amount,

        // NEW: sim-only link back to origin account
        originAccountStock,
        originAccountId:     accountRef ? accountRef.id : undefined,
      });

      stocksConso.push(flatData);
    }
  };

  await mapLatesInMemory('user_credit_cards_late_payments', cardLatesSnap);
  await mapLatesInMemory('user_loans_late_payments', loanLatesSnap);

  // ---------- mapCollections (3rd party) ----------
  tpcSnap.docs.forEach(doc => {
    const d = doc.data();

    // isPaid handling
    let isPaidField;
    let isPaidIsNullFlag;
    let isPaidIsMissingFlag;
    if (Object.prototype.hasOwnProperty.call(d, 'isPaid')) {
      if (d.isPaid === null) {
        isPaidIsNullFlag = true;
      } else if (typeof d.isPaid === 'boolean') {
        isPaidField = d.isPaid;
      }
    } else {
      isPaidIsMissingFlag = true;
    }

    const flatData = clean({
      id:              doc.id,
      userRef,
      stock:           'user_collections_3rd_party',
      subStock:        'Collection',
      lender:          d.originalProvider,
      name:            d.collectionsAgency,
      isPaid:          isPaidField,
      isPaidIsNull:    isPaidIsNullFlag,
      isPaidIsMissing: isPaidIsMissingFlag,
      DOFRecord:       d.DOFD,
      severity:        'Collection',
      amountsOwed:     d.amount,
    });

    stocksConso.push(flatData);
  });

  // ---------- offers & adjustments ----------
  const offers_loans = offersLoansSnap.docs.map(d => ({
    id: d.id,
    ref: d.ref,
    ...d.data(),
  }));

  const offers_cards = offersCardsSnap.docs.map(d => ({
    id: d.id,
    ref: d.ref,
    ...d.data(),
  }));

  const user_openAndCloseAdjustments = openCloseAdjSnap.docs.map(d => ({
    id: d.id,
    ref: d.ref,
    ...d.data(),
  }));

  // ---------- CHA targets ----------
  const credit_high_achiever_account_numbers = chaSnap.docs.map(d => ({
    id: d.id,
    ref: d.ref,
    ...d.data(),
  }));

  let chaSum = 0;
  credit_high_achiever_account_numbers.forEach(row => {
    // same logic as writeOpenActionsListORCH: Value / value
    const v = row.Value ?? row.value;
    chaSum += asNum(v, 0);
  });

  // ---------- CHA metrics (full collection; extract avg-age target) ----------
  const credit_high_achiever_metrics = chaMetricsSnap.docs.map(d => ({
    id: d.id,
    ref: d.ref,
    ...d.data(),
  }));

  const chaMetricsDoc =
    credit_high_achiever_metrics.find((d) => d && d.id === 'uMT5A6zpFmW7pjf16BnE') ||
    null;

  const chaAvgAgeMonthsTarget = chaMetricsDoc
    ? asNum(chaMetricsDoc.LH_averageAgeMonths, null)
    : null;

  // Return everything the sim needs to initialize SimulationState
  return {
    monthsInSim,
    user,
    stocksConso,
    offers_loans,
    offers_cards,
    user_openAndCloseAdjustments,
    credit_high_achiever_account_numbers,
    credit_high_achiever_metrics,
    chaSum,
    chaAvgAgeMonthsTarget,
  };

}

module.exports = { loadSimulationInputs };
