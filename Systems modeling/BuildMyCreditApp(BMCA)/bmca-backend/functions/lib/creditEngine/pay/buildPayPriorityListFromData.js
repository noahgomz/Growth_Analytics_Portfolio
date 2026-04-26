// functions/lib/creditEngine/3 - pay/buildPayPriorityListFromData.js

const MS_PER_DAY = 24 * 60 * 60 * 1000;

const asNum = (v, d = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};

/**
 * Convert DOFD / DOFRecord into a 30–180 day tier.
 * Accepts Firestore Timestamp, Date, or millis.
 */
const toTierFromDOFD = (dofdTs, nowMs) => {
  if (!dofdTs) return 30;

  let t = 0;

  // Firestore Timestamp
  if (dofdTs && typeof dofdTs.toDate === 'function') {
    t = dofdTs.toDate().getTime();
  } else if (dofdTs instanceof Date) {
    t = dofdTs.getTime();
  } else if (typeof dofdTs === 'number' && Number.isFinite(dofdTs)) {
    t = dofdTs;
  }

  if (!t) return 30;

  const days = Math.max(0, Math.floor((nowMs - t) / MS_PER_DAY));
  const raw = Math.ceil(days / 30) * 30;
  return Math.min(180, Math.max(30, raw)); // 30..180
};

/**
 * Pure Pay Priority engine (sim version).
 *
 * Inputs are already-enriched sim rows (from SimulationState):
 *   - accounts:    stocksConso rows where stock ∈ {user_credit_cards, user_loans}
 *   - lateItems:   stocksConso rows for card/loan lates
 *   - tpcItems:    stocksConso rows for 3rd-party collections
 *
 * It DOES NOT mutate the inputs or SimulationState.
 * It only returns allocations & ranking. tickOneMonth() then applies
 * the allocations to balances via applyPaymentToRow().
 */
function buildPayPriorityListFromData({
  nowMs,
  monthlyBudget,
  accounts,
  lateItems,
  tpcItems,
}) {
  const effectiveNowMs = Number.isFinite(nowMs) ? nowMs : Date.now();
  let budgetLeft = asNum(monthlyBudget, 0);

  // ---------- Build working rows ----------
  const items = [];

  // 1) Accounts (cards + loans)
  (accounts || []).forEach((src) => {
    if (src.stock !== 'user_credit_cards' && src.stock !== 'user_loans') return;

    const apr = asNum(src.apr, 0);

const minPayment = asNum(
  src.stock === 'user_credit_cards'
    ? (src.minPayment ?? src.minimumPayment)
    : src.monthlyPayment,
  0
);


    // amountsOwed is canonical in the sim; fall back to balance / totalBalance if present
    const balance = asNum(
      src.amountsOwed ?? src.balance ?? src.totalBalance,
      0
    );

    const dueDay = src.dayOfMonthDue != null
      ? asNum(src.dayOfMonthDue, null)
      : null;

    const isAnnualFee = !!src.isAnnualFee;
    const isCFA = !!src.isCFA;

    items.push({
      kind: 'account',
      id: src.id,
      stock: src.stock,
      name: src.name ?? null,
      lender: src.lender ?? null,
      sourceRef: src.ref || null,

      apr,
      minPayment,
      balance,
      dueDay,
      isAnnualFee,
      isCFA,

      isCollectionFromAccount: false, // never for base accounts

      alloc_total: 0,
      alloc_min: 0,
      alloc_card_extra: 0,
    });
  });

  // 2) Unpaid lates (card + loan)
  (lateItems || []).forEach((L) => {
    // In the sim, we don't have sentToCollections flag,
    // but mapLatesInMemory encoded "Collection" in severity when sentToCollections was true.
    const severityRaw = L.severity;
    const isCollectionFromAccount =
      typeof severityRaw === 'string' &&
      severityRaw.toLowerCase().includes('collection');

    const dofdLike = L.DOFRecord || L.DOFD || null;
    const severity = isCollectionFromAccount
      ? null // not needed for collections; they are handled in step 4
      : toTierFromDOFD(dofdLike, effectiveNowMs);

    const amount = asNum(L.amountsOwed ?? L.amount, 0);

    const lender = L.lender ?? null;
    const name = L.name ?? null;

    items.push({
      kind: isCollectionFromAccount ? 'collection_from_account' : 'late',
      id: L.id,
      stock: L.stock,
      name,
      lender,
      sourceRef: L.ref || null,

      severity,
      amount,
      isCollectionFromAccount,

      alloc_total: 0,
      alloc_late: 0,
      alloc_collections: 0,
    });
  });

  // 3) Third-party collections
  (tpcItems || []).forEach((C) => {
    const amount = asNum(C.amountsOwed ?? C.amount, 0);

    items.push({
      kind: 'collection_third_party',
      id: C.id,
      stock: 'user_collections_3rd_party',
      name: C.collectionsAgency ?? C.name ?? C.type ?? null,
      lender: C.originalProvider ?? C.lender ?? null,
      sourceRef: C.ref || null,

      amount,
      isCollectionFromAccount: false,

      alloc_total: 0,
      alloc_collections: 0,
    });
  });

  // ---------- minimumPreservationBudget (accounts' mins + unpaid lates, no collections) ----------
  const accountItems = items.filter((i) => i.kind === 'account');
  const totalMinAccounts = accountItems.reduce(
    (s, i) => s + asNum(i.minPayment, 0),
    0
  );
  const totalUnpaidLates = items
    .filter((i) => i.kind === 'late')
    .reduce((s, i) => s + asNum(i.amount, 0), 0);

  const minimumPreservationBudget = Number(
    (totalMinAccounts + totalUnpaidLates).toFixed(2)
  );

  // ---------- Allocation ----------

  // 1) Minimum monthly payments (cards + loans)
  const totalMin = totalMinAccounts;

  // Shared order (same as before): APR desc, earlier due day first
  const byAprThenDueForMins = (a, b) =>
    b.apr - a.apr || (a.dueDay ?? 32) - (b.dueDay ?? 32);

  // Helper: cap by remaining balance (Point #2)
  const remainingBalance = (i) =>
    Math.max(0, asNum(i.balance, 0) - asNum(i.alloc_total, 0));

  const payableMin = (i) =>
    Math.min(asNum(i.minPayment, 0), remainingBalance(i));

  if (budgetLeft > 0) {
    if (budgetLeft >= totalMin) {
      // Enough for all mins → pay mins in order APR desc, earlier due day first
      accountItems
        .sort(byAprThenDueForMins)
        .forEach((i) => {
          const pay = payableMin(i); // capped by remaining balance
          if (pay <= 0) return;

          const alloc = Math.min(budgetLeft, pay);
          i.alloc_min += alloc;
          i.alloc_total += alloc;
          budgetLeft -= alloc;
        });
    } else {
      // Not enough → NO pro-rata. Concentrate payments (Point #1).
      // Still cap by remaining balance (Point #2).

      const loanItems = accountItems.filter((i) => i.stock === 'user_loans');

      const canFullyCoverAnyLoanMin = loanItems.some((i) => {
        const need = payableMin(i);
        return need > 0 && budgetLeft >= need;
      });

      // Helper: remaining portion of the min, capped by remaining balance
      const remainingMinNeed = (i) => {
        const remainingMin = Math.max(0, asNum(i.minPayment, 0) - asNum(i.alloc_min, 0));
        return Math.min(remainingBalance(i), remainingMin);
      };

      if (canFullyCoverAnyLoanMin) {
        // Pass 1: Loans first by smallest payable min (FULL-OR-NOTHING)
        loanItems
          .sort((a, b) => payableMin(a) - payableMin(b))
          .forEach((i) => {
            const need = payableMin(i);
            if (need <= 0) return;
            if (budgetLeft < need) return; // full-or-nothing

            i.alloc_min += need;
            i.alloc_total += need;
            budgetLeft -= need;
          });

        // Pass 2: All accounts sequential by APR/dueDay (NOT full-or-nothing)
        // Pay up to the remaining portion of each min, capped by remaining balance
        accountItems
          .sort(byAprThenDueForMins)
          .forEach((i) => {
            if (budgetLeft <= 0) return;

            const need = remainingMinNeed(i);
            if (need <= 0) return;

            const pay = Math.min(budgetLeft, need);
            i.alloc_min += pay;
            i.alloc_total += pay;
            budgetLeft -= pay;
          });
      } else {
        // Otherwise: sequential by APR/dueDay (NOT full-or-nothing)
        // Pay up to each min portion, capped by remaining balance
        accountItems
          .sort(byAprThenDueForMins)
          .forEach((i) => {
            if (budgetLeft <= 0) return;

            const need = remainingMinNeed(i);
            if (need <= 0) return;

            const pay = Math.min(budgetLeft, need);
            i.alloc_min += pay;
            i.alloc_total += pay;
            budgetLeft -= pay;
          });
      }

    }
  }


  // 2) Unpaid lates by severity (180 → 30)
  items
    .filter((i) => i.kind === 'late')
    .sort((a, b) => (b.severity || 0) - (a.severity || 0))
    .forEach((i) => {
      if (budgetLeft <= 0) return;
      const need = asNum(i.amount, 0) - i.alloc_total;
      if (need <= 0) return;
      const pay = Math.min(budgetLeft, need);
      i.alloc_late = (i.alloc_late || 0) + pay;
      i.alloc_total += pay;
      budgetLeft -= pay;
    });

  // 3) Card extra principal — AF first, then CFA, then others (APR desc, earlier due day)
  const byAprThenDue = (a, b) =>
    b.apr - a.apr || (a.dueDay ?? 32) - (b.dueDay ?? 32);

  const cardsAF = items.filter(
    (a) =>
      a.kind === 'account' &&
      a.stock === 'user_credit_cards' &&
      a.isAnnualFee
  );

  const cardsCFA = items.filter(
    (a) =>
      a.kind === 'account' &&
      a.stock === 'user_credit_cards' &&
      a.isCFA &&
      !a.isAnnualFee
  );

  const cardsOther = items.filter(
    (a) =>
      a.kind === 'account' &&
      a.stock === 'user_credit_cards' &&
      !a.isAnnualFee &&
      !a.isCFA
  );

  [cardsAF, cardsCFA, cardsOther].forEach((group) => {
    group.sort(byAprThenDue).forEach((i) => {
      if (budgetLeft <= 0) return;
      const remaining = Math.max(
        0,
        asNum(i.balance, 0) - i.alloc_total
      );
      if (remaining <= 0) return;
      const pay = Math.min(budgetLeft, remaining);
      i.alloc_card_extra = (i.alloc_card_extra || 0) + pay;
      i.alloc_total += pay;
      budgetLeft -= pay;
    });
  });

  // 4) Collections — account-origin (sent-to-collections) then third-party
  items
    .filter((i) => i.kind === 'collection_from_account')
    .sort((a, b) => (b.amount || 0) - (a.amount || 0))
    .forEach((i) => {
      if (budgetLeft <= 0) return;
      const need = asNum(i.amount, 0) - i.alloc_total;
      if (need <= 0) return;
      const pay = Math.min(budgetLeft, need);
      i.alloc_collections = (i.alloc_collections || 0) + pay;
      i.alloc_total += pay;
      budgetLeft -= pay;
    });

  items
    .filter((i) => i.kind === 'collection_third_party')
    .sort((a, b) => (b.amount || 0) - (a.amount || 0))
    .forEach((i) => {
      if (budgetLeft <= 0) return;
      const need = asNum(i.amount, 0) - i.alloc_total;
      if (need <= 0) return;
      const pay = Math.min(budgetLeft, need);
      i.alloc_collections = (i.alloc_collections || 0) + pay;
      i.alloc_total += pay;
      budgetLeft -= pay;
    });

  const availableRemainder = Number(budgetLeft.toFixed(2));
  const totalAllocated = Number(
    (asNum(monthlyBudget, 0) - availableRemainder).toFixed(2)
  );

  // Priority buckets for ranking output
  const priorityKey = (i) =>
    i.kind === 'account' && i.alloc_min > 0
      ? 1
      : i.kind === 'late' && i.alloc_total > 0
      ? 2
      : i.kind === 'account' && i.alloc_card_extra > 0
      ? 3
      : (i.kind === 'collection_from_account' ||
          i.kind === 'collection_third_party') &&
        i.alloc_total > 0
      ? 4
      : 5;

  const rankedInternal = [...items].sort((a, b) => {
    const pa = priorityKey(a);
    const pb = priorityKey(b);
    if (pa !== pb) return pa - pb;
    return (b.alloc_total || 0) - (a.alloc_total || 0);
  });

  // Adapt to what tickOneMonth expects: id, stockType, budgetAllocated
  const ranked = rankedInternal.map((i) => ({
    ...i,
    stockType: i.stock,
    budgetAllocated: asNum(i.alloc_total, 0),
  }));

  return {
    items,
    ranked,
    availableRemainder,
    totalAllocated,
    minimumPreservationBudget,
  };

}

module.exports = { buildPayPriorityListFromData };
