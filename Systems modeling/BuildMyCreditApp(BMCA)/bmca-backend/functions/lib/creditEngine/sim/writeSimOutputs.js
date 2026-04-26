// functions/lib/creditEngine/2 - sim/writeSimOutputs.js

const admin = require('firebase-admin');

function toDate(x) {
  if (!x) return null;
  if (x instanceof Date) return x;
  const d = new Date(x);
  return isNaN(d.getTime()) ? null : d;
}

function monthsBetween(a, b) {
  // a, b are Date
  const ms = a.getTime() - b.getTime();
  return ms / (1000 * 60 * 60 * 24 * 30);
}

/**
 * Writes simulation outputs to Firestore:
 *
 * 1. Creates ONE run header doc:
 *      users/{uid}/SimulationRuns/{simRunId}
 *
 * 2. Writes MANY row docs to:
 *      user_sim_stocks_conso
 *
 * Each snapshot = one account at one monthIndex.
 */
async function writeSimOutputs(db, userRef, state) {
  // ---------- CREATE THE SIMULATION RUN HEADER DOC ----------
  const simRunRef = userRef.collection('SimulationRuns').doc();

const runMeta = {
  userRef,
  createdAt: admin.firestore.FieldValue.serverTimestamp(),
  totalCycles: state.totalCycles ?? (state.stockSnapshots?.length ?? null),
  startedAt: state.startDate ?? null,
  finishedAt: state.simDate ?? null,
  monthlySimulatedSpend: state.monthlySimulatedSpend ?? null,
  expectedNewCardLimit: state.expectedNewCardLimit ?? null,
  expectedNewLoanPrincipal: state.expectedNewLoanPrincipal ?? null,
  monthlyBudget: state.monthlyBudget ?? null,
};

  let batch = db.batch();
  let opCount = 0;

  async function commitIfNeeded(force = false) {
    // Keep headroom under Firestore's 500-op batch limit
    if (force || opCount >= 450) {
      await batch.commit();
      batch = db.batch();
      opCount = 0;
    }
  }

  function addSet(ref, data, opts) {
    if (opts) batch.set(ref, data, opts);
    else batch.set(ref, data);
    opCount += 1;
  }

  // Write the run header
  addSet(simRunRef, runMeta, { merge: true });
  await commitIfNeeded(false);

  // ---------- SET UP COLLECTION FOR SNAPSHOT ROWS ----------
  const simStocksColl = db.collection('user_sim_stocks_conso');

  // We rely on SimulationState.stockSnapshots[]
  const snapshots =
    Array.isArray(state.stockSnapshots) && state.stockSnapshots.length > 0
      ? state.stockSnapshots
      : [
          {
            cycleIndex: state.currentCycle || 0,
            simDate: state.simDate || new Date(),
            stocks: state.stocks || [],
          },
        ];

  // ---------- COMPUTE SUMMARY MARKERS ----------
  const utilDoc = Array.isArray(state.openCloseAdjustments)
    ? state.openCloseAdjustments.find((d) => d && d.Unique_Name === 'Utilization')
    : null;

  const utilTargetWhole =
    utilDoc && typeof utilDoc.User_Value === 'number' && isFinite(utilDoc.User_Value)
      ? utilDoc.User_Value
      : null;

  // Whole-number % tolerance: 30 -> 35 (NOT 31.5)
  const utilCeilingWhole = utilTargetWhole != null ? utilTargetWhole + 5 : null;

  const markers = {
    t_open_done: null,
    t_close_done: null,
    t_pay_done: null,
    t_util_stable: null,
    t_use_done: null,
    t_structure_done: null,
    t_age_done: null,
    totalMonths: state._totalMonths ?? snapshots.length,
  };

  const ageTargetMonths =
    typeof state.chaAvgAgeMonthsTarget === 'number' && isFinite(state.chaAvgAgeMonthsTarget)
      ? state.chaAvgAgeMonthsTarget
      : null;

  for (let sIdx = 0; sIdx < snapshots.length; sIdx++) {
    const snap = snapshots[sIdx];
    const monthIndex = snap.cycleIndex != null ? snap.cycleIndex : sIdx;
    const simDate = toDate(snap.simDate) || new Date();

    const stocks = Array.isArray(snap.stocks) ? snap.stocks : [];

    // --- UTIL (cards only, open only) ---
    let totalCardBal = 0;
    let totalCardLimit = 0;

    // --- unpaid late/collection balances ---
    let unpaidLateBal = 0;
    let unpaidCollBal = 0;

    // --- avg age months of open accounts (for age done) ---
    let ageSum = 0;
    let ageCount = 0;

    // --- open/close meta (pushed by cycles) ---
    const neededTotal =
      snap.openMeta && typeof snap.openMeta.neededTotal === 'number'
        ? snap.openMeta.neededTotal
        : null;

    const candidateCount =
      snap.closeMeta && typeof snap.closeMeta.candidateCount === 'number'
        ? snap.closeMeta.candidateCount
        : null;

    for (const row of stocks) {
      if (!row || !row.stock) continue;

      // Cards utilization
      if (row.stock === 'user_credit_cards' && row.isOpen !== false) {
        totalCardBal += Number(row.totalBalance ?? row.amountsOwed ?? row.balance ?? 0) || 0;
        totalCardLimit += Number(row.creditLimit ?? 0) || 0;

        // Age (open accounts)
        const od = toDate(
          row.DOFRecord ??
            row.dateOpened ??
            row.date_issued ??
            row.Date_issued ??
            row.dateIssued
        );
        if (od) {
          ageSum += monthsBetween(simDate, od);
          ageCount += 1;
        }
      }

      // Loans age (optional: if you want avg across ALL open accounts later, include loans here)
      if (row.stock === 'user_loans' && row.isOpen !== false) {
        const od = toDate(
          row.DOFRecord ??
            row.dateOpened ??
            row.date_issued ??
            row.Date_issued ??
            row.dateIssued
        );
        if (od) {
          ageSum += monthsBetween(simDate, od);
          ageCount += 1;
        }
      }

      // Unpaid lates
      if (
        (row.stock === 'user_credit_cards_late_payments' ||
          row.stock === 'user_loans_late_payments') &&
        row.isPaid !== true
      ) {
        unpaidLateBal += Number(row.amountsOwed ?? row.totalBalance ?? row.balance ?? 0) || 0;
      }

      // Unpaid third-party collections
      if (row.stock === 'user_collections_3rd_party' && row.isPaid !== true) {
        unpaidCollBal += Number(row.amountsOwed ?? row.totalBalance ?? row.balance ?? 0) || 0;
      }
    }

    const utilWhole = totalCardLimit > 0 ? (totalCardBal / totalCardLimit) * 100 : 0;
    const utilOK = utilCeilingWhole != null ? utilWhole <= utilCeilingWhole : false;

    // Conditions (your agreed definitions)
    const openDone = neededTotal === 0;
    const closeDone = candidateCount === 0;
    const payDone = unpaidLateBal === 0 && unpaidCollBal === 0 && utilOK;
    const useDone = utilOK;
    const utilStable = utilOK;

    const avgAgeMonths = ageCount > 0 ? ageSum / ageCount : 0;
    const ageDone = ageTargetMonths != null ? avgAgeMonths >= ageTargetMonths : false;

    // First-hit markers
    if (markers.t_open_done == null && openDone) markers.t_open_done = monthIndex;
    if (markers.t_close_done == null && closeDone) markers.t_close_done = monthIndex;
    if (markers.t_pay_done == null && payDone) markers.t_pay_done = monthIndex;
    if (markers.t_use_done == null && useDone) markers.t_use_done = monthIndex;
    if (markers.t_util_stable == null && utilStable) markers.t_util_stable = monthIndex;

    const structureDone = openDone && closeDone && payDone && useDone && utilStable;
    if (markers.t_structure_done == null && structureDone) markers.t_structure_done = monthIndex;

    if (markers.t_age_done == null && ageDone) markers.t_age_done = monthIndex;
  };

  // ---------- WRITE SUMMARY DOC (ONE PER RUN) ----------
  const summaryDocRef = simStocksColl.doc(`summary_run_${simRunRef.id}`);

  const summaryPayload = {
    userRef,
    simRunRef,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),

    // carry the util target (whole number) for debugging / UI
    utilTargetWhole: utilTargetWhole ?? null,
    utilCeilingWhole: utilCeilingWhole ?? null,

    // age target
    chaAvgAgeMonthsTarget: ageTargetMonths ?? null,

    // markers
    ...markers,
    monthlyBudget: state.monthlyBudget ?? null,
  };

  addSet(summaryDocRef, summaryPayload, { merge: true });
  await commitIfNeeded(false);


  // ---------- WRITE EACH (ACCOUNT, MONTH) SNAPSHOT ----------
  for (let sIdx = 0; sIdx < snapshots.length; sIdx++) {
    const snap = snapshots[sIdx];

    const monthIndex = snap.cycleIndex != null ? snap.cycleIndex : sIdx;
    const simDate = snap.simDate instanceof Date ? snap.simDate : new Date(snap.simDate);

    const stocks = Array.isArray(snap.stocks) ? snap.stocks : [];

    for (let rIdx = 0; rIdx < stocks.length; rIdx++) {
      const row = stocks[rIdx];
      if (!row || !row.id || !row.stock) continue;

      const payload = {
        ...row,
        userRef,
        simRunRef,
        monthIndex,
        simDate,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      };

      // new doc per (run, month, account)
      const docRef = simStocksColl.doc();
      addSet(docRef, payload);

      // IMPORTANT: allow rolling commits during fan-out
      await commitIfNeeded(false);
    }
  }


  // Commit remaining writes
  await commitIfNeeded(true);


  return { simRunId: simRunRef.id };
}

module.exports = { writeSimOutputs };
