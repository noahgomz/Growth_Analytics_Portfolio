// functions/cf/1 - simulatePlan.js

const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');

// -------- IMPORTS BASED ON YOUR ACTUAL FOLDER STRUCTURE --------

// Loaders
const {
  loadSimulationInputs,
} = require('../lib/creditEngine/loaders/loadSimulationInputs');

// Simulation engine core
const {
  SimulationState,
} = require('../lib/creditEngine/sim/SimulationState');

const {
  tickOneMonth,
} = require('../lib/creditEngine/sim/tickOneMonth');

const {
  writeSimOutputs,
} = require('../lib/creditEngine/sim/writeSimOutputs');

// -------- SAFETY LIMITS --------
const MAX_MONTHS = 120;   // simulation hard cap
const DEFAULT_MONTHS = 12;

exports.simulatePlan = functions
  .runWith({
    timeoutSeconds: 120,
    memory: '512MB',
  })
  .region('us-central1')
  .https.onCall(async (data, context) => {

    // ---------- AUTH ----------
    if (!context.auth || !context.auth.uid) {
      throw new functions.https.HttpsError(
        'unauthenticated',
        'Must be signed in.'
      );
    }

    const db = admin.firestore();
    const uid = context.auth.uid;
    const userRef = db.doc(`users/${uid}`);

    // ---------- UI INPUT NORMALIZATION ----------

    // Months (duration is emergent now; we only keep a hard cap)
    const numberOfMonthsInSim = MAX_MONTHS;

    // Simulated monthly spend
    const monthlySimulatedSpend =
      typeof data?.monthlySimulatedSpend === 'number' &&
      isFinite(data.monthlySimulatedSpend) &&
      data.monthlySimulatedSpend > 0
        ? data.monthlySimulatedSpend
        : 0;

    // Expected limits for new accounts
    const expectedNewCardLimit =
      typeof data?.expectedNewCardLimit === 'number' &&
      isFinite(data.expectedNewCardLimit) &&
      data.expectedNewCardLimit > 0
        ? data.expectedNewCardLimit
        : 0;

    const expectedNewLoanPrincipal =
      typeof data?.expectedNewLoanPrincipal === 'number' &&
      isFinite(data.expectedNewLoanPrincipal) &&
      data.expectedNewLoanPrincipal > 0
        ? data.expectedNewLoanPrincipal
        : 0;

    // ---------- LOAD ORIGIN DATA ----------
    const loaded = await loadSimulationInputs(
      db,
      userRef,
      numberOfMonthsInSim
    );

// Monthly budget MUST be passed from UI
const monthlyBudget =
  typeof data?.budget === 'number' &&
  isFinite(data.budget) &&
  data.budget > 0
    ? data.budget
    : null;

// If no budget, end sim immediately and write a summary fail flag
if (monthlyBudget == null) {
  const simRunRef = userRef.collection('SimulationRuns').doc();

  // write a minimal run header
  await simRunRef.set(
    {
      userRef,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  // write the summary doc with the fail flag
  await db
    .collection('user_sim_stocks_conso')
    .doc(`summary_run_${simRunRef.id}`)
    .set(
      {
        userRef,
        simRunRef,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        sim_failed_no_budget_found: true,
      },
      { merge: true }
    );

  return {
    success: false,
    simRunId: simRunRef.id,
    sim_failed_no_budget_found: true,
  };
}


    // ---------- INITIALIZE SIM STATE ----------
    const state = new SimulationState({
      ...loaded,
      monthlyBudget,              // 🔹 new
      monthlySimulatedSpend,
      expectedNewCardLimit,
      expectedNewLoanPrincipal,
      startDate: new Date(),
    });


    // ---------- RUN SIMULATION (EMERGENT DURATION) ----------
    let i = 0;

    // stop flags
    let openDone = false;
    let closeDone = false;
    let payDone = false;
    let useDone = false;
    let utilStable = false;
    let ageDone = false;

    while (i < MAX_MONTHS) {
      tickOneMonth(state);

      // ---- compute util ceiling (whole-number %: 30 -> 35) ----
      const utilDoc = Array.isArray(state.openCloseAdjustments)
        ? state.openCloseAdjustments.find((d) => d && d.Unique_Name === 'Utilization')
        : null;

      const utilTargetWhole =
        utilDoc && typeof utilDoc.User_Value === 'number' && isFinite(utilDoc.User_Value)
          ? utilDoc.User_Value
          : null;

      const utilCeilingWhole =
        utilTargetWhole != null ? utilTargetWhole + 5 : null;

      // ---- compute current utilization (whole-number %) ----
      let totalCardBal = 0;
      let totalCardLimit = 0;

      for (const row of state.accounts || []) {
        if (row && row.stock === 'user_credit_cards' && row.isOpen !== false) {
          totalCardBal += Number(row.totalBalance ?? row.amountsOwed ?? row.balance ?? 0) || 0;
          totalCardLimit += Number(row.creditLimit ?? 0) || 0;
        }
      }

      const utilWhole =
        totalCardLimit > 0 ? (totalCardBal / totalCardLimit) * 100 : 0;

      const utilOK =
        utilCeilingWhole == null ? false : utilWhole <= utilCeilingWhole;

      // ---- pay done: unpaid lates + unpaid collections are 0 AND util OK ----
      const unpaidLateBal = (state.lateItems || []).reduce(
        (sum, r) => sum + (Number(r.amountsOwed ?? r.totalBalance ?? r.balance ?? 0) || 0),
        0
      );
      const unpaidCollBal = (state.tpcItems || []).reduce(
        (sum, r) => sum + (Number(r.amountsOwed ?? r.totalBalance ?? r.balance ?? 0) || 0),
        0
      );

      payDone = unpaidLateBal === 0 && unpaidCollBal === 0 && utilOK;

      // ---- use done + util stable (v1: same condition; you can add streak later) ----
      useDone = utilOK;
      utilStable = utilOK;

      // ---- open/close done: read meta that we will push from cycles ----
      openDone =
        state._openMeta &&
        typeof state._openMeta.neededTotal === 'number' &&
        state._openMeta.neededTotal === 0;

      closeDone =
        state._closeMeta &&
        typeof state._closeMeta.candidateCount === 'number' &&
        state._closeMeta.candidateCount === 0;

      // ---- age done: will be set once we pass LH_averageAgeMonths target ----
      // Placeholder: we’ll wire this once LH_averageAgeMonths is loaded into state (next edits).
      ageDone =
        typeof state.chaAvgAgeMonthsTarget === 'number' &&
        isFinite(state.chaAvgAgeMonthsTarget) &&
        typeof state.avgAgeMonthsOpen === 'number' &&
        isFinite(state.avgAgeMonthsOpen) &&
        state.avgAgeMonthsOpen >= state.chaAvgAgeMonthsTarget;

      // ---- stop when ALL are done ----
      if (openDone && closeDone && payDone && utilStable && useDone && ageDone) {
        break;
      }

      i += 1;
    }

    // store for output writer (summary doc)
    // snapshots are the canonical “months actually simulated”
    state._totalMonths = Array.isArray(state.stockSnapshots)
      ? state.stockSnapshots.length
      : (i + 1);


    // ---------- WRITE RESULTS ----------
    const { simRunId } = await writeSimOutputs(db, userRef, state);

    return {
      success: true,
      simRunId,
      months: state._totalMonths ?? numberOfMonthsInSim,
    };
  });
