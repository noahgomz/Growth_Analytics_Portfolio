// functions/lib/creditEngine/sim/SimulationState.js

/**
 * SimulationState:
 * Represents the entire in-memory world for the simulation.
 *
 * Each tick (cycle/month) mutates:
 *   - this.stocks        (in-memory stocks_conso-style rows)
 *   - this.simDate       (moves forward by 1 month)
 *   - this.monthIndex    (incremented)
 *   - this.stockSnapshots array (one saved snapshot per month)
 *
 * The sim engine (tickOneMonth + pay/open/close/use steps)
 * reads & mutates this object.
 *
 * writeSimOutputs() then flushes all snapshots to Firestore.
 */

class SimulationState {
  constructor({
    // ----- REQUIRED (from loadSimulationInputs + simulatePlan) -----
    monthsInSim,
    user,
    stocksConso,
    offers_loans,
    offers_cards,
    user_openAndCloseAdjustments,
    credit_high_achiever_account_numbers, // 🔹 new
    chaSum,                               // 🔹 new (global CHA total)
    chaAvgAgeMonthsTarget,                // 🔹 new (from credit_high_achiever_metrics)


    // ----- SIMULATION KNOBS (from simulatePlan or UI) -----
    monthlySimulatedSpend = 0,
    expectedNewCardLimit = 0,
    expectedNewLoanPrincipal = 0,
    monthlyBudget,          // optional override
    startDate,              // optional override of sim start date
  }) {
    // Raw inputs
    this.monthsInSim = monthsInSim || 0;
    this.user = user || null;

    // Clone stocks so we never mutate the loader’s array
    this.stocks = Array.isArray(stocksConso)
      ? stocksConso.map((row) => ({ ...row }))
      : [];

    this.offers_loans = Array.isArray(offers_loans) ? offers_loans : [];
    this.offers_cards = Array.isArray(offers_cards) ? offers_cards : [];

    // User open/close adjustment docs from Firestore
    this.user_openAndCloseAdjustments =
      Array.isArray(user_openAndCloseAdjustments)
        ? user_openAndCloseAdjustments
        : [];

    /**
     * IMPORTANT:
     * Many of the open/close helpers (runOpenCycle / runCloseCycle)
     * already expect `state.openCloseAdjustments`.
     * We alias to keep that contract without changing their code.
     */
    this.openCloseAdjustments = this.user_openAndCloseAdjustments;

    // ----- CHA targets (global) -----
    // Raw docs, in case we later want per-row breakdowns
    this.credit_high_achiever_account_numbers =
      Array.isArray(credit_high_achiever_account_numbers)
        ? credit_high_achiever_account_numbers
        : [];

    // CHA total account-number target (Path B in open logic)
    this.chaSum =
      typeof chaSum === 'number' && Number.isFinite(chaSum) ? chaSum : 0;


    // Global sum of Value/value (same as writeOpenActionsListORCH)
    // CHA age target (months) — used for emergent-duration stop condition
    this.chaAvgAgeMonthsTarget =
      typeof chaAvgAgeMonthsTarget === 'number' && Number.isFinite(chaAvgAgeMonthsTarget)
        ? chaAvgAgeMonthsTarget
        : null;

// ----- Budget handling -----
// Budget must be passed from UI → simulatePlan enforces presence.
// No fallback to user doc fields.
this.monthlyBudget =
  typeof monthlyBudget === 'number' && Number.isFinite(monthlyBudget)
    ? monthlyBudget
    : 0;


    // ----- Use cycle knobs -----
    this.monthlySimulatedSpend =
      typeof monthlySimulatedSpend === 'number'
        ? monthlySimulatedSpend
        : 0;

    this.expectedNewCardLimit =
      typeof expectedNewCardLimit === 'number'
        ? expectedNewCardLimit
        : 0;

    this.expectedNewLoanPrincipal =
      typeof expectedNewLoanPrincipal === 'number'
        ? expectedNewLoanPrincipal
        : 0;

    // ----- Time tracking -----
    this.monthIndex = 0;
    this.simDate =
      startDate instanceof Date
        ? new Date(startDate)
        : new Date(); // can be overridden later if desired

    // Derived views over stocks_conso universe
    this.rebuildViewsFromStocks();

    // Snapshots: one element per month after tickOneMonth()
    this.stockSnapshots = [];
  }

  /**
   * Build the derived views the rest of the sim expects:
   *   - accounts: current revolving + installment accounts
   *   - lateItems: late-payment rows (cards + loans)
   *   - tpcItems: 3rd party collections
   *
   * This is the “single source of truth” for how we slice stocks_conso.
   */
  rebuildViewsFromStocks() {
    const all = Array.isArray(this.stocks) ? this.stocks : [];

    this.accounts = all.filter(
      (row) =>
        row &&
        (row.stock === 'user_credit_cards' ||
         row.stock === 'user_loans')
    );

    this.lateItems = all.filter(
      (row) =>
        row &&
        (row.stock === 'user_credit_cards_late_payments' ||
         row.stock === 'user_loans_late_payments') &&
        row.isPaid !== true
    );

    this.tpcItems = all.filter(
      (row) =>
        row &&
        row.stock === 'user_collections_3rd_party' &&
        row.isPaid !== true
    );

  }

  /**
   * Public helper so tickOneMonth() (or a step like open/close)
   * can rebuild the slices after structural mutations.
   */
  refreshDerivedViews() {
    this.rebuildViewsFromStocks();
  }

  /**
   * Capture the entire set of stocks for this cycle.
   * tickOneMonth() calls this after applying pay/open/close/use.
   */
  snapshotCurrentStocks() {
    // --- compute avg age (months) across OPEN accounts (cards + loans) ---
    let ageSum = 0;
    let ageCount = 0;

    const now = this.simDate instanceof Date ? this.simDate : new Date(this.simDate);

    for (const row of this.accounts || []) {
      if (!row || row.isOpen === false) continue;

      const od = new Date(
        row.DOFRecord ??
          row.dateOpened ??
          row.date_issued ??
          row.Date_issued ??
          row.dateIssued ??
          null
      );

      if (!isNaN(od.getTime())) {
        const months = (now.getTime() - od.getTime()) / (1000 * 60 * 60 * 24 * 30);
        ageSum += months;
        ageCount += 1;
      }
    }

    // store on state for simulatePlan stopping condition
    this.avgAgeMonthsOpen = ageCount > 0 ? ageSum / ageCount : 0;

    this.stockSnapshots.push({
      cycleIndex: this.monthIndex,
      simDate: this.simDate,

      // meta written by runOpenCycle / runCloseCycle (per-cycle)
      openMeta: this._openMeta ? { ...this._openMeta } : null,
      closeMeta: this._closeMeta ? { ...this._closeMeta } : null,

      // store on snapshot too (for summary doc / debugging)
      avgAgeMonthsOpen: this.avgAgeMonthsOpen,

      stocks: this.stocks.map((row) => ({ ...row })), // shallow clone rows
    });
  }
}

module.exports = { SimulationState };
