// functions/lib/creditEngine/sim/tickOneMonth.js

const {
  buildPayPriorityListFromData,
} = require('../pay/buildPayPriorityListFromData');

const { runOpenCycle } = require('../open/runOpenCycle');
const { runCloseCycle } = require('../close/runCloseCycle');
const { runUseCycle } = require('../use/runUseCycle');

// Helpers: min payment + isCurrent
const {
  recomputeCardMinPayment,
} = require('../helperFunctions/cardRecomputeMinPayments');

const {
  recomputeIsCurrentForAllAccounts,
} = require('../helperFunctions/recomputeIsCurrentForAllAccounts');

const {
  recomputeIsPaidForLateOrCollection,
} = require('../helperFunctions/latesAndCollectionsRecomputeIsPaid');


// ---------- utilities ----------

function addMonths(date, n) {
  const d = new Date(date);
  d.setMonth(d.getMonth() + n);
  return d;
}

function asNum(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

/**
 * Apply a payment allocation to a single in-memory row
 * (stocks_conso-shaped, or the account/late/collection mirrors).
 */
function applyPaymentToRow(row, stock, allocRaw) {
  if (!row) return;
  const alloc = asNum(allocRaw, 0);
  if (alloc <= 0) return;

  // --- Revolving / installment accounts ---

  if (stock === 'user_credit_cards') {
    const prev = asNum(
      row.amountsOwed ??
        row.totalBalance ??
        row.balance ??
        0,
      0
    );
    const next = Math.max(0, prev - alloc);

    row.amountsOwed = next;
    row.totalBalance = next;
    row.balance = next;

    // Keep minPayment in sync with new balance (cards only)
    recomputeCardMinPayment(row);

    return;
  }

  if (stock === 'user_loans') {
    const prev = asNum(
      row.amountsOwed ??
        row.balance ??
        0,
      0
    );
    const next = Math.max(0, prev - alloc);

    row.amountsOwed = next;
    row.balance = next;
    return;
  }

  // --- Lates ---

  if (
    stock === 'user_credit_cards_late_payments' ||
    stock === 'user_loans_late_payments'
  ) {
    const prev = asNum(
      row.amount ??
        row.amountsOwed ??
        0,
      0
    );
    const next = Math.max(0, prev - alloc);

    row.amount = next;
    row.amountsOwed = next;

    // If fully paid in sim, mark as paid
    if (next <= 0.0001) {
      row.isPaid = true;
    }

    // Keep isPaid consistent with live CF logic
    recomputeIsPaidForLateOrCollection(row);

    return;
  }

  // --- 3rd-party collections ---

  if (stock === 'user_collections_3rd_party') {
    const prev = asNum(
      row.amount ??
        row.amountsOwed ??
        0,
      0
    );
    const next = Math.max(0, prev - alloc);

    row.amount = next;
    row.amountsOwed = next;

    if (next <= 0.0001) {
      row.isPaid = true;
    }

    // Keep isPaid consistent with live CF logic
    recomputeIsPaidForLateOrCollection(row);

    return;
  }

  // --- Fallback for any other stock types ---

  const prev = asNum(
    row.amountsOwed ??
      row.balance ??
      row.totalBalance ??
      row.amount ??
      0,
    0
  );
  const next = Math.max(0, prev - alloc);
  row.amountsOwed = next;
}

/**
 * tickOneMonth(state)
 *
 *   1. PAY
 *   2. OPEN
 *   3. CLOSE
 *   4. USE
 *   5. SNAPSHOT
 *   6. ADVANCE simDate + monthIndex
 *
 * `state` is an instance of SimulationState.
 */
function tickOneMonth(state) {
  if (!state || !state.stocks) {
    console.warn('tickOneMonth called with invalid state');
    return state;
  }

  const now =
    state.simDate instanceof Date ? state.simDate : new Date();
  const nowMs = now.getTime();

  // ------------------- 1) PAY ACTIONS -------------------
  const payResult = buildPayPriorityListFromData({
    nowMs,
    monthlyBudget: state.monthlyBudget || 0,
    accounts: state.accounts || [],
    lateItems: state.lateItems || [],
    tpcItems: state.tpcItems || [],
  });

  if (payResult && Array.isArray(payResult.ranked)) {
    payResult.ranked.forEach((pItem) => {
      if (!pItem || !pItem.id || !pItem.stock) return;

      const id = pItem.id;
      const stock = pItem.stock;
      const alloc = asNum(pItem.alloc_total, 0);
      if (alloc <= 0) return;

      // --- Update canonical stocks_conso-shaped rows (ONLY ONCE) ---
      if (Array.isArray(state.stocks)) {
        const row = state.stocks.find(
          (s) => s.id === id && s.stock === stock
        );
        if (row) applyPaymentToRow(row, stock, alloc);
      }

    });
  }

  // After PAY, recompute isCurrent flags using your shared helper
  // Rebuild derived views after PAY mutated canonical stocks
if (typeof state.refreshDerivedViews === 'function') {
  state.refreshDerivedViews();
} else if (typeof state.rebuildViewsFromStocks === 'function') {
  state.rebuildViewsFromStocks();
}

  recomputeIsCurrentForAllAccounts(state);

  // ------------------- 2) OPEN ACTIONS -------------------
  runOpenCycle(state);

  // ------------------- 3) CLOSE ACTIONS -------------------
  runCloseCycle(state);

  // ------------------- 4) USE ACTIONS -------------------
  runUseCycle(state);

  // After USE, recompute isCurrent flags again
  // Rebuild derived views after USE mutated canonical stocks
if (typeof state.refreshDerivedViews === 'function') {
  state.refreshDerivedViews();
} else if (typeof state.rebuildViewsFromStocks === 'function') {
  state.rebuildViewsFromStocks();
}

  recomputeIsCurrentForAllAccounts(state);

  // ------------------- 5) SNAPSHOT FOR OUTPUT -------------------
  if (typeof state.snapshotCurrentStocks === 'function') {
    state.snapshotCurrentStocks();
  }

  // ------------------- 6) ADVANCE MONTH -------------------
  state.monthIndex += 1;
  state.simDate = addMonths(now, 1);

  return state;
}

module.exports = { tickOneMonth };
