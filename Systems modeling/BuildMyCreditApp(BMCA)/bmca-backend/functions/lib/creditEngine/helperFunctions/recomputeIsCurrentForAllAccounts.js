// functions/lib/creditEngine/helperFunctions/recomputeIsCurrentForAllAccounts.js

const {
  recomputeCardIsCurrent,
} = require('./cardRecomputeIsCurrent');

const {
  recomputeLoanIsCurrent,
} = require('./loanRecomputeIsCurrent');

/**
 * recomputeIsCurrentForAllAccounts(state)
 *
 * Runs the isCurrent logic for BOTH credit cards and loans.
 * Mirrors the live CF behavior, but operates purely in-memory in the sim.
 *
 * Reads:
 *   - state.stocks  (full flattened stocks_conso rows)
 *   - state.lateItems (flattened lates with originAccountStock + originAccountId)
 *
 * Mutates the appropriate account rows' `isCurrent` fields.
 */
function recomputeIsCurrentForAllAccounts(state) {
  if (!state || !Array.isArray(state.stocks)) return;

  const lateItems = Array.isArray(state.lateItems)
    ? state.lateItems
    : [];

  for (const row of state.stocks) {
    if (!row || !row.stock) continue;

    if (row.stock === 'user_credit_cards') {
      recomputeCardIsCurrent(row, lateItems);
    } else if (row.stock === 'user_loans') {
      recomputeLoanIsCurrent(row, lateItems);
    }
  }
}

module.exports = {
  recomputeIsCurrentForAllAccounts,
};
