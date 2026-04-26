// functions/lib/creditEngine/helperFunctions/loanRecomputeIsCurrent.js

/**
 * recomputeLoanIsCurrent(loanRow, lateItems)
 *
 * Sim-only helper that mirrors the live loanComputeIsCurrent behavior:
 *
 *   loan.isCurrent = true  IF there are NO unpaid late payments
 *                           linked to this loan.
 *   loan.isCurrent = false IF at least one linked late has isPaid === false.
 *
 * Inputs:
 *   - loanRow: a stocks_conso-style row for a loan
 *              (from state.stocks or state.accounts).
 *   - lateItems: array of flattened loan-late rows (state.lateItems), where
 *       each loan late has:
 *         originAccountStock: 'user_loans'
 *         originAccountId:    loan id string
 *         isPaid:             boolean (or missing/undefined)
 *
 * This function mutates loanRow in place and also returns it
 * for convenience.
 */
function recomputeLoanIsCurrent(loanRow, lateItems) {
  if (!loanRow) return loanRow;

  const stock = loanRow.stock || '';
  const id = loanRow.id;

  // Only loans are affected by this rule.
  if (stock !== 'user_loans' || !id) return loanRow;

  const hasUnpaidLate = (lateItems || []).some((late) => {
    if (!late) return false;

    const originStock = late.originAccountStock || '';
    const originId = late.originAccountId;

    // Only consider lates that belong to this loan
    if (originStock !== stock || originId !== id) return false;

    // Explicit false means "unpaid"; anything else is treated as paid/irrelevant
    return late.isPaid === false;
  });

  loanRow.isCurrent = !hasUnpaidLate;

  return loanRow;
}

module.exports = {
  recomputeLoanIsCurrent,
};
