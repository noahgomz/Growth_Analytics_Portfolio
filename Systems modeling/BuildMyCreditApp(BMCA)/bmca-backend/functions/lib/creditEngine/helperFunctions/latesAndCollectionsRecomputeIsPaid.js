// functions/lib/creditEngine/helperFunctions/latesAndCollectionsRecomputeIsPaid.js

// Simple numeric helper (mirrors the live CF logic structure)
function asNum(v, d = null) {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '' && !isNaN(Number(v))) {
    return Number(v);
  }
  return d;
}

/**
 * recomputeIsPaidForLateOrCollection(row)
 *
 * Sim-only helper that mirrors the live latesAndCollectionsIsPaid behavior:
 *   - If the (new) amount is effectively 0, set isPaid = true.
 *
 * This is called AFTER we have updated the `amount` / `amountsOwed` fields
 * for late/collection rows in applyPaymentToRow.
 *
 * It mutates the row in place and also returns it for convenience.
 */
function recomputeIsPaidForLateOrCollection(row) {
  if (!row) return row;

  const stock = row.stock || '';

  // Only apply to card lates, loan lates, and 3rd-party collections
  const isLate =
    stock === 'user_credit_cards_late_payments' ||
    stock === 'user_loans_late_payments';

  const isCollection = stock === 'user_collections_3rd_party';

  if (!isLate && !isCollection) return row;

  const amount = asNum(row.amount ?? row.amountsOwed, null);
  if (amount === null) return row;

  // Live CF checks for == 0; here we treat very small residuals as 0
  if (Math.abs(amount) < 0.0001 && row.isPaid !== true) {
    row.isPaid = true;
  }

  return row;
}

module.exports = {
  recomputeIsPaidForLateOrCollection,
};
