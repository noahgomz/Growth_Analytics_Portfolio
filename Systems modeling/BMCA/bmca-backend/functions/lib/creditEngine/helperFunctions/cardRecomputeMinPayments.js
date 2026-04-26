// functions/lib/creditEngine/helperFunctions/cardRecomputeMinPayments.js

/**
 * asNum(v, d)
 * Tiny numeric normalizer used only in this helper.
 */
function asNum(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

/**
 * recomputeCardMinPayment(cardRow)
 *
 * Sim-only helper that mirrors the live onWrite CF logic:
 *   minimumPayment = 10% of current balance
 *
 * - Expects a stocks_conso-style card row (from state.stocks or state.accounts).
 * - Mutates the row in place and also returns it for convenience.
 *
 * We update BOTH:
 *   - row.minPayment      (what the sim Pay engine uses)
 *   - row.minimumPayment  (to stay consistent with live schema naming)
 */
function recomputeCardMinPayment(cardRow) {
  if (!cardRow) return cardRow;

  const stock = cardRow.stock || '';

  // Only apply to credit cards; loans keep their monthlyPayment.
  if (stock !== 'user_credit_cards') return cardRow;

  // Prefer amountsOwed → totalBalance → balance, mirroring other sim logic.
  const rawBalance =
    cardRow.amountsOwed ??
    cardRow.totalBalance ??
    cardRow.balance ??
    0;

  const balance = asNum(rawBalance, 0);

  // Live rule: 10% of balance; if balance is 0, min payment is 0.
  const newMin = balance > 0 ? balance * 0.10 : 0;

  // Update both internal + live-style fields.
  cardRow.minPayment = newMin;
  cardRow.minimumPayment = newMin;

  return cardRow;
}

module.exports = {
  recomputeCardMinPayment,
};
