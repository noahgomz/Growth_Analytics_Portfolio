// functions/lib/creditEngine/helperFunctions/cardRecomputeIsCurrent.js

/**
 * recomputeCardIsCurrent(cardRow, lateItems)
 *
 * Sim-only helper that mirrors the live cardComputeIsCurrent behavior:
 *
 *   card.isCurrent = true  IF there are NO unpaid late payments
 *                           linked to this card.
 *   card.isCurrent = false IF at least one linked late has isPaid === false.
 *
 * Inputs:
 *   - cardRow: a stocks_conso-style row for a credit card.
 *   - lateItems: array of flattened late rows (state.lateItems), where
 *       each late row has:
 *         originAccountStock: 'user_credit_cards' | 'user_loans'
 *         originAccountId:    account id string
 *         isPaid:             boolean (or missing/undefined)
 *
 * Mutates cardRow in place and also returns it for convenience.
 */
function recomputeCardIsCurrent(cardRow, lateItems) {
  if (!cardRow) return cardRow;

  const stock = cardRow.stock || '';
  const id = cardRow.id;

  // Only cards are affected by this rule in the live system.
  if (stock !== 'user_credit_cards' || !id) return cardRow;

  const hasUnpaidLate = (lateItems || []).some((late) => {
    if (!late) return false;

    const originStock = late.originAccountStock || '';
    const originId = late.originAccountId;

    if (originStock !== stock || originId !== id) return false;

    // Treat explicit false as "unpaid", anything else = paid/irrelevant.
    return late.isPaid === false;
  });

  cardRow.isCurrent = !hasUnpaidLate;

  return cardRow;
}

module.exports = {
  recomputeCardIsCurrent,
};
