// functions/cf/latesAndCollectionsIsPaid.js

const functions = require('firebase-functions/v1');
// admin not actually needed here, but safe to keep if you prefer:
// const admin = require('firebase-admin');

// Simple numeric helper
const asNum = (v, d = null) => {
  if (typeof v === 'number' && isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '' && !isNaN(Number(v))) {
    return Number(v);
  }
  return d;
};

/**
 * Shared handler: when amount becomes 0 and isPaid is not yet true,
 * set isPaid: true.
 */
async function handleIsPaid(change, context) {
  // If the document was deleted, do nothing
  if (!change.after.exists) {
    return null;
  }

  const before = change.before.exists ? change.before.data() : null;
  const after = change.after.data();

  const newAmount = asNum(after.amount, null);
  const prevAmount = before ? asNum(before.amount, null) : null;

  // Only act when amount is exactly 0
  if (newAmount !== 0) {
    return null;
  }

  // If it was already 0 and isPaid was already true, no work to do
  if (prevAmount === 0 && before && before.isPaid === true) {
    return null;
  }

  // If isPaid is already true, skip to avoid infinite loops
  if (after.isPaid === true) {
    return null;
  }

  await change.after.ref.update({
    isPaid: true,
  });

  return null;
}

// --- One trigger per collection ---

// /user_credit_cards_late_payments
exports.setIsPaidOnCardLates = functions
  .firestore
  .document('user_credit_cards_late_payments/{docId}')
  .onWrite((change, context) => handleIsPaid(change, context));

// /user_loans_late_payments
exports.setIsPaidOnLoanLates = functions
  .firestore
  .document('user_loans_late_payments/{docId}')
  .onWrite((change, context) => handleIsPaid(change, context));

// /user_collections_3rd_party
exports.setIsPaidOnCollections = functions
  .firestore
  .document('user_collections_3rd_party/{docId}')
  .onWrite((change, context) => handleIsPaid(change, context));
