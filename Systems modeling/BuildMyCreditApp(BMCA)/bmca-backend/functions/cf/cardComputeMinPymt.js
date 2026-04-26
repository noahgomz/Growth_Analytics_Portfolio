const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');

// NOTE: Do NOT call admin.initializeApp()
// It's already called once in index.js

module.exports = functions
  .region('us-central1')
  .firestore
  .document('user_credit_cards/{cardId}')
  .onWrite(async (change, context) => {

    const beforeData = change.before.exists ? change.before.data() : null;
    const afterData  = change.after.exists  ? change.after.data()  : null;

    if (!afterData) {
      console.log(`cardComputeMinPymt: deleted, skipping ${context.params.cardId}`);
      return null;
    }

    const oldBalance = beforeData?.totalBalance;
    const newBalance = afterData.totalBalance;

    if (newBalance == null) {
      console.log(`cardComputeMinPymt: totalBalance missing on ${context.params.cardId}, skipping`);
      return null;
    }

    if (oldBalance === newBalance) {
      console.log(`cardComputeMinPymt: totalBalance unchanged (${newBalance}) on ${context.params.cardId}, skipping`);
      return null;
    }

    const newMin = newBalance * 0.10;
    const oldMin = afterData.minimumPayment;

    if (oldMin === newMin) {
      console.log(`cardComputeMinPymt: minimumPayment already ${newMin} on ${context.params.cardId}, skipping`);
      return null;
    }

    try {
      await change.after.ref.update({ minimumPayment: newMin });
      console.log(`cardComputeMinPymt: updated minimumPayment → ${newMin} on ${context.params.cardId}`);
    } catch (err) {
      console.error(`cardComputeMinPymt: ERROR updating on ${context.params.cardId}:`, err);
    }

    return null;
  });
