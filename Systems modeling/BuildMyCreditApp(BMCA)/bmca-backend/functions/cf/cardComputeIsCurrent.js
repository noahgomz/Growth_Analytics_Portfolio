const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');

// NOTE: Do NOT call admin.initializeApp() here
// It's already called once in index.js

module.exports = functions
  .region('us-central1')
  .firestore
  .document('user_credit_cards_late_payments/{latePaymentId}')
  .onWrite(async (change, context) => {
    const afterData  = change.after.exists  ? change.after.data()  : null;
    const beforeData = change.before.exists ? change.before.data() : null;

    // If neither before nor after have a userRef/cardRef, we can't proceed
    const cardRef = (afterData || beforeData)?.cardRef;
    const userRef = (afterData || beforeData)?.userRef;
    if (!cardRef || !userRef) {
      console.warn(
        `cardComputeIsCurrent: missing cardRef/userRef on ${context.params.latePaymentId}, skipping`
      );
      return null;
    }

    const db = admin.firestore();

    // 1) Re-fetch ALL late-payments for that card + user
    const snaps = await db
      .collection('user_credit_cards_late_payments')
      .where('cardRef','==',cardRef)
      .where('userRef','==',userRef)
      .get();

    // 2) If ANY unpaid remain ⇒ isCurrent=false; otherwise true
    const anyUnpaid = snaps.docs.some(d => d.data().isPaid === false);
    const newIsCurrent = !anyUnpaid;

    // 3) Fetch current state of the card
    const cardSnap = await cardRef.get();
    if (!cardSnap.exists) {
      console.warn(`cardComputeIsCurrent: card not found at ${cardRef.path}, skipping`);
      return null;
    }
    const oldIsCurrent = cardSnap.data().isCurrent;

    // 4) Only write if it actually changed
    if (oldIsCurrent === newIsCurrent) {
      console.log(
        `cardComputeIsCurrent: isCurrent already ${newIsCurrent} on ${cardRef.id}, skipping`
      );
      return null;
    }

    // 5) Update the card’s isCurrent flag
    await cardRef.update({ isCurrent: newIsCurrent });
    console.log(
      `cardComputeIsCurrent: updated isCurrent → ${newIsCurrent} on ${cardRef.id}`
    );
    return null;
  });
