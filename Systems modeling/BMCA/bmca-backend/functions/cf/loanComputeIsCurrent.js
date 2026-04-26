const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');

// NOTE: Do NOT call admin.initializeApp() here
// It's already called once in index.js

module.exports = functions
  .region('us-central1')
  .firestore
  .document('user_loans_late_payments/{latePaymentId}')
  .onWrite(async (change, context) => {
    const afterData  = change.after.exists  ? change.after.data()  : null;
    const beforeData = change.before.exists ? change.before.data() : null;

    // Grab loanRef and userRef from whichever exists
    const loanRef = (afterData || beforeData)?.loanRef;
    const userRef = (afterData || beforeData)?.userRef;
    if (!loanRef || !userRef) {
      console.warn(
        `loanComputeIsCurrent: missing loanRef/userRef on ${context.params.latePaymentId}, skipping`
      );
      return null;
    }

    const db = admin.firestore();

    // 1) Re-fetch all late-payments for this loan + user
    const snaps = await db
      .collection('user_loans_late_payments')
      .where('loanRef','==',loanRef)
      .where('userRef','==',userRef)
      .get();

    // 2) If any remain unpaid ⇒ isCurrent=false; otherwise true
    const anyUnpaid = snaps.docs.some(d => d.data().isPaid === false);
    const newIsCurrent = !anyUnpaid;

    // 3) Fetch the parent loan doc
    const loanSnap = await loanRef.get();
    if (!loanSnap.exists) {
      console.warn(`loanComputeIsCurrent: loan not found at ${loanRef.path}, skipping`);
      return null;
    }
    const oldIsCurrent = loanSnap.data().isCurrent;

    // 4) Only update if changed
    if (oldIsCurrent === newIsCurrent) {
      console.log(
        `loanComputeIsCurrent: isCurrent already ${newIsCurrent} on ${loanRef.id}, skipping`
      );
      return null;
    }

    // 5) Write back the new isCurrent value
    await loanRef.update({ isCurrent: newIsCurrent });
    console.log(
      `loanComputeIsCurrent: updated isCurrent → ${newIsCurrent} on ${loanRef.id}`
    );
    return null;
  });
