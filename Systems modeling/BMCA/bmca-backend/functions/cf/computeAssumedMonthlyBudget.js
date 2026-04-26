const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// To avoid deployment errors, do not call admin.initializeApp() in your code

exports.computeAssumedMonthlyBudget = functions
  .region('us-central1')
  .https.onCall(async (data, context) => {
    if (!context.auth || !context.auth.uid) {
      return;
    }

    const db = admin.firestore();
    const uid = context.auth.uid;
    const userDocRef = db.doc(`users/${uid}`);

    const asNum = (v, d = 0) => {
      const n = Number(v);
      return Number.isFinite(n) ? n : d;
    };

    const round2 = (n) =>
      Math.round((Number(n) + Number.EPSILON) * 100) / 100;

    // ---- Read inputs ----
    const userRef = userDocRef;

    const [loansSnap, cardsSnap, userSnap] = await Promise.all([
      db.collection('user_loans')
        .where('userRef', '==', userRef)
        .where('isOpen', '==', true)
        .get(),

      db.collection('user_credit_cards')
        .where('userRef', '==', userRef)
        .where('isOpen', '==', true)
        .get(),

      userDocRef.get(),
    ]);

    // ---- LP: sum monthly loan payments ----
    let loanPayments = 0;
    loansSnap.forEach((doc) => {
      const d = doc.data() || {};
      loanPayments += asNum(d.monthlyPayment, 0);
    });

    // ---- CB: sum revolving card balances ----
    let cardBalances = 0;
    cardsSnap.forEach((doc) => {
      const d = doc.data() || {};
      cardBalances += asNum(d.totalBalance, 0);
    });

    // ---- Formula ----
    const cardBudget_fromBalances = Math.max(100, 0.055 * cardBalances);
    const cardBudget_capFromLoans = Math.max(100, 0.5 * loanPayments);
    const cardBudget = Math.min(cardBudget_fromBalances, cardBudget_capFromLoans);

    const assumedMonthlyBudget_raw = loanPayments + cardBudget;

    // Round once (this is what we store + return)
    const assumedMonthlyBudget = round2(assumedMonthlyBudget_raw);
    loanPayments = round2(loanPayments);
    cardBalances = round2(cardBalances);
    const cardBudgetRounded = round2(cardBudget);

    // ---- Write to user doc ONLY if changed ----
    const prev = asNum(userSnap.exists ? userSnap.data()?.assumedBudget : 0, 0);
    const prevRounded = round2(prev);

    let didWrite = false;

    // Compare rounded values so you don't thrash writes due to tiny float diffs
    if (prevRounded !== assumedMonthlyBudget) {
      await userDocRef.update({
        assumedBudget: assumedMonthlyBudget,
      });
      didWrite = true;
    }

    // ---- Output ----
    return {
      assumedMonthlyBudget,
      loanPayments,
      cardBudget: cardBudgetRounded,
      cardBalances,
      didWrite,
      previousAssumedBudget: prevRounded,
    };
  });
