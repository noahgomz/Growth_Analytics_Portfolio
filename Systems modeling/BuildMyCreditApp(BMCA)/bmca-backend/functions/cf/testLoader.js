const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// FF initializes admin for us

const {
  loadSimulationInputs,
} = require('../lib/creditEngine/loaders/loadSimulationInputs');


exports.testLoader = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth?.uid) return;

    const db = admin.firestore();
    const userRef = db.doc(`users/${context.auth.uid}`);

    // optional months param
    const rawMonths = data?.months;

    // Load & normalize all data
    const result = await loadSimulationInputs(db, userRef, rawMonths);

    // --- LOGS (for inspection) ---
    console.log('===== TEST LOADER OUTPUT =====');
    console.log('monthsInSim:', result.monthsInSim);
    console.log('user:', JSON.stringify(result.user, null, 2));
    console.log('offers_loans count:', result.offers_loans.length);
    console.log('offers_cards count:', result.offers_cards.length);
    console.log('open/close adjustments count:', result.user_openAndCloseAdjustments.length);
    console.log('stocksConso count:', result.stocksConso.length);

    // log just the first few rows of stocksConso so logs don’t explode
    console.log('stocksConso sample:', JSON.stringify(result.stocksConso.slice(0, 3), null, 2));
    console.log('================================');

    // Returning nothing (UI won’t get data)
    return { ok: true };
  }
);
