// functions/cf/writeCloseActionsListORCH.js
const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');

// Admin is initialized once in index.js
const db = admin.firestore();
const serverTs = admin.firestore.FieldValue.serverTimestamp();

// ---------- helpers ----------
const asNum = (v, d = 0) =>
  typeof v === 'number' && isFinite(v) ? v : d;

const daysBetween = (a, b) =>
  Math.floor((a - b) / (1000 * 60 * 60 * 24));

const monthsToDays = (m) => asNum(m, 0) * 30;

// ---------- helper to clear list ----------
async function clearListForUser(db, userRef) {
  const q = await db
    .collection('user_close_actions_list')
    .where('userRef', '==', userRef)
    .get();

  if (q.empty) return;
  let batch = db.batch();
  let n = 0;
  for (const doc of q.docs) {
    batch.delete(doc.ref);
    n++;
    if (n >= 450) {
      await batch.commit();
      batch = db.batch();
      n = 0;
    }
  }
  if (n > 0) await batch.commit();
}

const writeCloseActionsListORCH = functions
  .region('us-central1')
  .https.onCall(async (data, context) => {
    if (!context.auth || !context.auth.uid) return;

    const userDocRef = db.doc(`users/${context.auth.uid}`);

    // ---------- cycleId (optional, from orchestrator) ----------
    let cycleId = null;
    if (data) {
      if (data.cycleID != null) {
        cycleId =
          typeof data.cycleID === 'string'
            ? data.cycleID
            : String(data.cycleID);
      } else if (data.cycleId != null) {
        cycleId =
          typeof data.cycleId === 'string'
            ? data.cycleId
            : String(data.cycleId);
      }
    }
    // If cycleId is null => standalone/testing: no cycleId field is written.

    // ---------- load candidates ----------
    const candSnap = await db
      .collection('user_card_close_candidates')
      .where('userRef', '==', userDocRef)
      .get();

    if (candSnap.empty) {
      await clearListForUser(db, userDocRef);

      const summaryRef = db
        .collection('user_close_actions_list')
        .doc(`summary_${context.auth.uid}`);

      const summaryData = {
        userRef: userDocRef,
        currentUtilizationPct: 0,
        utilizationRulePct: null,
        closesInLast365: 0,
        mostRecentClose: null,
        yearlyClosesAllowable: null,
        minIntervalBetweenClosesDays: null,
        closableNowCount: 0,

        // timing summary fields when there are no candidates
        remainingClosesYearWindow: 0,
        maxCardsClosableNowByInterval: 0,
        maxCardsClosableNowByTiming: 0,

        isSummaryDoc: true,
        created_time: serverTs,
      };
      if (cycleId) {
        summaryData.cycleId = cycleId; // tag summary as well when orchestrated
      }

      await summaryRef.set(summaryData, { merge: true });
      return { candidates: 0, closableNowCount: 0 };
    }

    // ---------- load adjustments (Close rules) ----------
    const adjSnap = await db
      .collection('user_openAndCloseAdjustments')
      .where('userRef', '==', userDocRef)
      .where('Action_Type', '==', 'Close')
      .get();

    let utilPct = null; // e.g., 30
    let yearlyClosesAllowable = null; // e.g., 6
    let minIntervalMonths = null; // e.g., 3

    adjSnap.forEach((d) => {
      const x = d.data() || {};
      switch (x.Unique_Name) {
        case 'Utilization':
          utilPct = asNum(x.User_Value, utilPct);
          break;
        case 'Yearly closes allowable':
          yearlyClosesAllowable = asNum(x.User_Value, yearlyClosesAllowable);
          break;
        case 'Min interval between closes in LTM':
          minIntervalMonths = asNum(x.User_Value, minIntervalMonths);
          break;
      }
    });

    if (utilPct == null) utilPct = 30;
    if (yearlyClosesAllowable == null) yearlyClosesAllowable = 2;
    if (minIntervalMonths == null) minIntervalMonths = 2;

    const utilThreshold = utilPct / 100;
    const minIntervalDays = monthsToDays(minIntervalMonths);

    // ---------- open cards for current utilization ----------
    const openCardsSnap = await db
      .collection('user_credit_cards')
      .where('userRef', '==', userDocRef)
      .where('isOpen', '==', true)
      .get();

    let sumBal = 0;
    let sumLimit = 0;
    openCardsSnap.forEach((d) => {
      const c = d.data() || {};
      sumBal += asNum(c.totalBalance, 0);
      sumLimit += asNum(c.creditLimit, 0);
    });
    const currentUtilization = sumLimit > 0 ? sumBal / sumLimit : 0;

    // ---------- closure history in last 365 days ----------
    const now = new Date();
    const cutoff = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);

    const closedCardsSnap = await db
      .collection('user_credit_cards')
      .where('userRef', '==', userDocRef)
      .where('isOpen', '==', false)
      .get();

    let closesInLast365 = 0;
    let mostRecentClose = null;

    closedCardsSnap.forEach((d) => {
      const c = d.data() || {};
      const dc =
        c.dateClosed && c.dateClosed.toDate
          ? c.dateClosed.toDate()
          : null;
      if (!dc) return;
      if (dc >= cutoff) closesInLast365 += 1;
      if (!mostRecentClose || dc > mostRecentClose) {
        mostRecentClose = dc;
      }
    });

    const daysSinceMostRecentClose = mostRecentClose
      ? daysBetween(now, mostRecentClose)
      : Infinity;

    // ----- timing constraints summary values (pre-loop) -----
    let remainingCloses = Math.max(
      0,
      yearlyClosesAllowable - closesInLast365
    );
    const remainingClosesYearWindow = remainingCloses;

    let maxCardsClosableNowByInterval;
    if (minIntervalDays <= 0) {
      // No interval constraint: interval allows as many as the year rule
      maxCardsClosableNowByInterval = remainingClosesYearWindow;
    } else {
      if (!Number.isFinite(daysSinceMostRecentClose)) {
        // No prior closes: interval satisfied, but we still limit to 1 at a time
        maxCardsClosableNowByInterval =
          remainingClosesYearWindow > 0 ? 1 : 0;
      } else if (daysSinceMostRecentClose < minIntervalDays) {
        // Still in cooldown window: cannot close anything now
        maxCardsClosableNowByInterval = 0;
      } else {
        // Interval satisfied and at least one close allowed this year: allow 1 now
        maxCardsClosableNowByInterval =
          remainingClosesYearWindow > 0 ? 1 : 0;
      }
    }

    const maxCardsClosableNowByTiming = Math.min(
      remainingClosesYearWindow,
      maxCardsClosableNowByInterval
    );

    // ---------- sort candidates by rank ----------
    const candidates = candSnap.docs
      .map((d) => ({ id: d.id, ...d.data() }))
      .sort(
        (a, b) => asNum(a.rank, 999999) - asNum(b.rank, 999999)
      );

    // ---------- clear previous output ----------
    await clearListForUser(db, userDocRef);

    // ---------- evaluate ----------
    let closableNowCount = 0;
    let batch = db.batch();
    let opCount = 0;

    for (const cand of candidates) {
      const cardRef = cand.cardRef || null;
      const cardSnap = cardRef ? await cardRef.get() : null;
      const card =
        cardSnap && cardSnap.exists ? cardSnap.data() || {} : {};

      const balance = asNum(card.totalBalance, 0);
      const limit = asNum(card.creditLimit, 0);
      const lender = card.lender || cand.lender || '';
      const reasons = [];

      // flags from card/candidate
      const isCFA =
        card.isCFA !== undefined ? card.isCFA : (cand.isCFA || false);
      const isAnnualFee =
        card.isAnnualFee !== undefined
          ? card.isAnnualFee
          : (cand.isAnnualFee || false);

      if (balance > 0) reasons.push('balance_gt_0');

      const newSumLimit = Math.max(0, sumLimit - limit);
      const newSumBal = Math.max(0, sumBal - balance);
      const simulatedUtil =
        newSumLimit > 0 ? newSumBal / newSumLimit : 0;

      const willBreachUtilization = simulatedUtil > utilThreshold;
      if (willBreachUtilization) reasons.push('utilization_breach');

      if (remainingCloses <= 0) {
        reasons.push('yearly_close_limit_reached');
      }
      if (daysSinceMostRecentClose < minIntervalDays) {
        reasons.push('min_interval_not_met');
      }

      const eligibleNow = reasons.length === 0;

      if (eligibleNow) {
        closableNowCount += 1;
        remainingCloses = Math.max(0, remainingCloses - 1);
        sumLimit = newSumLimit;
        sumBal = newSumBal;
      }

      // ----- flatten reasons for FlutterFlow -----
      const numIneligibilityReasons = reasons.length;
      const flatReasons = { numIneligibilityReasons };

      // Fill sequential fields (1-based)
      reasons.forEach((r, i) => {
        flatReasons[`ineligibilityReasons${i + 1}`] = r;
      });

      // Clear any stale reason fields up to a safe cap (e.g., 8)
      for (let i = reasons.length + 1; i <= 8; i++) {
        flatReasons[`ineligibilityReasons${i}`] =
          admin.firestore.FieldValue.delete();
      }

      const outRef = db
        .collection('user_close_actions_list')
        .doc(`${context.auth.uid}_${cand.id}`);

      const basePayload = {
        userRef: userDocRef,
        cardRef: cardRef || null,
        rank: asNum(cand.rank, null),
        lender,
        creditLimit: limit,
        totalBalance: balance,

        isCFA,
        isAnnualFee,

        simulatedUtilization: simulatedUtil, // 0–1
        willBreachUtilization,
        eligibleNow,

        // Original array (keep for debugging / API consumers)
        ineligibilityReasons: reasons,

        // FlutterFlow-friendly top-level fields
        ...flatReasons,

        isSummaryDoc: false,
        closeAccepted: false,

        created_time: serverTs,
      };

      if (cycleId) {
        basePayload.cycleId = cycleId; // tag each close action with cycleId when orchestrated
      }

      batch.set(outRef, basePayload, { merge: true });

      opCount++;
      if (opCount >= 450) {
        await batch.commit();
        batch = db.batch();
        opCount = 0;
      }
    }

    if (opCount > 0) await batch.commit();

    // ---------- summary ----------
    const summaryRef = db
      .collection('user_close_actions_list')
      .doc(`summary_${context.auth.uid}`);

    const summaryData = {
      userRef: userDocRef,
      currentUtilizationPct: currentUtilization * 100,
      utilizationRulePct: utilPct,
      closesInLast365,
      mostRecentClose: mostRecentClose || null,
      yearlyClosesAllowable,
      minIntervalBetweenClosesDays: minIntervalDays,
      closableNowCount,

      remainingClosesYearWindow,
      maxCardsClosableNowByInterval,
      maxCardsClosableNowByTiming,

      isSummaryDoc: true,
      created_time: serverTs,
    };

    if (cycleId) {
      summaryData.cycleId = cycleId; // summary also tied to this cycle
    }

    await summaryRef.set(summaryData, { merge: true });

    return { candidates: candidates.length, closableNowCount };
  });

module.exports = {
  writeCloseActionsListORCH,
};
