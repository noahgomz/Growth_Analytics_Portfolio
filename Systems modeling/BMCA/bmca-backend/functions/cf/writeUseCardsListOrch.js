const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');

module.exports = {
  writeUseCardsListOrch: functions
    .region('us-central1')
    .https.onCall(async (data, context) => {
      if (!context.auth || !context.auth.uid) return;

      const db = admin.firestore();
      const uid = context.auth.uid;
      const userRef = db.doc(`users/${uid}`);

      // ---------- cycleId (optional, from orchestrator) ----------
      let cycleId = null;
      if (data) {
        if (data.cycleID != null) {
          cycleId = typeof data.cycleID === 'string'
            ? data.cycleID
            : String(data.cycleID);
        } else if (data.cycleId != null) {
          cycleId = typeof data.cycleId === 'string'
            ? data.cycleId
            : String(data.cycleId);
        }
      }

      const now = new Date();
      const toJsDate = (ts) => {
        if (!ts) return null;
        if (typeof ts.toDate === 'function') return ts.toDate();
        if (ts._seconds != null) return new Date(ts._seconds * 1000);
        return null;
      };
      const monthDiffCalendar = (from, to) => {
        const y = to.getFullYear() - from.getFullYear();
        const m = to.getMonth() - from.getMonth();
        let diff = y * 12 + m;
        if (to.getDate() < from.getDate()) diff -= 1;
        return Math.max(0, diff);
      };
      const olderThanCalendarMonths = (d, months) => {
        if (!(d instanceof Date)) return false;
        const thresh = new Date(now);
        thresh.setMonth(thresh.getMonth() - months);
        return d.getTime() < thresh.getTime();
      };

      // 1) Pull ALL cards for this user
      const allCardsSnap = await db.collection('user_credit_cards')
        .where('userRef', '==', userRef)
        .get();

      const allIds = new Set();
      const openCards = [];

      allCardsSnap.forEach(doc => {
        allIds.add(doc.id);
        const c = doc.data() || {};
        if (c.isOpen === true) {
          openCards.push({ id: doc.id, data: c });
        }
      });

      // 2) Count open cards with balance > 0
      const cardsWithBalanceCount = openCards.reduce((acc, { data }) => {
        const bal = Number(data.totalBalance ?? 0);
        return acc + (bal > 0 ? 1 : 0);
      }, 0);

      // 3) Candidates: filter out CFA + annual fee cards,
      //    AND require totalBalance === 0
      const candidates = openCards.filter(({ data }) => {
        const bal = Number(data.totalBalance ?? 0);
        return !(data.isCFA === true || data.isAnnualFee === true) &&
               bal === 0;
      });

      // 4) Compute age + risk flags + rank
      const items = candidates.map(({ id, data }) => {
        const dlu = toJsDate(data.dateLastUsed);

        let useAgeMonths, useAgeDays, useAgeMs;
        if (dlu) {
          useAgeMonths = monthDiffCalendar(dlu, now);
          useAgeDays = Math.floor((now - dlu) / (24 * 60 * 60 * 1000));
          useAgeMs = now - dlu;
        } else {
          useAgeMonths = 1;
          useAgeDays = 30;
          useAgeMs = 30 * 24 * 60 * 60 * 1000;
        }

        const isOlderThan5M = dlu
          ? olderThanCalendarMonths(dlu, 5)
          : (useAgeMonths > 5);

        const atAgeRiskOrAZEOorBoth =
          (isOlderThan5M && cardsWithBalanceCount === 0) ||
          (isOlderThan5M && cardsWithBalanceCount < 2);

        return {
          id,
          raw: data,
          useAgeMonths,
          useAgeDays,
          useAgeMs,
          atAgeRiskOrAZEOorBoth
        };
      });

      items.sort((a, b) => b.useAgeMs - a.useAgeMs);
      items.forEach((it, i) => { it.rank = i + 1; });

      const batch = db.batch();

      for (const it of items) {
        const src = it.raw;
        const targetRef = db.collection('user_use_cards_list').doc(it.id);
        const docRefCard = db.collection('user_credit_cards').doc(it.id);

        const basePayload = {
          cardsWithBalanceCount: Number(cardsWithBalanceCount),
          rank: it.rank,
          useAgeMonths: it.useAgeMonths,
          useAgeDays: it.useAgeDays,
          atAgeRiskOrAZEOorBoth: it.atAgeRiskOrAZEOorBoth,

          lender: src.lender ?? null,
          commercialName: src.commercialName ?? null,
          creditLimit: src.creditLimit ?? null,
          apr: src.apr ?? null,
          totalBalance: src.totalBalance ?? null,
          dayOfMonthDue: src.dayOfMonthDue ?? null,
          minimumPayment: src.minimumPayment ?? null,
          rewardType: src.rewardType ?? null,
          isOpen: src.isOpen ?? null,
          isCFA: src.isCFA ?? null,
          isAnnualFee: src.isAnnualFee ?? null,

          dateLastUsed: src.dateLastUsed ?? null,
          dateIssued: src.dateIssued ?? null,

          userRef,
          DocRefCard: docRefCard,

          created_time: admin.firestore.FieldValue.serverTimestamp(),
          actionIsAccepted: false,
        };

        if (cycleId) {
          basePayload.cycleId = cycleId;
        }

        batch.set(targetRef, basePayload, { merge: true });
      }

      // 5) Cleanup: keep ONLY current candidates for this user
      const candidateIds = new Set(items.map(it => it.id));

      const existingListSnap = await db.collection('user_use_cards_list')
        .where('userRef', '==', userRef)
        .get();

      existingListSnap.forEach(doc => {
        if (!candidateIds.has(doc.id)) {
          batch.delete(doc.ref);
        }
      });

      await batch.commit();

      return {
        ok: true,
        stats: {
          open_count: openCards.length,
          cards_with_balance_count: cardsWithBalanceCount,
          written: items.length,
        },
      };
    })
};
