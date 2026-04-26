const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');

// Do NOT call admin.initializeApp() here

module.exports = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth || !context.auth.uid) {
      throw new functions.https.HttpsError('unauthenticated','Must be signed in.');
    }

    const uid = context.auth.uid;
    const db = admin.firestore();
    const userRef = db.doc(`users/${uid}`);

    // helpers
    const asNum = (v, d = null) => (typeof v === 'number' && isFinite(v))
      ? v : (typeof v === 'string' && v.trim() !== '' && isFinite(Number(v)) ? Number(v) : d);
    const pick = (o, keys, d=undefined) => { for (const k of keys) if (o && o[k] !== undefined) return o[k]; return d; };
    const isTrue = v => v === true || v === 'true';

    // 1) Prefilter AF/CFA in conso
    const cardConsoSnap = await db
      .collection('user_stocks_conso')
      .where('stock','==','user_credit_cards')
      .where('userRef','==',userRef)
      .get();

    const consoDocs = cardConsoSnap.docs.filter(d => {
      const c = d.data() || {};
      const hasAF = isTrue(pick(c, ['Has_annual_fee','has_annual_fee','isAnnualFee']));
      const isCFA =
        isTrue(pick(c, ['isCFA','Is_CFA'])) ||
        String(pick(c, ['accountType','Account_Type','category'], '')).toLowerCase() === 'cfa' ||
        String(pick(c, ['accountSubtype','Account_Subtype'], '')).toLowerCase().includes('consumer finance');
      return hasAF || isCFA;
    });

    // 2) Build balance/limit maps
    const payoffById = {};
    const creditLimitById = {};
    const lenderConsoById = {};
    const isAnnualFeeConsoById = {};
    const isCFAConsoById = {};

    for (const d of consoDocs) {
      const c = d.data() || {};
      payoffById[d.id] = asNum(pick(c, ['amountsOwed','current_Balance','Balance','totalBalance']), 0) || 0;
      creditLimitById[d.id] = asNum(pick(c, ['creditLimit','Credit_limit','limit','credit_limit']), null);
      lenderConsoById[d.id] = pick(c, ['Lender','lender'], '');
      isAnnualFeeConsoById[d.id] = !!isTrue(pick(c, ['Has_annual_fee','has_annual_fee','isAnnualFee']));
      isCFAConsoById[d.id] =
        !!(isTrue(pick(c, ['isCFA','Is_CFA'])) ||
           String(pick(c, ['accountType','Account_Type','category'], '')).toLowerCase() === 'cfa' ||
           String(pick(c, ['accountSubtype','Account_Subtype'], '')).toLowerCase().includes('consumer finance'));
    }

    // 3) Enforce OPEN using live docs
    const ids = consoDocs.map(d => d.id);
    const refs = ids.map(id => db.collection('user_credit_cards').doc(id));
    const liveSnaps = refs.length ? await db.getAll(...refs) : [];

    const items = [];
    for (let i = 0; i < ids.length; i++) {
      const id = ids[i];
      const live = (liveSnaps[i] && liveSnaps[i].exists) ? (liveSnaps[i].data() || {}) : null;

      // OPEN-only: prefer 'status', fallback 'isOpen'
      const openLive = live ? (live.status ?? live.isOpen) : false;
      if (openLive !== true) continue;

      const lender = pick(live, ['Lender','lender'], lenderConsoById[id] || '');
      const isAnnualFee = (live?.Has_annual_fee ?? live?.isAnnualFee ?? isAnnualFeeConsoById[id]) ? true : false;
      const isCFA = (live?.isCFA ?? live?.Is_CFA ?? isCFAConsoById[id]) ? true : false;
      const limit = creditLimitById[id];
      const limitSort = Number.isFinite(limit) ? limit : Number.POSITIVE_INFINITY;
      const payoff = payoffById[id] || 0;

      items.push({
        cardId: id,
        lender,
        isAnnualFee,
        isCFA,
        creditLimitSort: limitSort,
        creditLimit: Number.isFinite(limit) ? limit : null,
        payoff,
      });
    }

    // 4) Sort: lower limit → AF before CFA → lender A–Z
    items.sort((a, b) => {
      if (a.creditLimitSort !== b.creditLimitSort) return a.creditLimitSort - b.creditLimitSort;
      const aRank = a.isAnnualFee ? 0 : (a.isCFA ? 1 : 2);
      const bRank = b.isAnnualFee ? 0 : (b.isCFA ? 1 : 2);
      if (aRank !== bRank) return aRank - bRank;
      return (a.lender || '').toLowerCase().localeCompare((b.lender || '').toLowerCase());
    });

    // 5) Cleanup + write
    const existingSnap = await db
      .collection('user_card_close_candidates')
      .where('userRef','==',userRef)
      .get();
    const existingIds = new Set(existingSnap.docs.map(d => d.id));

    const batch = db.batch();

    if (items.length === 0) {
      existingSnap.forEach(doc => batch.delete(doc.ref));
      batch.set(
        db.collection('user_card_close_candidates').doc(`${uid}__nullPlaceholder`),
        { userRef, lender: 'nullPlaceholder' },
        { merge: true }
      );
      await batch.commit();
      return { success: true, count: 0 };
    }

    // delete stale
    const keepIds = new Set(items.map(it => it.cardId));
    existingSnap.forEach(doc => { if (!keepIds.has(doc.id)) batch.delete(doc.ref); });

    // upsert rows with FIELD TYPES matching your FF schema
    for (let i = 0; i < items.length; i++) {
      const it = items[i];
      const ref = db.collection('user_card_close_candidates').doc(it.cardId);

      batch.set(ref, {
        userRef,                                  // Doc Reference(users)
        lender: it.lender || null,                // String
        creditLimit: it.creditLimit,              // Double
        totalBalance: it.payoff,                  // Double (mirror)
        totalToPayBeforeClose: it.payoff,         // Double
        isOpen: true,                             // Boolean
        isAnnualFee: !!it.isAnnualFee,            // Boolean
        isCFA: !!it.isCFA,                        // Boolean
        mustBePaidOff: it.payoff > 0,             // Boolean
        cardRef: db.collection('user_credit_cards').doc(it.cardId), // Doc Ref
        rank: i + 1,                              // Integer
      }, { merge: true });
    }

    await batch.commit();
    return { success: true, count: items.length };
  }
);
