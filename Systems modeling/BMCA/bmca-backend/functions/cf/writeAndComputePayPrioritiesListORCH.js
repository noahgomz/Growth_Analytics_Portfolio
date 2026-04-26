// functions/cf/writeAndComputePayPrioritiesListORCH.js
const functions = require('firebase-functions/v1');
const admin     = require('firebase-admin');
// admin.initializeApp() is called once in index.js

module.exports = functions
  .region('us-central1')
  .https.onCall(async (data, context) => {
    if (!context.auth?.uid) {
      throw new functions.https.HttpsError('unauthenticated', 'Must be signed in.');
    }

    const db = admin.firestore();
    const uid = context.auth.uid;
    const userRef = db.doc(`users/${uid}`);

    const now = Date.now();
    const MS_PER_DAY = 24 * 60 * 60 * 1000;

    const asNum = (v, d = 0) => Number.isFinite(Number(v)) ? Number(v) : d;
    const toTierFromDOFD = (dofdTs) => {
      if (!dofdTs) return 30;
      const t = dofdTs?.toDate
        ? dofdTs.toDate().getTime()
        : (dofdTs instanceof Date ? dofdTs.getTime() : 0);
      if (!t) return 30;
      const days = Math.max(0, Math.floor((now - t) / MS_PER_DAY));
      const raw  = Math.ceil(days / 30) * 30;
      return Math.min(180, Math.max(30, raw)); // 30..180
    };

    // ---------- cycleId (optional, from mother CF) ----------
    // FF will pass data.cycleID (per your stub). We also accept data.cycleId for safety.
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
    // If cycleId is null => standalone/testing mode: no cycle field written.
    // -------------------------------------------------------------

    // ---------- Inputs ----------
    const userSnap = await userRef.get();
    const monthlyBudget = asNum(userSnap.get('monthly_budget'), 0);
    let budgetLeft = monthlyBudget;

    // Discover current stocks from conso (exclude hard pulls)
    const consoSnap = await db.collection('user_stocks_conso')
      .where('userRef', '==', userRef).get();

    const conso = consoSnap.docs
      .map(d => ({ id: d.id, ...d.data() }))
      .filter(d => d.stock !== 'user_hard_pulls');

    const cardIds = conso.filter(x => x.stock === 'user_credit_cards').map(x => x.id);
    const loanIds = conso.filter(x => x.stock === 'user_loans').map(x => x.id);

    // Pull origin docs (terms & balances come from origin)
    const [cardDocs, loanDocs] = await Promise.all([
      Promise.all(cardIds.map(id => db.collection('user_credit_cards').doc(id).get())),
      Promise.all(loanIds.map(id => db.collection('user_loans').doc(id).get()))
    ]);

    const cardsMap = {};
    cardDocs.forEach(s => {
      if (s.exists) cardsMap[s.id] = { id: s.id, ref: s.ref, ...s.data() };
    });

    const loansMap = {};
    loanDocs.forEach(s => {
      if (s.exists) loansMap[s.id] = { id: s.id, ref: s.ref, ...s.data() };
    });

    // Late payments (origin, unpaid only)
    const [lateCardsSnap, lateLoansSnap] = await Promise.all([
      db.collection('user_credit_cards_late_payments')
        .where('userRef', '==', userRef)
        .where('isPaid', '==', false)
        .get(),
      db.collection('user_loans_late_payments')
        .where('userRef', '==', userRef)
        .where('isPaid', '==', false)
        .get()
    ]);

    const lateItems = [
      ...lateCardsSnap.docs.map(d => ({
        id: d.id,
        ref: d.ref,
        kind: 'cardLate',
        ...d.data()
      })),
      ...lateLoansSnap.docs.map(d => ({
        id: d.id,
        ref: d.ref,
        kind: 'loanLate',
        ...d.data()
      }))
    ];

    // fetch origin accounts referenced by late docs (cardRef/loanRef)
    const keyForRef = (r) => (r ? `${r.parent.id}/${r.id}` : '');
    const lateAccountRefSet = new Set();
    lateItems.forEach(L => {
      if (L.cardRef) lateAccountRefSet.add(keyForRef(L.cardRef));
      if (L.loanRef) lateAccountRefSet.add(keyForRef(L.loanRef));
    });

    const lateAccountRefs = [...lateAccountRefSet].map(k => {
      const [col, id] = k.split('/');
      return db.collection(col).doc(id);
    });

    const lateAccountDocs = await Promise.all(lateAccountRefs.map(r => r.get()));
    const lateAccMap = {};
    lateAccountDocs.forEach(s => {
      if (s.exists) {
        const col = s.ref.parent.id; // user_credit_cards or user_loans
        lateAccMap[keyForRef(s.ref)] = {
          id: s.id,
          ref: s.ref,
          collection: col,
          ...s.data()
        };
      }
    });

    // 3rd-party collections (unpaid)
    const tpcSnap = await db.collection('user_collections_3rd_party')
      .where('userRef', '==', userRef)
      .where('isPaid', '==', false)
      .get();

    const tpcItems = tpcSnap.docs.map(d => ({
      id: d.id,
      ref: d.ref,
      ...d.data()
    }));

    // ---------- Build working rows ----------
    const items = [];

    // Accounts (cards + loans)
    conso.forEach(src => {
      if (src.stock !== 'user_credit_cards' && src.stock !== 'user_loans') return;

      const isCard = src.stock === 'user_credit_cards';
      const o = isCard ? cardsMap[src.id] : loansMap[src.id];
      if (!o) return;

      const apr        = asNum(o.apr, 0);
      const minPayment = isCard
        ? asNum(o.minimumPayment, 0)
        : asNum(o.monthlyPayment, 0);
      const balance    = isCard
        ? asNum(o.totalBalance, 0)
        : asNum(o.balance, 0);
      const dueDay     = asNum(o.dayOfMonthDue ?? null, null);

      // flags for Step 3 grouping (from conso flags)
      const isAnnualFee = !!src.isAnnualFee;
      const isCFA       = !!src.isCFA;

      items.push({
        kind: 'account',
        id: src.id,
        stock: src.stock,
        name: o.commercialName ?? src.name ?? null,
        lender: o.lender ?? src.lender ?? null,
        sourceRef: o.ref, // origin doc ref
        apr,
        minPayment,
        balance,
        dueDay,
        isAnnualFee,
        isCFA,
        isCollectionFromAccount: false,  // never a collection for base accounts
        alloc_total: 0,
        alloc_min: 0,
        alloc_card_extra: 0,
      });
    });

    // Unpaid lates (use origin account's lender/name via cardRef/loanRef)
    lateItems.forEach(L => {
      const severity = toTierFromDOFD(L.DOFD); // numeric tier 30..180
      const sentToCollections = !!L.sentToCollections;
      const amount = asNum(L.amount, 0);

      const originRefKey = L.cardRef
        ? keyForRef(L.cardRef)
        : (L.loanRef ? keyForRef(L.loanRef) : '');
      const originAcc = originRefKey ? lateAccMap[originRefKey] : null;

      const lender = originAcc?.lender ?? L.lender ?? null;
      const name   = originAcc?.commercialName ?? L.name ?? null;

      items.push({
        kind: sentToCollections ? 'collection_from_account' : 'late',
        id: L.id,
        stock: L.kind === 'cardLate'
          ? 'user_credit_cards_late_payments'
          : 'user_loans_late_payments',
        name,
        lender,
        sourceRef: L.ref,
        severity,
        amount,
        isCollectionFromAccount: sentToCollections,
        alloc_total: 0,
        alloc_late: 0,
        alloc_collections: 0,
      });
    });

    // Third-party collections
    tpcItems.forEach(C => {
      items.push({
        kind: 'collection_third_party',
        id: C.id,
        stock: 'user_collections_3rd_party',
        name: C.originalProvider ?? C.collectionsAgency ?? C.type ?? null,
        lender: C.collectionsAgency ?? null,
        sourceRef: C.ref,
        amount: asNum(C.amount, 0),
        isCollectionFromAccount: false,
        alloc_total: 0,
        alloc_collections: 0,
      });
    });

    // ---------- minimumPreservationBudget (accounts' mins + unpaid lates, no collections) ----------
    const accounts = items.filter(i => i.kind === 'account');
    const totalMinAccounts = accounts.reduce((s, i) => s + i.minPayment, 0);
    const totalUnpaidLates = items
      .filter(i => i.kind === 'late')
      .reduce((s, i) => s + i.amount, 0);

    const minimumPreservationBudget = Number(
      (totalMinAccounts + totalUnpaidLates).toFixed(2)
    );

    // ---------- Allocation ----------

    // 1) Minimum monthly payments (cards + loans)
    const totalMin = totalMinAccounts;

    if (budgetLeft > 0) {
      if (budgetLeft >= totalMin) {
        accounts
          .sort((a, b) =>
            (b.apr - a.apr) ||
            ((a.dueDay ?? 32) - (b.dueDay ?? 32))
          )
          .forEach(i => {
            // FIX: cap by remaining balance
            const remainingBal = Math.max(0, i.balance - i.alloc_total);
            const pay = Math.min(i.minPayment, remainingBal);
            const alloc = Math.min(budgetLeft, pay);
            i.alloc_min += alloc;
            i.alloc_total += alloc;
            budgetLeft -= alloc;
          });
      } else {
        // NEW: no pro-rata. Concentrate payments to avoid partially paying multiple accounts.

        const loans = accounts.filter(a => a.stock === 'user_loans');
        const cards = accounts.filter(a => a.stock === 'user_credit_cards');

        // FIX: check payable min (minPayment capped by balance)
        const canCoverAnyLoan = loans.some(l => {
          const remainingBal = Math.max(0, l.balance - l.alloc_total);
          const payableMin = Math.min(l.minPayment, remainingBal);
          return payableMin > 0 && payableMin <= budgetLeft;
        });

        if (canCoverAnyLoan) {
          // 1) Pay loans first by smallest monthly payment (full or nothing)
          loans
            .slice()
            .sort((a,b) =>
              (a.minPayment - b.minPayment) ||
              (b.apr - a.apr) ||
              ((a.dueDay ?? 32) - (b.dueDay ?? 32))
            )
            .forEach(l => {
              if (budgetLeft <= 0) return;
              // FIX: cap by remaining balance
              const remainingBal = Math.max(0, l.balance - l.alloc_total);
              const payableMin = Math.min(l.minPayment, remainingBal);
              if (payableMin > 0 && payableMin <= budgetLeft) {
                l.alloc_min += payableMin;
                l.alloc_total += payableMin;
                budgetLeft -= payableMin;
              }
            });

          // 2) Then pay remaining account minimums by APR order (sequential, not pro-rata)
          const remainingAccounts = [...cards, ...loans]
            .slice()
            .sort((a,b) =>
              (b.apr - a.apr) ||
              ((a.dueDay ?? 32) - (b.dueDay ?? 32))
            );

          remainingAccounts.forEach(a => {
            if (budgetLeft <= 0) return;
            // FIX: cap need by remaining balance
            const remainingBal = Math.max(0, a.balance - a.alloc_total);
            const need = Math.min(
              remainingBal,
              Math.max(0, a.minPayment - a.alloc_min)
            );
            if (need <= 0) return;
            const pay = Math.min(budgetLeft, need);
            a.alloc_min += pay;
            a.alloc_total += pay;
            budgetLeft -= pay;
          });

        } else {
          // Can't fully cover even one loan payment → rank by APR and pay sequentially
          accounts
            .slice()
            .sort((a,b) =>
              (b.apr - a.apr) ||
              ((a.dueDay ?? 32) - (b.dueDay ?? 32))
            )
            .forEach(a => {
              if (budgetLeft <= 0) return;
              // FIX: cap need by remaining balance
              const remainingBal = Math.max(0, a.balance - a.alloc_total);
              const need = Math.min(
                remainingBal,
                Math.max(0, a.minPayment - a.alloc_min)
              );
              if (need <= 0) return;
              const pay = Math.min(budgetLeft, need);
              a.alloc_min += pay;
              a.alloc_total += pay;
              budgetLeft -= pay;
            });
        }
      }
    }

    // 2) Unpaid lates by severity (180 → 30)
    items
      .filter(i => i.kind === 'late')
      .sort((a, b) => (b.severity || 0) - (a.severity || 0))
      .forEach(i => {
        if (budgetLeft <= 0) return;
        const need = i.amount - i.alloc_total;
        if (need <= 0) return;
        const pay = Math.min(budgetLeft, need);
        i.alloc_late += pay;
        i.alloc_total += pay;
        budgetLeft -= pay;
      });

    // 3) Card extra principal — AF first, then CFA, then others (APR desc, earlier due day)
    const byAprThenDue = (a, b) =>
      (b.apr - a.apr) ||
      ((a.dueDay ?? 32) - (b.dueDay ?? 32));

    const cardsAF   = items.filter(a =>
      a.kind === 'account' &&
      a.stock === 'user_credit_cards' &&
      a.isAnnualFee
    );
    const cardsCFA  = items.filter(a =>
      a.kind === 'account' &&
      a.stock === 'user_credit_cards' &&
      !a.isAnnualFee &&
      a.isCFA
    );
    const cardsRest = items.filter(a =>
      a.kind === 'account' &&
      a.stock === 'user_credit_cards' &&
      !a.isAnnualFee &&
      !a.isCFA
    );

    [cardsAF, cardsCFA, cardsRest].forEach(group => {
      group.sort(byAprThenDue).forEach(i => {
        if (budgetLeft <= 0) return;
        const remaining = Math.max(0, i.balance - i.alloc_total);
        if (remaining <= 0) return;
        const pay = Math.min(budgetLeft, remaining);
        i.alloc_card_extra += pay;
        i.alloc_total += pay;
        budgetLeft -= pay;
      });
    });

    // 4) Collections — account-origin (sentToCollections), then third-party
    items
      .filter(i => i.kind === 'collection_from_account')
      .sort((a, b) => (b.amount || 0) - (a.amount || 0))
      .forEach(i => {
        if (budgetLeft <= 0) return;
        const need = i.amount - i.alloc_total;
        if (need <= 0) return;
        const pay = Math.min(budgetLeft, need);
        i.alloc_collections += pay;
        i.alloc_total += pay;
        budgetLeft -= pay;
      });

    items
      .filter(i => i.kind === 'collection_third_party')
      .sort((a, b) => (b.amount || 0) - (a.amount || 0))
      .forEach(i => {
        if (budgetLeft <= 0) return;
        const need = i.amount - i.alloc_total;
        if (need <= 0) return;
        const pay = Math.min(budgetLeft, need);
        i.alloc_collections += pay;
        i.alloc_total += pay;
        budgetLeft -= pay;
      });

    // ---------- Totals for header-style fields ----------
    const availableRemainder = Number(budgetLeft.toFixed(2));
    const totalAllocated = Number((monthlyBudget - availableRemainder).toFixed(2));

    // ---------- Persist (delta-aware; write only new/changed) ----------
    const priorityKey = (i) => (
      i.kind === 'account' && i.alloc_min > 0 ? 1 :
      i.kind === 'late'    && i.alloc_total > 0 ? 2 :
      i.kind === 'account' && i.alloc_card_extra > 0 ? 3 :
      (i.kind === 'collection_from_account' || i.kind === 'collection_third_party') && i.alloc_total > 0 ? 4 : 5
    );

    const ranked = [...items].sort((a, b) => {
      const pa = priorityKey(a), pb = priorityKey(b);
      if (pa !== pb) return pa - pb;
      return (b.alloc_total || 0) - (a.alloc_total || 0);
    });

    const existingSnap = await db.collection('user_pay_priority_list')
      .where('userRef', '==', userRef).get();

    // Build a minimal previous map limited to fields we write
    const pickRelevant = (x) => {
      if (!x) return null;
      const base = {
        userRef: x.userRef,
        rank: x.rank,
        monthlyBudget: x.monthlyBudget,
        availableRemainder: x.availableRemainder,
        totalAllocated: x.totalAllocated,
        minimumPreservationBudget: x.minimumPreservationBudget,
        isCollectionFromAccount: !!x.isCollectionFromAccount,
        stockType: x.stockType,
        name: x.name,
        lender: x.lender,
        // NOTE: cycleId intentionally excluded so comparison ignores it.
      };
      if (x.stockType === 'user_credit_cards' || x.stockType === 'user_loans') {
        return {
          ...base,
          originDocRef: x.originDocRef || null,
          apr: x.apr,
          balance: x.balance,
          minPayment: x.minPayment,
          dayOfMonthDue: x.dayOfMonthDue ?? null,
          budgetAllocated: x.budgetAllocated,
          allocationBreakdown: x.allocationBreakdown || {},
        };
      } else if (
        x.stockType === 'user_credit_cards_late_payments' ||
        x.stockType === 'user_loans_late_payments'
      ) {
        return {
          ...base,
          originDocRef: x.originDocRef || null,
          severity: x.severity,
          amount: x.amount,
          budgetAllocated: x.budgetAllocated,
          allocationBreakdown: x.allocationBreakdown || {},
        };
      } else { // collections
        return {
          ...base,
          originDocRef: x.originDocRef || null,
          amount: x.amount,
          budgetAllocated: x.budgetAllocated,
          allocationBreakdown: x.allocationBreakdown || {},
        };
      }
    };

    const existingMap = {};
    existingSnap.forEach(d => {
      existingMap[d.id] = pickRelevant(d.data());
    });

    const currentIds = new Set(items.map(i => i.id));
    const batch = db.batch();

    // delete stale
    existingSnap.forEach(d => {
      if (!currentIds.has(d.id)) batch.delete(d.ref);
    });

    ranked.forEach((i, idx) => {
      const docRef = db.collection('user_pay_priority_list').doc(i.id);

      // Build FlutterFlow-friendly poly-ref object (only one field set)
      const originDocRef = (() => {
        if (i.kind === 'account' && i.stock === 'user_credit_cards') {
          return { CardsDocRef: i.sourceRef };
        }
        if (i.kind === 'account' && i.stock === 'user_loans') {
          return { LoansDocRef: i.sourceRef };
        }
        if (i.kind === 'late' && i.stock === 'user_credit_cards_late_payments') {
          return { CardLatesOrCollectionsDocRef: i.sourceRef };
        }
        if (i.kind === 'late' && i.stock === 'user_loans_late_payments') {
          return { LoanLatesOrCollectionsDocRef: i.sourceRef };
        }
        if (i.kind === 'collection_third_party') {
          return { Collections3rdPartDocRef: i.sourceRef };
        }
        if (i.kind === 'collection_from_account') {
          return i.stock === 'user_credit_cards_late_payments'
            ? { CardLatesOrCollectionsDocRef: i.sourceRef }
            : { LoanLatesOrCollectionsDocRef:  i.sourceRef };
        }
        return {};
      })();

      const base = {
        userRef,
        rank: idx + 1,
        monthlyBudget,
        availableRemainder,
        totalAllocated,
        minimumPreservationBudget,
        isCollectionFromAccount: !!i.isCollectionFromAccount,
        originDocRef, // DocRefMultiCollection custom object
      };

      let newDataCore;

      if (i.kind === 'account') {
        const breakdown =
          i.stock === 'user_credit_cards'
            ? {
                minPayment: Number(i.alloc_min.toFixed(2)),
                cardExtra: Number(i.alloc_card_extra.toFixed(2)), // only for cards
              }
            : {
                minPayment: Number(i.alloc_min.toFixed(2)),       // loans: no cardExtra key
              };

        newDataCore = {
          ...base,
          stockType: i.stock,
          name: i.name,
          lender: i.lender,
          apr: i.apr,
          balance: Number(i.balance.toFixed(2)),
          minPayment: Number(i.minPayment.toFixed(2)),
          dayOfMonthDue: i.dueDay ?? null,
          budgetAllocated: Number(i.alloc_total.toFixed(2)),
          allocationBreakdown: breakdown,
        };
      } else if (i.kind === 'late') {
        newDataCore = {
          ...base,
          stockType: i.stock,
          name: i.name,
          lender: i.lender,
          severity: i.severity,
          amount: Number(i.amount.toFixed(2)),
          budgetAllocated: Number(i.alloc_total.toFixed(2)),
          allocationBreakdown: {
            lateSeverityPayment: Number(i.alloc_late.toFixed(2)),
          },
        };
      } else {
        newDataCore = {
          ...base,
          stockType: i.stock,
          name: i.name,
          lender: i.lender,
          amount: Number(i.amount.toFixed(2)),
          budgetAllocated: Number(i.alloc_total.toFixed(2)),
          allocationBreakdown: {
            collections: Number(i.alloc_collections.toFixed(2)),
          },
        };
      }

      const prevRelevant = existingMap[i.id];

      // Compare only core fields (no cycleId) so:
      // - standalone runs don't care about cycleId
      // - mother runs can still stamp cycleId on docs even if core unchanged
      const coreChanged =
        !prevRelevant ||
        JSON.stringify(prevRelevant) !== JSON.stringify(newDataCore);

      if (coreChanged || cycleId) {
        const payload = { ...newDataCore };
        if (cycleId) {
          payload.cycleId = cycleId;
        }
        batch.set(docRef, payload, { merge: true });
      }
    });

    await batch.commit();
    return {
      success: true,
      count: items.length,
      availableRemainder,
      totalAllocated,
      minimumPreservationBudget,
    };
  });
