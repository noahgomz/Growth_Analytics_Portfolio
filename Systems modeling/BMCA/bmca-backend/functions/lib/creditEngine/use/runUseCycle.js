// functions/lib/creditEngine/use/runUseCycle.js

const {
  recomputeCardMinPayment,
} = require('../helperFunctions/cardRecomputeMinPayments');

const {
  recomputeIsCurrentForAllAccounts,
} = require('../helperFunctions/recomputeIsCurrentForAllAccounts');

function toJsDate(ts) {
  if (!ts) return null;
  if (ts instanceof Date) return ts;
  if (typeof ts.toDate === 'function') return ts.toDate();
  if (ts._seconds != null) return new Date(ts._seconds * 1000);
  const d = new Date(ts);
  return isNaN(d.getTime()) ? null : d;
}

function monthDiffCalendar(from, to) {
  const y = to.getFullYear() - from.getFullYear();
  const m = to.getMonth() - from.getMonth();
  let diff = y * 12 + m;
  if (to.getDate() < from.getDate()) diff -= 1;
  return Math.max(0, diff);
}

function olderThanCalendarMonths(d, months, now) {
  if (!(d instanceof Date)) return false;
  const thresh = new Date(now);
  thresh.setMonth(thresh.getMonth() - months);
  return d.getTime() < thresh.getTime();
}

function runUseCycle(state) {
  if (!state || !Array.isArray(state.stocks)) return state;

  const now = state.simDate instanceof Date ? state.simDate : new Date();

  // 1) All open cards from stocks
  const openCards = [];
  (state.stocks || []).forEach((row) => {
    if (!row) return;
    if (row.stock !== 'user_credit_cards') return;

    // 🔹 Match live CF: only explicitly true is considered open
    if (row.isOpen !== true) return;

    openCards.push(row);
  });

  // 2) Count open cards with balance > 0
  const cardsWithBalanceCount = openCards.reduce((acc, c) => {
    const balRaw =
      c.totalBalance ??
      c.amountsOwed ??
      c.balance ??
      0;
    const bal =
      typeof balRaw === 'number'
        ? balRaw
        : !isNaN(Number(balRaw))
        ? Number(balRaw)
        : 0;
    return acc + (bal > 0 ? 1 : 0);
  }, 0);

  // 3) Candidates: filter out CFA + annual fee cards AND require zero balance
  const candidates = openCards.filter((c) => {
    // Exclude CFA / AF
    if (c.isCFA === true || c.isAnnualFee === true) return false;

    // 🔹 Match live CF intent: only zero-balance cards are candidates
    const balRaw =
      c.totalBalance ??
      c.amountsOwed ??
      c.balance ??
      0;
    const bal =
      typeof balRaw === 'number'
        ? balRaw
        : !isNaN(Number(balRaw))
        ? Number(balRaw)
        : 0;

    return bal === 0;
  });

  // 4) Compute age + risk flags + rank
  const items = candidates.map((card) => {
    const dlu =
      toJsDate(card.dateLastUsedSim) ||
      toJsDate(card.dateLastUsed);

    let useAgeMonths, useAgeDays, useAgeMs;
    if (dlu) {
      useAgeMonths = monthDiffCalendar(dlu, now);
      useAgeDays = Math.floor((now - dlu) / (24 * 60 * 60 * 1000));
      useAgeMs = now - dlu;
    } else {
      // Default: treat as ~30 days ago if never used
      useAgeMonths = 1;
      useAgeDays = 30;
      useAgeMs = 30 * 24 * 60 * 60 * 1000;
    }

    const isOlderThan5M = dlu
      ? olderThanCalendarMonths(dlu, 5, now)
      : (useAgeMonths > 5);

    const atAgeRiskOrAZEOorBoth =
      (isOlderThan5M && cardsWithBalanceCount === 0) ||
      (isOlderThan5M && cardsWithBalanceCount < 2);

    return {
      id: card.id,
      stock: card.stock,
      lender: card.lender ?? null,
      commercialName: card.commercialName ?? card.name ?? null,
      creditLimit: card.creditLimit ?? null,
      apr: card.apr ?? null,
      totalBalance:
        card.totalBalance ??
        card.amountsOwed ??
        card.balance ??
        null,
      dayOfMonthDue: card.dayOfMonthDue ?? null,
      minimumPayment: card.minimumPayment ?? card.minPayment ?? null,
      rewardType: card.rewardType ?? null,
      isOpen: card.isOpen ?? true,
      isCFA: card.isCFA ?? false,
      isAnnualFee: card.isAnnualFee ?? false,

      dateLastUsed: dlu || null,
      dateIssued: toJsDate(card.dateIssued) || null,

      cardsWithBalanceCount,
      useAgeMonths,
      useAgeDays,
      useAgeMs,
      atAgeRiskOrAZEOorBoth,
    };
  });

  // Sort oldest-last-used first (largest useAgeMs first)
  items.sort((a, b) => b.useAgeMs - a.useAgeMs);
  items.forEach((it, i) => {
    it.rank = i + 1;
  });

  // Save the ranking for inspection/debug (sim-side mirror of user_use_cards_list)
  state.useCardsList = items;

  // ---------- APPLY SPEND (MVP, NO FALLBACK) ----------
  const spendRaw = state.monthlySimulatedSpend || 0;
  const spend =
    typeof spendRaw === 'number' && isFinite(spendRaw) ? spendRaw : 0;

  if (spend > 0 && items.length > 0) {
    // Prefer cards that truly meet the requirements.
    const eligible = items.filter((it) => it.atAgeRiskOrAZEOorBoth);

    // ⛔ NO FALLBACK:
    // If no card is atAgeRiskOrAZEOorBoth, we do nothing this cycle.
    const target = eligible[0];
    if (target) {
      const targetId = target.id;
      const targetStock = target.stock || 'user_credit_cards';

      for (const row of state.stocks) {
        if (!row) continue;
        if (row.stock === targetStock && row.id === targetId) {
          const prevRaw =
            row.amountsOwed ??
            row.totalBalance ??
            row.balance ??
            0;
          const prev = isNaN(Number(prevRaw)) ? 0 : Number(prevRaw);
          const next = prev + spend;

          row.amountsOwed = next;
          row.totalBalance = next;
          row.balance = next;

          // 🔹 Update minPayment to match new balance (cards only)
          recomputeCardMinPayment(row);

          // 🔹 Mark last used in sim AND keep "real" last-used in sync
          const ts = new Date(state.simDate);
          row.dateLastUsedSim = ts;
          row.dateLastUsed = ts;

          break;
        }
      }
    }
  }

  // 🔹 After Use completes, recompute isCurrent across all accounts
  recomputeIsCurrentForAllAccounts(state);

  return state;
}

module.exports = { runUseCycle };
