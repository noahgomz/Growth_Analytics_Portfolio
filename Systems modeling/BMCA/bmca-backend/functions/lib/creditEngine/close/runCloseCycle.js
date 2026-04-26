// functions/lib/creditEngine/4 - close/runCloseCycle.js
//
// Sim close-cycle logic that mirrors:
// 1) writeCloseCandidates (AF/CFA selection + rank)
// 2) writeCloseActionsListORCH (util thresholds, yearly caps, min-interval)
//
// Pure in-memory:
// - NO Firestore reads/writes
// - Mutates state.stocks only (and attaches a debug summary on state)
// - Uses state.user_openAndCloseAdjustments just like live CF uses
//   user_openAndCloseAdjustments(Action_Type === 'Close').

function asNum(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function daysBetween(a, b) {
  const ms = a.getTime() - b.getTime();
  return Math.floor(ms / (1000 * 60 * 60 * 24));
}

function monthsToDays(m) {
  return asNum(m, 0) * 30;
}

/**
 * Compute current utilization from all OPEN cards in state.stocks
 * (stock === 'user_credit_cards', isOpen !== false).
 */
function computeCurrentUtilization(stocks) {
  let sumBal = 0;
  let sumLimit = 0;

  (stocks || []).forEach((row) => {
    if (!row || row.stock !== 'user_credit_cards') return;
    if (row.isOpen === false) return;

    const balRaw =
      row.totalBalance ??
      row.amountsOwed ??
      row.balance ??
      0;
    const limitRaw =
      row.creditLimit ??
      row.limit ??
      0;

    const bal = asNum(balRaw, 0);
    const limit = asNum(limitRaw, 0);

    sumBal += bal;
    if (limit > 0) sumLimit += limit;
  });

  const currentUtil = sumLimit > 0 ? sumBal / sumLimit : 0;
  return { sumBal, sumLimit, currentUtil };
}

/**
 * Compute close history in the last 365 days based on cards that
 * are already CLOSED in the sim.
 *
 * We look at:
 * - dateClosedSim (set by this sim),
 * - OR a pre-existing dateClosed if it was mapped through.
 */
function computeSimCloseHistory(stocks, now) {
  const yearMs = 365 * 24 * 60 * 60 * 1000;

  let closesInLast365 = 0;
  let mostRecentClose = null;

  (stocks || []).forEach((row) => {
    if (!row || row.stock !== 'user_credit_cards') return;
    if (row.isOpen !== false) return;

    const rawDate =
      row.dateClosedSim ||
      row.dateClosed ||
      null;

    if (!rawDate) return;

    const d =
      rawDate instanceof Date
        ? rawDate
        : new Date(rawDate);

    if (isNaN(d.getTime())) return;

    const diff = now.getTime() - d.getTime();
    if (diff < 0) return;

    if (diff <= yearMs) {
      closesInLast365 += 1;
    }

    if (!mostRecentClose || d > mostRecentClose) {
      mostRecentClose = d;
    }
  });

  return { closesInLast365, mostRecentClose };
}

/**
 * Build AF / CFA close candidates directly from state.stocks,
 * mimicking writeCloseCandidates:
 *
 * - stock === 'user_credit_cards'
 * - OPEN only
 * - isAnnualFee === true OR isCFA === true
 * - rank: lower creditLimit → AF before CFA → lender A–Z
 */
function buildCloseCandidatesFromStocks(stocks) {
  const items = [];

  (stocks || []).forEach((row) => {
    if (!row) return;
    if (row.stock !== 'user_credit_cards') return;

    // OPEN-only: treat isOpen === false as closed; anything else = open
    if (row.isOpen === false) return;

    const isAnnualFee =
      row.isAnnualFee === true ||
      row.Has_annual_fee === true ||
      row.has_annual_fee === true;

    const isCFA =
      row.isCFA === true ||
      row.Is_CFA === true ||
      String(row.accountType || '').toLowerCase() === 'cfa' ||
      String(row.accountSubtype || '').toLowerCase().includes('consumer finance');

    if (!isAnnualFee && !isCFA) return;

    const limitRaw =
      row.creditLimit ??
      row.Credit_limit ??
      row.limit ??
      row.credit_limit;
    const payoffRaw =
      row.amountsOwed ??
      row.current_Balance ??
      row.Balance ??
      row.totalBalance ??
      row.balance ??
      0;

    const limit = asNum(limitRaw, null);
    const payoff = asNum(payoffRaw, 0);

    const lender =
      row.Lender ??
      row.lender ??
      '';

    const creditLimitSort = Number.isFinite(limit)
      ? limit
      : Number.POSITIVE_INFINITY;

    items.push({
      id: row.id,
      lender,
      isAnnualFee: !!isAnnualFee,
      isCFA: !!isCFA,
      creditLimitSort,
      creditLimit: Number.isFinite(limit) ? limit : null,
      payoff,
      row, // direct pointer to the card row in state.stocks
    });
  });

  // Sort: lower limit → AF before CFA → lender A–Z
  items.sort((a, b) => {
    if (a.creditLimitSort !== b.creditLimitSort) {
      return a.creditLimitSort - b.creditLimitSort;
    }

    // AF before CFA before others (though others shouldn't be here)
    const aRank = a.isAnnualFee ? 0 : (a.isCFA ? 1 : 2);
    const bRank = b.isAnnualFee ? 0 : (b.isCFA ? 1 : 2);
    if (aRank !== bRank) return aRank - bRank;

    const la = (a.lender || '').toLowerCase();
    const lb = (b.lender || '').toLowerCase();
    if (la < lb) return -1;
    if (la > lb) return 1;
    return 0;
  });

  // Attach rank (1-based) for parity/debug
  items.forEach((it, idx) => {
    it.rank = idx + 1;
  });

  return items;
}

/**
 * Main close-cycle function.
 *
 * Mirrors writeCloseActionsListORCH logic but instead of writing:
 * - user_close_actions_list docs
 * It directly:
 * - marks eligible cards as closed in state.stocks (isOpen = false)
 * - stamps dateClosedSim = state.simDate
 *
 * Also attaches a debugging summary on state._closeSummary for inspection.
 */

function runCloseCycle(state) {
  if (!state || !Array.isArray(state.stocks)) return state;

  const now =
    state.simDate instanceof Date ? state.simDate : new Date();

  // -------- 1) Adjustments: Utilization, yearly closes, min interval --------
  const allAdj =
    (Array.isArray(state.user_openAndCloseAdjustments)
      ? state.user_openAndCloseAdjustments
      : Array.isArray(state.openCloseAdjustments)
      ? state.openCloseAdjustments
      : []) || [];

  // Keep them on state in a consistent field, if not already
  if (!Array.isArray(state.user_openAndCloseAdjustments)) {
    state.user_openAndCloseAdjustments = allAdj;
  }

  let utilPct = null;              // e.g. 30
  let yearlyClosesAllowable = null; // e.g. 2
  let minIntervalMonths = null;     // e.g. 2

  allAdj.forEach((x) => {
    if (!x || x.Action_Type !== 'Close') return;
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
      default:
        break;
    }
  });

  if (utilPct == null) utilPct = 30;
  if (yearlyClosesAllowable == null) yearlyClosesAllowable = 2;
  if (minIntervalMonths == null) minIntervalMonths = 2;

  const utilThreshold = utilPct / 100;
  const minIntervalDays = monthsToDays(minIntervalMonths);

  // -------- 2) Current utilization from OPEN cards --------
  const utilInfo = computeCurrentUtilization(state.stocks);
  let { sumBal, sumLimit, currentUtil } = utilInfo;

  // -------- 3) Close history (last 365 days) --------
  const { closesInLast365, mostRecentClose } = computeSimCloseHistory(
    state.stocks,
    now
  );

  const daysSinceMostRecentClose = mostRecentClose
    ? daysBetween(now, mostRecentClose)
    : Infinity;

  // Remaining closes in the rolling year
  let remainingCloses = Math.max(
    0,
    yearlyClosesAllowable - closesInLast365
  );
  const remainingClosesYearWindow = remainingCloses;

  // These mirror the summary logic in writeCloseActionsListORCH
  let maxCardsClosableNowByInterval;
  if (minIntervalDays <= 0) {
    // No interval constraint
    maxCardsClosableNowByInterval = remainingClosesYearWindow;
  } else {
    if (!Number.isFinite(daysSinceMostRecentClose)) {
      // No prior closes → interval satisfied → allow at most 1 at a time
      maxCardsClosableNowByInterval =
        remainingClosesYearWindow > 0 ? 1 : 0;
    } else if (daysSinceMostRecentClose < minIntervalDays) {
      // Cooldown window
      maxCardsClosableNowByInterval = 0;
    } else {
      // Interval satisfied and at least one available this year
      maxCardsClosableNowByInterval =
        remainingClosesYearWindow > 0 ? 1 : 0;
    }
  }

  const maxCardsClosableNowByTiming = Math.min(
    remainingClosesYearWindow,
    maxCardsClosableNowByInterval
  );

  // -------- 4) Build & sort AF/CFA candidates from stocks --------
  const candidates = buildCloseCandidatesFromStocks(state.stocks);

  // --- META for emergent-duration stopping conditions ---
  state._closeMeta = { candidateCount: candidates.length };

  if (!candidates.length) {
    // Attach a small summary and exit
    state._closeSummary = {
      currentUtilizationPct: currentUtil * 100,
      utilizationRulePct: utilPct,
      closesInLast365,
      mostRecentClose,
      yearlyClosesAllowable,
      minIntervalBetweenClosesDays: minIntervalDays,
      closableNowCount: 0,
      remainingClosesYearWindow,
      maxCardsClosableNowByInterval,
      maxCardsClosableNowByTiming,
    };
    return state;
  }

  // -------- 5) Evaluate each candidate (mirror writeCloseActionsListORCH) --------
  let closableNowCount = 0;

  candidates.forEach((cand) => {
    const row = cand.row;
    if (!row) return;

    const balance = asNum(
      row.totalBalance ??
        row.amountsOwed ??
        row.balance ??
        cand.payoff ??
        0,
      0
    );
    const limit = asNum(
      row.creditLimit ??
        row.limit ??
        cand.creditLimit ??
        0,
      0
    );

    const reasons = [];

    const isCFA =
      row.isCFA !== undefined ? !!row.isCFA : !!cand.isCFA;
    const isAnnualFee =
      row.isAnnualFee !== undefined
        ? !!row.isAnnualFee
        : !!cand.isAnnualFee;

    // --- balance check ---
    if (balance > 0) {
      reasons.push('balance_gt_0');
    }

    // --- simulated utilization if this card is closed ---
    const newSumLimit = Math.max(0, sumLimit - limit);
    const newSumBal = Math.max(0, sumBal - balance);
    const simulatedUtil =
      newSumLimit > 0 ? newSumBal / newSumLimit : 0;

    const willBreachUtilization = simulatedUtil > utilThreshold;
    if (willBreachUtilization) {
      reasons.push('utilization_breach');
    }

    // --- yearly closes cap ---
    if (remainingCloses <= 0) {
      reasons.push('yearly_close_limit_reached');
    }

    // --- interval constraint (same as live CF: uses pre-loop daysSinceMostRecentClose) ---
    if (daysSinceMostRecentClose < minIntervalDays) {
      reasons.push('min_interval_not_met');
    }

    const eligibleNow = reasons.length === 0;

    if (!eligibleNow) {
      // We still could store reasons somewhere on row for debug, but
      // to keep sim clean we just skip the close.
      return;
    }

    // Mark as closable
    closableNowCount += 1;
    remainingCloses = Math.max(0, remainingCloses - 1);

    // Update running util basis for subsequent candidates
    sumLimit = newSumLimit;
    sumBal = newSumBal;
    currentUtil = simulatedUtil;

    // --- APPLY CLOSE IN SIM ---
    row.isOpen = false;
    // Leave creditLimit unchanged for historical parity with live.
    // Utilization calcs already ignore closed cards (isOpen === false),
    // so we do not need to zero the limit in the sim.
    row.dateClosedSim = new Date(now); // stable within this cycle

    // You could optionally attach these for diagnostics:
    row._simCloseMeta = {
      isCFA,
      isAnnualFee,
      simulatedUtilization: simulatedUtil,
      willBreachUtilization,
      ineligibilityReasons: [], // eligible → empty
    };

  });

  // -------- 6) Attach summary to state for debugging/inspection --------
  state._closeSummary = {
    currentUtilizationPct: currentUtil * 100,
    utilizationRulePct: utilPct,
    closesInLast365,
    mostRecentClose,
    yearlyClosesAllowable,
    minIntervalBetweenClosesDays: minIntervalDays,
    closableNowCount,
    remainingClosesYearWindow,
    maxCardsClosableNowByInterval,
    maxCardsClosableNowByTiming,
  };

  return state;
}

module.exports = { runCloseCycle };
