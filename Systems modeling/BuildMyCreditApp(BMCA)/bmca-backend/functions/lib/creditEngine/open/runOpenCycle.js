// functions/lib/creditEngine/4 - open/runOpenCycle.js
//
// runOpenCycle(state)
//
// Open-accounts simulation step that mirrors writeOpenActionsListORCH:
// - Recomputes the same logic that produces proposed_* fields:
//     • proposed_can_open_now     ⇔ slotsNow > 0
//     • proposed_count_now        ⇔ slotsNow
//     • proposed_sequence_now_*   ⇔ sequence[...]
// - Uses state.offers_cards, state.offers_loans
// - Uses state.expectedNewCardLimit, state.expectedNewLoanPrincipal
// - Uses state.user_openAndCloseAdjustments / state.openCloseAdjustments
// - Uses CHA target via state.chaSum
// - Derives lates / collections / opens / requests from state.stocks
// - Enforces yearly & half-year caps and min intervals
// - Allocates total “needed” accounts into cards vs loans
// - Builds a card/loan SEQUENCE for this cycle
// - Opens accounts in that sequence using TOP-RANKED offers:
//      * loans:  same ranking as writeUserLoanRecs
//      * cards:  eligibilityRankInteger-based ranking
// - Marks offers as _simAccepted so they’re not reused.
//
// Pure in-memory: NO Firestore access. Mutates state.stocks and relies
// on SimulationState.refreshDerivedViews() being called by tickOneMonth
// after this step.

function normLabel(s) {
  return (s || '')
    .toString()
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[-_]/g, ' ');
}

function asNum(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function buildAdjustmentMaps(adjustments) {
  const valMap = new Map();
  const timeMap = new Map();

  (adjustments || []).forEach((d) => {
    if (!d) return;

    const name =
      d.Unique_Name ||
      d.uniqueName ||
      d.Name ||
      d.name ||
      d.label ||
      d.Label;
    if (!name) return;
    const n = normLabel(name);

    const type = normLabel(d.Type || d.type || '');
    const rawVal =
      d.User_Value ??
      d.Value ??
      d.value ??
      d.Amount ??
      d.amount ??
      null;

    let numVal = 0;
    if (typeof rawVal === 'number' && isFinite(rawVal)) {
      numVal = rawVal;
    } else if (
      rawVal !== null &&
      rawVal !== undefined &&
      rawVal !== '' &&
      !isNaN(Number(rawVal))
    ) {
      numVal = Number(rawVal);
    }

    if (type === 'time') {
      timeMap.set(n, numVal);
    } else {
      valMap.set(n, numVal);
    }
  });

  return { vals: valMap, times: timeMap };
}

function asDate(maybe) {
  if (!maybe) return null;
  if (maybe instanceof Date) return maybe;
  if (typeof maybe.toDate === 'function') return maybe.toDate();
  if (maybe._seconds != null) return new Date(maybe._seconds * 1000);
  const d = new Date(maybe);
  return isNaN(d.getTime()) ? null : d;
}

function stockName(row) {
  return (row && (row.stock || row.stockType || '')).toString();
}

function isCard(row) {
  return stockName(row) === 'user_credit_cards';
}

function isLoan(row) {
  return stockName(row) === 'user_loans';
}

function isAccountRow(row) {
  return isCard(row) || isLoan(row);
}

function isLateRow(row) {
  const s = stockName(row);
  return (
    s === 'user_credit_cards_late_payments' ||
    s === 'user_loans_late_payments'
  );
}

function isCollectionRow(row) {
  return stockName(row) === 'user_collections_3rd_party';
}

function isHardPullRow(row) {
  return stockName(row) === 'user_hard_pulls';
}

/**
 * Scan state.stocks to derive:
 *  - openCards / openLoans
 *  - countLates / countCollections
 *  - opensIn365, mostRecentOpenDate, mostRecentOpenType
 *  - requestsIn180, mostRecentReqDate
 */
function deriveOpenStats(stocks, now) {
  const nowDate = now instanceof Date ? now : new Date();

  const d365 = new Date(nowDate);
  d365.setDate(d365.getDate() - 365);

  const d180 = new Date(nowDate);
  d180.setDate(d180.getDate() - 180);

  let openCards = 0;
  let openLoans = 0;
  let currentCards = 0;
  let currentLoans = 0;
  let countLates = 0;
  let countCollections = 0;

  let opensIn365 = 0;
  let mostRecentOpenDate = null;
  let mostRecentOpenType = null; // 'card' | 'loan' | null

  let requestsIn180 = 0;
  let mostRecentReqDate = null;

  (stocks || []).forEach((row) => {
    if (!row) return;

  // Accounts
  if (isAccountRow(row) && row.isOpen === true) {
    // Count OPEN accounts
    if (isCard(row)) openCards += 1;
    else if (isLoan(row)) openLoans += 1;

    // Count CURRENT accounts (open + current)
    if (row.isCurrent === true) {
      if (isCard(row)) currentCards += 1;
      else if (isLoan(row)) currentLoans += 1;
    }

    const od = asDate(
      row.DOFRecord ??
        row.dateOpened ??
        row.date_issued ??
        row.Date_issued ??
        row.dateIssued
    );
    if (od) {
      if (od >= d365) opensIn365 += 1;
      if (!mostRecentOpenDate || od > mostRecentOpenDate) {
        mostRecentOpenDate = od;
        mostRecentOpenType = isCard(row)
          ? 'card'
          : isLoan(row)
          ? 'loan'
          : null;
      } else if (
        mostRecentOpenDate &&
        od.getTime() === mostRecentOpenDate.getTime()
      ) {
        const thisType = isCard(row)
          ? 'card'
          : isLoan(row)
          ? 'loan'
          : null;
        if (thisType === 'card' && mostRecentOpenType !== 'card') {
          // tie-break toward card for alternation meta
          mostRecentOpenType = 'card';
        }
      }
    }
  }


    // Lates
    if (isLateRow(row)) countLates += 1;

    // Collections
    if (isCollectionRow(row)) countCollections += 1;

    // Hard pulls
    if (isHardPullRow(row)) {
      const rd = asDate(
        row.DOFRecord ??
          row.datePulled ??
          row.dateRequested ??
          row.dateOfRequest
      );
      if (rd) {
        if (rd >= d180) requestsIn180 += 1;
        if (!mostRecentReqDate || rd > mostRecentReqDate) {
          mostRecentReqDate = rd;
        }
      }
    }
  });

  return {
    openCards,
    openLoans,
    currentCards,
    currentLoans,
    countLates,
    countCollections,
    opensIn365,
    mostRecentOpenDate,
    mostRecentOpenType,
    requestsIn180,
    mostRecentReqDate,
  };

}

// ---------- Offer selection helpers ----------

function normalizeRank(raw) {
  if (raw === null || raw === undefined || raw === '') return Infinity;
  const n = Number(raw);
  return Number.isFinite(n) ? n : Infinity;
}

// Cards: primary key eligibilityRankInteger, then APR, then lender
function pickTopCardOffer(state) {
  const offers = state.offers_cards || [];
  if (!offers.length) return null;

  const eligible = offers.filter((o) => {
    if (!o) return false;
    if (o.isAccepted === true) return false;
    if (o._simAccepted === true) return false;
    return true;
  });

  // ---------- NEW: synthetic fallback when depleted ----------
  if (!eligible.length) {
    // Use the first REAL accepted offer as the base template (top rec),
    // falling back to last opened template if needed.
    const base =
      state._simBaseCardTemplate ||
      state._simLastOpenedCardTemplate ||
      null;

    if (!base) return null;

    // Increment synthetic version counter
    if (typeof state._simSyntheticCardVersion !== 'number') {
      state._simSyntheticCardVersion = 1; // next will be V2
    }
    state._simSyntheticCardVersion += 1;

    const v = state._simSyntheticCardVersion;

    const baseName =
      base.commercialName ||
      base.name ||
      base.productName ||
      base.cardName ||
      'SimulatedCard';

    // Create a lightweight synthetic "offer" object.
    // Important: do NOT mark _simAccepted on it (it isn't from offers list).
    const synth = {
      ...base,
      _isSynthetic: true,
      // Force a distinguishable name so IDs + UI labels differ
      name: `${baseName} V${v}`,
      commercialName: `${baseName} V${v}`,
    };

    return synth;
  }
  // -----------------------------------------------------------

  const ranked = [...eligible].sort((a, b) => {
    const ra = normalizeRank(
      a.eligibilityRankInteger ??
        a.eligibility_rank_integer ??
        a.rank
    );
    const rb = normalizeRank(
      b.eligibilityRankInteger ??
        b.eligibility_rank_integer ??
        b.rank
    );
    if (ra !== rb) return ra - rb;

    const aprA = asNum(a.aprLow ?? a.apr ?? a.aprHigh, Infinity);
    const aprB = asNum(b.aprLow ?? b.apr ?? b.aprHigh, Infinity);
    if (aprA !== aprB) return aprA - aprB;

    const lenderA = (a.lender || '').toString().toLowerCase();
    const lenderB = (b.lender || '').toString().toLowerCase();
    if (lenderA < lenderB) return -1;
    if (lenderA > lenderB) return 1;
    return 0;
  });

  const chosen = ranked[0];
  chosen._simAccepted = true;
  chosen._simUsed = true;

  // Persist the first real accepted offer as the base template
  if (!state._simBaseCardTemplate) {
    state._simBaseCardTemplate = chosen;
    state._simSyntheticCardVersion = 1; // base is "V1" conceptually
  }

  state._simLastOpenedCardTemplate = chosen;
  return chosen;
}


// Loans: same sort as writeUserLoanRecs
// Loans: same sort as writeUserLoanRecs
function pickTopLoanOffer(state) {
  const offers = state.offers_loans || [];
  if (!offers.length) return null;

  const eligible = offers.filter((o) => {
    if (!o) return false;
    if (o.isAccepted === true) return false;
    if (o._simAccepted === true) return false;
    return true;
  });

  // ---------- NEW: synthetic fallback when depleted ----------
  if (!eligible.length) {
    const base =
      state._simBaseLoanTemplate ||
      state._simLastOpenedLoanTemplate ||
      null;

    if (!base) return null;

    // Increment synthetic version counter
    if (typeof state._simSyntheticLoanVersion !== 'number') {
      state._simSyntheticLoanVersion = 1; // next will be V2
    }
    state._simSyntheticLoanVersion += 1;

    const v = state._simSyntheticLoanVersion;

    const baseName =
      base.commercialName ||
      base.name ||
      base.productName ||
      base.loanName ||
      'SimulatedLoan';

    // Lightweight synthetic offer (do NOT mark _simAccepted)
    const synth = {
      ...base,
      _isSynthetic: true,
      name: `${baseName} V${v}`,
      commercialName: `${baseName} V${v}`,
    };

    return synth;
  }
  // -----------------------------------------------------------

  const ranked = [...eligible].sort((a, b) => {
    const minA = asNum(a.minPrincipal, Infinity);
    const minB = asNum(b.minPrincipal, Infinity);
    if (minA !== minB) return minA - minB;

    const lenderA = (a.lender || '').toString().toLowerCase();
    const lenderB = (b.lender || '').toString().toLowerCase();
    if (lenderA < lenderB) return -1;
    if (lenderA > lenderB) return 1;

    const durA = asNum(a.durationLowMnths, Infinity);
    const durB = asNum(b.durationLowMnths, Infinity);
    if (durA !== durB) return durA - durB;

    const aprA = asNum(a.aprLow ?? a.aprHigh ?? a.apr, Infinity);
    const aprB = asNum(b.aprLow ?? b.aprHigh ?? b.apr, Infinity);
    return aprA - aprB;
  });

  const chosen = ranked[0];
  chosen._simAccepted = true;
  chosen._simUsed = true;

  // Persist first real accepted offer as base template
  if (!state._simBaseLoanTemplate) {
    state._simBaseLoanTemplate = chosen;
    state._simSyntheticLoanVersion = 1; // base is "V1" conceptually
  }

  state._simLastOpenedLoanTemplate = chosen;
  return chosen;
}


// ---------- New account + hard pull rows ----------

function createCardStockRow(state, offer) {
  const simDate = state.simDate instanceof Date ? state.simDate : new Date();
  const userRef = (state.user && state.user._userRef) || null;

  const creditLimit =
    asNum(state.expectedNewCardLimit, null) ??
    asNum(offer?.creditLimit, 0);

  const lender =
    offer?.lender ||
    offer?.issuer ||
    offer?.bankName ||
    'SimulatedCardLender';
  const name =
    offer?.commercialName ||
    offer?.name ||
    offer?.productName ||
    offer?.cardName ||
    'SimulatedCard';

  const cycleIdx =
    typeof state.monthIndex === 'number' ? state.monthIndex : 0;
  const id = `simCard_${lender}_${name}_${cycleIdx}_${Date.now()}`;

  return {
    id,
    stock: 'user_credit_cards',
    subStock: 'Revolving',
    userRef,
    name,
    lender,
    DOFRecord: simDate,

    // --- parity fields from loader ---
    isOpenIsNull: false,
    isOpenIsMissing: false,
    dateLastUsed: simDate,
    monthlyPayment: null,
    minPayment: 0,
    dayOfMonthDue: null,
    // --------------------------------

    isOpen: true,
    isCurrent: true,
    isAnnualFee: !!offer?.hasAnnualFee || !!offer?.isAnnualFee,
    isCFA: !!offer?.isCFA,
    creditLimit,

    // Cards start at zero util
    amountsOwed: 0,
    totalBalance: 0,
    balance: 0,
    apr: asNum(offer?.aprLow ?? offer?.apr ?? offer?.aprHigh, null),
  };
}


function createLoanStockRow(state, offer) {
  const simDate = state.simDate instanceof Date ? state.simDate : new Date();
  const userRef = (state.user && state.user._userRef) || null;

  const principal =
    asNum(state.expectedNewLoanPrincipal, null) ??
    asNum(
      offer?.principalOriginal ??
        offer?.loanAmount ??
        offer?.creditLimit,
      0
    );

  const lender =
    offer?.lender ||
    offer?.bankName ||
    offer?.issuer ||
    'SimulatedLoanLender';
  const name =
    offer?.commercialName ||
    offer?.name ||
    offer?.productName ||
    offer?.loanName ||
    'SimulatedLoan';

  const cycleIdx =
    typeof state.monthIndex === 'number' ? state.monthIndex : 0;
  const id = `simLoan_${lender}_${name}_${cycleIdx}_${Date.now()}`;

  // --- NEW: compute monthly payment (simple amortization) ---
  const aprPct = asNum(offer?.aprHigh ?? offer?.apr ?? offer?.aprLow, 0);
  const nMonths = asNum(
    offer?.durationHighMnths ?? offer?.durationLowMnths,
    0
  );

  let monthlyPayment = 0;

  if (nMonths > 0) {
    const r = (aprPct / 100) / 12; // monthly rate
    if (r === 0) {
      monthlyPayment = principal / nMonths;
    } else {
      const denom = 1 - Math.pow(1 + r, -nMonths);
      monthlyPayment = denom === 0 ? 0 : (principal * r) / denom;
    }
  } else {
    monthlyPayment = 0;
  }
  // ---------------------------------------------------------

  return {
    id,
    stock: 'user_loans',
    subStock: 'Installment',
    userRef,
    name,
    lender,
    DOFRecord: simDate,

    // --- parity fields from loader ---
    isOpenIsNull: false,
    isOpenIsMissing: false,
    monthlyPayment,
    minPayment: monthlyPayment,
    dayOfMonthDue: null,
    // --------------------------------

    isOpen: true,
    isCurrent: true,
    isCFA: !!offer?.isCFA,
    creditLimit: principal,

    // Loans originate fully drawn
    amountsOwed: principal,
    balance: principal,
    apr: asNum(offer?.aprLow ?? offer?.aprHigh ?? offer?.apr, null),
  };
}



function createHardPullRowForOpen(state, lender, productName, debtType) {
  const simDate = state.simDate instanceof Date ? state.simDate : new Date();
  const userRef = (state.user && state.user._userRef) || null;

  const cycleIdx =
    typeof state.monthIndex === 'number' ? state.monthIndex : 0;
  const id = `simPull_${lender}_${productName}_${cycleIdx}_${Date.now()}`;

  return {
    id,
    stock: 'user_hard_pulls',
    subStock: debtType, // 'Revolving' | 'Installment'
    userRef,
    lender,
    productName,
    DOFRecord: simDate,
  };
}

// ---------- Main Open cycle ----------

function runOpenCycle(state) {
  if (!state || !Array.isArray(state.stocks)) return state;

  const now = state.simDate instanceof Date ? state.simDate : new Date();
  const user = state.user || {};

  const wantsToAllowLoans = !!user.wantsToAllowLoans;
  const imposeMaxLoanNumber = !!user.imposeMaxLoanNumber;

  // Adjustments
  const adjustments =
    state.openCloseAdjustments ||
    state.user_openAndCloseAdjustments ||
    [];

  const { vals, times } = buildAdjustmentMaps(adjustments);

  const getVal = (label) => vals.get(normLabel(label)) ?? 0;
  const getTime = (label) => times.get(normLabel(label)) ?? 0;

  // ---- knobs from adjustments (same labels as writeOpenActionsListORCH) ----
  const currentPerLateMultiplier =
    getVal('current accounts / lates') ||
    getVal('current accounts / lates + collections') ||
    getVal('current accounts by lates') ||
    0;

  const pctRevolvingMin =
    getVal('revolving % of total') ||
    getVal('revolving percent of total') ||
    0;

  const maxLoansAllowed = getVal('max loans allowed') || null;

  const yearlyOpensMax =
    getTime('yearly opens allowable') ||
    getTime('yearly allowable opens') ||
    0;

  const halfYearRequestsMax =
    getTime('half yearly requests allowable') ||
    getTime('half yearly allowable requests') ||
    0;

  const minMonthsBetweenOpens =
    getTime('min interval between opens in ltm') || 0;

  const minMonthsBetweenRequests =
    getTime('min interval between requests in ltm') || 0;

  // ---- derive stats from in-memory stocks ----
  const stats = deriveOpenStats(state.stocks, now);
  const {
    openCards,
    openLoans,
    currentCards,
    currentLoans,
    countLates,
    countCollections,
    opensIn365,
    mostRecentOpenDate,
    mostRecentOpenType,
    requestsIn180,
    mostRecentReqDate,
  } = stats;


  // ---------- Step 1: Goal ----------
  const totalLatesAndCollections = countLates + countCollections;
  const pathA = currentPerLateMultiplier * totalLatesAndCollections;

  // Path B: CHA global target, if present
  const chaSum =
    typeof state.chaSum === 'number' && isFinite(state.chaSum)
      ? state.chaSum
      : 0;
  const pathB = chaSum;

  const goalTotal = Math.max(
    isFinite(pathA) ? pathA : 0,
    isFinite(pathB) ? pathB : 0
  );

  // ---------- Step 2: Current (exists now) ----------
  const currentOpenTotal = currentCards + currentLoans;

  // ---------- Step 3: Needed ----------
  const neededTotal = Math.max(0, goalTotal - currentOpenTotal);
  const isDone = neededTotal === 0;

  // --- META for emergent-duration stopping conditions ---
  state._openMeta = { neededTotal };


  // ---------- Plan epoch (baseline_open_loans; “cap only on future loans”) ----------
  if (typeof state._baselineOpenLoansSim !== 'number') {
    state._baselineOpenLoansSim = openLoans; // seed from current snapshot
  }
  const baselineOpenLoans = state._baselineOpenLoansSim;
  const loansOpenedSinceStart = Math.max(0, openLoans - baselineOpenLoans);

  // ---------- Step 4: Allocation ----------
  let cardsToOpen = 0;
  let loansToOpen = 0;
  let loanCapRemaining = null;

  if (!wantsToAllowLoans) {
    cardsToOpen = neededTotal;
    loansToOpen = 0;
  } else if (imposeMaxLoanNumber) {
    loanCapRemaining =
      maxLoansAllowed == null
        ? 0
        : Math.max(0, maxLoansAllowed - loansOpenedSinceStart);

    const minCards = Math.ceil(
      (Number(pctRevolvingMin || 0) / 100) * neededTotal
    );
    cardsToOpen = Math.min(neededTotal, minCards);
    loansToOpen = Math.min(
      Math.max(neededTotal - cardsToOpen, 0),
      loanCapRemaining
    );

    const shortfall = neededTotal - (cardsToOpen + loansToOpen);
    if (shortfall > 0) cardsToOpen += shortfall;
  } else {
    const minCards = Math.ceil(
      (Number(pctRevolvingMin || 0) / 100) * neededTotal
    );
    cardsToOpen = Math.min(neededTotal, minCards);
    loansToOpen = Math.max(0, neededTotal - cardsToOpen);
  }

  const allocHeadroom = cardsToOpen + loansToOpen;

  // ---------- Step 5: Timing gates (caps + intervals) ----------
  const yearlyHeadroom = Math.max(
    0,
    Number(yearlyOpensMax || 0) - Number(opensIn365 || 0)
  );
  const halfYearHeadroom = Math.max(
    0,
    Number(halfYearRequestsMax || 0) - Number(requestsIn180 || 0)
  );

  const addMonthsFractional = (date, months) => {
    if (!date) return null;
    const d = new Date(date.getTime());
    const whole = Math.trunc(months);
    const frac = months - whole;
    if (whole !== 0) d.setMonth(d.getMonth() + whole);
    if (frac !== 0) d.setDate(d.getDate() + frac * 30.4375);
    return d;
  };

  const nextByOpenInterval = mostRecentOpenDate
    ? addMonthsFractional(
        mostRecentOpenDate,
        Number(minMonthsBetweenOpens || 0)
      )
    : new Date(0);
  const nextByReqInterval = mostRecentReqDate
    ? addMonthsFractional(
        mostRecentReqDate,
        Number(minMonthsBetweenRequests || 0)
      )
    : new Date(0);

  const intervalsSatisfied =
    now >= nextByOpenInterval && now >= nextByReqInterval;

  const capsBlocked =
    yearlyHeadroom <= 0 || halfYearHeadroom <= 0;
  const intervalsBlocked = !intervalsSatisfied;

  let slotsNow = 0;
  if (!isDone && !capsBlocked && !intervalsBlocked && allocHeadroom > 0) {
    if (
      Number(minMonthsBetweenOpens || 0) === 0 &&
      Number(minMonthsBetweenRequests || 0) === 0
    ) {
      slotsNow = Math.min(
        allocHeadroom,
        yearlyHeadroom,
        halfYearHeadroom
      );
    } else {
      slotsNow = Math.min(
        1,
        allocHeadroom,
        yearlyHeadroom,
        halfYearHeadroom
      );
    }
  }

  // Optional sim knob: extra per-cycle cap
  let simMaxOpensPerCycle = user.simMaxOpensPerCycle;
  if (
    typeof simMaxOpensPerCycle !== 'number' ||
    !isFinite(simMaxOpensPerCycle) ||
    simMaxOpensPerCycle <= 0
  ) {
    simMaxOpensPerCycle = slotsNow || 0;
  }
  slotsNow = Math.min(slotsNow, simMaxOpensPerCycle);

  if (slotsNow <= 0) {
    // proposed_can_open_now = false
    return state;
  }

  // ---------- Step 6: Sequence for "now" ----------
  const sequence = [];
  let remCards = cardsToOpen;
  let remLoans = loansToOpen;

  const pickNextType = (prevType) => {
    let preferred = prevType
      ? prevType === 'card'
        ? 'loan'
        : 'card'
      : mostRecentOpenType === 'card'
      ? 'loan'
      : 'card';

    const prefOk =
      (preferred === 'card' && remCards > 0) ||
      (preferred === 'loan' && remLoans > 0 && wantsToAllowLoans);

    if (prefOk) return preferred;

    if (remCards > 0) return 'card';
    if (remLoans > 0 && wantsToAllowLoans) return 'loan';
    return null;
  };

  if (slotsNow > 0) {
    let prev = null;
    for (let i = 0; i < slotsNow; i++) {
      const t = pickNextType(prev);
      if (!t) break;
      sequence.push(t);
      if (t === 'card') remCards -= 1;
      else if (t === 'loan') remLoans -= 1;
      prev = t;
    }
    // safety: pad with cards if sequence shorter than slots
    while (sequence.length < slotsNow && remCards > 0) {
      sequence.push('card');
      remCards -= 1;
    }
    if (sequence.length > slotsNow) sequence.length = slotsNow;
  }

  if (!sequence.length) {
    return state;
  }

  // ---------- Step 7: Apply opens according to sequence ----------
  const canOpenCard =
    state.expectedNewCardLimit != null &&
    Number(state.expectedNewCardLimit) > 0;

  const canOpenLoan =
    state.expectedNewLoanPrincipal != null &&
    Number(state.expectedNewLoanPrincipal) > 0 &&
    wantsToAllowLoans &&
    (maxLoansAllowed == null ||
      loansOpenedSinceStart < maxLoansAllowed);

  if (!canOpenCard && !canOpenLoan) {
    return state;
  }

  for (const kind of sequence) {
    if (kind === 'card') {
      if (!canOpenCard) continue;
      const offer = pickTopCardOffer(state);
      if (!offer) continue;
      const row = createCardStockRow(state, offer);
      state.stocks.push(row);

      const pull = createHardPullRowForOpen(
        state,
        row.lender,
        row.name,
        'Revolving'
      );
      state.stocks.push(pull);
    } else if (kind === 'loan') {
      if (!canOpenLoan) continue;
      const offer = pickTopLoanOffer(state);
      if (!offer) continue;
      const row = createLoanStockRow(state, offer);
      state.stocks.push(row);

      const pull = createHardPullRowForOpen(
        state,
        row.lender,
        row.name,
        'Installment'
      );
      state.stocks.push(pull);
    }
  }

  return state;
}

module.exports = { runOpenCycle };
