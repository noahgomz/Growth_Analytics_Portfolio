// functions/cf/writeOpenActionsListORCH.js
const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');

// Use the already-initialized admin app from index.js
const db = admin.firestore();
db.settings({ ignoreUndefinedProperties: true });

const writeOpenActionsListORCH = functions
  .region('us-central1')
  .https.onCall(async (data, context) => {
    if (!context.auth || !context.auth.uid) return;
    const uid = context.auth.uid;
    const userRef = db.collection('users').doc(uid);

    // ---------- cycleId (optional, from orchestrator) ----------
    // FF will pass data.cycleID; we also accept data.cycleId.
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
    // If cycleId is null => standalone/testing: no cycle field is written.
    // -------------------------------------------------------------

    // ---------- helpers ----------
    const nz = (v, d = 0) => (typeof v === 'number' && !isNaN(v) ? v : d);
    const asBool = (v, d = false) => (typeof v === 'boolean' ? v : d);
    const isTs = (t) => t && typeof t.toDate === 'function';
    const toTs = (d) => admin.firestore.Timestamp.fromDate(d);
    const ceil = Math.ceil;
    const min = Math.min;

    const norm = (s) =>
      (s || '')
        .toString()
        .trim()
        .toLowerCase()
        .replace(/\s+/g, ' ')
        .replace(/[-_]/g, ' ');

    const nowInTZ = (tz = 'America/New_York') => {
      const fmt = new Intl.DateTimeFormat('en-US', {
        timeZone: tz,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      });
      const parts = Object.fromEntries(
        fmt.formatToParts(new Date()).map((p) => [p.type, p.value])
      );
      return new Date(
        `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}`
      );
    };

    const addMonthsFractional = (date, months) => {
      const d = new Date(date.getTime());
      const whole = Math.trunc(months);
      const frac = months - whole;
      if (whole !== 0) d.setMonth(d.getMonth() + whole);
      if (frac !== 0) d.setDate(d.getDate() + frac * 30.4375);
      return d;
    };

    const asDate = (maybeTsOrDate) => {
      if (!maybeTsOrDate) return null;
      if (isTs(maybeTsOrDate)) return maybeTsOrDate.toDate();
      if (maybeTsOrDate instanceof Date) return maybeTsOrDate;
      const t = new Date(maybeTsOrDate);
      return isNaN(+t) ? null : t;
    };

    // canonical comparison for flat objects (convert timestamps/dates to millis)
    const toComparable = (obj) => {
      const o = {};
      for (const [k, v] of Object.entries(obj)) {
        if (k === 'created_time' || k === 'last_updated_time') continue; // exclude volatile fields
        if (v && typeof v === 'object' && isTs(v)) o[k] = v.toDate().getTime();
        else if (v instanceof Date) o[k] = v.getTime();
        else o[k] = v;
      }
      return JSON.stringify(o);
    };

    // serialize for client (no DocumentReference, no Firestore Timestamp)
    const tsToMillis = (v) =>
      v && typeof v.toDate === 'function'
        ? v.toDate().getTime()
        : v instanceof Date
        ? v.getTime()
        : typeof v === 'number'
        ? v
        : null;

    const makeClientPayload = (id, data) => ({
      id,
      created_time_ms: tsToMillis(data.created_time) ?? null,
      last_updated_time_ms: tsToMillis(data.last_updated_time) ?? null,
      status: data.status ?? null,

      proposed_can_open_now: !!data.proposed_can_open_now,
      proposed_count_now: data.proposed_count_now ?? 0,
      proposed_sequence_now_len: data.proposed_sequence_now_len ?? 0,
      proposed_sequence_now_0: data.proposed_sequence_now_0 ?? null,
      proposed_sequence_now_1: data.proposed_sequence_now_1 ?? null,
      proposed_next_eligible_date_ms:
        tsToMillis(data.proposed_next_eligible_date) ?? null,
      proposed_cadence_opens_min_interval:
        data.proposed_cadence_opens_min_interval ?? 0,
      proposed_cadence_requests_min_interval:
        data.proposed_cadence_requests_min_interval ?? 0,

      allocation_goal_total: data.allocation_goal_total ?? 0,
      allocation_current_open_cards: data.allocation_current_open_cards ?? 0,
      allocation_current_open_loans: data.allocation_current_open_loans ?? 0,
      allocation_needed_total: data.allocation_needed_total ?? 0,
      allocation_revPctMinCards: data.allocation_revPctMinCards ?? 0,
      allocation_loan_cap_remaining:
        data.allocation_loan_cap_remaining ?? null,
      allocation_cards_to_open: data.allocation_cards_to_open ?? 0,
      allocation_loans_to_open: data.allocation_loans_to_open ?? 0,

      timing_is_blocked_caps: !!data.timing_is_blocked_caps,
      timing_is_blocked_intervals: !!data.timing_is_blocked_intervals,
      timing_block_reason: data.timing_block_reason ?? null,

      selection_protocol: data.selection_protocol ?? null,
      meta_baseline_open_loans: data.meta_baseline_open_loans ?? null,

      // expose cycleId to client if present on doc
      cycleId: data.cycleId ?? null,
    });

    // ---------- load knobs ----------
    const userDoc = await userRef.get();
    const user = userDoc.exists ? userDoc.data() : {};
    const wantsToAllowLoans = asBool(user.wantsToAllowLoans, false);
    const imposeMaxLoanNumber = asBool(user.imposeMaxLoanNumber, false);

    // Adjustments
    const adjSnap = await db
      .collection('user_openAndCloseAdjustments')
      .where('userRef', '==', userRef)
      .get();

    const valMap = new Map();
    const timeMap = new Map();

    adjSnap.forEach((doc) => {
      const d = doc.data();
      const name = d.Unique_Name ?? d.uniqueName ?? d.Name ?? d.name;
      const n = norm(name);
      const v = d.User_Value ?? d.Value ?? d.value;
      const type = d.Type ?? d.type ?? '';
      if (norm(type) === 'time') timeMap.set(n, nz(v, 0));
      else valMap.set(n, nz(v, 0));
    });

    const getVal = (label) => valMap.get(norm(label)) ?? 0;
    const getTime = (label) => timeMap.get(norm(label)) ?? 0;

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

    // CHA targets (GLOBAL — no user filter)
    const chaSnap = await db
      .collection('credit_high_achiever_account_numbers')
      .get();
    let chaSum = 0;
    chaSnap.forEach((doc) => {
      chaSum += nz(doc.data().Value ?? doc.data().value, 0);
    });

    // Conso (single source)
    const consoSnap = await db
      .collection('user_stocks_conso')
      .where('userRef', '==', userRef)
      .get();

    // One "now" for entirety of function
    const nowNY = nowInTZ('America/New_York');
    const d365 = new Date(nowNY);
    d365.setDate(d365.getDate() - 365);
    const d180 = new Date(nowNY);
    d180.setDate(d180.getDate() - 180);

    const stockName = (d) => (d.stock || d.stockType || '').toString();

    const isCard = (d) => stockName(d) === 'user_credit_cards';
    const isLoan = (d) => stockName(d) === 'user_loans';
    const isHardPull = (d) => stockName(d) === 'user_hard_pulls';
    const isAccountRow = (d) => isCard(d) || isLoan(d);

    // Actual LOD names:
    const isLateRow = (d) => stockName(d).endsWith('_late_payments');
    const isCollectionRow = (d) =>
      stockName(d) === 'user_collections_3rd_party';

    // ---------- parse conso ----------
    let openCards = 0,
      openLoans = 0;
    let currentCards = 0,
      currentLoans = 0;
    let countLates = 0,
      countCollections = 0;


    let opensIn365 = 0;
    let mostRecentOpenDate = null;
    let mostRecentOpenType = null; // 'card'|'loan'

    let requestsIn180 = 0;
    let mostRecentReqDate = null;

    consoSnap.forEach((doc) => {
      const d = doc.data();

      if (isAccountRow(d)) {
      if (d.isOpen === true) {
        if (isCard(d)) openCards++;
        else if (isLoan(d)) openLoans++;

        // CURRENT accounts = open + current
        if (d.isCurrent === true) {
          if (isCard(d)) currentCards++;
          else if (isLoan(d)) currentLoans++;
        }
      }


        const od = asDate(
          d.DOFRecord ?? d.dateOpened ?? d.date_issued ?? d.Date_issued
        );
        if (od) {
          if (od >= d365) opensIn365++;
          if (!mostRecentOpenDate || od > mostRecentOpenDate) {
            mostRecentOpenDate = od;
            mostRecentOpenType = isCard(d)
              ? 'card'
              : isLoan(d)
              ? 'loan'
              : null;
          } else if (
            mostRecentOpenDate &&
            od.getTime() === mostRecentOpenDate.getTime()
          ) {
            const thisType = isCard(d)
              ? 'card'
              : isLoan(d)
              ? 'loan'
              : null;
            if (thisType === 'card' && mostRecentOpenType !== 'card')
              mostRecentOpenType = 'card';
          }
        }
      }

      if (isLateRow(d)) countLates++;
      if (isCollectionRow(d)) countCollections++;

      if (isHardPull(d)) {
        const rd = asDate(d.DOFRecord ?? d.datePulled ?? d.dateRequested);
        if (rd) {
          if (rd >= d180) requestsIn180++;
          if (!mostRecentReqDate || rd > mostRecentReqDate)
            mostRecentReqDate = rd;
        }
      }
    });

    // ---------- Step 1: Goal ----------
    const totalLatesAndCollections = countLates + countCollections;
    const pathA = nz(currentPerLateMultiplier, 0) * totalLatesAndCollections;
    const pathB = chaSum;
    const goalTotal = Math.max(nz(pathA, 0), nz(pathB, 0));

    // ---------- Step 2: Current (exists now) ----------
    const currentOpenTotal = currentCards + currentLoans;

    // ---------- Step 3: Needed ----------
    const neededTotal = Math.max(0, goalTotal - currentOpenTotal);
    const isDone = neededTotal === 0;

    // ---------- Plan epoch (baseline_open_loans; “cap only on future loans”) ----------
    let baselineOpenLoans;
    await db.runTransaction(async (tx) => {
      const epochRef = userRef.collection('open_actions_epoch').doc('open_actions_epoch');
      const epochSnap = await tx.get(epochRef);
      if (!epochSnap.exists) {
        baselineOpenLoans = openLoans; // seed from current snapshot
        tx.create(epochRef, {
          baseline_open_loans: baselineOpenLoans,
          created_time: admin.firestore.FieldValue.serverTimestamp(),
        });
      } else {
        const ed = epochSnap.data() || {};
        baselineOpenLoans =
          typeof ed.baseline_open_loans === 'number'
            ? ed.baseline_open_loans
            : openLoans;
      }
    });

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

      const minCards = ceil((nz(pctRevolvingMin, 0) / 100) * neededTotal);
      cardsToOpen = Math.min(neededTotal, minCards);
      loansToOpen = Math.min(
        Math.max(neededTotal - cardsToOpen, 0),
        loanCapRemaining
      );

      const shortfall = neededTotal - (cardsToOpen + loansToOpen);
      if (shortfall > 0) cardsToOpen += shortfall; // guarantee total
    } else {
      const minCards = ceil((nz(pctRevolvingMin, 0) / 100) * neededTotal);
      cardsToOpen = Math.min(neededTotal, minCards);
      loansToOpen = Math.max(0, neededTotal - cardsToOpen);
    }

    const allocHeadroom = cardsToOpen + loansToOpen;

    // ---------- Step 5: Timing ----------
    const yearlyHeadroom = Math.max(
      0,
      nz(yearlyOpensMax, 0) - nz(opensIn365, 0)
    );
    const halfYearHeadroom = Math.max(
      0,
      nz(halfYearRequestsMax, 0) - nz(requestsIn180, 0)
    );

    const nextByOpenInterval = mostRecentOpenDate
      ? addMonthsFractional(mostRecentOpenDate, nz(minMonthsBetweenOpens, 0))
      : new Date(0);
    const nextByReqInterval = mostRecentReqDate
      ? addMonthsFractional(
          mostRecentReqDate,
          nz(minMonthsBetweenRequests, 0)
        )
      : new Date(0);

    const intervalsSatisfied =
      nowNY >= nextByOpenInterval && nowNY >= nextByReqInterval;
    const capsBlocked = yearlyHeadroom <= 0 || halfYearHeadroom <= 0;
    const intervalsBlocked = !intervalsSatisfied;

    let blockReason = null;
    if (!isDone) {
      if (capsBlocked) {
        if (yearlyHeadroom <= 0) blockReason = 'yearly_cap_reached';
        if (halfYearHeadroom <= 0)
          blockReason = blockReason
            ? `${blockReason}|halfyear_requests_cap_reached`
            : 'halfyear_requests_cap_reached';
      }
      if (intervalsBlocked) {
        blockReason = blockReason
          ? `${blockReason}|interval_not_satisfied`
          : 'interval_not_satisfied';
      }
    }

    // slots that can be executed *now*
    let slotsNow = 0;
    if (!isDone && !capsBlocked && !intervalsBlocked && allocHeadroom > 0) {
      if (
        nz(minMonthsBetweenOpens, 0) === 0 &&
        nz(minMonthsBetweenRequests, 0) === 0
      ) {
        slotsNow = min(allocHeadroom, yearlyHeadroom, halfYearHeadroom);
      } else {
        slotsNow = min(1, allocHeadroom, yearlyHeadroom, halfYearHeadroom);
      }
    }

    // Next eligible date only if blocked and not done
    let nextEligibleDate = null;
    if (!isDone && slotsNow === 0) {
      const candidates = [nextByOpenInterval, nextByReqInterval];
      if (yearlyHeadroom <= 0) {
        const d = new Date(nowNY);
        d.setDate(d.getDate() + 1);
        candidates.push(d);
      }
      if (halfYearHeadroom <= 0) {
        const d = new Date(nowNY);
        d.setDate(d.getDate() + 1);
        candidates.push(d);
      }
      nextEligibleDate = candidates.length
        ? new Date(Math.max(...candidates.map((d) => d.getTime())))
        : null;
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
        (preferred === 'loan' && remLoans > 0);
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
        if (t === 'card') remCards--;
        else if (t === 'loan') remLoans--;
        prev = t;
      }
      while (sequence.length < slotsNow && remCards > 0) {
        sequence.push('card');
        remCards--;
      }
      if (sequence.length > slotsNow) sequence.length = slotsNow;
    }

    // ---------- FLATTENED OUTPUT ----------
    const flat = {
      userRef, // stored in Firestore, NOT returned to client
      user_uid: uid, // handy filter in UI without composite index
      status:
        slotsNow > 0 ? 'proposed' : isDone ? 'done' : 'blocked',

      // proposed_*
      proposed_can_open_now: slotsNow > 0,
      proposed_count_now: slotsNow,
      proposed_next_eligible_date: isDone
        ? null
        : nextEligibleDate
        ? toTs(nextEligibleDate)
        : null,
      proposed_cadence_opens_min_interval: nz(minMonthsBetweenOpens, 0),
      proposed_cadence_requests_min_interval: nz(
        minMonthsBetweenRequests,
        0
      ),

      // allocation_*
      allocation_goal_total: nz(goalTotal, 0),
      allocation_current_open_cards: nz(currentCards, 0),
      allocation_current_open_loans: nz(currentLoans, 0),
      allocation_needed_total: nz(neededTotal, 0),
      allocation_revPctMinCards: nz(pctRevolvingMin, 0),
      allocation_loan_cap_remaining:
        imposeMaxLoanNumber && maxLoansAllowed != null
          ? Math.max(0, maxLoansAllowed - loansOpenedSinceStart)
          : null,
      allocation_cards_to_open: nz(cardsToOpen, 0),
      allocation_loans_to_open: nz(loansToOpen, 0),

      // timing_*
      timing_opens_in_365: nz(opensIn365, 0),
      timing_requests_in_180: nz(requestsIn180, 0),
      timing_most_recent_open: mostRecentOpenDate
        ? toTs(mostRecentOpenDate)
        : null,
      timing_most_recent_request: mostRecentReqDate
        ? toTs(mostRecentReqDate)
        : null,
      timing_yearly_opens_max: nz(yearlyOpensMax, 0),
      timing_halfyear_requests_max:
        nz(halfYearHeadroom > 0 ? halfYearHeadroom + requestsIn180 : halfYearHeadroom + requestsIn180, 0), // safe echo
      timing_min_months_between_opens: nz(minMonthsBetweenOpens, 0),
      timing_min_months_between_requests: nz(
        minMonthsBetweenRequests,
        0
      ),
      timing_is_blocked_caps:
        !isDone && !!(yearlyHeadroom <= 0 || halfYearHeadroom <= 0),
      timing_is_blocked_intervals:
        !isDone &&
        (nz(minMonthsBetweenOpens, 0) > 0 ||
        nz(minMonthsBetweenRequests, 0) > 0
          ? nextEligibleDate
            ? nowNY < nextEligibleDate
            : false
          : false),
      timing_block_reason:
        slotsNow > 0 || isDone
          ? null
          : (yearlyHeadroom <= 0 || halfYearHeadroom <= 0) &&
            (nowNY < nextByOpenInterval || nowNY < nextByReqInterval)
          ? 'caps_and_intervals'
          : yearlyHeadroom <= 0 || halfYearHeadroom <= 0
          ? 'caps'
          : 'intervals',

      // selection/meta
      selection_protocol: 'alternate_by_most_recent_type',
      meta_baseline_open_loans: baselineOpenLoans,
    };

    // index the sequence for FF binding
    sequence.forEach((t, i) => {
      flat[`proposed_sequence_now_${i}`] = t;
    });
    flat.proposed_sequence_now_len = sequence.length;

    // ---------- UPSERT (deterministic doc id, update-in-place, safe returns) ----------
    const docId = `${uid}:open_actions_current`;
    const outRef = db.collection('user_open_actions_list').doc(docId);

    const existingSnap = await outRef.get();
    if (!existingSnap.exists) {
      const toCreate = {
        ...flat,
        created_time: admin.firestore.Timestamp.now(),
        last_updated_time: admin.firestore.Timestamp.now(),
      };
      if (cycleId) {
        // tag initial create with cycleId when orchestrated
        toCreate.cycleId = cycleId;
      }
      await outRef.set(toCreate, { merge: false });
      return {
        created: true,
        updated: false,
        unchanged: false,
        ...makeClientPayload(outRef.id, toCreate),
      };
    }

    // Compare with existing (ignore created_time / last_updated_time / cycleId)
    const existing = existingSnap.data() || {};
    const {
      created_time: _ctPrev,
      last_updated_time: _lutPrev,
      cycleId: _cidPrev, // exclude cycleId from comparison
      ...prevComparable
    } = existing;
    const {
      created_time: _ctNow,
      last_updated_time: _lutNow,
      ...nowComparable
    } = flat;

    if (toComparable(prevComparable) === toComparable(nowComparable)) {
      // Core is unchanged.
      if (cycleId) {
        // But we still want to stamp / update cycleId when orchestrated.
        const minimalUpdate = {
          cycleId,
          last_updated_time: admin.firestore.Timestamp.now(),
        };
        await outRef.set(minimalUpdate, { merge: true });
        const merged = { ...existing, ...minimalUpdate };
        return {
          created: false,
          updated: true,
          unchanged: false,
          ...makeClientPayload(outRef.id, merged),
        };
      }

      // Truly unchanged, no cycleId action.
      return {
        created: false,
        updated: false,
        unchanged: true,
        ...makeClientPayload(outRef.id, existing),
      };
    }

    // Clean up sequence tail if the new sequence is shorter
    const cleanup = {};
    const prevLen =
      typeof existing.proposed_sequence_now_len === 'number'
        ? existing.proposed_sequence_now_len
        : 0;
    const newLen = flat.proposed_sequence_now_len;
    if (prevLen > newLen) {
      for (let i = newLen; i < prevLen; i++) {
        cleanup[`proposed_sequence_now_${i}`] =
          admin.firestore.FieldValue.delete();
      }
    }

    const toUpdate = {
      ...flat,
      last_updated_time: admin.firestore.Timestamp.now(),
      ...cleanup,
    };
    if (cycleId) {
      toUpdate.cycleId = cycleId; // tag updated doc with cycleId when orchestrated
    }

    await outRef.set(toUpdate, { merge: true });
    return {
      created: false,
      updated: true,
      unchanged: false,
      ...makeClientPayload(outRef.id, toUpdate),
    };
  });

module.exports = {
  writeOpenActionsListORCH,
};
