const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// To avoid deployment errors, do not call admin.initializeApp() in your code

exports.simVizOutput = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth || !context.auth.uid) return;

    const db = admin.firestore();
    const uid = context.auth.uid;
    const userRef = db.doc(`users/${uid}`);

    const SOURCE_COL = 'user_sim_stocks_conso';
    const OUT_COL = 'user_sim_viz_output';

    // ---------- helpers ----------
    const asNum = (v, d = 0) => {
      if (typeof v === 'number' && isFinite(v)) return v;
      if (typeof v === 'string' && v.trim() !== '' && isFinite(Number(v))) return Number(v);
      return d;
    };

    const round2 = (v) => Math.round(asNum(v, 0) * 100) / 100;

    // OPEN filter for accounts (cards/loans) in viz counts/util/age.
    // Default to open if no clear status is present (prevents accidentally zeroing counts).
    const isOpenAccountRow = (r) => {
      if (typeof r?.status === 'boolean') return r.status;        // common in your schema
      if (typeof r?.isOpen === 'boolean') return r.isOpen;
      if (typeof r?.isClosed === 'boolean') return !r.isClosed;

      // if there's a "closed" timestamp, treat as closed
      if (r?.dateDeleted) return false;
      if (r?.closedAt) return false;

      return true;
    };


    const parseDate = (v) => {
      if (!v) return null;
      // Firestore Timestamp
      if (typeof v === 'object' && v.toDate && typeof v.toDate === 'function') return v.toDate();
      // JS Date
      if (v instanceof Date) return v;
      // ISO or YYYY-MM-DD
      if (typeof v === 'string') {
        const d = new Date(v);
        return isNaN(d.getTime()) ? null : d;
      }
      // Firefoo __time__ objects sometimes come through in exports, but live sim rows should be Timestamp/string
      if (typeof v === 'object' && v.__time__) {
        const d = new Date(v.__time__);
        return isNaN(d.getTime()) ? null : d;
      }
      return null;
    };

    // Whole-month difference, floored, with day-of-month adjustment
    // monthsBetween(later, earlier) >= 0
    const monthsBetween = (later, earlier) => {
      const a = parseDate(later);
      const b = parseDate(earlier);
      if (!a || !b) return 0;

      let months = (a.getFullYear() - b.getFullYear()) * 12 + (a.getMonth() - b.getMonth());
      // If later day hasn't reached earlier day in the month, subtract one
      if (a.getDate() < b.getDate()) months -= 1;
      return Math.max(0, months);
    };

    const makeTimesteps = (end) => {
      if (end < 0) return [0];
      if (end < 4) {
        const arr = [];
        for (let i = 0; i <= end; i++) arr.push(i);
        return arr;
      }
      const t0 = 0;
      const t25 = Math.round(end * 0.25);
      const t50 = Math.round(end * 0.5);
      const t75 = Math.round(end * 0.75);
      const tEnd = end;
      return [t0, t25, t50, t75, tEnd];
    };

    const labelsFromTimesteps = (ts) => ts.map((t) => `${t} mo`);

    // ---------- high achiever targets (top-level collections) ----------
    // NOTE: We fetch once per invocation; used as constants in each timestep doc.
    let ha_cardCount = null;
    let ha_loanCount = null;

    let ha_LH_averageAgeMonths = null;

    let ha_AO_cardBal = null;
    let ha_AO_loanBal = null;

    let ha_AO_revolvingUtilization = null;
    let ha_AO_installmentUtilization = null;
    let ha_AO_totalUtilization = null;


    let ha_paid_count = null;
    let ha_unpaid_count = null;
    let ha_paid_totalAmountOwed = null;
    let ha_unpaid_totalAmountOwed = null;

    try {
      const haAccountsSnap = await db.collection('credit_high_achiever_account_numbers').get();
      haAccountsSnap.forEach((doc) => {
        const d = doc.data() || {};
        if (d.DebtType === 'Card' && d.Metric === 'Count') ha_cardCount = asNum(d.Value, null);
        if (d.DebtType === 'Loan' && d.Metric === 'Count') ha_loanCount = asNum(d.Value, null);
      });

      const haMetricsSnap = await db.collection('credit_high_achiever_metrics').limit(1).get();
      if (!haMetricsSnap.empty) {
        const d = haMetricsSnap.docs[0].data() || {};
        ha_LH_averageAgeMonths = asNum(d.LH_averageAgeMonths, null);

        ha_AO_cardBal = asNum(d.AO_cardBal, null);
        ha_AO_loanBal = asNum(d.AO_loanBal, null);

        ha_AO_revolvingUtilization = asNum(d.AO_revolvingUtilization, null);
        ha_AO_installmentUtilization = asNum(d.AO_installmentUtilization, null);
        ha_AO_totalUtilization = asNum(d.AO_totalUtilization, null);

        ha_paid_count = asNum(d.paid_count, null);
        ha_unpaid_count = asNum(d.unpaid_count, null);
        ha_paid_totalAmountOwed = asNum(d.paid_totalAmountOwed, null);
        ha_unpaid_totalAmountOwed = asNum(d.unpaid_totalAmountOwed, null);
      }
    } catch (e) {
      // swallow: targets are optional; leave nulls
    }

    const ha_target_totalAccounts_cards = ha_cardCount;
    const ha_target_totalAccounts_loans = ha_loanCount;
    const ha_target_totalAccounts_accountsOnly =
      ha_cardCount != null && ha_loanCount != null ? (ha_cardCount + ha_loanCount) : null;

    const ha_target_totalBalance_cards = ha_AO_cardBal;
    const ha_target_totalBalance_loans = ha_AO_loanBal;
    const ha_target_totalBalance_accountsOnly =
      ha_AO_cardBal != null && ha_AO_loanBal != null ? (ha_AO_cardBal + ha_AO_loanBal) : null;

    const ha_target_avgAgeMonths_accountsOnly = ha_LH_averageAgeMonths;
    const ha_target_rev_utilPct =
    ha_AO_revolvingUtilization != null ? round2(ha_AO_revolvingUtilization * 100) : null;

    const ha_target_inst_utilPct =
      ha_AO_installmentUtilization != null ? round2(ha_AO_installmentUtilization * 100) : null;

    const ha_target_all_utilPct =
      ha_AO_totalUtilization != null ? round2(ha_AO_totalUtilization * 100) : null;


    // Derog targets (if you want to draw a line)
    const ha_target_derog_paid_count = ha_paid_count;
    const ha_target_derog_unpaid_count = ha_unpaid_count;
    const ha_target_derog_paid_totalAmountOwed = ha_paid_totalAmountOwed;
    const ha_target_derog_unpaid_totalAmountOwed = ha_unpaid_totalAmountOwed;

    // ---------- NEW: ratio target (user_openAndCloseAdjustments) ----------
    // doc where Unique_Name == "Current accounts / lates", filtered by userRef
    let target_ratio_currentAccounts_to_lates = null;
    try {
      const ratioSnap = await db
        .collection('user_openAndCloseAdjustments')
        .where('userRef', '==', userRef)
        .where('Unique_Name', '==', 'Current accounts / lates')
        .limit(1)
        .get();

      if (!ratioSnap.empty) {
        const d = ratioSnap.docs[0].data() || {};
        const v = asNum(d.User_Value, null);
        target_ratio_currentAccounts_to_lates = v != null ? v : asNum(d.Default_Value, null);

      }
    } catch (e) {
      // optional; leave null
    }

    // ---------- determine which sim runs to process ----------
    const simRunId =
      typeof data?.simRunId === 'string' && data.simRunId.trim() !== '' ? data.simRunId.trim() : null;

    const simRunRefPath =
      typeof data?.simRunRefPath === 'string' && data.simRunRefPath.trim() !== ''
        ? data.simRunRefPath.trim()
        : null;

    let simRunRefsToProcess = [];

    if (simRunRefPath) {
      simRunRefsToProcess = [db.doc(simRunRefPath)];
    } else if (simRunId) {
      simRunRefsToProcess = [db.doc(`users/${uid}/SimulationRuns/${simRunId}`)];
    } else {
      // No simRun provided: default to MOST RECENT sim run for this user.
      // Prefer SimulationRuns metadata (createdAt) to avoid scanning all source rows.
      try {
        const srSnap = await db
          .collection(`users/${uid}/SimulationRuns`)
          .orderBy('createdAt', 'desc')
          .limit(1)
          .get();
        if (!srSnap.empty) {
          simRunRefsToProcess = [srSnap.docs[0].ref];
        } else {
          throw new Error('No SimulationRuns found (createdAt)');
        }
      } catch (e1) {
        // If createdAt doesn't exist / no index, try generatedAt as a fallback.
        try {
          const srSnap2 = await db
            .collection(`users/${uid}/SimulationRuns`)
            .orderBy('generatedAt', 'desc')
            .limit(1)
            .get();
          if (!srSnap2.empty) {
            simRunRefsToProcess = [srSnap2.docs[0].ref];
          }
        } catch (e2) {
          // Final fallback: infer all distinct simRunRefs for this user from source data.
          const snap = await db.collection(SOURCE_COL).where('userRef', '==', userRef).get();
          const uniq = new Map(); // path -> DocumentReference
          snap.forEach((doc) => {
            const d = doc.data() || {};
            const ref = d.simRunRef;
            if (ref && typeof ref.path === 'string') {
              uniq.set(ref.path, ref);
            }
          });
          simRunRefsToProcess = Array.from(uniq.values());
        }
      }
    }

    if (!simRunRefsToProcess.length) {
      return { ok: true, written: 0, reason: 'No sim runs found for user.' };
    }

    // ---------- process each sim run ----------
    const writtenIds = [];

    // Stock identifiers (canonical)
    const STOCK = {
      cards: 'user_credit_cards',
      loans: 'user_loans',
      cardLates: 'user_credit_cards_late_payments',
      loanLates: 'user_loans_late_payments',
      collections: 'user_collections_3rd_party',
      hardPulls: 'user_hard_pulls',
    };

    for (const simRunRef of simRunRefsToProcess) {
      // Pull all rows for this sim run + user
      const qSnap = await db
        .collection(SOURCE_COL)
        .where('userRef', '==', userRef)
        .where('simRunRef', '==', simRunRef)
        .get();

      if (qSnap.empty) continue;

      const rows = [];
      qSnap.forEach((doc) => {
        const d = doc.data() || {};
        const stock = d.stock;
        if (!stock) return;
        // include ALL stocks now (including hard pulls)
        rows.push(d);
      });

      // Determine duration
      let end = 0;
      for (const r of rows) {
        const mi = asNum(r.monthIndex, 0);
        if (mi > end) end = mi;
      }

      const timesteps = makeTimesteps(end);
      const labels = labelsFromTimesteps(timesteps);

      // Index by month
      const byMonth = new Map();
      for (const r of rows) {
        const mi = asNum(r.monthIndex, 0);
        if (!byMonth.has(mi)) byMonth.set(mi, []);
        byMonth.get(mi).push(r);
      }

      const simRunIdOut = simRunRef.id;
      const simRunPathOut = simRunRef.path;

      // ---- DELETE ALL existing viz docs for this user (across prior sim runs) ----
      // Needed because FlutterFlow charts often can't filter; they read the whole collection.
      const existingSnap = await db
        .collection(OUT_COL)
        .where('userRef', '==', userRef)
        .get();

      if (!existingSnap.empty) {
        // Delete in chunks to avoid batch limits (500 ops max).
        let batch = db.batch();
        let opCount = 0;

        for (const d of existingSnap.docs) {
          batch.delete(d.ref);
          opCount += 1;

          if (opCount >= 450) {
            await batch.commit();
            batch = db.batch();
            opCount = 0;
          }
        }

        if (opCount > 0) {
          await batch.commit();
        }
      }


      // ---- compute per timestep aggregates (stock-level + rollups) ----
      const series = []; // one entry per timestep, used for deltas

      for (let i = 0; i < timesteps.length; i++) {
        const t = timesteps[i];
        const label = labels[i];
        const bucket = byMonth.get(t) || [];

        // snapshot date (for ages)
        const snapshotDate = bucket.length ? parseDate(bucket[0].simDate) : null;

        // Stock-level accumulators
        const acc = {
          // counts
          cnt_cards: 0,
          cnt_loans: 0,
          cnt_cardLates_paid: 0,
          cnt_cardLates_unpaid: 0,
          cnt_loanLates_paid: 0,
          cnt_loanLates_unpaid: 0,
          cnt_collections_paid: 0,
          cnt_collections_unpaid: 0,
          cnt_hardPulls: 0,

          // balances
          bal_cards: 0,
          bal_loans: 0,
          bal_cardLates_paid: 0,
          bal_cardLates_unpaid: 0,
          bal_loanLates_paid: 0,
          bal_loanLates_unpaid: 0,
          bal_collections_paid: 0,
          bal_collections_unpaid: 0,

          // ages (sum + count) per grouping as needed
          age_all_sum: 0,
          age_all_cnt: 0,

          age_accountsOnly_sum: 0,
          age_accountsOnly_cnt: 0,

          age_derog_sum: 0,
          age_derog_cnt: 0,

          age_derog_paid_sum: 0,
          age_derog_paid_cnt: 0,

          age_derog_unpaid_sum: 0,
          age_derog_unpaid_cnt: 0,

          // utilization components
          rev_bal_sum: 0,
          rev_lim_sum: 0,

          inst_bal_sum: 0,
          inst_prin_sum: 0,

          all_bal_sum: 0,
          all_denom_sum: 0,
        };

        for (const r of bucket) {
          const stock = r.stock;
          const amt = asNum(r.amountsOwed, 0);
          const dof = r.DOFRecord ? parseDate(r.DOFRecord) : null;

          // denominators for utilization
          // cards => creditLimit; loans => originalLoanAmount (or principal-like)
          const denom =
            (stock === STOCK.cards || stock === STOCK.loans)
              ? asNum(r.creditLimit, 0)
              : 0;

          // Age is tracked for all stocks (including hard pulls) for "all accounts" age
          if (snapshotDate && dof) {
            const ageM = monthsBetween(snapshotDate, dof);
            acc.age_all_sum += ageM;
            acc.age_all_cnt += 1;
          }

          // Cards
          if (stock === STOCK.cards) {
            if (!isOpenAccountRow(r)) continue;

            acc.cnt_cards += 1;
            acc.bal_cards += amt;

            if (snapshotDate && dof) {
              const ageM = monthsBetween(snapshotDate, dof);
              acc.age_accountsOnly_sum += ageM;
              acc.age_accountsOnly_cnt += 1;
            }

            // utilization (revolving + allAccounts)
            acc.rev_bal_sum += amt;
            acc.rev_lim_sum += denom;

            acc.all_bal_sum += amt;
            acc.all_denom_sum += denom;

            continue;
          }

          // Loans
          if (stock === STOCK.loans) {
            if (!isOpenAccountRow(r)) continue;

            acc.cnt_loans += 1;
            acc.bal_loans += amt;

            if (snapshotDate && dof) {
              const ageM = monthsBetween(snapshotDate, dof);
              acc.age_accountsOnly_sum += ageM;
              acc.age_accountsOnly_cnt += 1;
            }

            // utilization (installment + allAccounts)
            acc.inst_bal_sum += amt;
            acc.inst_prin_sum += denom;

            acc.all_bal_sum += amt;
            acc.all_denom_sum += denom;

            continue;
          }

          // Card lates
          if (stock === STOCK.cardLates) {
            const paid = !!r.isPaid;
            if (paid) {
              acc.cnt_cardLates_paid += 1;
              acc.bal_cardLates_paid += amt;
            } else {
              acc.cnt_cardLates_unpaid += 1;
              acc.bal_cardLates_unpaid += amt;
            }

            if (snapshotDate && dof) {
              const ageM = monthsBetween(snapshotDate, dof);
              acc.age_derog_sum += ageM;
              acc.age_derog_cnt += 1;
              if (paid) {
                acc.age_derog_paid_sum += ageM;
                acc.age_derog_paid_cnt += 1;
              } else {
                acc.age_derog_unpaid_sum += ageM;
                acc.age_derog_unpaid_cnt += 1;
              }
            }
            continue;
          }

          // Loan lates
          if (stock === STOCK.loanLates) {
            const paid = !!r.isPaid;
            if (paid) {
              acc.cnt_loanLates_paid += 1;
              acc.bal_loanLates_paid += amt;
            } else {
              acc.cnt_loanLates_unpaid += 1;
              acc.bal_loanLates_unpaid += amt;
            }

            if (snapshotDate && dof) {
              const ageM = monthsBetween(snapshotDate, dof);
              acc.age_derog_sum += ageM;
              acc.age_derog_cnt += 1;
              if (paid) {
                acc.age_derog_paid_sum += ageM;
                acc.age_derog_paid_cnt += 1;
              } else {
                acc.age_derog_unpaid_sum += ageM;
                acc.age_derog_unpaid_cnt += 1;
              }
            }
            continue;
          }

          // Collections
          if (stock === STOCK.collections) {
            const paid = !!r.isPaid;
            if (paid) {
              acc.cnt_collections_paid += 1;
              acc.bal_collections_paid += amt;
            } else {
              acc.cnt_collections_unpaid += 1;
              acc.bal_collections_unpaid += amt;
            }

            if (snapshotDate && dof) {
              const ageM = monthsBetween(snapshotDate, dof);
              acc.age_derog_sum += ageM;
              acc.age_derog_cnt += 1;
              if (paid) {
                acc.age_derog_paid_sum += ageM;
                acc.age_derog_paid_cnt += 1;
              } else {
                acc.age_derog_unpaid_sum += ageM;
                acc.age_derog_unpaid_cnt += 1;
              }
            }
            continue;
          }

          // Hard pulls (count only; no balance; age already counted in all age above)
          if (stock === STOCK.hardPulls) {
            acc.cnt_hardPulls += 1;
            continue;
          }
        }

        // rollups
        const totalAccounts_accountsOnly = acc.cnt_cards + acc.cnt_loans;
        const totalBalance_accountsOnly = acc.bal_cards + acc.bal_loans;

        const totalAccounts_derog =
          acc.cnt_cardLates_paid +
          acc.cnt_cardLates_unpaid +
          acc.cnt_loanLates_paid +
          acc.cnt_loanLates_unpaid +
          acc.cnt_collections_paid +
          acc.cnt_collections_unpaid;

        const totalBalance_derog =
          acc.bal_cardLates_paid +
          acc.bal_cardLates_unpaid +
          acc.bal_loanLates_paid +
          acc.bal_loanLates_unpaid +
          acc.bal_collections_paid +
          acc.bal_collections_unpaid;

        const totalAccounts_derog_paid =
          acc.cnt_cardLates_paid + acc.cnt_loanLates_paid + acc.cnt_collections_paid;

        const totalBalance_derog_paid =
          acc.bal_cardLates_paid + acc.bal_loanLates_paid + acc.bal_collections_paid;

        const totalAccounts_derog_unpaid =
          acc.cnt_cardLates_unpaid + acc.cnt_loanLates_unpaid + acc.cnt_collections_unpaid;

        const totalBalance_derog_unpaid =
          acc.bal_cardLates_unpaid + acc.bal_loanLates_unpaid + acc.bal_collections_unpaid;

        const totalAccounts_allAccounts = totalAccounts_accountsOnly + totalAccounts_derog + acc.cnt_hardPulls;
        const totalBalance_allAccounts = totalBalance_accountsOnly + totalBalance_derog; // hard pulls no balance

        // averages (simple)
        const avgAgeMonths_allAccounts = acc.age_all_cnt ? (acc.age_all_sum / acc.age_all_cnt) : 0;
        const avgAgeMonths_accountsOnly = acc.age_accountsOnly_cnt ? (acc.age_accountsOnly_sum / acc.age_accountsOnly_cnt) : 0;
        const avgAgeMonths_derog = acc.age_derog_cnt ? (acc.age_derog_sum / acc.age_derog_cnt) : 0;
        const avgAgeMonths_derog_paid = acc.age_derog_paid_cnt ? (acc.age_derog_paid_sum / acc.age_derog_paid_cnt) : 0;
        const avgAgeMonths_derog_unpaid = acc.age_derog_unpaid_cnt ? (acc.age_derog_unpaid_sum / acc.age_derog_unpaid_cnt) : 0;

        // utilization (%)
        const rev_utilPct = acc.rev_lim_sum ? (acc.rev_bal_sum / acc.rev_lim_sum) * 100 : 0;
        const inst_utilPct = acc.inst_prin_sum ? (acc.inst_bal_sum / acc.inst_prin_sum) * 100 : 0;
        const all_utilPct = acc.all_denom_sum ? (acc.all_bal_sum / acc.all_denom_sum) * 100 : 0;

        // ratios (actual + target)
        const ratio_currentAccounts_to_lates =
          totalAccounts_derog > 0 ? (totalAccounts_accountsOnly / totalAccounts_derog) : 0;

        series.push({
          t,
          label,

          simRunId: simRunIdOut,
          simRunRefPath: simRunPathOut,

          totalAccounts_allAccounts,
          totalAccounts_accountsOnly,
          totalAccounts_derog,
          totalAccounts_derog_paid,
          totalAccounts_derog_unpaid,

          totalBalance_allAccounts: round2(totalBalance_allAccounts),
          totalBalance_accountsOnly: round2(totalBalance_accountsOnly),
          totalBalance_derog: round2(totalBalance_derog),
          totalBalance_derog_paid: round2(totalBalance_derog_paid),
          totalBalance_derog_unpaid: round2(totalBalance_derog_unpaid),

          totalAccounts_cards: acc.cnt_cards,
          totalAccounts_loans: acc.cnt_loans,
          totalAccounts_cardLates_paid: acc.cnt_cardLates_paid,
          totalAccounts_cardLates_unpaid: acc.cnt_cardLates_unpaid,
          totalAccounts_loanLates_paid: acc.cnt_loanLates_paid,
          totalAccounts_loanLates_unpaid: acc.cnt_loanLates_unpaid,
          totalAccounts_collections_paid: acc.cnt_collections_paid,
          totalAccounts_collections_unpaid: acc.cnt_collections_unpaid,
          totalAccounts_hardPulls: acc.cnt_hardPulls,

          totalBalance_cards: round2(acc.bal_cards),
          totalBalance_loans: round2(acc.bal_loans),
          totalBalance_cardLates_paid: round2(acc.bal_cardLates_paid),
          totalBalance_cardLates_unpaid: round2(acc.bal_cardLates_unpaid),
          totalBalance_loanLates_paid: round2(acc.bal_loanLates_paid),
          totalBalance_loanLates_unpaid: round2(acc.bal_loanLates_unpaid),
          totalBalance_collections_paid: round2(acc.bal_collections_paid),
          totalBalance_collections_unpaid: round2(acc.bal_collections_unpaid),

          avgAgeMonths_allAccounts: round2(avgAgeMonths_allAccounts),
          avgAgeMonths_accountsOnly: round2(avgAgeMonths_accountsOnly),
          avgAgeMonths_derog: round2(avgAgeMonths_derog),
          avgAgeMonths_derog_paid: round2(avgAgeMonths_derog_paid),
          avgAgeMonths_derog_unpaid: round2(avgAgeMonths_derog_unpaid),

          rev_utilPct: round2(rev_utilPct),
          inst_utilPct: round2(inst_utilPct),
          all_utilPct: round2(all_utilPct),

          target_ratio_currentAccounts_to_lates,
          ratio_currentAccounts_to_lates: round2(ratio_currentAccounts_to_lates),

          ha_target_totalAccounts_cards,
          ha_target_totalAccounts_loans,
          ha_target_totalAccounts_accountsOnly,

          ha_target_totalBalance_cards,
          ha_target_totalBalance_loans,
          ha_target_totalBalance_accountsOnly,

          ha_target_avgAgeMonths_accountsOnly,
          ha_target_rev_utilPct,
          ha_target_inst_utilPct,
          ha_target_all_utilPct,

          ha_target_derog_paid_count,
          ha_target_derog_unpaid_count,
          ha_target_derog_paid_totalAmountOwed,
          ha_target_derog_unpaid_totalAmountOwed,
        });
      }

      // NEW: end-based implied target for accountsOnly using ratio target * end derog count
      const endDerog = series.length ? asNum(series[series.length - 1].totalAccounts_derog, 0) : 0;
      const target_accountsOnly_fromRatio =
        (target_ratio_currentAccounts_to_lates != null)
          ? Math.round(endDerog * asNum(target_ratio_currentAccounts_to_lates, 0))
          : null;

      // write this same constant onto each timestep doc for UI simplicity
      for (let i = 0; i < series.length; i++) {
        series[i].target_accountsOnly_fromRatio = target_accountsOnly_fromRatio;
      }

      // ---- deltas (first delta = 0) ----
      for (let i = 0; i < series.length; i++) {
        const cur = series[i];
        const prev = i === 0 ? null : series[i - 1];

        const delta = (k) => (prev ? asNum(cur[k], 0) - asNum(prev[k], 0) : 0);

        cur.delta_totalAccounts_allAccounts = delta('totalAccounts_allAccounts');
        cur.delta_totalAccounts_accountsOnly = delta('totalAccounts_accountsOnly');
        cur.delta_totalAccounts_derog = delta('totalAccounts_derog');
        cur.delta_totalAccounts_derog_paid = delta('totalAccounts_derog_paid');
        cur.delta_totalAccounts_derog_unpaid = delta('totalAccounts_derog_unpaid');

        cur.delta_totalBalance_allAccounts = round2(delta('totalBalance_allAccounts'));
        cur.delta_totalBalance_accountsOnly = round2(delta('totalBalance_accountsOnly'));
        cur.delta_totalBalance_derog = round2(delta('totalBalance_derog'));
        cur.delta_totalBalance_derog_paid = round2(delta('totalBalance_derog_paid'));
        cur.delta_totalBalance_derog_unpaid = round2(delta('totalBalance_derog_unpaid'));

        cur.delta_avgAgeMonths_allAccounts = round2(delta('avgAgeMonths_allAccounts'));
        cur.delta_avgAgeMonths_accountsOnly = round2(delta('avgAgeMonths_accountsOnly'));
        cur.delta_avgAgeMonths_derog = round2(delta('avgAgeMonths_derog'));
        cur.delta_avgAgeMonths_derog_paid = round2(delta('avgAgeMonths_derog_paid'));
        cur.delta_avgAgeMonths_derog_unpaid = round2(delta('avgAgeMonths_derog_unpaid'));

        cur.delta_rev_utilPct = round2(delta('rev_utilPct'));
        cur.delta_inst_utilPct = round2(delta('inst_utilPct'));
        cur.delta_all_utilPct = round2(delta('all_utilPct'));

        cur.delta_ratio_currentAccounts_to_lates = round2(delta('ratio_currentAccounts_to_lates'));
      }

      // ---- write docs (one per timestep) ----
      const batch = db.batch();
      for (const cur of series) {
        const docId = `${simRunIdOut}__m${cur.t}`;
        const outRef = db.collection(OUT_COL).doc(docId);

        batch.set(outRef, {
          userRef,
          simRunRef,
          simRunId: simRunIdOut,

          timestep: cur.t,
          label: cur.label,
          generatedAt: admin.firestore.FieldValue.serverTimestamp(),

          target_ratio_currentAccounts_to_lates: cur.target_ratio_currentAccounts_to_lates,
          target_accountsOnly_fromRatio: cur.target_accountsOnly_fromRatio,

          // totals
          totalAccounts_allAccounts: cur.totalAccounts_allAccounts,
          totalAccounts_accountsOnly: cur.totalAccounts_accountsOnly,
          totalAccounts_derog: cur.totalAccounts_derog,
          totalAccounts_derog_paid: cur.totalAccounts_derog_paid,
          totalAccounts_derog_unpaid: cur.totalAccounts_derog_unpaid,

          totalBalance_allAccounts: cur.totalBalance_allAccounts,
          totalBalance_accountsOnly: cur.totalBalance_accountsOnly,
          totalBalance_derog: cur.totalBalance_derog,
          totalBalance_derog_paid: cur.totalBalance_derog_paid,
          totalBalance_derog_unpaid: cur.totalBalance_derog_unpaid,

          avgAgeMonths_allAccounts: cur.avgAgeMonths_allAccounts,
          avgAgeMonths_accountsOnly: cur.avgAgeMonths_accountsOnly,
          avgAgeMonths_derog: cur.avgAgeMonths_derog,
          avgAgeMonths_derog_paid: cur.avgAgeMonths_derog_paid,
          avgAgeMonths_derog_unpaid: cur.avgAgeMonths_derog_unpaid,

          // utilization
          rev_utilPct: cur.rev_utilPct,
          inst_utilPct: cur.inst_utilPct,
          all_utilPct: cur.all_utilPct,

          // ratios
          ratio_currentAccounts_to_lates: cur.ratio_currentAccounts_to_lates,

          // deltas
          delta_totalAccounts_allAccounts: cur.delta_totalAccounts_allAccounts,
          delta_totalAccounts_accountsOnly: cur.delta_totalAccounts_accountsOnly,
          delta_totalAccounts_derog: cur.delta_totalAccounts_derog,
          delta_totalAccounts_derog_paid: cur.delta_totalAccounts_derog_paid,
          delta_totalAccounts_derog_unpaid: cur.delta_totalAccounts_derog_unpaid,

          delta_totalBalance_allAccounts: cur.delta_totalBalance_allAccounts,
          delta_totalBalance_accountsOnly: cur.delta_totalBalance_accountsOnly,
          delta_totalBalance_derog: cur.delta_totalBalance_derog,
          delta_totalBalance_derog_paid: cur.delta_totalBalance_derog_paid,
          delta_totalBalance_derog_unpaid: cur.delta_totalBalance_derog_unpaid,

          delta_avgAgeMonths_allAccounts: cur.delta_avgAgeMonths_allAccounts,
          delta_avgAgeMonths_accountsOnly: cur.delta_avgAgeMonths_accountsOnly,
          delta_avgAgeMonths_derog: cur.delta_avgAgeMonths_derog,
          delta_avgAgeMonths_derog_paid: cur.delta_avgAgeMonths_derog_paid,
          delta_avgAgeMonths_derog_unpaid: cur.delta_avgAgeMonths_derog_unpaid,

          delta_rev_utilPct: cur.delta_rev_utilPct,
          delta_inst_utilPct: cur.delta_inst_utilPct,
          delta_all_utilPct: cur.delta_all_utilPct,

          delta_ratio_currentAccounts_to_lates: cur.delta_ratio_currentAccounts_to_lates,

          // high achiever targets
          ha_target_totalAccounts_cards: cur.ha_target_totalAccounts_cards,
          ha_target_totalAccounts_loans: cur.ha_target_totalAccounts_loans,
          ha_target_totalAccounts_accountsOnly: cur.ha_target_totalAccounts_accountsOnly,

          ha_target_totalBalance_cards: cur.ha_target_totalBalance_cards,
          ha_target_totalBalance_loans: cur.ha_target_totalBalance_loans,
          ha_target_totalBalance_accountsOnly: cur.ha_target_totalBalance_accountsOnly,

          ha_target_avgAgeMonths_accountsOnly: cur.ha_target_avgAgeMonths_accountsOnly,
          ha_target_rev_utilPct: cur.ha_target_rev_utilPct,
          ha_target_inst_utilPct: cur.ha_target_inst_utilPct,
          ha_target_all_utilPct: cur.ha_target_all_utilPct,

          ha_target_derog_paid_count: cur.ha_target_derog_paid_count,
          ha_target_derog_unpaid_count: cur.ha_target_derog_unpaid_count,
          ha_target_derog_paid_totalAmountOwed: cur.ha_target_derog_paid_totalAmountOwed,
          ha_target_derog_unpaid_totalAmountOwed: cur.ha_target_derog_unpaid_totalAmountOwed,
        });
      }

      await batch.commit();
      writtenIds.push(simRunIdOut);
    }

    return { ok: true, written: writtenIds.length, simRunIds: writtenIds };
  }
);
