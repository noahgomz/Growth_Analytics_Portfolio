const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// To avoid deployment errors, do not call admin.initializeApp() in your code

// ---- Hard-coded matching spec (from stage_origin_matching_fingerprints.csv) ----
const MATCHING_SPEC = {
  user_credit_cards: {
    originCollection: 'user_credit_cards',
    candidatePairs: [
      { staging: 'lender', origin: 'lender' },
      { staging: 'DOFRecord', origin: 'dateIssued' },
      { staging: 'accountNumber', origin: 'accountNumber' },
      { staging: 'isCFA', origin: 'isCFA' },
    ],
    disambiguationFields: ['isOpen', 'creditLimit', 'amountsOwed'],
  },

  user_loans: {
    originCollection: 'user_loans',
    candidatePairs: [
      { staging: 'lender', origin: 'lender' },
      { staging: 'DOFRecord', origin: 'dateIssued' },
      { staging: 'accountNumber', origin: 'accountNumber' },
      { staging: 'isCFA', origin: 'isCFA' },
    ],
    disambiguationFields: ['isOpen', 'creditLimit', 'amountsOwed'],
  },

  user_credit_cards_late_payments: {
    originCollection: 'user_credit_cards_late_payments',
    candidatePairs: [
      { staging: 'DOFRecord', origin: 'DOFD' },
      { staging: 'lateDisambiguousParentAccountString', origin: 'lateDisambiguousParentAccountString' },
    ],
    disambiguationFields: ['isPaid', 'lateDisambiguousParentAccountString', 'amountsOwed'],
  },

  user_loans_late_payments: {
    originCollection: 'user_loans_late_payments',
    candidatePairs: [
      { staging: 'DOFRecord', origin: 'DOFD' },
      { staging: 'lateDisambiguousParentAccountString', origin: 'lateDisambiguousParentAccountString' },
    ],
    disambiguationFields: ['isPaid', 'lateDisambiguousParentAccountString', 'amountsOwed'],
  },

  user_collections_3rd_party: {
    originCollection: 'user_collections_3rd_party',
    candidatePairs: [
      { staging: 'lender', origin: 'originalProvider' },
      { staging: 'collections_agency', origin: 'collectionsAgency' },
      { staging: 'DOFRecord', origin: 'DOFD' },
    ],
    disambiguationFields: ['isPaid'],
  },

  // Hard pulls matching
  hard_pull: {
    originCollection: 'user_hard_pulls',
    candidatePairs: [
      { staging: 'lender', origin: 'lender' },
      { staging: 'DOFRecord', origin: 'dateOfRequest' },
    ],
    disambiguationFields: [],
  },
};

exports.stageFromUpload = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth || !context.auth.uid) return;

    const uid = context.auth.uid;
    const uploadId = (data && data.uploadId) ? String(data.uploadId).trim() : '';
    if (!uploadId) {
      throw new functions.https.HttpsError('invalid-argument', 'Missing required field: uploadId');
    }

    const db = admin.firestore();
    const userRef = db.doc(`users/${uid}`);
    const stagingCol = db.collection('user_Staging_Accounts');
    const snapsCol = db.collection(`users/${uid}/reportUploads/${uploadId}/stocks_conso_report_uploads`);

    // ---------------- helpers ----------------
    const asStr = (v, fallback = '') => (typeof v === 'string' ? v : (v == null ? fallback : String(v)));
    const asNum = (v, fallback = null) => {
      if (v == null) return fallback;
      const n = Number(v);
      return Number.isFinite(n) ? n : fallback;
    };

    const monthIndex = (m) => {
      const map = { Jan:0, Feb:1, Mar:2, Apr:3, May:4, Jun:5, Jul:6, Aug:7, Sep:8, Oct:9, Nov:10, Dec:11 };
      return (m in map) ? map[m] : null;
    };

    const tsFromMonthYear = (year, monStr) => {
      const mi = monthIndex(monStr);
      if (!Number.isFinite(year) || mi == null) return null;
      return admin.firestore.Timestamp.fromDate(new Date(Date.UTC(year, mi, 1, 0, 0, 0)));
    };

    // Payment history code sets
    const NEG = new Set(['30','60','90','120','150','180','CO']);
    const SOFT = new Set(['D','NR']);                 // do not end event by themselves
    const HARD = new Set(['OK','NO','UN','PP','FC']); // ends event immediately (FC ends lates)

    const normalizeCode = (c) => asStr(c, '').trim().toUpperCase();

    const sortPaymentHistoryChronological = (ph) => {
      if (!Array.isArray(ph)) return [];
      const withKey = ph
        .map((x) => {
          const year = asNum(x && x.year, null);
          const mon = asStr(x && x.month, '');
          const mi = monthIndex(mon);
          return { ...x, _y: year, _mi: mi };
        })
        .filter((x) => Number.isFinite(x._y) && x._mi != null);

      withKey.sort((a, b) => (a._y - b._y) || (a._mi - b._mi));
      return withKey;
    };

    // Derive late events:
    // - NEG starts/continues
    // - SOFT (D/NR) is transparent (doesn't end)
    // - HARD ends; isPaid true only if the ending code is OK
    const deriveLateEvents = (paymentHistory) => {
      const months = sortPaymentHistoryChronological(paymentHistory);

      const events = [];
      let inEvent = false;
      let startMonth = null;
      let lastNegMonth = null;
      let maxNum = null;
      let hasCO = false;

      const flush = (paid) => {
        if (!inEvent || !startMonth || !lastNegMonth) return;

        let severity = '';
        if (maxNum != null) severity = String(maxNum);
        if (hasCO) severity = 'CO';

        events.push({
          startMonth,
          endMonth: lastNegMonth,
          severity,
          isPaid: !!paid,
        });

        inEvent = false;
        startMonth = null;
        lastNegMonth = null;
        maxNum = null;
        hasCO = false;
      };

      for (let i = 0; i < months.length; i++) {
        const m = months[i];
        const code = normalizeCode(m.code);

        if (NEG.has(code)) {
          if (!inEvent) {
            inEvent = true;
            startMonth = { year: m._y, month: m.month };
          }
          lastNegMonth = { year: m._y, month: m.month };

          if (code === 'CO') {
            hasCO = true;
          } else {
            const n = Number(code);
            if (Number.isFinite(n)) {
              if (maxNum == null || n > maxNum) maxNum = n;
            }
          }
          continue;
        }

        if (SOFT.has(code)) continue;

        if (inEvent) {
          const paid = (code === 'OK');
          flush(paid);
        }
      }

      if (inEvent) flush(false);
      return events;
    };

    const buildLateDisambiguousParentAccountString = (snap) => {
      const lender = asStr(snap.lender || snap.companyName, '');
      const acctNum = asStr(snap.accountNumber, '');
      const loanType = asStr(snap.loanType, '');
      const openDate = snap.openDate && typeof snap.openDate.toDate === 'function'
        ? snap.openDate.toDate().toISOString()
        : asStr(snap.openDate, '');
      return `${lender}|${acctNum}|${openDate}|${loanType}`;
    };

    // ---------------- matching helpers ----------------
    const normalizeAlphaNum = (s) => asStr(s, '').toUpperCase().replace(/[^A-Z0-9]/g, '');
    const normalizeStringLoose = (s) => asStr(s, '').trim().toUpperCase();

    const sameDayKey = (ts) => {
      if (!ts || typeof ts.toDate !== 'function') return '';
      const d = ts.toDate();
      const y = d.getUTCFullYear();
      const m = String(d.getUTCMonth() + 1).padStart(2, '0');
      const day = String(d.getUTCDate()).padStart(2, '0');
      return `${y}-${m}-${day}`;
    };

    const isMeaningfulAccountNumber = (acct) => {
      const n = normalizeAlphaNum(acct);
      if (n.length < 4) return false;
      return new Set(n.split('')).size >= 2; // characters change
    };

    const last4 = (acct) => {
      const n = normalizeAlphaNum(acct);
      if (n.length < 4) return '';
      return n.slice(-4);
    };

    const fieldHasMeaningfulValue = (doc, field) => {
      const v = doc[field];
      if (v == null) return false;
      if (typeof v === 'string') return v.trim().length > 0;
      if (typeof v === 'boolean') return true;
      return true;
    };

    const valuesMatch = (stagingVal, originVal, stagingField, originField) => {
      const isTs = (x) => x && typeof x.toDate === 'function';

      if (isTs(stagingVal) && isTs(originVal)) {
        return sameDayKey(stagingVal) === sameDayKey(originVal);
      }

      if (typeof stagingVal === 'boolean' || typeof originVal === 'boolean') {
        return !!stagingVal === !!originVal;
      }

      if (typeof stagingVal === 'number' || typeof originVal === 'number') {
        const a = (stagingVal == null) ? null : Number(stagingVal);
        const b = (originVal == null) ? null : Number(originVal);
        if (!Number.isFinite(a) || !Number.isFinite(b)) return false;
        return a === b;
      }

      if (stagingField === 'accountNumber' || originField === 'accountNumber') {
        if (!isMeaningfulAccountNumber(stagingVal) || !isMeaningfulAccountNumber(originVal)) return false;
        return last4(stagingVal) === last4(originVal);
      }

      return normalizeStringLoose(stagingVal) === normalizeStringLoose(originVal);
    };

    // Load origin docs for an origin collection (top-level only) filtered by userRef.
    const loadOriginDocs = async (collectionName) => {
      const out = [];
      const topSnap = await db.collection(collectionName).where('userRef', '==', userRef).get();
      topSnap.docs.forEach((d) => out.push({ path: `${collectionName}/${d.id}`, data: d.data() || {} }));
      return out;
    };

    const computeMatchMeta = (stagingDoc, originDocs, specEntry) => {
      const base = {
        matchStatus: 'new',
        matchMethod: 'candidateFields',
        matchOnFields: [],
        matchCandidates: [],
        userSelectedOriginDocRef: '',
      };

      // 0) skipMatching
      if (stagingDoc.skipMatching === true) {
        return { ...base, matchStatus: 'new', matchMethod: 'skipMatching' };
      }

      // 1) originDocRef (if present)
      const existing = asStr(stagingDoc.originDocRef, '');
      if (existing) {
        return { ...base, matchStatus: 'matched', matchMethod: 'originDocRef', originDocRef: existing };
      }

      if (!specEntry || !Array.isArray(originDocs)) return base;

      const stock = asStr(stagingDoc.stock, '');
      const isAccountStock = (stock === 'user_credit_cards' || stock === 'user_loans');
      const isLateStock = (stock === 'user_credit_cards_late_payments' || stock === 'user_loans_late_payments');
      const isHardPullStock = (stock === 'hard_pull');

      const toPathsSorted = (docs) => docs.map(d => d.path).sort();

      // Late matching special: DOFRecord + lateDisambiguousParentAccountString
      if (isLateStock) {
        const pairs = [
          { staging: 'DOFRecord', origin: 'DOFD' },
          { staging: 'lateDisambiguousParentAccountString', origin: 'lateDisambiguousParentAccountString' },
        ];

        const candidates = originDocs.filter((od) => {
          const o = od.data || {};
          for (const p of pairs) {
            if (!fieldHasMeaningfulValue(stagingDoc, p.staging)) return false;
            if (!valuesMatch(stagingDoc[p.staging], o[p.origin], p.staging, p.origin)) return false;
          }
          return true;
        });

        if (candidates.length === 0) {
          return { ...base, matchStatus: 'new', matchMethod: 'candidateFields', matchOnFields: pairs.map(p => p.staging) };
        }

        return {
          ...base,
          matchStatus: 'needs_disambiguation',
          matchMethod: 'candidateFields',
          matchOnFields: pairs.map(p => p.staging),
          matchCandidates: toPathsSorted(candidates),
        };
      }

      const candidatePairs = specEntry.candidatePairs || [];
      const pairsNoAcct = candidatePairs.filter(p => p.staging !== 'accountNumber');

      // 2) strong match: last4 + other candidate fields (accounts only)
      if (isAccountStock && isMeaningfulAccountNumber(stagingDoc.accountNumber)) {
        const sLast4 = last4(stagingDoc.accountNumber);

        const candidates = originDocs.filter((od) => {
          const o = od.data || {};
          if (!isMeaningfulAccountNumber(o.accountNumber)) return false;
          if (last4(o.accountNumber) !== sLast4) return false;

          for (const p of pairsNoAcct) {
            if (!fieldHasMeaningfulValue(stagingDoc, p.staging)) return false;
            if (!valuesMatch(stagingDoc[p.staging], o[p.origin], p.staging, p.origin)) return false;
          }
          return true;
        });

        const onFields = ['accountNumber', ...pairsNoAcct.map(p => p.staging)];

        if (candidates.length === 1) {
          return {
            ...base,
            matchStatus: 'matched',
            matchMethod: 'acctLast4+candidateFields',
            matchOnFields: onFields,
            originDocRef: candidates[0].path,
            matchCandidates: [candidates[0].path],
          };
        }

        if (candidates.length > 1) {
          return {
            ...base,
            matchStatus: 'needs_disambiguation',
            matchMethod: 'acctLast4+candidateFields',
            matchOnFields: onFields,
            matchCandidates: toPathsSorted(candidates),
          };
        }
      }

      // 3) candidate-only
      const candOnly = originDocs.filter((od) => {
        const o = od.data || {};
        for (const p of pairsNoAcct) {
          if (!fieldHasMeaningfulValue(stagingDoc, p.staging)) return false;
          if (!valuesMatch(stagingDoc[p.staging], o[p.origin], p.staging, p.origin)) return false;
        }
        return true;
      });

      // Hard pulls: auto-match if exactly 1 (no disambiguation fields anyway)
      if (isHardPullStock) {
        const onFields = pairsNoAcct.map(p => p.staging);
        if (candOnly.length === 1) {
          return {
            ...base,
            matchStatus: 'matched',
            matchMethod: 'candidateFields',
            matchOnFields: onFields,
            originDocRef: candOnly[0].path,
            matchCandidates: [candOnly[0].path],
          };
        }
        if (candOnly.length === 0) {
          return { ...base, matchStatus: 'new', matchMethod: 'candidateFields', matchOnFields: onFields };
        }
        return {
          ...base,
          matchStatus: 'needs_disambiguation',
          matchMethod: 'candidateFields',
          matchOnFields: onFields,
          matchCandidates: toPathsSorted(candOnly),
        };
      }

      if (candOnly.length === 0) {
        return { ...base, matchStatus: 'new', matchMethod: 'candidateFields', matchOnFields: pairsNoAcct.map(p => p.staging) };
      }

      return {
        ...base,
        matchStatus: 'needs_disambiguation',
        matchMethod: 'candidateFields',
        matchOnFields: pairsNoAcct.map(p => p.staging),
        matchCandidates: toPathsSorted(candOnly),
      };
    };

    // Build a UI preview candidate doc for stageMatchCandidates
    const buildCandidatePreview = (originCollection, originData) => {
      const o = originData || {};

      // Common preview defaults
      const out = {
        lender: '',
        DOFRecord: null,
        amountsOwed: null,
        creditLimit: null,
        collections_agency: '',
        isPaid: null,
      };

      // Cards / Loans
      if (originCollection === 'user_credit_cards' || originCollection === 'user_loans') {
        out.lender = asStr(o.lender, '');
        out.DOFRecord = o.dateIssued || o.DOFRecord || null;
        out.amountsOwed = (o.amountsOwed == null ? (o.balance == null ? null : asNum(o.balance, null)) : asNum(o.amountsOwed, null));
        out.creditLimit = (o.creditLimit == null ? null : asNum(o.creditLimit, null));
        return out;
      }

      // Hard pulls
      if (originCollection === 'user_hard_pulls') {
        out.lender = asStr(o.lender, '');
        out.DOFRecord = o.dateOfRequest || null;
        return out;
      }

      // 3rd-party collections
      if (originCollection === 'user_collections_3rd_party') {
        out.lender = asStr(o.originalProvider, '');
        out.DOFRecord = o.DOFD || null;
        out.amountsOwed = (o.amount == null ? null : asNum(o.amount, null));
        out.collections_agency = asStr(o.collectionsAgency, '');
        out.isPaid = (o.isPaid == null ? null : !!o.isPaid);
        return out;
      }

      // Lates
      if (originCollection === 'user_credit_cards_late_payments' || originCollection === 'user_loans_late_payments') {
        // lender may not exist on late docs; best-effort
        out.DOFRecord = o.DOFD || null;
        out.amountsOwed = (o.amountsOwed == null ? (o.amount == null ? null : asNum(o.amount, null)) : asNum(o.amountsOwed, null));
        out.isPaid = (o.isPaid == null ? null : !!o.isPaid);

        const parentStr = asStr(o.lateDisambiguousParentAccountString, '');
        if (parentStr.includes('|')) out.lender = parentStr.split('|')[0];
        else out.lender = asStr(o.lender, '');

        return out;
      }

      return out;
    };

    // ---------------- parserVersion ----------------
    let parserVersion = '';
    try {
      const uploadDoc = await db.doc(`users/${uid}/reportUploads/${uploadId}`).get();
      if (uploadDoc.exists) {
        const d = uploadDoc.data() || {};
        parserVersion = asStr(d.parserVersion, '');
      }
    } catch (e) {
      parserVersion = '';
    }

    // ---------------- 1) Clear staging for this user ----------------
    const deleteQuery = stagingCol.where('userRef', '==', userRef);
    let deleted = 0;

    while (true) {
      const snap = await deleteQuery.limit(400).get();
      if (snap.empty) break;

      const b = db.batch();
      snap.docs.forEach((doc) => b.delete(doc.ref));
      await b.commit();
      deleted += snap.size;
    }

    // ---------------- 2) Preload origin docs (by spec) ----------------
    const originDocsByCollection = {};
    const uniqueOriginCollections = new Set(
      Object.values(MATCHING_SPEC).map(x => x.originCollection).filter(Boolean)
    );

    for (const oc of uniqueOriginCollections) {
      originDocsByCollection[oc] = await loadOriginDocs(oc);
    }

    // ---------------- 3) Read upload snapshot docs ----------------
    const snapshotSnap = await snapsCol.get();

    let stagedAccounts = 0;
    let stagedInquiries = 0;
    let stagedLates = 0;
    let stagedCollectionsFromOrigin = 0;
    let skipped = 0;

    // Batch writes
    let batch = db.batch();
    let ops = 0;
    const commitIfNeeded = async () => {
      if (ops >= 400) {
        await batch.commit();
        batch = db.batch();
        ops = 0;
      }
    };

    // Local helper: when ambiguous, write stageMatchCandidates subcollection
    const writeStageMatchCandidates = (stagingRef, mm, originCollection, originDocs) => {
      if (!mm || mm.matchStatus !== 'needs_disambiguation') return;
      if (!Array.isArray(mm.matchCandidates) || mm.matchCandidates.length === 0) return;

      // Build map path -> origin data
      const originByPath = {};
      for (const od of (originDocs || [])) originByPath[od.path] = od.data || {};

      for (let i = 0; i < mm.matchCandidates.length; i++) {
        const path = mm.matchCandidates[i];
        const odata = originByPath[path] || {};
        const preview = buildCandidatePreview(originCollection, odata);

        const candDoc = {
          originDocRefPath: path,
          originCollection: originCollection,
          rank: i + 1,
          matchMethod: asStr(mm.matchMethod, ''),
          matchOnFields: Array.isArray(mm.matchOnFields) ? mm.matchOnFields : [],

          lender: asStr(preview.lender, ''),
          DOFRecord: preview.DOFRecord || null,
          amountsOwed: (preview.amountsOwed == null ? null : asNum(preview.amountsOwed, null)),
          creditLimit: (preview.creditLimit == null ? null : asNum(preview.creditLimit, null)),
          collections_agency: asStr(preview.collections_agency, ''),
          isPaid: (preview.isPaid == null ? null : !!preview.isPaid),
        };

        const candRef = stagingRef.collection('stageMatchCandidates').doc();
        batch.set(candRef, candDoc);
        ops++;
      }
    };

    // ---------------- 3A) Stage accounts + inquiries (+ derived lates) ----------------
    for (const doc of snapshotSnap.docs) {
      const snap = doc.data() || {};
      const recordType = asStr(snap.recordType, '');

      if (recordType === 'account') {
        const loanType = asStr(snap.loanType, '');
        const isCard = (loanType === 'Credit Card');
        const stock = isCard ? 'user_credit_cards' : 'user_loans';
        const subStock = isCard ? 'Revolving' : 'Installment';

        const lender = asStr(snap.lender || snap.companyName, '');
        const name = asStr(snap.companyName || snap.lender, '');

        // LOANS ONLY: creditLimit = highBalance ?? creditLimit
        const rawLoanLimit = (snap.highBalance ?? snap.creditLimit);
        const creditLimitVal = isCard
          ? (snap.creditLimit == null ? null : asNum(snap.creditLimit, null))
          : (rawLoanLimit == null ? null : asNum(rawLoanLimit, null));

        const stagingDoc = {
          userRef,
          stock,
          subStock,
          lender,
          name,
          accountNumber: asStr(snap.accountNumber, ''),

          isConfirmed: false,
          skipMatching: false,

          DOFRecord: snap.openDate || null,
          creditLimit: creditLimitVal,
          amountsOwed: snap.balance == null ? null : asNum(snap.balance, null),
          isOpen: snap.closedDate ? false : true,

          isCFA: false,
          isAnnualFee: false,
          apr: 0,
          interestRate: null,
          severity: '',
          isPaid: null,
          isCurrent: null,
          collections_agency: '',

          stagingSource: 'report_upload',
          uploadID: uploadId,
          parserVersion,

          originDocRef: '',
          lateOriginRef: 'Irrelevant',
          lateDisambiguousParentAccountString: null,
        };

        const specEntry = MATCHING_SPEC[stock];
        const originCollection = specEntry ? specEntry.originCollection : '';
        const originDocs = (originCollection && originDocsByCollection[originCollection]) ? originDocsByCollection[originCollection] : [];
        const mm = computeMatchMeta(stagingDoc, originDocs, specEntry);

        stagingDoc.matchStatus = mm.matchStatus;
        stagingDoc.matchMethod = mm.matchMethod;
        stagingDoc.matchOnFields = mm.matchOnFields;
        stagingDoc.matchCandidates = mm.matchCandidates;
        stagingDoc.userSelectedOriginDocRef = mm.userSelectedOriginDocRef;
        if (mm.originDocRef && mm.matchStatus === 'matched') stagingDoc.originDocRef = mm.originDocRef;

        const stagingRef = stagingCol.doc();
        batch.set(stagingRef, stagingDoc);
        ops++; stagedAccounts++;
        await commitIfNeeded();

        // NEW: Write candidate subcollection for UI disambiguation
        if (stagingDoc.skipMatching === false) {
          writeStageMatchCandidates(stagingRef, mm, originCollection, originDocs);
          await commitIfNeeded();
        }

        // Derive lates
        const lateEvents = deriveLateEvents(snap.paymentHistory);
        if (lateEvents.length) {
          const parentString = buildLateDisambiguousParentAccountString(snap);

          for (const ev of lateEvents) {
            const dof = tsFromMonthYear(ev.startMonth.year, ev.startMonth.month);

            const lateStock = isCard
              ? 'user_credit_cards_late_payments'
              : 'user_loans_late_payments';

            const lateDoc = {
              userRef,
              stock: lateStock,
              subStock,

              lender,
              name,
              accountNumber: asStr(snap.accountNumber, ''),

              isConfirmed: false,
              skipMatching: false,

              // NEW: pointer to parent account staging doc (required for report-derived lates)
              parentStagingDocId: stagingRef.id,

              DOFRecord: dof,
              severity: asStr(ev.severity, ''),
              isPaid: !!ev.isPaid,
              isCurrent: null,

              amountsOwed: 0,
              creditLimit: null,
              isOpen: null,

              apr: 0,
              interestRate: null,
              isCFA: false,
              isAnnualFee: false,

              originDocRef: '',
              lateOriginRef: 'Irrelevant',
              collections_agency: '',

              lateDisambiguousParentAccountString: parentString,

              stagingSource: 'report_upload',
              uploadID: uploadId,
              parserVersion,
            };

            const lateSpec = MATCHING_SPEC[lateStock];
            const lateOriginCollection = lateSpec ? lateSpec.originCollection : '';
            const lateOriginDocs = (lateOriginCollection && originDocsByCollection[lateOriginCollection]) ? originDocsByCollection[lateOriginCollection] : [];
            const lmm = computeMatchMeta(lateDoc, lateOriginDocs, lateSpec);

            lateDoc.matchStatus = lmm.matchStatus;
            lateDoc.matchMethod = lmm.matchMethod;
            lateDoc.matchOnFields = lmm.matchOnFields;
            lateDoc.matchCandidates = lmm.matchCandidates;
            lateDoc.userSelectedOriginDocRef = lmm.userSelectedOriginDocRef;
            if (lmm.originDocRef && lmm.matchStatus === 'matched') lateDoc.originDocRef = lmm.originDocRef;

            const lateRef = stagingCol.doc();
            batch.set(lateRef, lateDoc);
            ops++; stagedLates++;
            await commitIfNeeded();

            // NEW: Write candidate subcollection for UI disambiguation (lates)
            if (lateDoc.skipMatching === false) {
              writeStageMatchCandidates(lateRef, lmm, lateOriginCollection, lateOriginDocs);
              await commitIfNeeded();
            }
          }
        }

        continue;
      }

      if (recordType === 'inquiry') {
        const lender = asStr(snap.lender, '');

        const inquiryDoc = {
          userRef,
          stock: 'hard_pull',
          subStock: 'Inquiry',
          lender,
          name: lender,
          accountNumber: '',

          isConfirmed: false,
          skipMatching: false,

          DOFRecord: snap.inquiryDate || null,

          isCurrent: null,
          severity: '',
          isPaid: null,

          creditLimit: null,
          amountsOwed: null,
          isOpen: null,
          apr: 0,
          interestRate: null,
          isCFA: false,
          isAnnualFee: false,

          collections_agency: '',

          stagingSource: 'report_upload',
          uploadID: uploadId,
          parserVersion,

          originDocRef: '',
          lateOriginRef: 'Irrelevant',
          lateDisambiguousParentAccountString: null,
        };

        const specEntry = MATCHING_SPEC['hard_pull'];
        const originCollection = specEntry ? specEntry.originCollection : '';
        const originDocs = (originCollection && originDocsByCollection[originCollection]) ? originDocsByCollection[originCollection] : [];
        const mm = computeMatchMeta(inquiryDoc, originDocs, specEntry);

        inquiryDoc.matchStatus = mm.matchStatus;
        inquiryDoc.matchMethod = mm.matchMethod;
        inquiryDoc.matchOnFields = mm.matchOnFields;
        inquiryDoc.matchCandidates = mm.matchCandidates;
        inquiryDoc.userSelectedOriginDocRef = mm.userSelectedOriginDocRef;
        if (mm.originDocRef && mm.matchStatus === 'matched') inquiryDoc.originDocRef = mm.originDocRef;

        const stagingRef = stagingCol.doc();
        batch.set(stagingRef, inquiryDoc);
        ops++; stagedInquiries++;
        await commitIfNeeded();

        // NEW: Only if needs_disambiguation (rare for hard pulls, but supported)
        if (inquiryDoc.skipMatching === false) {
          writeStageMatchCandidates(stagingRef, mm, originCollection, originDocs);
          await commitIfNeeded();
        }

        continue;
      }

      // Skip collections + public records for now (per spec)
      skipped++;
    }

    if (ops > 0) await batch.commit();

    return {
      ok: true,
      uploadId,
      parserVersion,
      deletedExistingStaging: deleted,
      stagedAccounts,
      stagedInquiries,
      stagedLates,
      stagedCollectionsFromOrigin,
      skipped,
      totalSnapshotDocsRead: snapshotSnap.size,
    };
  }
);
