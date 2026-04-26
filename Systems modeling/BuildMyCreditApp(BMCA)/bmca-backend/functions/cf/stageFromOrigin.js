const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// To avoid deployment errors, do not call admin.initializeApp() in your code

exports.stageFromOrigin = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth || !context.auth.uid) {
      throw new functions.https.HttpsError('unauthenticated', 'Missing auth.');
    }

    const db = admin.firestore();
    const uid = context.auth.uid;

    const userRef = db.doc(`users/${uid}`);

    // -----------------------------
    // Helpers
    // -----------------------------
    const chunk = (arr, size) => {
      const out = [];
      for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
      return out;
    };

    // Writes in batches of 450 to stay under 500 ops even if we add more fields later
    const commitBatches = async (ops) => {
      const groups = chunk(ops, 450);
      for (const group of groups) {
        const batch = db.batch();
        for (const op of group) op(batch);
        await batch.commit();
      }
    };

    const getDocsByUserRef = async (collectionName) => {
      const snap = await db.collection(collectionName).where('userRef', '==', userRef).get();
      return snap.docs;
    };

    // We standardize originDocRef to RELATIVE paths (collection/docId),
    // and CF3 can rebuild full doc refs with the userRef.
    const relPath = (collectionName, docId) => `${collectionName}/${docId}`;

    const sanitizeString = (v, fallback = '') => (typeof v === 'string' ? v : fallback);
    const sanitizeNumber = (v, fallback = 0) => (typeof v === 'number' && !Number.isNaN(v) ? v : fallback);
    const sanitizeBool = (v, fallback = false) => (typeof v === 'boolean' ? v : fallback);

    const baseStagingMeta = (stock, subStock, originDocRefValue, extra = {}) => ({
      userRef,
      stock,
      subStock,
      stagingSource: 'origin_snapshot',
      skipMatching: true,
      matchStatus: 'matched', // optional, but useful for UI consistency
      isConfirmed: true,

      // fields that exist in schema but not used for origin snapshot
      uploadID: '',
      parserVersion: '',
      matchCandidates: [],
      matchMethod: '',
      matchOnFields: [],
      userSelectedOriginDocRef: '',

      // required by schema on many stocks; set defaults when irrelevant
      lateOriginRef: 'Irrelevant',
      collections_agency: '',
      lateDisambiguousParentAccountString: '',

      originDocRef: originDocRefValue,

      ...extra,
    });

    // -----------------------------
    // Step 1 — Clear existing staging (single active funnel)
    // -----------------------------
    const stagingCol = db.collection('user_Staging_Accounts');
    const existingStagingSnap = await stagingCol.where('userRef', '==', userRef).get();

    const deleteOps = existingStagingSnap.docs.map((d) => (batch) => batch.delete(d.ref));
    await commitBatches(deleteOps);

    // -----------------------------
    // Step 2 — Read origin collections
    // -----------------------------
    const [
      cardDocs,
      loanDocs,
      cardLateDocs,
      loanLateDocs,
      collectionDocs,
      hardPullDocs,
    ] = await Promise.all([
      getDocsByUserRef('user_credit_cards'),
      getDocsByUserRef('user_loans'),
      getDocsByUserRef('user_credit_cards_late_payments'),
      getDocsByUserRef('user_loans_late_payments'),
      getDocsByUserRef('user_collections_3rd_party'),
      getDocsByUserRef('user_hard_pulls'),
    ]);

    // Build parent caches so lates can inherit lender/name/accountNumber without extra reads
    // Keyed by RELATIVE path: "user_credit_cards/{id}" or "user_loans/{id}"
    const cardByRelPath = new Map();
    for (const d of cardDocs) cardByRelPath.set(relPath('user_credit_cards', d.id), d.data());

    const loanByRelPath = new Map();
    for (const d of loanDocs) loanByRelPath.set(relPath('user_loans', d.id), d.data());

    // -----------------------------
    // Step 3 — Write staging docs (docId == origin docId)
    // -----------------------------
    const writeOps = [];
    let stagedCount = 0;

    const stageDoc = (docId, payload) => {
      // Ensure we ONLY write fields that exist in staging schema:
      // (we control payload construction below, so no spreading origin objects)
      const ref = stagingCol.doc(docId);
      writeOps.push((batch) => batch.set(ref, payload, { merge: false }));
      stagedCount += 1;
    };

    // ---- Cards -> staging
    for (const d of cardDocs) {
      const card = d.data();
      const originDocRefValue = relPath('user_credit_cards', d.id);

      stageDoc(
        d.id,
        baseStagingMeta('user_credit_cards', 'Revolving', originDocRefValue, {
          lender: sanitizeString(card.lender, ''),
          name: sanitizeString(card.commercialName, ''),
          accountNumber: sanitizeString(card.accountNumber, ''),
          isCFA: sanitizeBool(card.isCFA, false),
          isAnnualFee: sanitizeBool(card.isAnnualFee, false),
          isPaid: null,
          isCurrent: typeof card.isCurrent === 'boolean' ? card.isCurrent : null,
          severity: '',
          DOFRecord: card.dateIssued ?? null,
          creditLimit: card.creditLimit ?? null,
          amountsOwed: card.totalBalance ?? null,
          isOpen: sanitizeBool(card.isOpen, true),
          apr: card.apr ?? null,
          interestRate: null,
          collections_agency: '',
        })
      );
    }

    // ---- Loans -> staging
    for (const d of loanDocs) {
      const loan = d.data();
      const originDocRefValue = relPath('user_loans', d.id);

      stageDoc(
        d.id,
        baseStagingMeta('user_loans', 'Installment', originDocRefValue, {
          lender: sanitizeString(loan.lender, ''),
          name: sanitizeString(loan.commercialName, ''),
          accountNumber: sanitizeString(loan.accountNumber, ''),
          isCFA: sanitizeBool(loan.isCFA, false),
          isAnnualFee: false,
          isPaid: null,
          isCurrent: typeof loan.isCurrent === 'boolean' ? loan.isCurrent : null,
          severity: '',
          DOFRecord: loan.dateIssued ?? null,
          creditLimit: loan.principalOriginal ?? null,
          amountsOwed: loan.balance ?? null,
          isOpen: sanitizeBool(loan.isOpen, true),
          apr: loan.apr ?? null,
          interestRate: null,
          collections_agency: '',
        })
      );
    }

    // ---- Card lates -> staging
    for (const d of cardLateDocs) {
      const late = d.data();

      const parentRel = late.cardRef && typeof late.cardRef === 'object' && late.cardRef.path
        ? late.cardRef.path
        : (late.cardRef && typeof late.cardRef === 'string' ? late.cardRef : '');

      const parent = cardByRelPath.get(parentRel) || null;

      const inferredSeverity = (() => {
        // Origin lates extract doesn’t always have severity; infer minimally.
        if (late && typeof late.chargedOff === 'boolean' && late.chargedOff) return 'CO';
        if (late && typeof late.sentToCollections === 'boolean' && late.sentToCollections) return 'Collection';
        return '';
      })();

      const lateRel = relPath('user_credit_cards_late_payments', d.id);

      stageDoc(
        d.id,
        baseStagingMeta('user_credit_cards_late_payments', 'Revolving', lateRel, {
          lender: parent ? sanitizeString(parent.lender, '') : '',
          name: parent ? sanitizeString(parent.commercialName, '') : '',
          accountNumber: parent ? sanitizeString(parent.accountNumber, '') : '',
          DOFRecord: late.DOFD ?? null,
          severity: sanitizeString(late.severity, inferredSeverity),
          isPaid: typeof late.isPaid === 'boolean' ? late.isPaid : null,
          isCurrent: null,
          amountsOwed: typeof late.amount === 'number' ? late.amount : null,

          // CF2 semantics: parent account ref goes here
          lateOriginRef: parentRel || 'user_credit_cards/UNKNOWN',
        })
      );
    }

    // ---- Loan lates -> staging
    for (const d of loanLateDocs) {
      const late = d.data();

      const parentRel = late.loanRef && typeof late.loanRef === 'object' && late.loanRef.path
        ? late.loanRef.path
        : (late.loanRef && typeof late.loanRef === 'string' ? late.loanRef : '');

      const parent = loanByRelPath.get(parentRel) || null;

      const inferredSeverity = (() => {
        if (late && typeof late.chargedOff === 'boolean' && late.chargedOff) return 'CO';
        if (late && typeof late.sentToCollections === 'boolean' && late.sentToCollections) return 'Collection';
        return '';
      })();

      const lateRel = relPath('user_loans_late_payments', d.id);

      stageDoc(
        d.id,
        baseStagingMeta('user_loans_late_payments', 'Installment', lateRel, {
          lender: parent ? sanitizeString(parent.lender, '') : '',
          name: parent ? sanitizeString(parent.commercialName, '') : '',
          accountNumber: parent ? sanitizeString(parent.accountNumber, '') : '',
          DOFRecord: late.DOFD ?? null,
          severity: sanitizeString(late.severity, inferredSeverity),
          isPaid: typeof late.isPaid === 'boolean' ? late.isPaid : null,
          isCurrent: null,
          amountsOwed: typeof late.amount === 'number' ? late.amount : null,
          lateOriginRef: parentRel || 'user_loans/UNKNOWN',
        })
      );
    }

    // ---- 3rd-party collections -> staging
    for (const d of collectionDocs) {
      const c = d.data();
      const originDocRefValue = relPath('user_collections_3rd_party', d.id);

      stageDoc(
        d.id,
        baseStagingMeta('user_collections_3rd_party', 'Collection', originDocRefValue, {
          lender: sanitizeString(c.originalProvider, ''),
          name: sanitizeString(c.originalProvider, ''),
          collections_agency: sanitizeString(c.collectionsAgency, ''),
          DOFRecord: c.DOFD ?? null,
          amountsOwed: typeof c.amount === 'number' ? c.amount : null,
          isPaid: typeof c.isPaid === 'boolean' ? c.isPaid : null,
          severity: 'Collection',

          // Not meaningful for collections
          accountNumber: '',
          isCFA: false,
          isAnnualFee: false,
          isOpen: null,
          isCurrent: null,
          apr: null,
          creditLimit: null,
          interestRate: null,
        })
      );
    }

    // ---- Hard pulls -> staging
    for (const d of hardPullDocs) {
      const hp = d.data();
      const originDocRefValue = relPath('user_hard_pulls', d.id);

      stageDoc(
        d.id,
        baseStagingMeta('hard_pull', 'Inquiry', originDocRefValue, {
          lender: sanitizeString(hp.lender, ''),
          name: sanitizeString(hp.productName, ''),
          DOFRecord: hp.dateOfRequest ?? null,

          // Not meaningful for hard pulls
          accountNumber: '',
          amountsOwed: null,
          apr: null,
          creditLimit: null,
          interestRate: null,
          isAnnualFee: false,
          isCFA: false,
          isOpen: null,
          isCurrent: null,
          isPaid: null,
          severity: '',
          collections_agency: '',
        })
      );
    }

    // Commit staging writes
    await commitBatches(writeOps);

    return {
      ok: true,
      clearedCount: existingStagingSnap.size,
      stagedCount,
      breakdown: {
        cards: cardDocs.length,
        loans: loanDocs.length,
        cardLates: cardLateDocs.length,
        loanLates: loanLateDocs.length,
        collections: collectionDocs.length,
        hardPulls: hardPullDocs.length,
      },
    };
  }
);
