// functions/cf/writeUserLoanRecs.js
const functions = require('firebase-functions/v1');
const admin     = require('firebase-admin');
const crypto    = require('crypto');
// admin.initializeApp() is called once in index.js

// ---------- helpers ----------
const canonicalize = (obj) => {
  if (obj === null || typeof obj !== 'object') return obj;

  // Firestore Timestamp → ISO string
  if (typeof obj.toDate === 'function') return obj.toDate().toISOString();

  // DocumentReference → path
  if (obj && typeof obj.path === 'string') return obj.path;

  // Arrays
  if (Array.isArray(obj)) return obj.map(canonicalize);

  // Plain objects, sorted keys, no undefined
  const out = {};
  Object.keys(obj)
    .filter((k) => obj[k] !== undefined)
    .sort()
    .forEach((k) => {
      out[k] = canonicalize(obj[k]);
    });
  return out;
};

const hashPayload = (obj) =>
  crypto
    .createHash('sha256')
    .update(JSON.stringify(canonicalize(obj)))
    .digest('hex');

const asNum = (v, d = null) =>
  typeof v === 'number' && isFinite(v)
    ? v
    : typeof v === 'string' &&
      v.trim() !== '' &&
      !isNaN(Number(v))
    ? Number(v)
    : d;

module.exports = functions.region('us-central1').https.onCall(
  async (data, context) => {
    if (!context.auth || !context.auth.uid) return;

    const uid = context.auth.uid;
    const db = admin.firestore();
    const serverTs = admin.firestore.FieldValue.serverTimestamp();
    const userRef = db.collection('users').doc(uid);

    const userLoanRecsRef = db.collection('user_loan_recommendations');
    const offersLoansRef = db.collection('offers_loans');

    // 1) Load existing user loan recs
    const existingSnap = await userLoanRecsRef
      .where('userRef', '==', userRef)
      .get();

    const existingMap = {};
    existingSnap.docs.forEach((doc) => {
      existingMap[doc.id] = doc.data() || {};
    });
    const existingIds = new Set(Object.keys(existingMap));

    // 2) Load all current loan offers
    const offersSnap = await offersLoansRef.get();
    const offersById = {};
    offersSnap.forEach((doc) => {
      offersById[doc.id] = doc.data() || {};
    });
    const offerIds = new Set(Object.keys(offersById));

    // 3) Build desired payloads + hashes from offers
    const desiredById = {};
    const desiredHashById = {};
    for (const [id, o] of Object.entries(offersById)) {
      const desired = {
        lender: o.lender ?? null,
        productName: o.productName ?? null,
        offerLink: o.offerLink ?? null,

        minPrincipal: asNum(o.minPrincipal, null),
        maxPrincipal: asNum(o.maxPrincipal, null),

        aprLow: asNum(o.aprLow, null),
        aprHigh: asNum(o.aprHigh, null),

        durationLowMnths: asNum(o.durationLowMnths, null),
        durationHighMnths: asNum(o.durationHighMnths, null),

        // other UI-needed fields kept explicit for stable hash
        feesOrigination: asNum(o.feesOrigination, null),
        notes: o.notes ?? null,
      };
      desiredById[id] = desired;
      desiredHashById[id] = hashPayload(desired);
    }

    // 4) Compute rank order
    const rankOrder = Object.entries(offersById)
      .map(([id, o]) => ({
        id,
        minPrincipal: asNum(
          o.minPrincipal,
          Number.POSITIVE_INFINITY
        ),
        lender: (o.lender || '').toString().toLowerCase(),
        durationLowMnths: asNum(
          o.durationLowMnths,
          Number.POSITIVE_INFINITY
        ),
        aprLow: asNum(o.aprLow, Number.POSITIVE_INFINITY),
      }))
      .sort((a, b) => {
        if (a.minPrincipal !== b.minPrincipal) {
          return a.minPrincipal - b.minPrincipal;
        }
        if (a.lender !== b.lender) {
          return a.lender < b.lender ? -1 : 1;
        }
        if (a.durationLowMnths !== b.durationLowMnths) {
          return a.durationLowMnths - b.durationLowMnths;
        }
        return a.aprLow - b.aprLow;
      });

    const rankById = new Map();
    rankOrder.forEach((row, idx) => rankById.set(row.id, idx + 1)); // 1-based

    // 5) Figure out creates / deletes / keeps
    const toCreate = [...offerIds].filter(
      (id) => !existingIds.has(id)
    );
    const toDelete = [...existingIds].filter((id) => {
      const stillOffered = offerIds.has(id);
      const wasAccepted =
        existingMap[id]?.isAccepted === true;
      // keep accepted ones even if upstream removed
      return !stillOffered && !wasAccepted;
    });
    const toKeep = [...existingIds].filter((id) =>
      offerIds.has(id)
    );

    // 6) For keeps, detect payload changes via hash and rank changes
    const toUpdate = [];
    for (const id of toKeep) {
      const existing = existingMap[id] || {};
      const prevHash = existing.sourceHash || null;
      const nextHash = desiredHashById[id];
      const needsOfferUpdate = prevHash !== nextHash;

      const newRank = rankById.get(id) || null;
      const oldRank =
        typeof existing.rank === 'number' &&
        isFinite(existing.rank)
          ? existing.rank
          : null;
      const needsRankUpdate =
        newRank !== null && newRank !== oldRank;

      const missingUserRef = !existing.userRef;

      if (
        needsOfferUpdate ||
        needsRankUpdate ||
        missingUserRef
      ) {
        toUpdate.push({
          id,
          needsOfferUpdate,
          needsRankUpdate,
          missingUserRef,
          nextHash,
          newRank,
        });
      }
    }

    // 7) Batch writes: delete stale, create new, update changed
    const batch = db.batch();

    // Deletes
    toDelete.forEach((id) =>
      batch.delete(userLoanRecsRef.doc(id))
    );

    // Creates
    toCreate.forEach((id) => {
      const desired = desiredById[id];
      const sourceHash = desiredHashById[id];
      const rank = rankById.get(id) || 0;
      batch.set(
        userLoanRecsRef.doc(id),
        {
          ...desired,
          sourceHash,
          userRef,
          isAccepted: false,
          isDenied: false,
          rank,
          createdAt: serverTs,
          updatedAt: serverTs,
        },
        { merge: true }
      );
    });

    // Updates (patch only what changed)
    toUpdate.forEach(
      ({
        id,
        needsOfferUpdate,
        needsRankUpdate,
        missingUserRef,
        nextHash,
        newRank,
      }) => {
        const patch = { updatedAt: serverTs };
        if (missingUserRef) patch.userRef = userRef;
        if (needsOfferUpdate) {
          Object.assign(patch, {
            ...desiredById[id],
            sourceHash: nextHash,
          });
        }
        if (needsRankUpdate) patch.rank = newRank || 0;

        batch.set(userLoanRecsRef.doc(id), patch, {
          merge: true,
        });
      }
    );

    await batch.commit();

    return {
      success: true,
      created: toCreate.length,
      deleted: toDelete.length,
      kept: toKeep.length,
      updated: toUpdate.length,
      ranked: rankOrder.length,
    };
  }
);
