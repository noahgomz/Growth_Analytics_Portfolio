const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
const crypto = require('crypto');

// ---------- helpers ----------
function makeDocId(rewardType = '', lender = '', name = '') {
  return `${(rewardType || '')}-${(lender || '')}-${(name || '')}`
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')   // non-alphanumerics → dash
    .replace(/^-+|-+$/g, '');      // trim leading/trailing dashes
}

// Canonicalize values for stable hashing (handles Timestamps, DocRefs, arrays, object key order)
const canonicalize = (obj) => {
  if (obj === null || typeof obj !== 'object') return obj;
  // Firestore Timestamp
  if (typeof obj.toDate === 'function') return obj.toDate().toISOString();
  // Firestore DocumentReference
  if (obj && typeof obj.path === 'string') return obj.path;
  if (Array.isArray(obj)) return obj.map(canonicalize);
  const out = {};
  Object.keys(obj)
    .filter(k => obj[k] !== undefined) // drop undefined
    .sort()
    .forEach(k => { out[k] = canonicalize(obj[k]); });
  return out;
};

const hashPayload = (obj) =>
  crypto.createHash('sha256').update(JSON.stringify(canonicalize(obj))).digest('hex');

module.exports = functions
  .region('us-central1')
  .https.onCall(async (data, context) => {
    if (!context.auth?.uid) {
      throw new functions.https.HttpsError(
        'unauthenticated',
        'You must be logged in to perform this action.'
      );
    }

    const uid = context.auth.uid;
    const db = admin.firestore();
    const serverTs = admin.firestore.FieldValue.serverTimestamp();

    const userRef = db.collection('users').doc(uid);
    const userSnap = await userRef.get();
    const rewardPref = userSnap.data()?.card_reward_preference;
    if (!rewardPref) {
      throw new functions.https.HttpsError(
        'failed-precondition',
        'User does not have a card_reward_preference set.'
      );
    }

    const userCardsRef   = db.collection('user_card_recommendations');
    const offersCardsRef = db.collection('offers_cards');

    // 1) Load existing user recs
    const existingSnap = await userCardsRef.where('userRef', '==', userRef).get();
    const existingMap = {};
    existingSnap.docs.forEach(doc => { existingMap[doc.id] = doc.data() || {}; });
    const existingIds = new Set(Object.keys(existingMap));

    // 2) Load offers for this reward preference
    const offersSnap = await offersCardsRef.where('rewardType', '==', rewardPref).get();

    // Build offer maps keyed by stable doc id (slug from rewardType + lender + commercialName)
    const offersById = {};
    const desiredById = {};
    const desiredHashById = {};
    offersSnap.docs.forEach(doc => {
      const o = doc.data() || {};
      const id = makeDocId(o.rewardType, o.lender, o.commercialName);
      offersById[id] = o;

      const desired = {
        // core identity/labels
        rewardType: o.rewardType ?? null,
        lender: o.lender ?? null,
        commercialName: o.commercialName ?? null,

        // links & marketing
        offerLink: o.offerLink ?? null,

        // economics
        annualFee: o.annualFee ?? null,
        introAprPurchase: o.introAprPurchase ?? null,
        introAprBT: o.introAprBT ?? null,
        aprPurchase: o.aprPurchase ?? null,
        aprBT: o.aprBT ?? null,
        signupBonus: o.signupBonus ?? null,
        rewardsRate: o.rewardsRate ?? null,

        // other useful fields
        categories: Array.isArray(o.categories) ? o.categories : (o.categories ?? null),
        network: o.network ?? null,
        minCredit: o.minCredit ?? null,
      };

      desiredById[id] = desired;
      desiredHashById[id] = hashPayload(desired);
    });
    const offerIds = new Set(Object.keys(offersById));

    // 3) Figure out creates / deletes / keeps
    const toCreate = [...offerIds].filter(id => !existingIds.has(id));
    const toDelete = [...existingIds].filter(id => {
      const stillOffered = offerIds.has(id);
      const wasAccepted  = existingMap[id]?.isAccepted === true;
      return !stillOffered && !wasAccepted; // keep accepted ones even if removed upstream
    });
    const toKeep = [...existingIds].filter(id => offerIds.has(id));

    // 4) For keeps, detect if the *offer payload* changed (via sourceHash)
    const toUpdate = [];
    for (const id of toKeep) {
      const existing = existingMap[id] || {};
      const prevHash = existing.sourceHash || null;
      const nextHash = desiredHashById[id];
      const needsOfferUpdate = prevHash !== nextHash;

      // Also backfill userRef if it's missing
      const missingUserRef = !existing.userRef;

      if (needsOfferUpdate || missingUserRef) {
        toUpdate.push({ id, needsOfferUpdate, missingUserRef, nextHash });
      }
    }

    // 5) Batch writes: delete stale, create new, update changed
    const batch = db.batch();

    // Deletes (only if not accepted)
    toDelete.forEach(id => batch.delete(userCardsRef.doc(id)));

    // Creates
    toCreate.forEach(id => {
      const desired = desiredById[id];
      const sourceHash = desiredHashById[id];
      batch.set(
        userCardsRef.doc(id),
        {
          ...desired,
          sourceHash,          // track what we wrote
          userRef,
          isAccepted: false,
          isDenied:   false,
          createdAt:  serverTs,
          updatedAt:  serverTs,
        },
        { merge: true }
      );
    });

    // Updates (only changed fields + metadata; never overwrite isAccepted/isDenied)
    toUpdate.forEach(({ id, needsOfferUpdate, missingUserRef, nextHash }) => {
      const patch = { updatedAt: serverTs };
      if (missingUserRef) patch.userRef = userRef;
      if (needsOfferUpdate) {
        Object.assign(patch, {
          ...desiredById[id],
          sourceHash: nextHash,
        });
      }
      batch.set(userCardsRef.doc(id), patch, { merge: true });
    });

    await batch.commit();

    return {
      success: true,
      created: toCreate.length,
      deleted: toDelete.length,
      kept: toKeep.length,
      updated: toUpdate.length
    };
  });
