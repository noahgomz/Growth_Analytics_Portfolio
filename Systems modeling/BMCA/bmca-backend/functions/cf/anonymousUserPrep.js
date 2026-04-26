const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// To avoid deployment errors, do not call admin.initializeApp() in your code

// ---- Load canonical Firefoo extracts (place these files in functions/demo_seed/)
const seedCards = require('../demo_seed/user_credit_cards.json');
const seedCardLates = require('../demo_seed/user_credit_cards_late_payments.json');
const seedLoans = require('../demo_seed/user_loans.json');
const seedLoanLates = require('../demo_seed/user_loans_late_payments.json');
const seedCollections = require('../demo_seed/user_collections_3rd_party.json');
const seedHardPulls = require('../demo_seed/user_hard_pulls.json');
const seedOpenClose = require('../demo_seed/user_openAndCloseAdjustments.json');
const seedSimViz = require('../demo_seed/user_sim_viz_output.json');

// ---- NEW: Cycle-0 action lists (place these files in functions/demo_seed/)
const seedPayPriorityList = require('../demo_seed/user_pay_priority_list.json');
const seedOpenActionsList = require('../demo_seed/user_open_actions_list.json');
const seedCloseActionsList = require('../demo_seed/user_close_actions_list.json');
const seedUseCardsList = require('../demo_seed/user_use_cards_list.json');

// -------------------- Firefoo transform helpers --------------------
function toTimestamp(iso) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return admin.firestore.Timestamp.fromDate(d);
}

function isFirestoreDocRef(x) {
  return !!(
    x &&
    typeof x === 'object' &&
    x.constructor &&
    x.constructor.name === 'DocumentReference' &&
    typeof x.path === 'string'
  );
}

function isFirestoreTimestamp(x) {
  return !!(
    x &&
    typeof x === 'object' &&
    x.constructor &&
    x.constructor.name === 'Timestamp' &&
    typeof x.toDate === 'function'
  );
}

/**
 * Converts Firefoo atoms:
 *  - {__time__: "..."} -> Timestamp
 *  - {__ref__: "path"} -> DocumentReference
 * Strips __collections__.
 */
function deepTransform(obj) {
  // Never traverse Firestore SDK objects
  if (isFirestoreDocRef(obj) || isFirestoreTimestamp(obj)) return obj;

  if (Array.isArray(obj)) return obj.map(deepTransform);

  if (obj && typeof obj === 'object') {
    if (obj.__time__) return toTimestamp(obj.__time__);
    if (obj.__ref__) return admin.firestore().doc(obj.__ref__);

    if (Object.prototype.hasOwnProperty.call(obj, '__collections__')) {
      const clone = { ...obj };
      delete clone.__collections__;
      return deepTransform(clone);
    }

    const out = {};
    for (const [k, v] of Object.entries(obj)) out[k] = deepTransform(v);
    return out;
  }

  return obj;
}

function getSeedUidFromExtract(extract) {
  const first = extract?.data ? Object.values(extract.data)[0] : null;
  const ref = first?.userRef?.__ref__ || first?.userRef?.path || '';
  const m = typeof ref === 'string' ? ref.match(/^users\/([^/]+)$/) : null;
  return m ? m[1] : null;
}

// -------------------- Firestore ops helpers --------------------
async function deleteByUserRef(db, collectionName, userRef) {
  const col = db.collection(collectionName);
  while (true) {
    const snap = await col.where('userRef', '==', userRef).limit(400).get();
    if (snap.empty) break;
    const batch = db.batch();
    snap.docs.forEach(d => batch.delete(d.ref));
    await batch.commit();
  }
}

function makeGuestDocId(guestUid, seedDocId) {
  // Deterministic and unique per user; keeps your seed ids recognizable.
  return `${guestUid}__${seedDocId}`;
}

// ---- NEW: remap doc IDs from seedUid -> guestUid for action-list doc IDs
function remapSeedDocId(seedDocId, seedUid, guestUid) {
  const s = String(seedDocId || '');

  // If docId looks like "<uid>__<rest>", replace the uid with guestUid.
  if (s.includes('__')) {
    return `${guestUid}__${s.split('__').slice(1).join('__')}`;
  }

  // Fallback to previous behavior
  if (!seedUid) return makeGuestDocId(guestUid, s);
  return s.split(seedUid).join(guestUid);
}


/**
 * Walk object and rewrite DocumentReferences to new paths based on mapping.
 * (e.g. user_credit_cards/<seedId> -> user_credit_cards/<guestUid__seedId>)
 */
function remapDocRefs(obj, db, maps) {
  // maps: { cardIdMap, loanIdMap, guestUid, seedUid }
  if (isFirestoreDocRef(obj)) {
    const p = obj.path;

    // ---- NEW: generic "<someUid>__<rest>" docId rewrite for all demo-seeded collections
const seededCollections = new Set([
  'user_credit_cards',
  'user_loans',
  'user_credit_cards_late_payments',
  'user_loans_late_payments',
  'user_collections_3rd_party',
  'user_hard_pulls',
  'user_openAndCloseAdjustments',
  'user_sim_viz_output',
  'user_pay_priority_list',
  'user_open_actions_list',
  'user_close_actions_list',
  'user_use_cards_list',
]);

const parts = p.split('/');
if (parts.length === 2) {
  const [coll, docId] = parts;
  if (seededCollections.has(coll) && typeof docId === 'string' && docId.includes('__')) {
    const rest = docId.split('__').slice(1).join('__');
    return db.doc(`${coll}/${maps.guestUid}__${rest}`);
  }
}


    // Remap card refs
    if (p.startsWith('user_credit_cards/')) {
      let seedId = p.split('/')[1];

      // ---- NEW: if ref id is "seedUid__<id>", strip the seedUid prefix
      if (typeof seedId === 'string' && seedId.includes('__')) {
        // strip "<someUid>__" so we can map by the underlying seed id
        seedId = seedId.split('__').slice(1).join('__');
      }


      const newId = maps.cardIdMap.get(seedId);
      if (newId) return db.doc(`user_credit_cards/${newId}`);
      return obj;
    }

    // Remap loan refs
    if (p.startsWith('user_loans/')) {
      let seedId = p.split('/')[1];

      // ---- NEW: if ref id is "seedUid__<id>", strip the seedUid prefix
      if (maps.seedUid && typeof seedId === 'string' && seedId.startsWith(`${maps.seedUid}__`)) {
        seedId = seedId.slice(`${maps.seedUid}__`.length);
      }

      const newId = maps.loanIdMap.get(seedId);
      if (newId) return db.doc(`user_loans/${newId}`);
      return obj;
    }

    // Remap simRunRef from seed user -> guest user (if present)
    if (maps.seedUid && p.startsWith(`users/${maps.seedUid}/SimulationRuns/`)) {
      const parts = p.split('/');
      const simRunId = parts[parts.length - 1];
      return db.doc(`users/${maps.guestUid}/SimulationRuns/${simRunId}`);
    }

    return obj;
  }

  if (isFirestoreTimestamp(obj)) return obj;

  if (Array.isArray(obj)) return obj.map(v => remapDocRefs(v, db, maps));

  if (obj && typeof obj === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(obj)) out[k] = remapDocRefs(v, db, maps);
    return out;
  }

  return obj;
}

async function batchSetDocs(db, writes) {
  // writes: Array<{ ref: DocumentReference, data: object }>
  let batch = db.batch();
  let ops = 0;

  for (const w of writes) {
    batch.set(w.ref, w.data, { merge: false });
    ops++;
    if (ops >= 450) {
      await batch.commit();
      batch = db.batch();
      ops = 0;
    }
  }

  if (ops > 0) await batch.commit();
}

// -------------------- Main CF --------------------
exports.anonymousUserPrep = functions.region('us-central1').https.onCall(async (data, context) => {
  if (!context?.auth?.uid) {
    throw new functions.https.HttpsError('unauthenticated', 'Must be authenticated.');
  }

  const userId = String(data?.userId || '');
  if (!userId || userId !== context.auth.uid) {
    throw new functions.https.HttpsError('permission-denied', 'userId must match caller.');
  }

  const db = admin.firestore();
  const guestUserRef = db.doc(`users/${userId}`);

  // Identify seed uid for simRunRef remap
  const seedUid =
    getSeedUidFromExtract(seedCards) ||
    getSeedUidFromExtract(seedLoans) ||
    getSeedUidFromExtract(seedOpenClose) ||
    getSeedUidFromExtract(seedSimViz) ||
    getSeedUidFromExtract(seedPayPriorityList) ||
    getSeedUidFromExtract(seedOpenActionsList) ||
    getSeedUidFromExtract(seedCloseActionsList) ||
    getSeedUidFromExtract(seedUseCardsList);

  // 1) Clear any prior guest seed docs (ONLY docs where userRef == this guest)
  const collectionsToClear = [
    'user_credit_cards',
    'user_credit_cards_late_payments',
    'user_loans',
    'user_loans_late_payments',
    'user_collections_3rd_party',
    'user_hard_pulls',
    'user_openAndCloseAdjustments',
    'user_sim_viz_output',

    // ---- NEW: cycle-0 action lists
    'user_pay_priority_list',
    'user_open_actions_list',
    'user_close_actions_list',
    'user_use_cards_list',
  ];
  for (const c of collectionsToClear) {
    await deleteByUserRef(db, c, guestUserRef);
  }

  console.log('[anonymousUserPrep] seed cards:', Object.keys(seedCards.data || {}).length);
  console.log('[anonymousUserPrep] seed loans:', Object.keys(seedLoans.data || {}).length);

  // 2) Build ID maps so refs can be rewritten
  const cardIdMap = new Map();
  for (const seedId of Object.keys(seedCards.data || {})) {
    cardIdMap.set(seedId, makeGuestDocId(userId, seedId));
  }

  const loanIdMap = new Map();
  for (const seedId of Object.keys(seedLoans.data || {})) {
    loanIdMap.set(seedId, makeGuestDocId(userId, seedId));
  }

  const maps = { cardIdMap, loanIdMap, guestUid: userId, seedUid };

  // 3) Write cards (NEW IDs, but deterministic)
  {
    const writes = [];
    for (const [seedId, raw] of Object.entries(seedCards.data || {})) {
      const newId = cardIdMap.get(seedId);
      const dataOut = deepTransform(raw);
      dataOut.userRef = guestUserRef;
      const finalOut = remapDocRefs(dataOut, db, maps);

      writes.push({
        ref: db.collection('user_credit_cards').doc(newId),
        data: finalOut,
      });
    }
    await batchSetDocs(db, writes);
  }

  // 4) Write loans (NEW IDs)
  {
    const writes = [];
    for (const [seedId, raw] of Object.entries(seedLoans.data || {})) {
      const newId = loanIdMap.get(seedId);
      const dataOut = deepTransform(raw);
      dataOut.userRef = guestUserRef;
      const finalOut = remapDocRefs(dataOut, db, maps);

      writes.push({
        ref: db.collection('user_loans').doc(newId),
        data: finalOut,
      });
    }
    await batchSetDocs(db, writes);
  }

  // 5) Write card lates (NEW IDs, and rewrite cardRef -> NEW parent id)
  {
    const writes = [];
    for (const [seedId, raw] of Object.entries(seedCardLates.data || {})) {
      const newId = makeGuestDocId(userId, seedId);
      const dataOut = deepTransform(raw);
      dataOut.userRef = guestUserRef;

      const finalOut = remapDocRefs(dataOut, db, maps);

      writes.push({
        ref: db.collection('user_credit_cards_late_payments').doc(newId),
        data: finalOut,
      });
    }
    await batchSetDocs(db, writes);
  }

  // 6) Write loan lates (rewrite loanRef -> NEW parent id)
  {
    const writes = [];
    for (const [seedId, raw] of Object.entries(seedLoanLates.data || {})) {
      const newId = makeGuestDocId(userId, seedId);
      const dataOut = deepTransform(raw);
      dataOut.userRef = guestUserRef;

      const finalOut = remapDocRefs(dataOut, db, maps);

      writes.push({
        ref: db.collection('user_loans_late_payments').doc(newId),
        data: finalOut,
      });
    }
    await batchSetDocs(db, writes);
  }

  // 7) Write collections + pulls (NEW IDs but no parent refs needed)
  {
    const writes = [];
    for (const [seedId, raw] of Object.entries(seedCollections.data || {})) {
      const newId = makeGuestDocId(userId, seedId);
      const dataOut = deepTransform(raw);
      dataOut.userRef = guestUserRef;
      const finalOut = remapDocRefs(dataOut, db, maps);

      writes.push({
        ref: db.collection('user_collections_3rd_party').doc(newId),
        data: finalOut,
      });
    }
    await batchSetDocs(db, writes);
  }

  {
    const writes = [];
    for (const [seedId, raw] of Object.entries(seedHardPulls.data || {})) {
      const newId = makeGuestDocId(userId, seedId);
      const dataOut = deepTransform(raw);
      dataOut.userRef = guestUserRef;
      const finalOut = remapDocRefs(dataOut, db, maps);

      writes.push({
        ref: db.collection('user_hard_pulls').doc(newId),
        data: finalOut,
      });
    }
    await batchSetDocs(db, writes);
  }

  // 8) Write open/close adjustments
  {
    const writes = [];
    for (const [seedId, raw] of Object.entries(seedOpenClose.data || {})) {
      const newId = makeGuestDocId(userId, seedId);
      const dataOut = deepTransform(raw);
      dataOut.userRef = guestUserRef;
      const finalOut = remapDocRefs(dataOut, db, maps);

      writes.push({
        ref: db.collection('user_openAndCloseAdjustments').doc(newId),
        data: finalOut,
      });
    }
    await batchSetDocs(db, writes);
  }

  // 9) Write sim viz output
  {
    const writes = [];
    for (const [seedId, raw] of Object.entries(seedSimViz.data || {})) {
      const newId = makeGuestDocId(userId, seedId);
      const dataOut = deepTransform(raw);
      dataOut.userRef = guestUserRef;

      // Ensure simRunRef points to guest run if simRunId present
      if (dataOut.simRunId) {
        dataOut.simRunRef = db.doc(`users/${userId}/SimulationRuns/${dataOut.simRunId}`);
      }

      const finalOut = remapDocRefs(dataOut, db, maps);

      writes.push({
        ref: db.collection('user_sim_viz_output').doc(newId),
        data: finalOut,
      });
    }
    await batchSetDocs(db, writes);
  }

  // 10) Create placeholder SimulationRuns doc so simRunRef resolves (if UI dereferences)
  {
    const simRunIds = Object.values(seedSimViz?.data || {})
      .map(d => d?.simRunId)
      .filter(Boolean);

    if (simRunIds.length > 0) {
      const simRunId = simRunIds[0];
      await db.doc(`users/${userId}/SimulationRuns/${simRunId}`).set(
        {
          userRef: guestUserRef,
          isAnonymousGuestSeed: true,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );
    }
  }

  // 11) Write plan_meta doc so UI bottom sheets don’t gray out
  {
    const planMetaDocId = 'CycleID-0';
    await db.doc(`users/${userId}/plan_meta/${planMetaDocId}`).set(
      {
        actionGroupsIncluded: [],
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        cycleID: '0',
        monthKey: 'guest',
      },
      { merge: true }
    );
  }

  // -------------------- NEW: Write cycle-0 action lists --------------------

  // A) PayPriorityList
  {
    const writes = [];
    for (const [seedDocId, raw] of Object.entries(seedPayPriorityList.data || {})) {
      const newDocId = remapSeedDocId(seedDocId, seedUid, userId);
      const dataOut = deepTransform(raw);
      dataOut.userRef = guestUserRef;

      const finalOut = remapDocRefs(dataOut, db, maps);

      writes.push({
        ref: db.collection('user_pay_priority_list').doc(newDocId),
        data: finalOut,
      });
    }
    await batchSetDocs(db, writes);
  }

  // B) OpenActionsList
  {
    const writes = [];
    for (const [seedDocId, raw] of Object.entries(seedOpenActionsList.data || {})) {
      const newDocId = remapSeedDocId(seedDocId, seedUid, userId);
      const dataOut = deepTransform(raw);
      dataOut.userRef = guestUserRef;

      // Minimal: keep field name exactly; just set it to guest uid if it exists
      if (typeof dataOut.user_uid === 'string') dataOut.user_uid = userId;

      const finalOut = remapDocRefs(dataOut, db, maps);

      writes.push({
        ref: db.collection('user_open_actions_list').doc(newDocId),
        data: finalOut,
      });
    }
    await batchSetDocs(db, writes);
  }

  // C) CloseActionsList
  {
    const writes = [];
    for (const [seedDocId, raw] of Object.entries(seedCloseActionsList.data || {})) {
      const newDocId = remapSeedDocId(seedDocId, seedUid, userId);
      const dataOut = deepTransform(raw);
      dataOut.userRef = guestUserRef;

      const finalOut = remapDocRefs(dataOut, db, maps);

      writes.push({
        ref: db.collection('user_close_actions_list').doc(newDocId),
        data: finalOut,
      });
    }
    await batchSetDocs(db, writes);
  }

  // D) UseCardsList
  {
    const writes = [];
    for (const [seedDocId, raw] of Object.entries(seedUseCardsList.data || {})) {
      const newDocId = remapSeedDocId(seedDocId, seedUid, userId);
      const dataOut = deepTransform(raw);
      dataOut.userRef = guestUserRef;

      const finalOut = remapDocRefs(dataOut, db, maps);

      writes.push({
        ref: db.collection('user_use_cards_list').doc(newDocId),
        data: finalOut,
      });
    }
    await batchSetDocs(db, writes);
  }

  // 12) Mark user as guest
  await guestUserRef.set(
    {
      isAnonymousGuest: true,
      guestSeededAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  return {
    ok: true,
    notes: {
      docIdStrategy: 'guestUid__seedDocId',
      seedUidUsedForSimRunRefRemap: seedUid || null,
    },
  };
});
