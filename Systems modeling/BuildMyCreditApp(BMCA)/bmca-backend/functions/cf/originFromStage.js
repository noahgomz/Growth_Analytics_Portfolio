const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// To avoid deployment errors, do not call admin.initializeApp() in your code

exports.originFromStage = functions.region('us-central1').https.onCall(
  async (data, context) => {
    const db = admin.firestore();

    try {
      // -----------------------------
      // Unauth caller fingerprint logs (3 logs)
      // -----------------------------
      if (!context.auth || !context.auth.uid) {
        const req = context.rawRequest;
        const headers = (req && req.headers) ? req.headers : {};

        // (1) marker
        console.error('[originFromStage] UNAUTH CALL', { ts: new Date().toISOString() });

        // (2) header fingerprint (helps pinpoint which UI path / environment is calling)
        console.error('[originFromStage] UNAUTH HEADERS', {
          origin: headers.origin,
          referer: headers.referer,
          userAgent: headers['user-agent'],
          xForwardedFor: headers['x-forwarded-for'],
          appEngineUserIp: headers['x-appengine-user-ip'],
        });

        // (3) payload fingerprint (often reveals which FF action chain fired)
        console.error('[originFromStage] UNAUTH PAYLOAD', {
          keys: data ? Object.keys(data) : [],
          callSite: data?.callSite,
          uploadID: data?.uploadID,
          uploadId: data?.uploadId,
        });

        throw new functions.https.HttpsError('unauthenticated', 'Not signed in.');
      }

      const uid = context.auth.uid;

      // -----------------------------
      // Input (uploadID is OPTIONAL)
      // -----------------------------
      const rawUploadID =
        (data && typeof data.uploadID === 'string' && data.uploadID) ||
        (data && typeof data.uploadId === 'string' && data.uploadId) ||
        '';

      const uploadID = String(rawUploadID).trim(); // may be empty
      const mode = uploadID ? 'report_upload_session' : 'origin_snapshot_session';

      // -----------------------------
      // Helpers
      // -----------------------------
      const isNonEmptyString = (v) => typeof v === 'string' && v.trim().length > 0;
      const isUsablePath = (p) =>
        isNonEmptyString(p) && p !== 'Irrelevant' && p.split('/').length % 2 === 0;

      const safeDoc = (path) => {
        if (!isUsablePath(path)) return null;
        return db.doc(path);
      };

      const mapStageToOriginPayload = (stock, stage) => {
        // Only include keys present on staging (no null/undefined writes)
        const out = {};

        const put = (originField, stageField) => {
          if (Object.prototype.hasOwnProperty.call(stage, stageField)) {
            const v = stage[stageField];
            if (v !== undefined && v !== null) out[originField] = v;
          }
        };

        // shared-ish
        put('lender', 'lender');

        switch (stock) {
          case 'user_credit_cards': {
            put('commercialName', 'name');
            put('isCFA', 'isCFA');
            put('isAnnualFee', 'isAnnualFee');
            put('isCurrent', 'isCurrent');
            put('dateIssued', 'DOFRecord');
            put('accountNumber', 'accountNumber');

            put('creditLimit', 'creditLimit');
            put('totalBalance', 'amountsOwed');
            put('isOpen', 'isOpen');
            put('apr', 'apr');
            break;
          }

          case 'user_loans': {
            put('commercialName', 'name');
            put('isCFA', 'isCFA');
            put('isCurrent', 'isCurrent');
            put('dateIssued', 'DOFRecord');
            put('accountNumber', 'accountNumber');

            put('principalOriginal', 'creditLimit');
            put('balance', 'amountsOwed');
            put('isOpen', 'isOpen');
            put('apr', 'apr');
            break;
          }

          case 'user_collections_3rd_party': {
            put('originalProvider', 'lender');
            put('collectionsAgency', 'collections_agency');
            put('name', 'name');
            put('DOFD', 'DOFRecord');
            put('amount', 'amountsOwed');
            put('isPaid', 'isPaid');
            put('severity', 'severity');
            break;
          }

          case 'hard_pull': {
            put('productName', 'name');
            put('dateOfRequest', 'DOFRecord');
            break;
          }

          case 'user_credit_cards_late_payments': {
            put('DOFD', 'DOFRecord');
            put('amount', 'amountsOwed');
            put('isPaid', 'isPaid');
            put('severity', 'severity');
            put('lateDisambiguousParentAccountString', 'lateDisambiguousParentAccountString');
            break;
          }

          case 'user_loans_late_payments': {
            put('DOFD', 'DOFRecord');
            put('amount', 'amountsOwed');
            put('isPaid', 'isPaid');
            put('severity', 'severity');
            put('lateDisambiguousParentAccountString', 'lateDisambiguousParentAccountString');
            break;
          }

          default:
            break;
        }

        return out;
      };

      // Mutable field allowlist for UPDATEs (identity fields never updated)
      const MUTABLE_FIELDS_BY_STOCK = {
        user_credit_cards: new Set(['isOpen', 'creditLimit', 'totalBalance', 'apr', 'isAnnualFee', 'isCurrent']),
        user_loans: new Set(['isOpen', 'principalOriginal', 'balance', 'apr', 'isCurrent']),
        user_collections_3rd_party: new Set(['amount', 'isPaid', 'severity']),
        hard_pull: new Set([]), // create-only by default
        user_credit_cards_late_payments: new Set(['amount', 'isPaid', 'severity']),
        user_loans_late_payments: new Set(['amount', 'isPaid', 'severity']),
      };

      // ✅ ALL origin collections are TOP-LEVEL
      const getOriginCollectionRefForCreate = (stock) => {
        switch (stock) {
          case 'user_credit_cards':
            return db.collection('user_credit_cards');
          case 'user_loans':
            return db.collection('user_loans');
          case 'user_credit_cards_late_payments':
            return db.collection('user_credit_cards_late_payments');
          case 'user_loans_late_payments':
            return db.collection('user_loans_late_payments');
          case 'hard_pull':
            return db.collection('user_hard_pulls');
          case 'user_collections_3rd_party':
            return db.collection('user_collections_3rd_party');
          default:
            return null;
        }
      };

      const readSelectedCandidatePath = async (stagingDocRef) => {
        const snap = await stagingDocRef
          .collection('stageMatchCandidates')
          .where('userSelectedForMatch', '==', true)
          .limit(2)
          .get();

        if (snap.empty) return '';
        const p = snap.docs[0].get('originDocRefPath');
        return isNonEmptyString(p) ? p.trim() : '';
      };

      const filterMutableChangedFields = (stock, originData, proposedPayload) => {
        const allowed = MUTABLE_FIELDS_BY_STOCK[stock] || new Set();
        const updates = {};
        const isTimestampLike = (x) => x && typeof x === 'object' && typeof x.toMillis === 'function';

        for (const [k, v] of Object.entries(proposedPayload)) {
          if (!allowed.has(k)) continue;

          const prev = originData ? originData[k] : undefined;
          const equal =
            (isTimestampLike(prev) && isTimestampLike(v) && prev.toMillis() === v.toMillis()) ||
            (!isTimestampLike(prev) && !isTimestampLike(v) && prev === v);

          if (!equal) updates[k] = v;
        }
        return updates;
      };

      // -----------------------------
      // Main: load staging docs
      // - If uploadID provided -> report upload session only
      // - Else -> origin_snapshot session only
      // Robust against userRef stored as DocumentReference OR string path
      // -----------------------------
      const userRefDoc = db.doc(`users/${uid}`);
      const userRefPath = userRefDoc.path;

      let baseQuery = db.collection('user_Staging_Accounts');

      if (uploadID) {
        baseQuery = baseQuery
          .where('uploadID', '==', uploadID)
          .where('stagingSource', 'in', ['report_upload', 'report_upload_user_manual_entry']);
      } else {
        baseQuery = baseQuery
          .where('stagingSource', '==', 'origin_snapshot');
      }

      const [snapRef, snapStr] = await Promise.all([
        baseQuery.where('userRef', '==', userRefDoc).get(),
        baseQuery.where('userRef', '==', userRefPath).get(),
      ]);

      const merged = new Map();
      for (const d of snapRef.docs) merged.set(d.id, d);
      for (const d of snapStr.docs) merged.set(d.id, d);
      const stagingDocs = Array.from(merged.values());

      console.log('[originFromStage] mode:', mode, 'stagingDocs:', stagingDocs.length, 'uploadID:', uploadID || '(none)');

      // If uploadID was provided but nothing found, hard-error (prevents dummy uploadID no-op)
      if (uploadID && stagingDocs.length === 0) {
        throw new functions.https.HttpsError(
          'failed-precondition',
          `No staging docs found for uploadID=${uploadID}.`
        );
      }

      const now = admin.firestore.FieldValue.serverTimestamp();
      let created = 0;
      let updated = 0;
      let skipped = 0;
      let unresolvedCount = 0;

      const isLateStock = (stock) =>
        stock === 'user_credit_cards_late_payments' || stock === 'user_loans_late_payments';

      // Two-pass commit so report-derived lates can resolve parent origin refs created in the same run.
      const nonLateDocs = stagingDocs.filter((d) => !isLateStock((d.data() || {}).stock));
      const lateDocs = stagingDocs.filter((d) => isLateStock((d.data() || {}).stock));

      // Map stagingDocId -> origin doc path (for parent resolution of report-derived lates)
      const stageIdToOriginPath = new Map();

      const cacheStageOriginPath = (stagingId, pathMaybe) => {
        if (!isNonEmptyString(stagingId)) return;
        if (!isNonEmptyString(pathMaybe)) return;
        stageIdToOriginPath.set(String(stagingId), String(pathMaybe).trim());
      };

      const getParentOriginPathFromStaging = async (parentStagingDocId) => {
        const pid = isNonEmptyString(parentStagingDocId) ? String(parentStagingDocId).trim() : '';
        if (!pid) return '';

        // Prefer same-run cache (fast, avoids extra read)
        const cached = stageIdToOriginPath.get(pid);
        if (isNonEmptyString(cached)) return String(cached).trim();

        // Fallback: read parent staging doc
        const parentSnap = await db.collection('user_Staging_Accounts').doc(pid).get();
        if (!parentSnap.exists) return '';
        const p = parentSnap.get('originDocRef');
        return isNonEmptyString(p) ? String(p).trim() : '';
      };

      const processNonLate = async (stagingDoc) => {
        const stagingRef = stagingDoc.ref;
        const s = stagingDoc.data() || {};

        // Only commit approved docs
        if (s.isConfirmed !== true) {
          skipped++;
          return;
        }

        const stock = s.stock;
        if (!isNonEmptyString(stock)) {
          skipped++;
          return;
        }

        const skipMatching = s.skipMatching === true;

        // Trust originDocRef if present
        let originDocRefPath = isNonEmptyString(s.originDocRef) ? String(s.originDocRef).trim() : '';
        let originDocRef = safeDoc(originDocRefPath);

        // Disambiguation resolution (primarily for report sessions)
        if (!originDocRef && !skipMatching && s.matchStatus === 'needs_disambiguation') {
          const selectedPath = await readSelectedCandidatePath(stagingRef);
          if (isNonEmptyString(selectedPath)) {
            originDocRefPath = selectedPath.trim();
            originDocRef = safeDoc(originDocRefPath);

            if (originDocRef) {
              await stagingRef.update({
                originDocRef: originDocRefPath,
                userSelectedOriginDocRef: originDocRefPath,
                matchStatus: 'matched',
              });
            }
          } else {
            unresolvedCount++;
            return;
          }
        }

        const proposed = mapStageToOriginPayload(stock, s);

        // Always attach userRef on creates (top-level filtering depends on it)
        if (!Object.prototype.hasOwnProperty.call(proposed, 'userRef')) {
          proposed.userRef = userRefDoc; // change to userRefPath if your origin stores strings
        }

        const shouldCreate = !originDocRef;

        if (shouldCreate) {
          const collRef = getOriginCollectionRefForCreate(stock);
          if (!collRef) {
            skipped++;
            return;
          }

          proposed.createdAt = now;
          proposed.updatedAt = now;

          const newDocRef = collRef.doc();
          await newDocRef.set(proposed, { merge: false });

          await stagingRef.update({ originDocRef: newDocRef.path });
          cacheStageOriginPath(stagingDoc.id, newDocRef.path);

          created++;
          return;
        }

        // Update existing
        const originSnap = await originDocRef.get();
        if (!originSnap.exists) {
          unresolvedCount++;
          return;
        }

        const originData = originSnap.data() || {};
        const updatesObj = filterMutableChangedFields(stock, originData, proposed);

        if (Object.keys(updatesObj).length > 0) {
          updatesObj.updatedAt = now;
          await originDocRef.update(updatesObj);
          updated++;
        } else {
          skipped++;
        }

        // Cache parent origin path for late resolution (accounts only really matter, but harmless)
        cacheStageOriginPath(stagingDoc.id, originDocRef.path);
      };

      const processLate = async (stagingDoc) => {
        const stagingRef = stagingDoc.ref;
        const s = stagingDoc.data() || {};

        if (s.isConfirmed !== true) {
          skipped++;
          return;
        }

        const stock = s.stock;
        if (!isNonEmptyString(stock) || !isLateStock(stock)) {
          skipped++;
          return;
        }

        const stagingSource = String(s.stagingSource || '').trim();

        // Resolve late origin doc ref + parent origin doc ref depending on stagingSource semantics:
        // - report_upload*: parent via parentStagingDocId; late doc may or may not already exist (originDocRef if matched)
        // - origin_snapshot: originDocRef is the late doc; lateOriginRef is the parent account doc
        let lateDocRefPath = '';
        let parentOriginPath = '';

        if (stagingSource === 'origin_snapshot') {
          lateDocRefPath = isNonEmptyString(s.originDocRef) ? String(s.originDocRef).trim() : '';
          parentOriginPath = isNonEmptyString(s.lateOriginRef) ? String(s.lateOriginRef).trim() : '';
        } else {
          // report_upload / report_upload_user_manual_entry
          lateDocRefPath = isNonEmptyString(s.originDocRef) ? String(s.originDocRef).trim() : '';
          parentOriginPath = await getParentOriginPathFromStaging(s.parentStagingDocId);
        }

        const lateDocRef = safeDoc(lateDocRefPath);
        const parentOriginRef = safeDoc(parentOriginPath);

        if (!parentOriginRef) {
          unresolvedCount++;
          return;
        }

        const proposed = mapStageToOriginPayload(stock, s);

        // Always attach userRef on creates
        if (!Object.prototype.hasOwnProperty.call(proposed, 'userRef')) {
          proposed.userRef = userRefDoc; // change to userRefPath if your origin stores strings
        }

        // Ensure parent linkage field in origin late doc
        if (stock === 'user_credit_cards_late_payments') proposed.cardRef = parentOriginRef;
        else proposed.loanRef = parentOriginRef;

        const shouldCreate = !lateDocRef;

        if (shouldCreate) {
          const collRef = getOriginCollectionRefForCreate(stock);
          if (!collRef) {
            skipped++;
            return;
          }

          proposed.createdAt = now;
          proposed.updatedAt = now;

          const newLateRef = collRef.doc();
          await newLateRef.set(proposed, { merge: false });

          // Persist CF2 semantics on late staging docs after create:
          // - originDocRef = late origin doc path
          // - lateOriginRef = parent account origin doc path
          await stagingRef.update({
            originDocRef: newLateRef.path,
            lateOriginRef: parentOriginRef.path,
          });

          created++;
          return;
        }

        // Update existing late doc
        const lateSnap = await lateDocRef.get();
        if (!lateSnap.exists) {
          unresolvedCount++;
          return;
        }

        const originData = lateSnap.data() || {};
        const updatesObj = filterMutableChangedFields(stock, originData, proposed);

        // Also correct parent linkage if needed (compare by path)
        const existingParent =
          stock === 'user_credit_cards_late_payments'
            ? originData.cardRef
            : originData.loanRef;

        const existingParentPath =
          existingParent && typeof existingParent === 'object' && existingParent.path
            ? existingParent.path
            : (isNonEmptyString(existingParent) ? String(existingParent).trim() : '');

        if (existingParentPath !== parentOriginRef.path) {
          if (stock === 'user_credit_cards_late_payments') updatesObj.cardRef = parentOriginRef;
          else updatesObj.loanRef = parentOriginRef;
        }

        if (Object.keys(updatesObj).length > 0) {
          updatesObj.updatedAt = now;
          await lateDocRef.update(updatesObj);
          updated++;
        } else {
          skipped++;
        }
      };

      // Pass A: non-late docs first
      for (const d of nonLateDocs) {
        await processNonLate(d);
      }

      // Pass B: lates (only parentStagingDocId resolution for report-derived lates; origin_snapshot already has refs)
      for (const d of lateDocs) {
        await processLate(d);
      }

      console.log('[originFromStage] results', { mode, created, updated, skipped, unresolvedCount });

      // Callable-safe return
      return { ok: true, mode, created, updated, skipped, unresolvedCount };
    } catch (e) {
      console.error('originFromStage error:', e);
      if (e instanceof functions.https.HttpsError) throw e;
      throw new functions.https.HttpsError('internal', e?.message || 'Unknown error');
    }
  }
);
