const functions = require('firebase-functions/v1');
const admin = require('firebase-admin');
// To avoid deployment errors, do not call admin.initializeApp() in your code

async function loadPdfJs() {
  try {
    const m = await import('pdfjs-dist/legacy/build/pdf.mjs');
    console.log('[validateReportFormat] pdfjs loaded: legacy/build/pdf.mjs');
    return m;
  } catch (_) {
    const m = await import('pdfjs-dist/build/pdf.mjs');
    console.log('[validateReportFormat] pdfjs loaded: build/pdf.mjs');
    return m;
  }
}

// Node 18+ has global fetch. If you're on older runtimes, add node-fetch or axios.
const HAS_FETCH = typeof fetch === 'function';

exports.validateReportFormat = functions
  .region('us-central1')
  .https.onCall(async (data, context) => {
    // --- Auth: keep consistent with your current style (no hard throw) ---
    // If you DO want to enforce auth later, switch this to throw HttpsError.
    if (!context.auth || !context.auth.uid) {
      console.log('[validateReportFormat] unauthenticated call -> return false');
      return false;
    }

    const downloadURL = (data && data.downloadURL) ? String(data.downloadURL) : '';
    if (!downloadURL || !/^https?:\/\//i.test(downloadURL)) {
      console.log('[validateReportFormat] invalid/missing downloadURL -> return false');
      return false;
    }

    // --- Helper: normalize extracted text for robust anchor detection ---
    const norm = (s) =>
      String(s || '')
        .replace(/\u00ad/g, '')        // soft hyphen
        .replace(/\s+/g, ' ')          // collapse whitespace
        .trim()
        .toUpperCase();

    // --- Download PDF bytes (cap size + timeout-ish via AbortController) ---
    let pdfBytes;
    try {
      if (!HAS_FETCH) {
        console.log('[validateReportFormat] fetch() not available in this runtime -> return false');
        return false;
      }

      const controller = new AbortController();
      const timeoutMs = 12000;
      const t = setTimeout(() => controller.abort(), timeoutMs);

      const res = await fetch(downloadURL, { method: 'GET', signal: controller.signal });
      clearTimeout(t);

      if (!res.ok) {
        console.log('[validateReportFormat] download failed', { status: res.status });
        return false;
      }

      // Optional size guard. If content-length missing, we still proceed.
      const contentLength = res.headers.get('content-length');
      const maxBytes = 12 * 1024 * 1024; // 12 MB safety cap (tune as needed)
      if (contentLength && Number(contentLength) > maxBytes) {
        console.log('[validateReportFormat] pdf too large', { contentLength });
        return false;
      }

      const ab = await res.arrayBuffer();
      if (ab.byteLength > maxBytes) {
        console.log('[validateReportFormat] pdf too large (post-download)', { size: ab.byteLength });
        return false;
      }

      pdfBytes = new Uint8Array(ab);
    } catch (e) {
      console.log('[validateReportFormat] download error', {
        message: e && e.message ? e.message : String(e),
      });
      return false;
    }

    // --- Extract text from page 1 only (Tier A + Tier B are in TOC) ---
    let page1Text = '';
    try {
      const pdfjsLib = await loadPdfJs();
      const loadingTask = pdfjsLib.getDocument({ data: pdfBytes });
      const pdf = await loadingTask.promise;

      if (!pdf || !pdf.numPages || pdf.numPages < 1) {
        console.log('[validateReportFormat] pdf has no pages -> return false');
        return false;
      }

      const page = await pdf.getPage(1);
      const content = await page.getTextContent();

      // Join items in reading order as best-effort.
      // (We only need anchors; we don't need coordinate reconstruction here.)
      const raw = (content.items || [])
        .map((it) => (it && it.str ? it.str : ''))
        .join(' ');

      page1Text = norm(raw);

      // Clean up worker to avoid warnings / leaks
      try {
        await pdf.destroy();
      } catch (_) {}
    } catch (e) {
      console.log('[validateReportFormat] pdfjs parse error', {
        message: e && e.message ? e.message : String(e),
      });
      return false;
    }

    // --- Tier A: ALL must be present ---
    // User-chosen anchors:
    // 1) PREPARED FOR
    // 2) Personal & Confidential
    // 3) TABLE OF CONTENTS
    // 4) FICO + SCORE (token pair)
    const tierA = {
      preparedFor: page1Text.includes('PREPARED FOR'),
      personalConfidential: page1Text.includes('PERSONAL & CONFIDENTIAL'),
      tableOfContents: page1Text.includes('TABLE OF CONTENTS'),
      ficoAndScore: page1Text.includes('FICO') && page1Text.includes('SCORE'),
    };

    // --- Tier B: >= 4 of 5 must be present ---
    const tierBTargets = [
      'ACCOUNTS',
      'COLLECTIONS',
      'PUBLIC RECORDS',
      'INQUIRIES',
      'PERSONAL INFO',
    ];

    const tierBHits = {};
    let tierBCount = 0;
    for (const t of tierBTargets) {
      const hit = page1Text.includes(t);
      tierBHits[t] = hit;
      if (hit) tierBCount += 1;
    }

    // --- Decision ---
    const tierAOk = Object.values(tierA).every(Boolean);
    const tierBOk = tierBCount >= 4;

    // --- Debug logging (requested) ---
    console.log('[validateReportFormat] Tier A checks:', tierA);
    console.log('[validateReportFormat] Tier B hits:', tierBHits, 'count=', tierBCount);
    console.log('[validateReportFormat] Result:', { tierAOk, tierBOk, accepted: tierAOk && tierBOk });

    return !!(tierAOk && tierBOk);
  });
