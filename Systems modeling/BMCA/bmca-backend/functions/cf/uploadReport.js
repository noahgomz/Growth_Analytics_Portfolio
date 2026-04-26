const functions = require("firebase-functions/v1");
const admin = require("firebase-admin");
// DO NOT call admin.initializeApp()

const crypto = require("crypto");

/* =========================
   Helpers
========================= */

function sha1(input) {
  return crypto.createHash("sha1").update(String(input)).digest("hex");
}

function extractBucketAndPath(downloadURL) {
  const u = new URL(downloadURL);
  const parts = u.pathname.split("/").filter(Boolean);
  const bIdx = parts.indexOf("b");
  const oIdx = parts.indexOf("o");
  return {
    bucket: parts[bIdx + 1],
    path: decodeURIComponent(parts.slice(oIdx + 1).join("/")),
  };
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const TZ = "America/New_York";

function norm(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function cleanDashes(s) {
  // Collapse common dash artifacts: "– –", "— —", "- -", etc.
  return norm(String(s || "").replace(/[\u2012\u2013\u2014\u2015-]\s*[\u2012\u2013\u2014\u2015-]+/g, " "));
}

function cleanValue(s) {
  s = cleanDashes(s);
  if (!s) return null;
  if (/^–+$/.test(s)) return null;
  if (/^-\s*-\s*$/.test(s)) return null;
  if (/^(none|n\/a)$/i.test(s)) return null;
  if (s === "–" || s === "-") return null;
  return s;
}

function isJunkLine(s) {
  s = cleanDashes(s);
  if (!s) return true;

  // timestamps like "1/15/26, 1:25 PM ..."
  if (/^\d{1,2}\/\d{1,2}\/\d{2},\s*\d{1,2}:\d{2}\s*(AM|PM)\b/i.test(s)) return true;

  if (s.includes("myFICO")) return true;
  if (/^https?:\/\//i.test(s)) return true;

  if (/Equifax\W*TransUnion\W*Experian/i.test(s)) return true;
  if (s === "Equifax" || s === "TransUnion" || s === "Experian") return true;

  return false;
}

function findLenderAbove(lines, idx) {
  for (let j = idx - 1; j >= 0; j--) {
    const s = cleanDashes(lines[j]);
    if (isJunkLine(s)) continue;
    if (s === "CLOSED") continue;
    return s;
  }
  return null;
}

function guessLenderFromBlock(blockLines) {
  for (let i = 0; i < blockLines.length; i++) {
    const ln = cleanDashes(blockLines[i]);
    if (/^Last Updated\b/i.test(ln)) {
      for (let j = i - 1; j >= 0; j--) {
        const s = cleanDashes(blockLines[j]);
        if (isJunkLine(s)) continue;
        if (s === "CLOSED") continue;
        return s;
      }
    }
  }
  return null;
}

function extractLabelValue(lines, label) {
  const lbl = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const reSpaced = new RegExp("^" + lbl + "\\s+(.*)$", "i");
  const reConcat = new RegExp("^" + lbl + "(.*)$", "i");

  for (const raw of lines) {
    const s = cleanDashes(raw);

    let m = s.match(reSpaced);
    if (m) return cleanValue(m[1]);

    m = s.match(reConcat);
    if (m) return cleanValue(m[1]);
  }
  return null;
}

function extractFollowingLineAfterLabel(lines, label) {
  const labelNorm = cleanDashes(label).toLowerCase();
  for (let i = 0; i < lines.length - 1; i++) {
    if (cleanDashes(lines[i]).toLowerCase() === labelNorm) {
      return cleanValue(lines[i + 1]);
    }
  }
  return null;
}

/* =========================
   Normalize money -> Number
========================= */

function parseMoneyToNumber(v) {
  v = cleanValue(v);
  if (!v) return null;

  let neg = false;
  if (/^\(.*\)$/.test(v)) {
    neg = true;
    v = v.slice(1, -1);
  }

  v = v.replace(/[$,\s]/g, "");
  if (!/^-?\d+(\.\d+)?$/.test(v)) return null;

  const num = Number(v);
  if (!Number.isFinite(num)) return null;
  return neg ? -num : num;
}

/* =========================
   Dates -> Firestore Timestamp
   (midnight America/New_York)
========================= */

function tzOffsetMillisAt(utcMillis, timeZone) {
  const d = new Date(utcMillis);
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  const parts = Object.fromEntries(
    fmt
      .formatToParts(d)
      .filter((p) => p.type !== "literal")
      .map((p) => [p.type, p.value])
  );

  const ly = Number(parts.year);
  const lm = Number(parts.month);
  const ld = Number(parts.day);
  const lh = Number(parts.hour);
  const lmin = Number(parts.minute);
  const ls = Number(parts.second);

  const localAsUtc = Date.UTC(ly, lm - 1, ld, lh, lmin, ls);
  return localAsUtc - utcMillis;
}

function zonedMidnightUtcMillis(y, m, d, timeZone) {
  const base = Date.UTC(y, m - 1, d, 0, 0, 0);
  let utc = base - tzOffsetMillisAt(base, timeZone);
  utc = base - tzOffsetMillisAt(utc, timeZone);
  return utc;
}

function parseDateToTimestamp(val) {
  val = cleanValue(val);
  if (!val) return null;

  // MM/YYYY -> day=1
  let m = val.match(/^(\d{1,2})\/(\d{4})$/);
  if (m) {
    const mm = Number(m[1]);
    const yy = Number(m[2]);
    if (mm < 1 || mm > 12) return null;

    const utcMillis = zonedMidnightUtcMillis(yy, mm, 1, TZ);
    return admin.firestore.Timestamp.fromMillis(utcMillis);
  }

  // MM/DD/YYYY
  m = val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) {
    const mm = Number(m[1]);
    const dd = Number(m[2]);
    const yy = Number(m[3]);
    if (mm < 1 || mm > 12) return null;
    if (dd < 1 || dd > 31) return null;

    const utcMillis = zonedMidnightUtcMillis(yy, mm, dd, TZ);
    return admin.firestore.Timestamp.fromMillis(utcMillis);
  }

  return null;
}

/* =========================
   pdfjs-dist extraction
========================= */

async function extractPdfPagesFromUint8(uint8) {
  // dynamic import keeps this CommonJS file deployable
  const pdfjs = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const { getDocument } = pdfjs;

  const loadingTask = getDocument({
    data: uint8,
    disableWorker: true,
  });

  const pdf = await loadingTask.promise;
  const pages = [];

  for (let p = 1; p <= pdf.numPages; p++) {
    const page = await pdf.getPage(p);
    const tc = await page.getTextContent();

    const items = (tc.items || [])
      .map((it) => {
        const t = it.transform || [];
        const x = t[4];
        const y = t[5];
        const str = norm(it.str);
        if (!str) return null;
        return { str, x, y };
      })
      .filter(Boolean);

    const byY = new Map();
    for (const it of items) {
      const yKey = Math.round(it.y * 2) / 2; // 0.5 precision
      if (!byY.has(yKey)) byY.set(yKey, []);
      byY.get(yKey).push(it);
    }

    const lines = Array.from(byY.entries())
      .sort((a, b) => b[0] - a[0]) // top -> bottom
      .map(([, row]) => {
        row.sort((a, b) => (a.x ?? 0) - (b.x ?? 0));
        return cleanDashes(row.map((r) => r.str).join(" "));
      })
      .filter(Boolean);

    pages.push({ pageNumber: p, lines });
  }

  return pages;
}

/* =========================
   Section helpers
========================= */

const SECTION_HEADERS = [
  "ACCOUNTS",
  "COLLECTIONS",
  "PUBLIC RECORDS",
  "INQUIRIES",
  "PERSONAL INFO",
  "NEXT STEPS",
];

function isSectionHeaderLine(s) {
  s = cleanDashes(s);
  if (!s) return false;
  return SECTION_HEADERS.includes(s);
}

function findSectionIdx(lines, header, startAt = 0) {
  const h = cleanDashes(header);
  for (let i = Math.max(0, startAt); i < lines.length; i++) {
    if (cleanDashes(lines[i]) === h) return i;
  }
  return -1;
}

function findNextSectionIdx(lines, startAt, headersInPriority) {
  for (const h of headersInPriority) {
    const idx = findSectionIdx(lines, h, startAt);
    if (idx !== -1) return idx;
  }
  return -1;
}

function sliceSection(lines, linePages, startIdx, endIdxExclusive) {
  const s = Math.max(0, startIdx);
  const e = Math.max(s, endIdxExclusive);
  return {
    lines: lines.slice(s, e).map(cleanDashes).filter(Boolean),
    pages: linePages.slice(s, e),
  };
}

/* =========================
   Account blocks
========================= */

function buildAccountBlocks(lines, linePages, accountsIdx, accountsEndIdx) {
  if (accountsIdx === -1) return [];
  if (accountsEndIdx === -1 || accountsEndIdx <= accountsIdx) return [];

  const slice = lines.slice(accountsIdx, accountsEndIdx);
  const slicePages = linePages.slice(accountsIdx, accountsEndIdx);

  const blocks = [];
  let cur = null;

  for (let idx = 0; idx < slice.length; idx++) {
    const ln = cleanDashes(slice[idx]);

    if (/^Last Updated\b/i.test(ln)) {
      if (cur) blocks.push(cur);

      const lender = findLenderAbove(slice, idx);
      const startPageNumber = slicePages[idx] ?? null;

      cur = { startPageNumber, lender, lines: [] };
      if (lender) cur.lines.push(lender);
    }

    if (cur) cur.lines.push(ln);
  }

  if (cur) blocks.push(cur);
  return blocks;
}

/* =========================
   Payment history
   output: flat months list
========================= */

function tokenizeCodes(s) {
  // supports OK, NR, NO, 30/60/90/120/150/180, CO, D
  const matches = String(s || "").match(/OK|NR|NO|CO|D|180|150|120|90|60|30/g);
  return matches || [];
}

function parsePaymentHistoryRows(blockLines) {
  const out = [];

  const startIdx = blockLines.findIndex((l) => /^2-YEAR PAYMENT HISTORY\b/i.test(cleanDashes(l)));
  if (startIdx === -1) return out;

  const endIdx = (() => {
    for (let i = startIdx + 1; i < blockLines.length; i++) {
      if (/^MORE DETAILS\b/i.test(cleanDashes(blockLines[i]))) return i;
    }
    return blockLines.length;
  })();

  const slice = blockLines.slice(startIdx + 1, endIdx).map(cleanDashes).filter(Boolean);

  for (let i = 0; i < slice.length - 1; i++) {
    const yearRow = slice[i];
    const bureauRow = slice[i + 1];

    const y = yearRow.match(/^(\d{4})\s+Jan\s+Feb\s+Mar\s+Apr\s+May\s+Jun\s+Jul\s+Aug\s+Sep\s+Oct\s+Nov\s+Dec\b/i);
    if (!y) continue;
    const year = parseInt(y[1], 10);

    const b = bureauRow.match(/^(Equifax|TransUnion|Experian)\s+(.+)$/i);
    if (!b) continue;

    const bureau = b[1];
    const rest = cleanDashes(b[2]);

    let tokens = rest.split(/\s+/).filter(Boolean);
    if (tokens.length < 12) {
      tokens = tokenizeCodes(rest);
    } else {
      const tok2 = tokenizeCodes(rest);
      if (tok2.length >= 12) tokens = tok2;
    }
    if (tokens.length < 12) continue;

    const months = {};
    for (let mm = 0; mm < 12; mm++) months[MONTHS[mm]] = tokens[mm] ?? null;

    out.push({ bureau, year, months });
    i += 1; // consume bureau line
  }

  return out;
}

function flattenPaymentHistory(rows) {
  const flat = [];
  for (const r of rows) {
    for (const m of MONTHS) {
      flat.push({
        bureau: r.bureau,
        year: r.year,
        month: m,
        code: r.months?.[m] ?? null,
      });
    }
  }
  return flat;
}

/* =========================
   Generic block splitter
========================= */

function splitBlocksByAnchors(sectionLines, sectionPages, anchorRegexes) {
  const blocks = [];
  let cur = null;

  for (let i = 0; i < sectionLines.length; i++) {
    const ln = cleanDashes(sectionLines[i]);
    if (!ln) continue;

    if (isSectionHeaderLine(ln)) continue;

    const isAnchor = anchorRegexes.some((re) => re.test(ln));

    if (!cur) {
      cur = { startPageNumber: sectionPages[i] ?? null, lines: [] };
    } else if (isAnchor && cur.lines.length) {
      blocks.push(cur);
      cur = { startPageNumber: sectionPages[i] ?? null, lines: [] };
    }

    cur.lines.push(ln);
  }

  if (cur && cur.lines.length) blocks.push(cur);
  return blocks;
}

/* =========================
   Collections / Public Records / Inquiries
========================= */

function parseCollectionsSection(sectionLines, sectionPages) {
  const cleaned = sectionLines.map(cleanDashes).filter(Boolean);
  const meaningful = cleaned.filter((l) => {
    if (isSectionHeaderLine(l)) return false;
    if (isJunkLine(l)) return false;
    if (/^Collection/i.test(l)) return false;
    return true;
  });
  if (!meaningful.length) return [];

  const anchors = [/^Date Reported\b/i, /^Reported\b/i, /^Date Opened\b/i, /^Opened\b/i, /^Status\b/i, /^Amount\b/i, /^Balance\b/i];
  const blocks = splitBlocksByAnchors(cleaned, sectionPages, anchors);

  return blocks.map((b, idx) => {
    const blockLines = b.lines;

    const lender = (() => {
      for (const l of blockLines) {
        if (isJunkLine(l)) continue;
        if (
          /^(Date Reported|Reported|Date Opened|Opened|Status|Amount|Balance|Original Creditor|Collection Agency|Creditor)\b/i.test(l)
        )
          continue;
        return l;
      }
      return null;
    })();

    const amountRaw = extractLabelValue(blockLines, "Amount") || extractLabelValue(blockLines, "Balance");
    const status = extractLabelValue(blockLines, "Status");
    const dateReportedRaw = extractLabelValue(blockLines, "Date Reported") || extractLabelValue(blockLines, "Reported");
    const dateOpenedRaw = extractLabelValue(blockLines, "Date Opened") || extractLabelValue(blockLines, "Opened");
    const originalCreditor = extractLabelValue(blockLines, "Original Creditor") || extractLabelValue(blockLines, "Creditor");
    const collectionAgency = extractLabelValue(blockLines, "Collection Agency");

    return {
      recordType: "collection",
      index: idx + 1,
      startPageNumber: b.startPageNumber ?? null,

      lender: cleanValue(lender),
      collectionAgency: cleanValue(collectionAgency),
      originalCreditor: cleanValue(originalCreditor),
      status: cleanValue(status),

      amount: parseMoneyToNumber(amountRaw),

      dateReported: parseDateToTimestamp(dateReportedRaw),
      dateOpened: parseDateToTimestamp(dateOpenedRaw),

      warnings: [
        ...(lender ? [] : ["missing_lender"]),
        ...(parseMoneyToNumber(amountRaw) != null ? [] : ["missing_amount"]),
      ],
    };
  });
}

function parsePublicRecordsSection(sectionLines, sectionPages) {
  const cleaned = sectionLines.map(cleanDashes).filter(Boolean);
  const meaningful = cleaned.filter((l) => {
    if (isSectionHeaderLine(l)) return false;
    if (isJunkLine(l)) return false;
    if (/^Public Records/i.test(l)) return false;
    return true;
  });
  if (!meaningful.length) return [];

  const anchors = [/^Type\b/i, /^Filing Date\b/i, /^Filed\b/i, /^Status\b/i, /^Court\b/i, /^Liability\b/i, /^Amount\b/i];
  const blocks = splitBlocksByAnchors(cleaned, sectionPages, anchors);

  return blocks.map((b, idx) => {
    const blockLines = b.lines;
    const type = extractLabelValue(blockLines, "Type");
    const status = extractLabelValue(blockLines, "Status");
    const filingDateRaw = extractLabelValue(blockLines, "Filing Date") || extractLabelValue(blockLines, "Filed");
    const court = extractLabelValue(blockLines, "Court");
    const amountRaw = extractLabelValue(blockLines, "Amount") || extractLabelValue(blockLines, "Liability");

    const typeFallback = cleanValue(type) || cleanValue(blockLines[0]);

    return {
      recordType: "public_record",
      index: idx + 1,
      startPageNumber: b.startPageNumber ?? null,

      type: typeFallback,
      status: cleanValue(status),
      filingDate: parseDateToTimestamp(filingDateRaw),
      court: cleanValue(court),
      amount: parseMoneyToNumber(amountRaw),

      warnings: [...(typeFallback ? [] : ["missing_type"])],
    };
  });
}

function parseInquiriesSection(sectionLines, sectionPages) {
  const cleaned = sectionLines.map(cleanDashes).filter(Boolean);
  const meaningful = cleaned.filter((l) => {
    if (isSectionHeaderLine(l)) return false;
    if (isJunkLine(l)) return false;
    if (/^Credit Inquiries\b/i.test(l)) return false;
    if (/Equifax\W*TransUnion\W*Experian/i.test(l)) return false;
    return true;
  });
  if (!meaningful.length) return [];

  const reMonthYear = new RegExp(
    "^(January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{4}$",
    "i"
  );
  const reDate = /^\d{1,2}\/\d{1,2}\/\d{4}$/;

  const out = [];
  let currentMonthYear = null;
  let lenderParts = [];
  let startPageNumber = null;

  for (let i = 0; i < cleaned.length; i++) {
    const ln = cleaned[i];

    if (isSectionHeaderLine(ln)) continue;
    if (isJunkLine(ln)) continue;
    if (/^Credit Inquiries\b/i.test(ln)) continue;
    if (/Equifax\W*TransUnion\W*Experian/i.test(ln)) continue;

    if (reMonthYear.test(ln)) {
      currentMonthYear = ln;
      lenderParts = [];
      startPageNumber = sectionPages[i] ?? null;
      continue;
    }

    if (reDate.test(ln)) {
      const lender = cleanValue(lenderParts.join(" "));
      const inquiryDate = parseDateToTimestamp(ln);
      if (lender || inquiryDate) {
        out.push({
          recordType: "inquiry",
          index: out.length + 1,
          startPageNumber,
          inquiryMonthYear: currentMonthYear,
          lender,
          inquiryDate,
          warnings: [
            ...(currentMonthYear ? [] : ["missing_inquiryMonthYear"]),
            ...(lender ? [] : ["missing_lender"]),
            ...(inquiryDate ? [] : ["missing_inquiryDate"]),
          ],
        });
      }
      lenderParts = [];
      continue;
    }

    lenderParts.push(ln);
  }

  return out;
}

/* =========================
   Firestore batch chunking
========================= */

async function commitBatchesInChunks(docWrites, maxOpsPerBatch = 450) {
  // docWrites: [{ ref, data }]
  let idx = 0;
  while (idx < docWrites.length) {
    const batch = admin.firestore().batch();
    const slice = docWrites.slice(idx, idx + maxOpsPerBatch);
    for (const w of slice) {
      batch.set(w.ref, w.data, { merge: true });
    }
    await batch.commit();
    idx += maxOpsPerBatch;
  }
}

/* =========================
   Cloud Function
========================= */

exports.uploadReport = functions.region("us-central1").https.onCall(async (data, context) => {
  try {
    if (!context.auth?.uid) {
      throw new functions.https.HttpsError("unauthenticated");
    }

    const { downloadURL } = data || {};
    const uid = context.auth.uid;

    const uploadId =
      data && typeof data.uploadId === "string" && data.uploadId.trim() ? data.uploadId.trim() : crypto.randomBytes(12).toString("hex");

    if (!downloadURL || typeof downloadURL !== "string" || !downloadURL.trim()) {
      throw new functions.https.HttpsError("invalid-argument", "downloadURL must be a non-empty string.");
    }

    const { bucket, path } = extractBucketAndPath(downloadURL);

    if (!bucket || !String(bucket).trim()) {
      throw new functions.https.HttpsError("invalid-argument", "downloadURL did not contain a valid bucket.");
    }
    if (!path || !String(path).trim()) {
      throw new functions.https.HttpsError("invalid-argument", "downloadURL did not contain a valid object path.");
    }

    const storage = admin.storage().bucket(bucket);
    const [buf] = await storage.file(path).download();

    // ---- PDF -> stable lines
    const uint8 = new Uint8Array(buf);
    const pages = await extractPdfPagesFromUint8(uint8);

    const lines = [];
    const linePages = [];
    for (const p of pages) {
      for (const l of p.lines) {
        lines.push(l);
        linePages.push(p.pageNumber);
      }
    }

    // ---- Boundaries (empty-safe)
    const accountsIdx = findSectionIdx(lines, "ACCOUNTS");
    const collectionsIdx = findSectionIdx(lines, "COLLECTIONS", Math.max(0, accountsIdx));
    const publicRecordsIdx = findSectionIdx(lines, "PUBLIC RECORDS", Math.max(0, collectionsIdx));
    const inquiriesIdx = findSectionIdx(lines, "INQUIRIES", Math.max(0, publicRecordsIdx));
    const personalInfoIdx = findSectionIdx(lines, "PERSONAL INFO", Math.max(0, inquiriesIdx));

    // Accounts end = first available of COLLECTIONS / PUBLIC RECORDS / INQUIRIES / PERSONAL INFO / EOF
    const accountsEndIdx =
      accountsIdx === -1
        ? -1
        : (() => {
            const next = findNextSectionIdx(lines, accountsIdx + 1, ["COLLECTIONS", "PUBLIC RECORDS", "INQUIRIES", "PERSONAL INFO"]);
            return next === -1 ? lines.length : next;
          })();

    // Collections end
    const collectionsEndIdx =
      collectionsIdx === -1
        ? -1
        : (() => {
            const next = findNextSectionIdx(lines, collectionsIdx + 1, ["PUBLIC RECORDS", "INQUIRIES", "PERSONAL INFO"]);
            return next === -1 ? lines.length : next;
          })();

    // Public records end
    const publicRecordsEndIdx =
      publicRecordsIdx === -1
        ? -1
        : (() => {
            const next = findNextSectionIdx(lines, publicRecordsIdx + 1, ["INQUIRIES", "PERSONAL INFO"]);
            return next === -1 ? lines.length : next;
          })();

    // Inquiries end
    const inquiriesEndIdx =
      inquiriesIdx === -1
        ? -1
        : (() => {
            const next = findNextSectionIdx(lines, inquiriesIdx + 1, ["PERSONAL INFO"]);
            return next === -1 ? lines.length : next;
          })();

    // ---- Accounts parse (empty-safe)
    const blocks = buildAccountBlocks(lines, linePages, accountsIdx, accountsEndIdx);
    const accounts = blocks.map((b, idx) => {
      const blockLines = b.lines.map(cleanDashes);
      const lender = b.lender || guessLenderFromBlock(blockLines);

      const lastUpdated = parseDateToTimestamp(extractLabelValue(blockLines, "Last Updated"));

      let paymentStatus = extractLabelValue(blockLines, "Payment Status");
      if (!paymentStatus) paymentStatus = extractFollowingLineAfterLabel(blockLines, "Payment Status");
      paymentStatus = cleanValue(paymentStatus);

      const worstDelinquency = cleanValue(extractLabelValue(blockLines, "Worst Delinquency"));

      const balance = parseMoneyToNumber(extractLabelValue(blockLines, "Balance"));
      const creditLimit = parseMoneyToNumber(extractLabelValue(blockLines, "Credit Limit"));

      const openDate = parseDateToTimestamp(extractLabelValue(blockLines, "Open Date"));
      const closedDate = parseDateToTimestamp(extractLabelValue(blockLines, "Closed Date"));
      const lastActivity = parseDateToTimestamp(extractLabelValue(blockLines, "Last Activity"));

      const termsRaw = extractLabelValue(blockLines, "Terms") || extractFollowingLineAfterLabel(blockLines, "Terms");
      const terms = cleanValue(termsRaw);

      const scheduledPaymentRaw =
        extractLabelValue(blockLines, "Scheduled Payment") || extractFollowingLineAfterLabel(blockLines, "Scheduled Payment");
      const scheduledPayment = parseMoneyToNumber(scheduledPaymentRaw);

      const highBalance = parseMoneyToNumber(extractLabelValue(blockLines, "High Balance"));

      const loanType = cleanValue(extractLabelValue(blockLines, "Loan Type"));
      const responsibility = cleanValue(extractLabelValue(blockLines, "Responsibility"));
      const companyName = cleanValue(extractLabelValue(blockLines, "Company Name"));
      const accountNumber = cleanValue(extractLabelValue(blockLines, "Account Number"));

      const paymentHistoryRows = parsePaymentHistoryRows(blockLines);
      const paymentHistory = flattenPaymentHistory(paymentHistoryRows);

      const warnings = [];
      if (!lender) warnings.push("missing_lender");
      if (!lastUpdated) warnings.push("missing_lastUpdated");
      if (!paymentStatus) warnings.push("missing_paymentStatus");
      if (!loanType) warnings.push("missing_loanType");
      if (!paymentHistory.length) warnings.push("missing_paymentHistory");

      return {
        recordType: "account",
        index: idx + 1,
        startPageNumber: b.startPageNumber ?? null,

        lender,

        lastUpdated,
        paymentStatus,
        worstDelinquency,

        balance,
        creditLimit,

        openDate,
        closedDate,
        lastActivity,

        loanType,
        responsibility,
        companyName,
        accountNumber,
        highBalance,
        scheduledPayment,
        terms,

        paymentHistory,

        warnings,
      };
    });

    // ---- Collections / Public records / Inquiries (all empty-safe)
    const collections =
      collectionsIdx === -1 || collectionsEndIdx === -1 || collectionsEndIdx <= collectionsIdx
        ? []
        : (() => {
            const sec = sliceSection(lines, linePages, collectionsIdx, collectionsEndIdx);
            return parseCollectionsSection(sec.lines, sec.pages);
          })();

    const publicRecords =
      publicRecordsIdx === -1 || publicRecordsEndIdx === -1 || publicRecordsEndIdx <= publicRecordsIdx
        ? []
        : (() => {
            const sec = sliceSection(lines, linePages, publicRecordsIdx, publicRecordsEndIdx);
            return parsePublicRecordsSection(sec.lines, sec.pages);
          })();

    const inquiries =
      inquiriesIdx === -1 || inquiriesEndIdx === -1 || inquiriesEndIdx <= inquiriesIdx
        ? []
        : (() => {
            const sec = sliceSection(lines, linePages, inquiriesIdx, inquiriesEndIdx);
            return parseInquiriesSection(sec.lines, sec.pages);
          })();

    // ---- Firestore writes
    const uploadRef = admin.firestore().doc(`users/${uid}/reportUploads/${uploadId}`);

    await uploadRef.set(
      {
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        source: "myfico_equifax_b1_condensed",
        status: "parsed_report",
        parserVersion: "pdfjs_accounts_collections_publicrecords_inquiries_v1",
      },
      { merge: true }
    );

    const baseRef = uploadRef.collection("stocks_conso_report_uploads");

    // meta doc
    await baseRef.doc("meta").set(
      {
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        accountsCount: accounts.length,
        collectionsCount: collections.length,
        publicRecordsCount: publicRecords.length,
        inquiriesCount: inquiries.length,
      },
      { merge: true }
    );

    const writes = [];

    // Accounts (keep your existing seed style, just add recordType in payload)
    for (const a of accounts) {
      const seed = `${uploadId}|acct|${a.index}|${a.lender || ""}|${a.accountNumber || ""}|${a.loanType || ""}`;
      const id = sha1(seed);
      writes.push({ ref: baseRef.doc(id), data: a });
    }

    for (const c of collections) {
      const seed = `${uploadId}|collection|${c.index}|${c.lender || ""}|${c.originalCreditor || ""}|${c.amount ?? ""}|${c.dateOpened?.seconds ?? ""}|${c.dateReported?.seconds ?? ""}`;
      const id = sha1(seed);
      writes.push({ ref: baseRef.doc(id), data: c });
    }

    for (const pr of publicRecords) {
      const seed = `${uploadId}|public_record|${pr.index}|${pr.type || ""}|${pr.filingDate?.seconds ?? ""}|${pr.status || ""}|${pr.amount ?? ""}`;
      const id = sha1(seed);
      writes.push({ ref: baseRef.doc(id), data: pr });
    }

    for (const iq of inquiries) {
      const seed = `${uploadId}|inquiry|${iq.index}|${iq.lender || ""}|${iq.inquiryDate?.seconds ?? ""}|${iq.inquiryMonthYear || ""}`;
      const id = sha1(seed);
      writes.push({ ref: baseRef.doc(id), data: iq });
    }

    // Commit in chunks (avoids 500-write ceiling)
    await commitBatchesInChunks(writes, 450);

    // delete source PDF
    await storage.file(path).delete();

    return {
      ok: true,
      uploadId,
      accountsCount: accounts.length,
      collectionsCount: collections.length,
      publicRecordsCount: publicRecords.length,
      inquiriesCount: inquiries.length,
    };
  } catch (err) {
    console.error("UPLOADREPORT_ERROR", {
      message: err?.message || String(err),
      code: err?.code || null,
      stack: err?.stack || null,
    });
    throw err;
  }
});
