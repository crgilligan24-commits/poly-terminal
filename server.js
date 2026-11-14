const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

/* The POLY_* overrides exist so the server can be pointed at a local stand-in
   for testing. Left unset, it reads Polymarket's public APIs on port 8890. */
const DATA_API = process.env.POLY_DATA_API || 'https://data-api.polymarket.com';
const GAMMA_API = process.env.POLY_GAMMA_API || 'https://gamma-api.polymarket.com';
const CLOB_API = process.env.POLY_CLOB_API || 'https://clob.polymarket.com';
const SITE = process.env.POLY_SITE || 'https://polymarket.com';
const POLL_INTERVAL = parseInt(process.env.POLY_POLL_MS, 10) || 60_000;
const PORT = parseInt(process.env.POLY_PORT, 10) || 8890;
const DATA_DIR = process.env.POLY_DATA_DIR || path.join(__dirname, 'data');   // history that survives a restart
const POS_LIMIT = 250;                           // rows fetched per wallet, per poll
const TRADE_WINDOW_SHOWN = 24;                   // plays the Trade Window lists by default (and the scorecard logs)
const TRADE_WINDOW_SENT = 80;                    // plays sent, so the page can filter without a round trip

/* ── Your account ──────────────────────────────────────────────────
   Accepts a Polymarket username OR a 0x wallet address. A username is
   resolved via the public profile page; a wallet is used directly.
   Override without editing this file:  set POLY_ME=0x...  */
const MY_ACCOUNT = process.env.POLY_ME || '';
const RESOLVE_RETRY_MS = 5 * 60_000;   // re-attempt a failed lookup this often

const store = {
    leaderboards: {}, topTrader: null, consensus: [], allTrades: [],
    traderPositions: {},
    sparklines: { DAY: {}, WEEK: {}, ALL: {} },
    liveChanges: [],
    convictionPlays: [],
    dayVsWeek: { day: [], week: [] },
    quickMoney: [],
    // live market layer
    hourlyPlays: [], momentumMovers: [], resolvingSoon: [],
    marketPrices: {}, marketMeta: {},
    tape: [],
    stats: {},
    // personal tracking
    me: { input: MY_ACCOUNT, wallet: null, resolved: false, error: null,
          positions: [], portfolioValue: null, signals: [], alerts: [] },
    top: null, briefing: null,
    error: null,            // plain-language reason the last poll failed, if it did
    lastUpdated: null, pollFailed: false, initialized: false,
    rev: 0,                 // bumped whenever served data changes; lets the UI skip unchanged fetches
};
let traderMeta = new Map();    // wallet -> name, boards, exposure (trader panel, watchlist)
let tokenIndex = new Map();    // conditionId -> { outcomes:[names], tokens:[outcome token ids] }
let priceIndex = new Map();    // conditionId -> { outcome name -> last known price }
let liveNow = {};              // the live market universe from the last good poll
let heldNow = {};              // end dates, prices and categories for markets outside it
let marketIndex = new Map();   // market key -> full allTrades row (holder detail, served on demand)
let payloadCache = new Map();  // tab -> JSON string, rebuilt lazily after each poll

/* personal-tracking state, retained across polls */
let _me = { wallet: null, resolved: false, error: null, lastTry: 0, userName: null };
let _myMktStats = {};     // conditionId -> { seen:Set, exited:Set, entered:Set, initialSide }
let _myAlerts = [];

let _prev = {};
/* Last successful position snapshot per wallet:
   wallet -> { ts, truncated, pos: { key -> {size, price, value, avg, cid, outcome, title, slug, first} } }
   A wallet is only present if its fetch succeeded on the previous poll. */
let _snap = {};
let _seq = 0;             // event id counter for Order Flow rows and overlap alerts
let pnlHistory = {};
let priceHistory = {};
let priceSeen = {};       // conditionId -> last time a price point was recorded
let liveChanges = [];
let polling = false;

const num = (v, d = 0) => { const n = parseFloat(v); return isNaN(n) ? d : n; };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const r2 = n => Math.round(n * 100) / 100;
const r4 = n => Math.round(n * 10000) / 10000;

function jparse(s, fb) {
    if (Array.isArray(s)) return s;
    try { const v = JSON.parse(s); return Array.isArray(v) ? v : fb; } catch { return fb; }
}
const chunk = (arr, n) => { const out = []; for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n)); return out; };

/* ── Persistence ───────────────────────────────────────────────────
   Price history, PnL history, the Order Flow feed, overlap alerts and the
   position baseline are saved to ./data as JSON once per poll and loaded on
   start. Each file is written to a .tmp sibling and renamed over the real
   one, so a crash mid-write can never leave a half-written file behind. */
const PRICE_MAX_GAP = 2 * 3600_000;     // older price points would draw a misleading sparkline
const PNL_MAX_GAP = 24 * 3600_000;
const FEED_MAX_AGE = 48 * 3600_000;
const SNAP_MAX_AGE = 10 * 60_000;       // a baseline older than this is not "last poll" any more

function readJson(name) {
    try { return JSON.parse(fs.readFileSync(path.join(DATA_DIR, name), 'utf-8')); } catch { return null; }
}
const _writeQueue = {};     // one write at a time per file, so two quick saves cannot trip over the same .tmp
function writeAtomic(name, obj) {
    const file = path.join(DATA_DIR, name), tmp = file + '.tmp';
    const text = JSON.stringify(obj);          // captured now; the newest call is always written last
    const run = async () => {
        await fs.promises.writeFile(tmp, text);
        try { await fs.promises.rename(tmp, file); }
        catch {  // Windows can briefly lock the target (antivirus, indexer) — one retry
            await sleep(200);
            await fs.promises.rename(tmp, file);
        }
    };
    const p = (_writeQueue[name] || Promise.resolve()).then(run);
    _writeQueue[name] = p.catch(() => {});     // a failed write must not block the next one
    return p;
}
function loadState() {
    try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch { /* reported on first save */ }
    const now = Date.now(), loaded = [];
    const ph = readJson('price-history.json');
    if (ph?.series && now - (ph.savedAt || 0) < PRICE_MAX_GAP) {
        priceHistory = ph.series; priceSeen = ph.seen || {};
        loaded.push(`${Object.keys(priceHistory).length} price series`);
    }
    const pn = readJson('pnl-history.json');
    if (pn?.series && now - (pn.savedAt || 0) < PNL_MAX_GAP) {
        pnlHistory = pn.series;
        loaded.push(`${Object.keys(pnlHistory).length} pnl series`);
    }
    const lc = readJson('live-changes.json');
    if (Array.isArray(lc?.changes)) {
        liveChanges = lc.changes.filter(c => now - (c.ts || 0) < FEED_MAX_AGE);
        _seq = Math.max(_seq, num(lc.seq));
        store.liveChanges = liveChanges;
        loaded.push(`${liveChanges.length} flow events`);
    }
    const al = readJson('alerts.json');
    if (al) {
        _myAlerts = (Array.isArray(al.alerts) ? al.alerts : []).filter(a => now - (a.ts || 0) < FEED_MAX_AGE);
        _seq = Math.max(_seq, num(al.seq));
        for (const [cid, st] of Object.entries(al.mktStats || {}))
            _myMktStats[cid] = { seen: new Set(st.seen || []), exited: new Set(st.exited || []),
                                 entered: new Set(st.entered || []), initialSide: st.initialSide || null };
        loaded.push(`${_myAlerts.length} alerts`);
    }
    const sn = readJson('snapshot.json');
    if (sn?.wallets && now - (sn.savedAt || 0) < SNAP_MAX_AGE) {
        // titles and slugs are stored once per market in the file; put them back on each row
        for (const w of Object.values(sn.wallets))
            for (const p of Object.values(w.pos || {})) {
                const m = sn.markets?.[p.cid] || [];
                p.title = m[0] || '?'; p.slug = m[1] || '';
            }
        _snap = sn.wallets; _prev = sn.boards || {};
        loaded.push(`position baseline for ${Object.keys(_snap).length} wallets`);
    }
    const sc = readJson('scorecard.json');
    if (Array.isArray(sc?.plays)) {
        scorecard = { seq: num(sc.seq), plays: sc.plays };
        loaded.push(`${scorecard.plays.length} scorecard plays`);
    }
    const mb = readJson('manual-book.json');
    if (Array.isArray(mb?.rows)) {
        manualBook = mb.rows; manualSeq = num(mb.seq);
        if (manualBook.length) loaded.push(`${manualBook.length} manual positions`);
    }
    console.log(`  data/  ${loaded.length ? 'loaded ' + loaded.join(', ') : 'nothing to load, starting fresh'}`);
}
async function saveState(nowMs) {
    try {
        await fs.promises.mkdir(DATA_DIR, { recursive: true });
        const mktStats = {};
        for (const [cid, st] of Object.entries(_myMktStats))
            mktStats[cid] = { seen: [...st.seen], exited: [...st.exited], entered: [...st.entered],
                              initialSide: st.initialSide || null };
        await writeAtomic('price-history.json', { savedAt: nowMs, series: priceHistory, seen: priceSeen });
        await writeAtomic('pnl-history.json', { savedAt: nowMs, series: pnlHistory });
        await writeAtomic('live-changes.json', { savedAt: nowMs, seq: _seq, changes: liveChanges });
        await writeAtomic('alerts.json', { savedAt: nowMs, seq: _seq, alerts: _myAlerts, mktStats });
        // the baseline repeats each market title once per holder; store titles once instead
        const markets = {}, slim = {};
        for (const [w, s] of Object.entries(_snap)) {
            const pos = {};
            for (const [k, p] of Object.entries(s.pos)) {
                markets[p.cid] ||= [p.title, p.slug];
                pos[k] = { size: p.size, price: p.price, value: p.value, avg: p.avg,
                           cid: p.cid, outcome: p.outcome, first: p.first };
            }
            slim[w] = { ts: s.ts, truncated: s.truncated, pos };
        }
        await writeAtomic('snapshot.json', { savedAt: nowMs, wallets: slim, markets, boards: _prev });
        if (scDirty) { scDirty = false; await writeAtomic('scorecard.json', { savedAt: nowMs, seq: scorecard.seq, plays: scorecard.plays }); }
    } catch (e) {
        console.error('  data/  save failed —', e.message || e);
    }
}

/* ── Per-poll-cycle endpoint status tally ──────────────────────────
   Every apiGet records the final status of each request (after retries)
   keyed by "host path"; poll() prints and resets it once per cycle. */
let _statusLog = {};
function recordStatus(u, status) {
    const host = u.hostname.split('.')[0];            // data-api / gamma-api
    const key = `${host} ${u.pathname}`;
    const bucket = (_statusLog[key] ||= {});
    bucket[status] = (bucket[status] || 0) + 1;
}
function formatStatusLog() {
    const parts = Object.entries(_statusLog).sort(([a], [b]) => a.localeCompare(b))
        .map(([k, codes]) => `${k} ` + Object.entries(codes).map(([c, n]) => n > 1 ? `${c}×${n}` : c).join(','));
    return parts.length ? parts.join(' | ') : '(no requests)';
}

/* Last answer from each upstream host, for the status bar: did it answer, with
   what, and how long it took. */
const health = {};        // 'data-api' | 'gamma-api' | 'clob' -> { ok, status, ms, at }
let nextPollAt = 0;
function noteHealth(u, ok, status, ms) {
    health[u.hostname.split('.')[0]] = { ok, status, ms, at: Date.now() };
}
function healthPayload() {
    return {
        sources: ['data-api', 'gamma-api', 'clob'].map(name => ({ name, ...(health[name] || { ok: null }) })),
        nextPollAt, pollMs: POLL_INTERVAL, polling, rev: store.rev,
        lastUpdated: store.lastUpdated, pollFailed: store.pollFailed,
    };
}

/* Why the last request failed, turned into a sentence the page can show. */
let _lastNetErr = null;
function describeFailure() {
    const e = _lastNetErr;
    if (!e) return 'Polymarket returned no leaderboard data.';
    const c = String(e.code || '');
    if (/CERT|ISSUER|SELF_SIGNED|ALTNAME/.test(c))
        return 'The secure connection to ' + e.host + ' was answered by something other than Polymarket ' +
               '(certificate not trusted: ' + c + '). The network this computer is on is most likely filtering ' +
               'polymarket.com. The dashboard fills in as soon as the server can reach Polymarket.';
    if (c === 'ENOTFOUND' || c === 'EAI_AGAIN') return 'Could not look up ' + e.host + '. Check the internet connection.';
    if (c === 'ECONNREFUSED' || c === 'ECONNRESET' || c === 'ETIMEDOUT' || c === 'TIMEOUT' || /UND_ERR/.test(c))
        return 'Could not connect to ' + e.host + ' (' + c + '). Polymarket may be down or unreachable from this network.';
    if (typeof e.status === 'number') return e.host + ' answered HTTP ' + e.status + '.';
    return 'Could not reach ' + e.host + ': ' + e.msg;
}

async function apiGet(baseUrl, params = {}) {
    const u = new URL(baseUrl);
    for (const [k, v] of Object.entries(params)) {
        if (Array.isArray(v)) v.forEach(x => u.searchParams.append(k, String(x)));
        else u.searchParams.set(k, String(v));
    }
    for (let attempt = 0; attempt < 3; attempt++) {
        let status = 'ERR';
        const t0 = Date.now();
        try {
            const r = await fetch(u.toString(), { signal: AbortSignal.timeout(30_000) });
            status = r.status;
            if (r.status === 429 && attempt < 2) { await sleep(2 ** (attempt + 1) * 1000); continue; }
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            const body = await r.json();
            recordStatus(u, status);
            noteHealth(u, true, status, Date.now() - t0);
            return body;
        } catch (e) {
            if (status === 'ERR' && e.name === 'TimeoutError') status = 'TIMEOUT';
            _lastNetErr = { code: e.cause?.code || (status === 'TIMEOUT' ? 'TIMEOUT' : ''),
                            msg: e.cause?.message || e.message || String(e), host: u.hostname, status };
            if (attempt === 2) { recordStatus(u, status); noteHealth(u, false, status, Date.now() - t0); throw e; }
            await sleep(2 ** attempt * 1000);
        }
    }
}

/* ── Data API v2 ───────────────────────────────────────────────────
   v1 was retired 2026-11-02 (410 Gone). v2 wraps every payload in
   { data, pagination? }, uses snake_case fields, and renames several.
   dataGet() unwraps .data; the adapters below map v2 rows back onto the
   v1 field names the rest of this file (and the UI) already consume, so
   downstream logic is untouched. */
async function dataGet(route, params = {}) {
    const body = await apiGet(`${DATA_API}/v2${route}`, params);
    if (!body || typeof body !== 'object' || !('data' in body))
        throw new Error(`v2 ${route}: response missing data envelope`);
    return body.data;
}
const v2Date = d => (d && d !== '1970-01-01') ? d : null;   // v2 sentinel for "no end date"
const v2Leader = e => ({
    rank: e.rank, proxyWallet: e.user_id || '', userName: e.user_name || '',
    profileImage: e.profile_image || '', xUsername: e.x_username || '', verifiedBadge: !!e.verified,
    pnl: e.pnl, vol: e.volume,
});
const v2Position = p => ({
    proxyWallet: p.proxy_wallet, asset: p.token_id, conditionId: p.condition_id,
    size: p.current_size, avgPrice: p.avg_price, initialValue: p.entry_cost_usdc,
    currentValue: p.current_value, cashPnl: p.unrealized_pnl, percentPnl: p.percent_pnl,
    totalBought: p.total_size, realizedPnl: p.realized_pnl, percentRealizedPnl: p.percent_realized_pnl,
    curPrice: p.current_price, redeemable: p.redeemable, mergeable: p.mergeable,
    title: p.title, slug: p.slug, icon: p.icon, eventId: p.event_id, eventSlug: p.event_slug,
    outcome: p.outcome, outcomeIndex: p.outcome_index, oppositeOutcome: p.opposite_outcome,
    oppositeAsset: p.opposite_token_id, endDate: v2Date(p.end_date), negativeRisk: p.negative_risk,
    firstEntryAt: p.first_entry_at, lastEventAt: p.last_event_at,
});
const v2Activity = a => ({
    proxyWallet: a.proxy_wallet, timestamp: a.timestamp, conditionId: a.condition_id, type: a.type,
    size: a.size, usdcSize: a.usdc_size, transactionHash: a.transaction_hash, price: a.price,
    asset: a.token_id, side: a.side, outcomeIndex: a.outcome_index, title: a.title, slug: a.slug,
    icon: a.icon, eventSlug: a.event_slug, outcome: a.outcome, name: a.name,
    pseudonym: a.pseudonym, profileImage: a.profile_image,
});
const mapRows = (rows, fn) => Array.isArray(rows) ? rows.map(fn) : rows;
// v1 sizeThreshold=0.01 / sortBy=CURRENT equivalents
const POS_FILTER = { filter_type: 'TOKENS', filter_amount: 0.01 };
const getPositions = (user, limit) => dataGet('/positions', {
    user, limit, ...POS_FILTER, sort_by: 'CURRENT_VALUE', sort_direction: 'DESC',
}).then(r => mapRows(r, v2Position));
/* Same call, but also reports whether the book continues past this page.
   v2 pages by cursor only; pagination.has_more is the only way to know the
   list was cut off at `limit`. Many top wallets hold 500+ positions. */
async function getPositionsPage(user, limit) {
    const body = await apiGet(`${DATA_API}/v2/positions`, {
        user, limit, ...POS_FILTER, sort_by: 'CURRENT_VALUE', sort_direction: 'DESC',
    });
    if (!body || !Array.isArray(body.data)) throw new Error('v2 /positions: response missing data envelope');
    return { rows: body.data.map(v2Position), truncated: !!body.pagination?.has_more };
}

async function mapPool(items, limit, fn) {
    const results = new Array(items.length);
    let idx = 0;
    await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
        while (idx < items.length) {
            const i = idx++;
            try { results[i] = await fn(items[i], i); } catch { results[i] = null; }
        }
    }));
    return results;
}

/* ── Is this position actually still open? ─────────────────────────
   Verified against polymarket.com's own Active/Closed split.

   `redeemable` is the settlement flag, and it correlates EXACTLY with
   curPrice being 0 or 1 — 1039/1039 positions across 6 wallets, zero
   disagreements in either direction. Both are checked so a null/missing
   flag still gets caught by the price.

   Deliberately NOT filtered on end date: Polymarket keeps a market active
   past its scheduled end until the oracle resolves it, sometimes by hours.
   Filtering on gamma's endDate wrongly deleted positions the site itself
   lists as Active (e.g. Brøndby/Viborg O/U 2.5 at 99.95c, unsettled).
   A price of 0.9995 is extreme but still live; only exactly 0 or 1 is settled.

   Every section MUST use this one function so they cannot drift apart. */
function isPositionOpen(p) {
    if (p.redeemable === true) return false;
    const cur = parseFloat(p.curPrice);
    if (!isNaN(cur) && (cur >= 0.9999 || cur <= 0.0001)) return false;
    return true;
}

/* ── Categories ────────────────────────────────────────────────────
   Each market gets one category. Gamma's tag labels decide it when they are
   present (asked for with include_tag); otherwise the slug and title are
   matched against the same word lists. First match wins, in this order. */
const CATS = ['Sports', 'Crypto', 'Politics', 'Economy', 'Tech', 'Culture', 'World', 'Other'];
const CAT_RULES = [
    ['Sports', /\b(sports?|nba|wnba|nfl|mlb|nhl|mls|ncaa[bf]?|cfb|cbb|soccer|football|basketball|baseball|hockey|tennis|golf|pga|ufc|mma|boxing|cricket|ipl|rugby|f1|formula 1|nascar|epl|premier league|la liga|serie a|bundesliga|ligue 1|champions league|uefa|fifa|world cup|olympics?|esports?|league of legends|lol|dota|cs2|counter-strike|valorant|chess|super bowl|playoffs?|vs\.?|spread|moneyline|o\/u|over\/under)\b/i],
    ['Crypto', /\b(crypto|bitcoin|btc|ethereum|eth|solana|sol|xrp|doge|dogecoin|memecoins?|altcoins?|stablecoins?|defi|nft|airdrop|binance|coinbase|hyperliquid|token|blockchain|up or down)\b/i],
    ['Politics', /\b(politics|elections?|trump|biden|harris|vance|congress|senate|house|governor|mayor|president(ial)?|primary|primaries|democrat(s|ic)?|republican(s)?|gop|parliament|prime minister|cabinet|supreme court|scotus|impeach|veto|shutdown|poll(s|ing)?|nominee|nomination)\b/i],
    ['Economy', /\b(economy|economics|finance|business|fed|fomc|interest rates?|rate cut|inflation|cpi|gdp|recession|jobs report|unemployment|stocks?|s&p|nasdaq|dow|earnings|ipo|tariffs?|treasury|oil|gold|commodit(y|ies)|housing|mortgage)\b/i],
    ['Tech', /\b(tech|technology|ai|openai|chatgpt|gpt|anthropic|claude|gemini|google|apple|microsoft|meta|nvidia|tesla|spacex|starship|twitter|tiktok|science|space|nasa|launch)\b/i],
    ['Culture', /\b(culture|pop culture|entertainment|movies?|film|box office|oscars?|grammys?|emmys?|music|album|spotify|billboard|tv|netflix|celebrit(y|ies)|taylor swift|kanye|mrbeast|youtube|streamer|awards?|gaming|video games?|gta)\b/i],
    ['World', /\b(world|geopolitics|global|war|ceasefire|ukraine|russia|putin|zelensky|israel|gaza|iran|china|taiwan|india|pakistan|north korea|nato|un|eu|europe|middle east|venezuela|climate|weather|hurricane|earthquake|pandemic)\b/i],
];
function categorize(labels, text) {
    for (const l of labels || [])
        for (const [cat, re] of CAT_RULES) if (re.test(String(l))) return cat;
    for (const [cat, re] of CAT_RULES) if (re.test(text || '')) return cat;
    return 'Other';
}
const tagLabels = m => {
    const out = [];
    const take = arr => {
        if (!Array.isArray(arr)) return;
        for (const t of arr) { const l = t?.label || t?.slug || (typeof t === 'string' ? t : ''); if (l) out.push(l); }
    };
    take(m.tags); take(m.events?.[0]?.tags);
    if (m.category) out.push(m.category);
    if (m.events?.[0]?.category) out.push(m.events[0].category);
    return out;
};
const marketCat = m => categorize(tagLabels(m), (m.slug || '').replace(/-/g, ' ') + ' ' + (m.question || ''));
/* include_tag is how Gamma attaches tags to market rows. If Gamma ever rejects
   the parameter, it is dropped for the rest of the run and categories fall
   back to the title matching above. */
let _useTagParam = true;
const withTags = p => _useTagParam ? { ...p, include_tag: true } : p;

/* ── Resolve every market a tracked wallet holds ────────────────── */
async function fetchMarketsByCondition(ids) {
    const out = {};
    const batches = [];
    for (let i = 0; i < ids.length; i += 50) batches.push(ids.slice(i, i + 50));
    await mapPool(batches, 8, async (batch) => {
        try {
            const rows = await apiGet(`${GAMMA_API}/markets`, withTags({
                limit: 50, condition_ids: batch,
            }));
            if (!Array.isArray(rows)) return;
            for (const m of rows) {
                if (!m.conditionId) continue;
                out[m.conditionId] = {
                    conditionId: m.conditionId,
                    closed: m.closed === true,
                    active: m.active !== false,
                    endDate: m.endDate || null,
                    endMs: m.endDate ? new Date(m.endDate).getTime() : 0,
                    acceptingOrders: m.acceptingOrders !== false,
                    question: m.question || '',
                    slug: m.events?.[0]?.slug || m.slug || '',
                    outcomes: jparse(m.outcomes, []),
                    prices: jparse(m.outcomePrices, []).map(x => num(x, null)),
                    tokens: jparse(m.clobTokenIds, []).map(String),
                    cat: marketCat(m),
                };
            }
        } catch { /* batch failed — those fall back to the coarse date */ }
    });
    return out;
}

/* ── Position diffing ──────────────────────────────────────────────
   One diff feeds both Order Flow and the overlap alerts, so the two cannot
   disagree. Rules:

   • A wallet is diffed only if it has a snapshot from the previous poll.
     A wallet that just joined a leaderboard, or whose fetch failed last
     time, has no baseline: this poll becomes its baseline and emits nothing.
   • NEW  — a position that was not in the previous snapshot.
     ADD  — size went up.   TRIM — size went down but is still held.
     EXIT — the position is gone and the market has not settled.
   • Books bigger than one page (POS_LIMIT rows, sorted by value) are a
     moving window: a row can enter or leave it without a trade. So for those
     wallets a NEW needs a first-entry time after the previous snapshot, and
     a disappearance is confirmed with a direct per-market lookup.
   • A disappearance is never reported while the market is resolved or
     closed: the losing side simply drops off the list when a market settles.

   Size thresholds are applied by the two consumers, not here. */
const posKey = p => p.asset || ((p.conditionId || p.eventSlug || p.title || '?') + '|' + (p.outcome || '?'));

function buildSnapshot(wallets, positions, fetchOk, truncated, nowMs) {
    const snap = {};
    for (const w of wallets) {
        if (!fetchOk[w]) continue;               // failed fetch => no snapshot => not diffed next poll
        const pos = {};
        for (const p of positions[w] || []) {
            pos[posKey(p)] = {
                size: num(p.size), price: r4(num(p.curPrice)), value: r2(num(p.currentValue)),
                avg: r4(num(p.avgPrice)), cid: p.conditionId || '', outcome: p.outcome || '?',
                title: p.title || '?', slug: p.eventSlug || p.slug || '', first: num(p.firstEntryAt),
            };
        }
        snap[w] = { ts: nowMs, truncated: !!truncated[w], pos };
    }
    return snap;
}

const MAX_EXIT_CHECKS = 150;   // disappearances verified per poll, largest first

async function diffPositions(prevSnap, curSnap, rawKeys, nowMs, myConds) {
    const events = [], gone = [];
    for (const [w, cur] of Object.entries(curSnap)) {
        const prev = prevSnap[w];
        if (!prev || nowMs - prev.ts > SNAP_MAX_AGE) continue;      // no baseline — this poll is it
        const windowed = prev.truncated || cur.truncated;
        for (const [k, c] of Object.entries(cur.pos)) {
            const p = prev.pos[k];
            const ev = { wallet: w, key: k, cid: c.cid, outcome: c.outcome, title: c.title, slug: c.slug,
                         size: c.size, price: c.price, value: c.value };
            if (!p) {
                // in a windowed book, only trust a row that was first bought since the last snapshot
                if (windowed && !(c.first && c.first * 1000 >= prev.ts - 180_000)) continue;
                events.push({ ...ev, kind: 'NEW', prevSize: 0 });
            } else if (c.size > p.size + 0.5) events.push({ ...ev, kind: 'ADD', prevSize: p.size });
            else if (c.size < p.size - 0.5) events.push({ ...ev, kind: 'TRIM', prevSize: p.size });
        }
        for (const [k, p] of Object.entries(prev.pos)) {
            if (cur.pos[k]) continue;
            if (rawKeys[w]?.has(k)) continue;                        // still listed, just settled
            if (p.price >= 0.995 || p.price <= 0.005) continue;      // was already at a settlement price
            if (!(p.size > 50 || myConds.has(p.cid))) continue;      // too small for either feed
            gone.push({ wallet: w, key: k, windowed, ...p });
        }
    }
    if (!gone.length) return { events, checked: 0, skipped: 0 };

    gone.sort((a, b) => b.value - a.value);
    const check = gone.slice(0, MAX_EXIT_CHECKS);

    // 1. has the market settled? true / false / undefined (could not find out)
    const settled = {};
    const cids = [...new Set(check.map(g => g.cid).filter(Boolean))];
    await mapPool(chunk(cids, 10), 4, async (batch) => {
        const rows = await dataGet('/tokens', { condition: batch.join(',') });
        if (!Array.isArray(rows)) return;
        for (const t of rows) {
            if (!t.condition_id) continue;
            settled[t.condition_id] = settled[t.condition_id] || !!(t.resolved || t.closed);
        }
    });
    const unknown = cids.filter(c => !(c in settled));
    if (unknown.length) {
        const g = await fetchMarketsByCondition(unknown);
        for (const c of unknown) if (g[c]) settled[c] = g[c].closed;
    }

    // 2. windowed books: is the row really gone, or just outside the page we fetched?
    const still = {};           // wallet -> { key -> size }   (present only if the lookup worked)
    const byWallet = {};
    for (const g of check)
        if (g.windowed && settled[g.cid] === false) (byWallet[g.wallet] ||= new Set()).add(g.cid);
    const jobs = [];
    for (const [w, set] of Object.entries(byWallet))
        for (const batch of chunk([...set], 20)) jobs.push({ w, batch });
    await mapPool(jobs, 4, async ({ w, batch }) => {
        const rows = await dataGet('/positions', { user: w, condition: batch.join(','), limit: 200, ...POS_FILTER });
        if (!Array.isArray(rows)) return;
        const m = (still[w] ||= { _ok: new Set() });
        for (const c of batch) m._ok.add(c);
        // -1 marks "listed but settled", which must not be reported as an exit either
        for (const r of rows.map(v2Position)) m[posKey(r)] = isPositionOpen(r) ? num(r.size) : -1;
    });

    let skipped = gone.length - check.length;
    for (const g of check) {
        if (settled[g.cid] !== false) { if (settled[g.cid] === undefined) skipped++; continue; }
        const ev = { wallet: g.wallet, key: g.key, cid: g.cid, outcome: g.outcome, title: g.title,
                     slug: g.slug, price: g.price, prevSize: g.size };
        if (g.windowed) {
            const m = still[g.wallet];
            if (!m || !m._ok.has(g.cid)) { skipped++; continue; }    // lookup failed — say nothing
            const sz = m[g.key];
            if (sz != null) {                                          // still held, just off the page
                if (sz >= 0 && sz < g.size - 0.5)
                    events.push({ ...ev, kind: 'TRIM', size: sz, value: r2(sz * g.price) });
                continue;
            }
        }
        events.push({ ...ev, kind: 'EXIT', size: 0, value: g.value });
    }
    return { events, checked: check.length, skipped };
}

/* Which side leads each market, counting only the given wallets. `strict`
   is false on a tie, so a tie never counts as a consensus side. */
function leadBySide(snap, wallets) {
    const m = {};
    for (const w of wallets) {
        for (const p of Object.values(snap[w].pos)) {
            if (!p.cid) continue;
            const o = ((m[p.cid] ||= {})[p.outcome] ||= { count: 0, value: 0 });
            o.count++; o.value += p.value;
        }
    }
    const out = {};
    for (const [cid, sides] of Object.entries(m)) {
        const arr = Object.entries(sides).sort((a, b) => b[1].count - a[1].count);
        out[cid] = {
            lead: arr[0][0], sides: arr.length,
            total: arr.reduce((s, x) => s + x[1].count, 0),
            strict: arr.length === 1 || arr[0][1].count > arr[1][1].count,
        };
    }
    return out;
}

/* ── Resolve a username (or wallet) to a proxy wallet ──────────────
   There is no public username->wallet API: gamma /profiles requires auth
   and /public-search returns 0 results even for known-good names (verified
   against swisstony and RN1). The public profile page is the one reliable
   path — the owner's address dominates the markup (~59 occurrences vs 3-4
   for unrelated contract addresses), so the most frequent 0x match is the
   wallet. Confirmed exact for swisstony and RN1.
   Returns { wallet } or { error } — never a fabricated address. */
async function resolveAccount(input) {
    const raw = String(input || '').trim().replace(/^@/, '');
    if (!raw) return { error: 'No account configured.' };

    if (/^0x[a-fA-F0-9]{40}$/.test(raw)) {
        const w = raw.toLowerCase();
        try {
            const p = await getPositions(w, 1);
            if (!Array.isArray(p)) return { error: `Wallet ${raw.slice(0, 10)}… returned no position data.` };
            return { wallet: w };
        } catch { return { error: `Could not reach the positions API for ${raw.slice(0, 10)}….` }; }
    }

    let res;
    try {
        res = await fetch(`${SITE}/@${encodeURIComponent(raw)}`, {
            signal: AbortSignal.timeout(25_000),
            headers: { 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
        });
    } catch {
        return { error: `Could not reach polymarket.com to look up @${raw}.` };
    }
    if (res.status === 404)
        return { error: `No public Polymarket profile exists at polymarket.com/@${raw}. ` +
                        `The account may be private, renamed, or not yet public.` };
    if (!res.ok) return { error: `polymarket.com/@${raw} returned HTTP ${res.status}.` };

    const html = await res.text();
    const hits = html.match(/0x[a-fA-F0-9]{40}/g) || [];
    if (!hits.length) return { error: `No wallet address found on the profile page for @${raw}.` };
    const freq = {};
    for (const h of hits) { const k = h.toLowerCase(); freq[k] = (freq[k] || 0) + 1; }
    const [best, count] = Object.entries(freq).sort((a, b) => b[1] - a[1])[0];
    if (count < 5) return { error: `Could not confidently identify the wallet for @${raw}.` };

    try {
        const p = await getPositions(best, 1);
        if (!Array.isArray(p)) return { error: `Resolved @${raw} but the positions API returned no data.` };
    } catch { return { error: `Resolved @${raw} but could not fetch its positions.` }; }
    return { wallet: best, userName: raw };
}

/* ── Live market universe from Gamma ───────────────────────────── */
async function fetchLiveMarkets() {
    const nowMs = Date.now();
    const endMax = new Date(nowMs + 72 * 3600_000).toISOString().slice(0, 19) + 'Z';
    const reqs = [];
    // Deep coverage of the liquid universe
    for (let off = 0; off < 600; off += 100)
        reqs.push({ limit: 100, offset: off, closed: false, active: true,
                    order: 'volume24hr', ascending: false });
    // Everything resolving in the next 72h — the short-trade pool
    for (let off = 0; off < 300; off += 100)
        reqs.push({ limit: 100, offset: off, closed: false, active: true,
                    end_date_max: endMax, order: 'volume24hr', ascending: false });

    let res = await Promise.allSettled(reqs.map(p => apiGet(`${GAMMA_API}/markets`, withTags(p))));
    if (_useTagParam && res.every(r => r.status !== 'fulfilled' || !Array.isArray(r.value))
        && typeof _lastNetErr?.status === 'number' && _lastNetErr.status >= 400 && _lastNetErr.status < 500) {
        _useTagParam = false;          // Gamma refused the request shape — retry once without tags
        console.log('  gamma  include_tag was refused; categories will come from market titles');
        res = await Promise.allSettled(reqs.map(p => apiGet(`${GAMMA_API}/markets`, p)));
    }
    const byCond = {};
    for (const r of res) {
        if (r.status !== 'fulfilled' || !Array.isArray(r.value)) continue;
        for (const m of r.value) {
            const cid = m.conditionId;
            if (!cid || byCond[cid]) continue;
            const outcomes = jparse(m.outcomes, []);
            const prices = jparse(m.outcomePrices, []).map(num);
            const end = m.endDate ? new Date(m.endDate).getTime() : 0;
            byCond[cid] = {
                conditionId: cid,
                question: m.question || '',
                slug: m.slug || '',
                eventSlug: m.events?.[0]?.slug || m.slug || '',
                outcomes, prices,
                bestBid: m.bestBid == null ? null : num(m.bestBid),
                bestAsk: m.bestAsk == null ? null : num(m.bestAsk),
                spread: m.spread == null ? null : num(m.spread),
                lastTradePrice: m.lastTradePrice == null ? null : num(m.lastTradePrice),
                vol24: num(m.volume24hr),
                vol1wk: num(m.volume1wk),
                volTotal: num(m.volumeNum ?? m.volume),
                liquidity: num(m.liquidityNum ?? m.liquidity),
                oneHour: m.oneHourPriceChange == null ? null : num(m.oneHourPriceChange),
                oneDay: m.oneDayPriceChange == null ? null : num(m.oneDayPriceChange),
                oneWeek: m.oneWeekPriceChange == null ? null : num(m.oneWeekPriceChange),
                competitive: num(m.competitive),
                endDate: m.endDate || null,
                endMs: end,
                hoursLeft: end ? (end - nowMs) / 3600_000 : null,
                acceptingOrders: m.acceptingOrders !== false,
                minTick: num(m.orderPriceMinTickSize, 0.01),
                minSize: num(m.orderMinSize, 5),
                negRisk: !!m.negRisk,
                icon: m.icon || '',
                tokens: jparse(m.clobTokenIds, []).map(String),
                cat: marketCat(m),
            };
        }
    }
    return byCond;
}

/* ── Smart-money helpers ───────────────────────────────────────────
   A wallet holding both sides of one market is hedged or market-making, not
   expressing a view, so it is flagged and left out of every side count and
   dollar total below. */
const sideSummary = s => ({ name: s.name, count: s.count, value: s.value, hedged: s.hedged });
function usdShare(sides) {
    const tot = sides.reduce((s, x) => s + x.value, 0);
    return tot > 0 && sides[0] ? sides[0].value / tot : 0;
}
/* Size-weighted average entry of the wallets on one side. */
function paidBy(side) {
    let sh = 0, cost = 0;
    for (const t of side?.traders || []) {
        if (t.hedged || !(t.size > 0)) continue;
        sh += t.size; cost += t.size * t.avgPrice;
    }
    return sh > 0 ? r4(cost / sh) : null;
}
/* What buying `side` costs right now. The book is quoted for the first outcome:
   buying it pays the ask; buying the second outcome pays one minus the bid. */
function payNowFor(live, side) {
    if (!live) return null;
    const i = (live.outcomes || []).indexOf(side);
    if (i === 0 && live.ask != null) return r4(live.ask);
    if (i === 1 && live.bid != null) return r4(1 - live.bid);
    return i >= 0 && live.prices?.[i] != null ? r4(live.prices[i]) : null;
}
const durTxt = h => h < 1 ? Math.max(1, Math.round(h * 60)) + ' minutes'
    : h < 24 ? (Math.round(h * 10) / 10) + ' hours' : Math.round(h / 24) + ' days';
const cents = p => (p * 100).toFixed(1).replace(/\.0$/, '') + 'c';

/* ── Scorecard ─────────────────────────────────────────────────────
   Every play that reaches the default Trade Window is logged once per market
   side, with the entry price and score it had at that moment. When Polymarket
   reports the market resolved, the row is marked won or lost. Nothing is
   back-filled or re-scored later, so the record cannot flatter itself. */
let scorecard = { seq: 0, plays: [] };
let scDirty = false;
const SC_MAX = 4000;
const SC_MIN_SAMPLE = 30;           // below this many resolved plays a hit rate is noise
const SC_BUCKETS = [[30, 39], [40, 49], [50, 59], [60, 69], [70, 100]];
const SC_CAL = [[0, 20], [20, 40], [40, 60], [60, 80], [80, 101]];     // entry price bands, in cents

function logPlays(plays, nowMs) {
    const known = new Map();
    for (const p of scorecard.plays) known.set(p.cid + '|' + p.sideIdx, p);
    for (const p of plays) {
        const entry = p.entry != null ? p.entry : p.price;
        if (!(entry > 0 && entry < 1)) continue;
        const old = known.get(p.conditionId + '|' + p.sideIdx);
        if (old) {
            if (old.status === 'open' && p.score > (old.peak || 0)) { old.peak = p.score; scDirty = true; }
            continue;
        }
        const row = {
            id: ++scorecard.seq, ts: nowMs, cid: p.conditionId, market: p.market, slug: p.slug,
            side: p.side, sideIdx: p.sideIdx, entry: r4(entry), score: p.score, peak: p.score,
            parts: p.parts, cat: p.cat, endMs: p.endDate ? Date.parse(p.endDate) || 0 : 0,
            hoursLeft: p.hoursLeft, status: 'open',
        };
        scorecard.plays.push(row); known.set(row.cid + '|' + row.sideIdx, row);
        scDirty = true;
    }
    if (scorecard.plays.length > SC_MAX) scorecard.plays = scorecard.plays.slice(-SC_MAX);
}

/* Ask Polymarket whether logged plays have resolved. A play past its end time
   is re-checked every 3 minutes, one still running every 30 (markets can
   resolve early). At most 40 markets per poll. */
async function settlePlays(nowMs) {
    const due = scorecard.plays.filter(p => p.status === 'open' &&
        nowMs - (p.checked || p.ts) >= ((p.endMs && nowMs >= p.endMs) ? 3 : 30) * 60_000)
        .sort((a, b) => (a.checked || 0) - (b.checked || 0));
    const cids = [...new Set(due.map(p => p.cid))].slice(0, 40);
    if (!cids.length) return 0;
    const byCid = {}, asked = new Set();
    await mapPool(chunk(cids, 10), 4, async (batch) => {
        const rows = await dataGet('/tokens', { condition: batch.join(',') });
        if (!Array.isArray(rows)) return;
        for (const c of batch) asked.add(c);
        for (const t of rows) (byCid[String(t.condition_id || '').toLowerCase()] ||= []).push(t);
    });
    let settled = 0;
    for (const p of due) {
        if (!asked.has(p.cid)) continue;
        p.checked = nowMs; scDirty = true;
        const toks = (byCid[p.cid.toLowerCase()] || []).filter(t => t.resolved && t.final_price != null);
        const mine = toks.find(t => t.outcome_index === p.sideIdx) || toks.find(t => t.outcome === p.side);
        const fp = mine ? num(mine.final_price, NaN) : NaN;
        if (!isNaN(fp)) {
            p.status = fp >= 0.99 ? 'won' : fp <= 0.01 ? 'lost' : 'void';
            p.finalPrice = fp; p.resolvedAt = nowMs; settled++;
        } else if (p.endMs && nowMs - p.endMs > 14 * 86400_000) {
            p.status = 'void'; p.resolvedAt = nowMs;      // never resolved: leave it out of the record
        }
    }
    return settled;
}

function scorecardSummary() {
    const ret = p => p.status === 'won' ? (1 - p.entry) / p.entry * 100 : -100;
    const agg = rows => {
        const d = rows.filter(p => p.status === 'won' || p.status === 'lost');
        const w = d.filter(p => p.status === 'won').length;
        return {
            n: d.length, wins: w, open: rows.filter(p => p.status === 'open').length,
            hit: d.length ? r2(w / d.length * 100) : null,
            avgRet: d.length ? r2(d.reduce((s, p) => s + ret(p), 0) / d.length) : null,
            breakEven: d.length ? r2(d.reduce((s, p) => s + p.entry, 0) / d.length * 100) : null,
            small: d.length < SC_MIN_SAMPLE,
        };
    };
    const slim = p => ({
        id: p.id, ts: p.ts, cid: p.cid, market: p.market, slug: p.slug, side: p.side, entry: p.entry,
        score: p.score, parts: p.parts, cat: p.cat, endMs: p.endMs, status: p.status,
        finalPrice: p.finalPrice ?? null, resolvedAt: p.resolvedAt || null,
        ret: p.status === 'won' || p.status === 'lost' ? r2(ret(p)) : null,
    });
    const all = scorecard.plays;
    const done = all.filter(p => p.status === 'won' || p.status === 'lost')
        .sort((a, b) => (a.resolvedAt || 0) - (b.resolvedAt || 0));
    /* Calibration: plays grouped by the price paid. A market price is a claimed
       probability, so a group bought near 30c should win near 30% of the time. */
    const calibration = SC_CAL.map(([lo, hi]) => {
        const d = done.filter(p => p.entry * 100 >= lo && p.entry * 100 < hi);
        const w = d.filter(p => p.status === 'won').length;
        return { lo, hi: Math.min(hi, 100), n: d.length, wins: w,
                 hit: d.length ? r2(w / d.length * 100) : null,
                 avgEntry: d.length ? r2(d.reduce((s, p) => s + p.entry, 0) / d.length * 100) : null };
    });
    /* One unit staked on every resolved play, in the order they resolved. */
    let cum = 0;
    let curve = done.map(p => { cum += ret(p) / 100; return [p.resolvedAt || p.ts, r2(cum), p.status === 'won' ? 1 : 0]; });
    if (curve.length > 500) curve = curve.slice(-500);
    return {
        minSample: SC_MIN_SAMPLE, logged: all.length, since: all.length ? all[0].ts : null,
        calibration, curve,
        overall: agg(all),
        buckets: SC_BUCKETS.map(([lo, hi]) => ({ lo, hi, ...agg(all.filter(p => p.score >= lo && p.score <= hi)) })),
        byCat: CATS.map(c => ({ cat: c, ...agg(all.filter(p => p.cat === c)) })).filter(x => x.n || x.open),
        recent: all.filter(p => p.status !== 'open').sort((a, b) => (b.resolvedAt || 0) - (a.resolvedAt || 0)).slice(0, 60).map(slim),
        pending: all.filter(p => p.status === 'open').sort((a, b) => (a.endMs || 0) - (b.endMs || 0)).slice(0, 60).map(slim),
    };
}

/* ── Manual book ───────────────────────────────────────────────────
   Positions typed in by hand (for an account with no public profile). They
   are stored in data/manual-book.json, valued with the same live prices as
   everything else, and run through the same overlap signals. This is a
   record only: nothing here can place, change or cancel an order. */
let manualBook = [];      // { id, cid, title, slug, outcome, entry, shares, addedAt, lastPrice }
let manualSeq = 0;
let _ctx = null;          // what the last good poll knew, for rebuilding My Book between polls

async function saveManual() {
    try {
        await fs.promises.mkdir(DATA_DIR, { recursive: true });
        await writeAtomic('manual-book.json', { savedAt: Date.now(), seq: manualSeq, rows: manualBook });
    } catch (e) { console.error('  data/  manual book save failed —', e.message || e); }
}
function manualPositions(held, pIdx) {
    return manualBook.map(b => {
        const hm = held[b.cid];
        const i = hm?.outcomes ? hm.outcomes.indexOf(b.outcome) : -1;
        let cur = i >= 0 && hm.prices?.[i] != null ? hm.prices[i] : null;
        if (cur == null) cur = pIdx.get(b.cid)?.[b.outcome] ?? null;
        const known = cur != null;
        if (known) b.lastPrice = cur; else cur = b.lastPrice ?? b.entry;
        return {
            manual: true, id: b.id, asset: 'manual:' + b.id, conditionId: b.cid, title: b.title,
            eventSlug: b.slug, outcome: b.outcome, size: b.shares, avgPrice: b.entry, curPrice: cur,
            currentValue: b.shares * cur, initialValue: b.shares * b.entry,
            cashPnl: b.shares * (cur - b.entry),
            percentPnl: b.entry > 0 ? (cur - b.entry) / b.entry * 100 : 0,
            endDate: hm?.endDate || null, addedAt: b.addedAt, priceKnown: known,
            settled: !!hm?.closed || cur >= 0.9999 || cur <= 0.0001,
        };
    });
}
function addManual(b) {
    const cid = String(b?.cid || ''), outcome = String(b?.outcome || '');
    const entry = Number(b?.entry), shares = Number(b?.shares);
    const lm = liveNow[cid], t = marketIndex.get(cid), hm = heldNow[cid];
    if (!cid || (!lm && !t && !hm)) return { error: 'Pick a market from the search results first.' };
    const outcomes = lm?.outcomes?.length ? lm.outcomes : (tokenIndex.get(cid)?.outcomes || hm?.outcomes || []);
    if (!outcomes.includes(outcome)) return { error: 'That side is not an outcome of this market.' };
    if (!(entry > 0 && entry < 1)) return { error: 'Entry price must be between 0.1c and 99.9c.' };
    if (!(shares > 0) || shares > 1e9) return { error: 'Shares must be a number above zero.' };
    const row = {
        id: ++manualSeq, cid, title: lm?.question || t?.market || hm?.question || '?',
        slug: lm?.eventSlug || lm?.slug || t?.slug || hm?.slug || '', outcome,
        entry: r4(entry), shares: r2(shares), addedAt: Date.now(),
    };
    manualBook.push(row);
    saveManual(); rebuildMe();
    return { ok: true, id: row.id };
}
function removeManual(id) {
    const n = manualBook.length;
    manualBook = manualBook.filter(b => String(b.id) !== String(id));
    if (manualBook.length === n) return { error: 'No such entry.' };
    saveManual(); rebuildMe();
    return { ok: true };
}
/* Rebuild My Book from what the last poll already fetched, so an added or
   removed entry shows up at once instead of after the next poll. */
function rebuildMe() {
    if (!_ctx) return;
    try {
        store.me = buildMe(_ctx.myBook, manualPositions(_ctx.heldMarkets, priceIndex), [], _ctx.positions,
                           _ctx.wmeta, store.allTrades, _ctx.heldMarkets, Date.now());
    } catch (e) { console.error('  me     rebuild failed —', e.message || e); }
    commit();
}
function searchMarkets(text) {
    const words = String(text || '').trim().toLowerCase().split(/\s+/).filter(Boolean);
    if (!words.length || words.join('').length < 2) return [];
    const hit = s => words.every(w => s.includes(w));
    const out = [], seen = new Set();
    for (const lm of Object.values(liveNow)) {
        if (!hit(lm.question.toLowerCase())) continue;
        seen.add(lm.conditionId);
        out.push({ cid: lm.conditionId, title: lm.question, outcomes: lm.outcomes, prices: lm.prices,
                   hoursLeft: lm.endMs ? (lm.endMs - Date.now()) / 3600_000 : null, vol24: Math.round(lm.vol24), live: true });
    }
    for (const t of store.allTrades || []) {
        if (!t.conditionId || seen.has(t.conditionId) || !hit(t.market.toLowerCase())) continue;
        const tk = tokenIndex.get(t.conditionId), pr = priceIndex.get(t.conditionId) || {};
        const outcomes = tk?.outcomes?.filter(Boolean).length === 2 ? tk.outcomes : t.sides.map(x => x.name);
        out.push({ cid: t.conditionId, title: t.market, outcomes, prices: outcomes.map(o => pr[o] ?? null),
                   hoursLeft: t.hoursLeft, vol24: null, live: false });
    }
    out.sort((a, b) => (b.vol24 || 0) - (a.vol24 || 0));
    return out.slice(0, 12);
}

/* Outcome names and token ids for one market. Markets in the live universe
   or held by a tracked trader are already indexed; anything else is asked for. */
async function tokensFor(cid, oi) {
    let tk = tokenIndex.get(cid);
    if (!tk?.tokens?.[oi] && liveNow[cid]?.tokens?.[oi]) {
        tk = { outcomes: [...(liveNow[cid].outcomes || [])], tokens: [...liveNow[cid].tokens] };
        tokenIndex.set(cid, tk);
    }
    if (!tk?.tokens?.[oi]) {
        try {
            const rows = await dataGet('/tokens', { condition: cid });
            tk = tk || { outcomes: [], tokens: [] };
            for (const t of Array.isArray(rows) ? rows : []) {
                const i = t.outcome_index;
                if ((i === 0 || i === 1) && !tk.tokens[i]) { tk.tokens[i] = String(t.token_id); tk.outcomes[i] ||= t.outcome; }
            }
            tokenIndex.set(cid, tk);
        } catch { /* the caller reports the missing token */ }
    }
    return tk;
}

/* ── Order book for one outcome token ──────────────────────────────
   The public CLOB book, fetched only when a market is opened and cached for
   8 seconds. Read-only: nothing here can place or cancel an order. */
const depthCache = new Map();
async function getDepth(cid, oi) {
    oi = oi === 1 ? 1 : 0;
    const key = cid + '|' + oi, hit = depthCache.get(key);
    if (hit && Date.now() - hit.ts < 8_000) return hit.body;
    const tk = await tokensFor(cid, oi);
    const token = tk?.tokens?.[oi];
    if (!token) return { error: 'No outcome token is known for this market, so its order book cannot be requested.', bids: [], asks: [] };
    let b;
    try { b = await apiGet(CLOB_API + '/book', { token_id: token }); }
    catch (e) {
        return { error: /404/.test(String(e.message)) ? 'This market has no open order book. It is closed or resolved.'
                 : 'The order book is unavailable right now (' + (e.message || e) + ').', bids: [], asks: [] };
    }
    const lv = rows => (Array.isArray(rows) ? rows : []).map(r => [r4(num(r.price)), r2(num(r.size))])
        .filter(r => r[0] > 0 && r[1] > 0);
    const bids = lv(b?.bids).sort((x, y) => y[0] - x[0]);      // best (highest) bid first
    const asks = lv(b?.asks).sort((x, y) => x[0] - y[0]);      // best (lowest) ask first
    const usd = rows => r2(rows.reduce((s, r) => s + r[0] * r[1], 0));
    const body = {
        conditionId: cid, oi, outcome: tk.outcomes[oi] || (oi ? 'No' : 'Yes'),
        outcomes: [tk.outcomes[0] || 'Yes', tk.outcomes[1] || 'No'],
        bids: bids.slice(0, 60), asks: asks.slice(0, 60),
        bidLevels: bids.length, askLevels: asks.length, bidUsd: usd(bids), askUsd: usd(asks),
        ts: Date.now(),
    };
    depthCache.set(key, { ts: Date.now(), body });
    if (depthCache.size > 200) depthCache.delete(depthCache.keys().next().value);
    return body;
}

/* ── Large trades ──────────────────────────────────────────────────
   Recent public prints above a dollar size, across all of Polymarket and not
   only the tracked wallets. Fetched while the Now tab is open, cached 20s. */
const TAPE_SIZES = [1000, 5000, 10000, 25000];
const tapeCache = new Map();
async function getTape(min) {
    min = TAPE_SIZES.includes(min) ? min : 5000;
    const hit = tapeCache.get(min);
    if (hit && Date.now() - hit.ts < 20_000) return hit.body;
    let rows;
    try { rows = await dataGet('/trades', { limit: 80, filter_type: 'CASH', filter_amount: min }); }
    catch (e) {
        return hit ? hit.body
            : { min, sizes: TAPE_SIZES, error: 'Recent trades are unavailable right now (' + (e.message || e) + ').', trades: [] };
    }
    const trades = (Array.isArray(rows) ? rows : []).map(t => {
        const raw = String(t.proxy_wallet || ''), w = raw.toLowerCase(), size = num(t.size), price = num(t.price);
        const meta = traderMeta.get(raw) || traderMeta.get(w);
        return {
            ts: num(t.timestamp) * 1000, wallet: meta && traderMeta.has(raw) ? raw : w,
            trader: meta?.userName || t.name || t.pseudonym || (w.slice(0, 6) + '…' + w.slice(-4)),
            top: !!meta, buy: t.side !== 'SELL', side: t.outcome || '', market: t.title || '',
            conditionId: t.condition_id || '', size: r2(size), price: r4(price), notional: r2(size * price),
        };
    }).filter(t => t.conditionId && t.notional > 0);
    const body = { min, sizes: TAPE_SIZES, trades, ts: Date.now() };
    tapeCache.set(min, { ts: Date.now(), body });
    return body;
}

/* ── Price history for one outcome token ───────────────────────────
   data-api v2 /prices-history, keyed by outcome token id. Fetched only when
   a chart is opened, cached for 45 seconds. */
const CHART_RANGES = ['1h', '6h', '1d', '1w', '1m', 'max'];
const chartCache = new Map();
async function getChart(cid, oi, range) {
    if (!CHART_RANGES.includes(range)) range = '1d';
    oi = oi === 1 ? 1 : 0;
    const key = cid + '|' + oi + '|' + range, hit = chartCache.get(key);
    if (hit && Date.now() - hit.ts < 45_000) return hit.body;
    const tk = await tokensFor(cid, oi);
    const token = tk?.tokens?.[oi];
    if (!token) return { error: 'No outcome token is known for this market, so its price history cannot be requested.', points: [] };
    let rows;
    try { rows = await dataGet('/prices-history', { token_id: token, interval: range }); }
    catch (e) { return { error: 'Price history is unavailable right now (' + (e.message || e) + ').', points: [] }; }
    let pts = (Array.isArray(rows) ? rows : []).map(r => [num(r.timestamp), r4(num(r.price))]).filter(p => p[0] > 0);
    if (pts.length > 700) {         // thin evenly; the last real tick is always kept
        const step = pts.length / 700, thin = [];
        for (let i = 0; i < 700; i++) thin.push(pts[Math.floor(i * step)]);
        thin.push(pts[pts.length - 1]);
        pts = thin;
    }
    const body = { conditionId: cid, oi, range, outcome: tk.outcomes[oi] || (oi ? 'No' : 'Yes'),
                   outcomes: [tk.outcomes[0] || 'Yes', tk.outcomes[1] || 'No'], points: pts };
    chartCache.set(key, { ts: Date.now(), body });
    if (chartCache.size > 300) chartCache.delete(chartCache.keys().next().value);
    return body;
}

/* ── Personal positions, overlap alerts and rule-based signals ─────
   Reuses the same positions endpoint, the same isPositionOpen() filter,
   and the same 60s poll as everything else. */
/* Step 1 of 2: resolve the account and load its book. Runs BEFORE the
   position diff so the diff knows which markets are "mine". */
async function fetchMyBook(nowMs) {
    // 1. resolve the account (cached; retried periodically while failing)
    if (!_me.resolved && (nowMs - _me.lastTry) > RESOLVE_RETRY_MS) {
        _me.lastTry = nowMs;
        const r = await resolveAccount(MY_ACCOUNT);
        if (r.wallet) { _me.wallet = r.wallet; _me.resolved = true; _me.error = null;
                        _me.userName = r.userName || null; }
        else { _me.error = r.error; }
    }
    const base = { input: MY_ACCOUNT, wallet: _me.wallet, resolved: _me.resolved,
                   error: _me.error, userName: _me.userName,
                   positions: [], portfolioValue: null, signals: [], alerts: _myAlerts.slice(0, 60) };
    if (!_me.resolved) return { base, mine: [], value: null, activity: [], ok: false };

    // 2. my open positions — same endpoint, same open-filter as every other wallet
    let mine = [], value = null, activity = [];
    try {
        const [pos, val, act] = await Promise.all([
            getPositions(_me.wallet, 250),
            dataGet('/value', { user: _me.wallet }).catch(() => null),
            dataGet('/activity', { user: _me.wallet, limit: 500 }).then(r => mapRows(r, v2Activity)).catch(() => []),
        ]);
        mine = (Array.isArray(pos) ? pos : []).filter(isPositionOpen);
        if (Array.isArray(val) && val[0]) value = num(val[0].value);
        else if (val && typeof val === 'object' && val.value != null) value = num(val.value);
        activity = Array.isArray(act) ? act : [];
    } catch (e) {
        return { base: { ...base, error: 'Could not load positions for this account: ' + (e.message || e) },
                 mine: [], value: null, activity: [], ok: false };
    }
    return { base, mine, value, activity, ok: true };
}

/* Step 2 of 2: overlap alerts and rule-based signals for my positions.
   Alerts come from the shared position diff (see diffPositions), so they
   follow exactly the same rules as Order Flow: no alert for a wallet with
   no baseline, exits confirmed, settled markets ignored. */
function buildMe(book, manualRows, events, topPositions, wmeta, allTrades, heldMarkets, nowMs) {
    const acctErr = book.ok ? null : (book.base?.error || _me.error || null);
    const mine = [...(book.ok ? book.mine : []), ...manualRows];
    if (!mine.length)
        return { ...book.base, error: acctErr, positions: [], signals: [], portfolioValue: null,
                 alerts: _myAlerts.slice(0, 60), manualCount: 0 };
    const value = book.ok ? book.value : null, activity = book.ok ? book.activity : [];

    // earliest BUY per outcome token => how long the position has been open
    const firstBuy = {};
    for (const a of activity) {
        if (!a.asset || String(a.side).toUpperCase() !== 'BUY') continue;
        const ts = num(a.timestamp) * 1000;
        if (!ts) continue;
        if (!firstBuy[a.asset] || ts < firstBuy[a.asset]) firstBuy[a.asset] = ts;
    }

    // 3. which top wallets are in MY markets right now
    const myConds = new Set(mine.map(p => p.conditionId).filter(Boolean));
    for (const cid of myConds)
        _myMktStats[cid] ||= { seen: new Set(), exited: new Set(), entered: new Set(), initialSide: null };
    for (const [w, plist] of Object.entries(topPositions))
        for (const p of plist)
            if (p.conditionId && myConds.has(p.conditionId)) _myMktStats[p.conditionId].seen.add(w);

    // 4. exit / trim / entry alerts from the shared diff
    const fresh = [];
    for (const e of events) {
        if (!myConds.has(e.cid)) continue;
        const st = _myMktStats[e.cid];
        const row = { market: e.title, slug: e.slug, conditionId: e.cid,
                      trader: wmeta[e.wallet]?.userName || (e.wallet.slice(0, 6) + '…' + e.wallet.slice(-4)),
                      wallet: e.wallet, side: e.outcome, hadSize: r2(e.prevSize), nowSize: r2(e.size),
                      delta: r2(e.size - e.prevSize), value: r2(e.value), ts: nowMs };
        if (e.kind === 'EXIT') {
            st.exited.add(e.wallet); st.entered.delete(e.wallet);
            fresh.push({ id: ++_seq, type: 'EXIT', ...row });
        } else if (e.kind === 'TRIM' && e.size < e.prevSize * 0.85 && (e.prevSize - e.size) > 50) {
            fresh.push({ id: ++_seq, type: 'TRIM', ...row });
        } else if (e.kind === 'NEW' && e.size >= 1) {
            st.entered.add(e.wallet); st.exited.delete(e.wallet);
            fresh.push({ id: ++_seq, type: 'ENTRY', ...row });
        }
    }
    if (fresh.length) _myAlerts = [...fresh, ..._myAlerts].slice(0, 80);

    // 5. build each position + its transparent signal
    const positions = [], signals = [];
    for (const p of mine) {
        const cid = p.conditionId || '';
        const lm = cid ? heldMarkets[cid] : null;
        const trade = cid ? allTrades.find(t => t.conditionId === cid) : null;
        const avg = num(p.avgPrice), cur = num(p.curPrice);
        const openedAt = p.manual ? p.addedAt : (p.asset ? (firstBuy[p.asset] || null) : null);

        const row = {
            // `asset` is the outcome token — unique per side. conditionId alone
            // collides when the same wallet holds BOTH sides of a market.
            asset: p.asset || (cid + '|' + (p.outcome || '')),
            title: p.title || '?', outcome: p.outcome || '?', conditionId: cid,
            slug: p.eventSlug || p.slug || '',
            size: num(p.size), avgPrice: r4(avg), curPrice: r4(cur),
            currentValue: r2(num(p.currentValue)), initialValue: r2(num(p.initialValue)),
            cashPnl: r2(num(p.cashPnl)), percentPnl: r2(num(p.percentPnl)),
            openedAt, heldMs: openedAt ? nowMs - openedAt : null,
            endDate: lm?.endDate || p.endDate || null,
            hoursLeft: lm?.endMs ? (lm.endMs - nowMs) / 3600_000 : null,
            manual: !!p.manual, id: p.manual ? p.id : null,
            settled: p.manual ? p.settled : false, priceKnown: p.manual ? p.priceKnown : true,
        };
        positions.push(row);

        // ── signal: counts only, every number surfaced ──
        const st = _myMktStats[cid] || { seen: new Set(), exited: new Set(), entered: new Set() };
        const sides = trade?.sides || [];
        const total = sides.reduce((s, x) => s + x.count, 0);
        const sameSide = sides.find(s => s.name === row.outcome);
        const sameN = sameSide?.count || 0;
        const oppN = total - sameN;
        const topSide = sides[0]?.name || null;
        if (st.initialSide == null && topSide) st.initialSide = topSide;
        const shifted = !!(st.initialSide && topSide && st.initialSide !== topSide);
        const exited = st.exited.size, entered = st.entered.size;
        // average entry of top wallets on my side
        let avgTop = null;
        if (sameSide?.traders?.length)
            avgTop = r4(sameSide.traders.reduce((s, t) => s + t.avgPrice, 0) / sameSide.traders.length);

        let flag = 'MIXED';
        if (entered > exited && sameN >= oppN) flag = 'BUILDING';
        else if (exited > entered || (shifted && oppN > sameN)) flag = 'EXITING';

        const facts = [];
        facts.push(`${sameN} top wallet${sameN === 1 ? '' : 's'} on your side (${row.outcome})` +
                   (oppN ? `, ${oppN} on the other side` : ''));
        facts.push(exited ? `${exited} exited since tracking began` : 'none have exited since tracking began');
        if (entered) facts.push(`${entered} newly entered since tracking began`);
        if (shifted) facts.push(`consensus moved from ${st.initialSide} to ${topSide}`);
        else if (topSide) facts.push(`consensus side is ${topSide}`);
        facts.push(`your entry ${(avg * 100).toFixed(1)}c vs now ${(cur * 100).toFixed(1)}c`);
        if (avgTop != null) facts.push(`top wallets on your side entered at ~${(avgTop * 100).toFixed(1)}c`);

        signals.push({
            asset: row.asset, conditionId: cid, title: row.title, outcome: row.outcome, flag,
            topSameSide: sameN, topOppSide: oppN, topTotal: total,
            exitedCount: exited, enteredCount: entered,
            consensusSide: topSide, initialSide: st.initialSide, consensusShifted: shifted,
            myEntry: r4(avg), curPrice: r4(cur), avgTopEntry: avgTop,
            facts,
        });
    }

    const manualValue = manualRows.reduce((s, p) => s + p.currentValue, 0);
    return { input: MY_ACCOUNT, wallet: _me.wallet, resolved: _me.resolved, error: acctErr,
             userName: _me.userName, positions,
             portfolioValue: value == null ? r2(positions.reduce((s, p) => s + p.currentValue, 0))
                                           : r2(value + manualValue),
             signals, alerts: _myAlerts.slice(0, 60), manualCount: manualRows.length };
}

async function poll() {
    if (polling) return;
    polling = true;
    _statusLog = {};
    try {
        const nowMs = Date.now();

        // 1. Leaderboards — 6 views in parallel
        const cfgs = [];
        for (const p of ['DAY', 'WEEK', 'ALL'])
            for (const o of ['PNL', 'VOL'])
                cfgs.push({ key: `${p}_${o}`, params: { time_period: p.toLowerCase(),
                                                     sort_by: o === 'VOL' ? 'VOLUME' : 'PNL', limit: 25 } });

        const [lbResults, liveMarkets] = await Promise.all([
            Promise.allSettled(cfgs.map(c => dataGet('/leaderboard', c.params).then(r => mapRows(r, v2Leader)))),
            fetchLiveMarkets().catch(() => ({})),
        ]);

        const boards = {};
        cfgs.forEach((c, i) => {
            const r = lbResults[i];
            if (r.status === 'fulfilled' && Array.isArray(r.value)) boards[c.key] = r.value;
        });
        if (!Object.keys(boards).length) {
            store.pollFailed = true; store.error = describeFailure(); commit(); return;
        }

        // 2. Unique wallets, and which kind of leaderboard each one came from
        const wmeta = {};
        for (const [key, entries] of Object.entries(boards)) {
            const onVol = key.endsWith('_VOL');
            for (const e of entries) {
                const w = e.proxyWallet || '';
                if (!w) continue;
                const m = (wmeta[w] ||= { userName: e.userName || '', profileImage: e.profileImage || '',
                                          pnl: false, vol: false });
                if (onVol) m.vol = true; else m.pnl = true;
            }
        }
        for (const m of Object.values(wmeta)) m.src = m.pnl && m.vol ? 'BOTH' : m.pnl ? 'PNL' : 'VOL';
        const wallets = Object.keys(wmeta);

        // 3. Positions (5 concurrent)
        const posResults = await mapPool(wallets, 5, async (w) => {
            try {
                // ~50% of returned rows are settled and get filtered out, so fetch
                // deeper to keep a meaningful book of genuinely-open positions
                const r = await getPositionsPage(w, POS_LIMIT);
                return { wallet: w, positions: r.rows, ok: true, truncated: r.truncated };
            } catch { return { wallet: w, positions: [], ok: false, truncated: false }; }
        });
        const rawPositions = {}, fetchOk = {}, truncated = {}, rawKeys = {};
        for (const r of posResults) {
            if (!r) continue;
            rawPositions[r.wallet] = r.positions;
            fetchOk[r.wallet] = r.ok;
            truncated[r.wallet] = r.truncated;
            rawKeys[r.wallet] = new Set(r.positions.map(posKey));   // includes settled rows
        }
        const fetchFailed = wallets.filter(w => !fetchOk[w]).length;

        // 3b. Filter to genuinely-open positions ONCE, here, so every downstream
        //     section shares the same truth and cannot drift out of sync.
        const positions = {};
        let rawCount = 0, openCount = 0;
        for (const [w, plist] of Object.entries(rawPositions)) {
            rawCount += plist.length;
            positions[w] = plist.filter(isPositionOpen);
            openCount += positions[w].length;
        }
        const filteredOut = rawCount - openCount;

        // 3c. Resolve accurate end dates for the OPEN markets only (used for
        //     countdowns, not for filtering — most sit outside the volume sweep).
        const heldIds = [...new Set(
            Object.values(positions).flat().map(p => p.conditionId)
                .concat(manualBook.map(b => b.cid)).filter(Boolean)
        )].filter(c => !liveMarkets[c]);
        const heldMarkets = await fetchMarketsByCondition(heldIds);
        for (const [cid, lm] of Object.entries(liveMarkets)) {
            heldMarkets[cid] = {
                conditionId: cid, closed: false, active: true,
                endDate: lm.endDate, endMs: lm.endMs, acceptingOrders: lm.acceptingOrders,
                question: lm.question, slug: lm.eventSlug || lm.slug, outcomes: lm.outcomes,
                prices: lm.prices, tokens: lm.tokens, cat: lm.cat,
            };
        }

        // 3d. Outcome token ids and last known prices per market (charts, manual book, search)
        const tIdx = new Map(), pIdx = new Map();
        for (const plist of Object.values(rawPositions)) {
            for (const p of plist) {
                const cid = p.conditionId, i = p.outcomeIndex;
                if (!cid) continue;
                if (i === 0 || i === 1) {
                    const t = tIdx.get(cid) || { outcomes: [], tokens: [] };
                    if (!t.outcomes[i] && p.outcome) t.outcomes[i] = p.outcome;
                    if (!t.tokens[i] && p.asset) t.tokens[i] = String(p.asset);
                    if (!t.outcomes[1 - i] && p.oppositeOutcome) t.outcomes[1 - i] = p.oppositeOutcome;
                    if (!t.tokens[1 - i] && p.oppositeAsset) t.tokens[1 - i] = String(p.oppositeAsset);
                    tIdx.set(cid, t);
                }
                if (p.outcome && p.curPrice != null) {
                    if (!pIdx.has(cid)) pIdx.set(cid, {});
                    pIdx.get(cid)[p.outcome] = r4(num(p.curPrice));
                }
            }
        }
        for (const hm of Object.values(heldMarkets)) {         // Gamma is the fresher source where it has the market
            if (!hm.outcomes?.length) continue;
            const t = tIdx.get(hm.conditionId) || { outcomes: [], tokens: [] };
            hm.outcomes.forEach((name, i) => { t.outcomes[i] = name; if (hm.tokens?.[i]) t.tokens[i] = hm.tokens[i]; });
            tIdx.set(hm.conditionId, t);
            if (!pIdx.has(hm.conditionId)) pIdx.set(hm.conditionId, {});
            hm.outcomes.forEach((name, i) => { if (hm.prices?.[i] != null) pIdx.get(hm.conditionId)[name] = r4(hm.prices[i]); });
        }

        // 4. Enrich leaderboard entries
        const enriched = {};
        for (const [key, entries] of Object.entries(boards)) {
            const pm = _prev[key] || {};
            enriched[key] = entries.map(e => {
                const w = e.proxyWallet || '';
                const pnl = num(e.pnl), vol = num(e.vol);
                const rank = parseInt(e.rank, 10) || 0;
                // `pos` is already filtered to genuinely-open positions
                const pos = positions[w] || [];
                const rawN = (rawPositions[w] || []).length;
                const top3 = [...pos].sort((a, b) => num(b.currentValue) - num(a.currentValue)).slice(0, 3);
                const exposure = pos.reduce((s, p) => s + num(p.currentValue), 0);
                const unreal = pos.reduce((s, p) => s + num(p.cashPnl), 0);
                return {
                    rank, wallet: w,
                    userName: e.userName || (w ? w.slice(0, 6) + '…' + w.slice(-4) : '?'),
                    pnl, vol,
                    roi: vol ? r2(pnl / vol * 100) : 0,   // PnL divided by volume, in percent
                    src: wmeta[w]?.src || 'PNL',          // which kind of board this wallet is on
                    positionCount: pos.length,          // open only — post-filter
                    settledHidden: rawN - pos.length,   // resolved rows we suppressed
                    exposure: r2(exposure),
                    unrealized: r2(unreal),
                    topPositions: top3.map(p => ({
                        title: p.title || '?', outcome: p.outcome || '?',
                        size: num(p.size), avgPrice: r4(num(p.avgPrice)), curPrice: r4(num(p.curPrice)),
                    })),
                    rankChange: pm[w] ? pm[w].rank - rank : 0,
                    pnlChange: pm[w] ? r2(pnl - pm[w].pnl) : 0,
                };
            });
        }

        const tMeta = new Map();
        for (const [key, rows] of Object.entries(enriched)) {
            for (const e of rows) {
                const m = tMeta.get(e.wallet) || {
                    wallet: e.wallet, userName: e.userName, src: e.src, exposure: e.exposure,
                    unrealized: e.unrealized, positionCount: e.positionCount,
                    settledHidden: e.settledHidden, boards: {},
                };
                m.boards[key] = { rank: e.rank, pnl: e.pnl, vol: e.vol, roi: e.roi };
                tMeta.set(e.wallet, m);
            }
        }

        const newPrev = {};
        for (const [key, entries] of Object.entries(boards)) {
            newPrev[key] = {};
            for (const e of entries)
                newPrev[key][e.proxyWallet || ''] = { rank: parseInt(e.rank, 10) || 0, pnl: num(e.pnl) };
        }
        _prev = newPrev;

        let topTrader = null;
        if (enriched.ALL_PNL?.length) {
            const t = enriched.ALL_PNL[0];
            topTrader = {
                userName: t.userName, wallet: t.wallet, pnl: t.pnl, vol: t.vol,
                positionCount: t.positionCount, exposure: t.exposure, unrealized: t.unrealized,
                topPosition: t.topPositions[0] || null,
            };
        }

        // 5. Market aggregation (keyed by conditionId when available)
        const mkt = {};
        for (const [w, plist] of Object.entries(positions)) {
            const tname = wmeta[w]?.userName || (w.slice(0, 6) + '…' + w.slice(-4));
            for (const p of plist) {
                const cid = p.conditionId || '';
                const key = cid || p.eventSlug || p.title || '?';
                const outcome = p.outcome || '?';
                if (!mkt[key])
                    mkt[key] = {
                        market: p.title || '?', slug: p.eventSlug || p.slug || '',
                        conditionId: cid,
                        endDate: heldMarkets[cid]?.endDate || p.endDate || null,
                        walletOutcomes: {}, outcomeTraders: {}, totalSize: 0, totalValue: 0,
                    };
                const m = mkt[key];
                if (!m.walletOutcomes[w]) m.walletOutcomes[w] = new Set();
                m.walletOutcomes[w].add(outcome);
                if (!m.outcomeTraders[outcome]) m.outcomeTraders[outcome] = {};
                if (!m.outcomeTraders[outcome][w])
                    m.outcomeTraders[outcome][w] = {
                        name: tname, wallet: w, size: 0, currentValue: 0,
                        avgPrice: num(p.avgPrice), curPrice: num(p.curPrice), cashPnl: 0,
                    };
                const td = m.outcomeTraders[outcome][w];
                td.size += num(p.size);
                td.currentValue += num(p.currentValue);
                td.cashPnl += num(p.cashPnl);
                m.totalSize += num(p.size);
                m.totalValue += num(p.currentValue);
            }
        }

        const allTrades = Object.values(mkt).map(m => {
            const lm = m.conditionId ? liveMarkets[m.conditionId] : null;
            const hm = m.conditionId ? heldMarkets[m.conditionId] : null;
            // wallets holding more than one outcome of this market: flagged, never counted
            const hedged = new Set(Object.entries(m.walletOutcomes)
                .filter(([, set]) => set.size > 1).map(([w]) => w));
            const sides = Object.entries(m.outcomeTraders).map(([outcome, tmap]) => {
                const all = Object.values(tmap), clean = all.filter(t => !hedged.has(t.wallet));
                return {
                    name: outcome,
                    count: clean.length,
                    value: r2(clean.reduce((s, t) => s + t.currentValue, 0)),
                    hedged: all.length - clean.length,
                    traders: all.map(t => ({
                        name: t.name, wallet: t.wallet,
                        size: r2(t.size), avgPrice: r4(t.avgPrice), curPrice: r4(t.curPrice),
                        currentValue: r2(t.currentValue), cashPnl: r2(t.cashPnl),
                        hedged: hedged.has(t.wallet), src: wmeta[t.wallet]?.src || 'PNL',
                    })).sort((a, b) => b.size - a.size),
                };
            }).sort((a, b) => b.count - a.count || b.value - a.value);
            return {
                // `key` identifies exactly one market. Event slugs are shared by up to
                // dozens of markets (O/U 7.5, 8.5, ...), so they can never be a key.
                key: m.conditionId || ('x:' + m.slug + '|' + m.market),
                market: m.market, slug: m.slug, conditionId: m.conditionId,
                traderCount: Object.keys(m.walletOutcomes).length, hedgedCount: hedged.size, sides,
                cat: hm?.cat || categorize([], (m.slug || '').replace(/-/g, ' ') + ' ' + m.market),
                totalSize: r2(m.totalSize), totalValue: r2(m.totalValue),
                endDate: lm?.endDate || m.endDate || null,
                // most held markets are outside the volume sweep — fall back to the
                // per-condition lookup so every row has a real countdown
                hoursLeft: lm?.hoursLeft ?? (hm?.endMs ? (hm.endMs - nowMs) / 3600_000 : null),
                live: lm ? {
                    bid: lm.bestBid, ask: lm.bestAsk, spread: lm.spread,
                    vol24: lm.vol24, liquidity: lm.liquidity,
                    oneHour: lm.oneHour, oneDay: lm.oneDay,
                    outcomes: lm.outcomes, prices: lm.prices,
                    acceptingOrders: lm.acceptingOrders,
                } : null,
            };
        }).sort((a, b) => b.traderCount - a.traderCount);

        // most-crowded first, but markets still open for trading ahead of ones waiting on a result
        const crowded = [...allTrades].sort((a, b) =>
            (isEnded(a) - isEnded(b)) || (b.traderCount - a.traderCount)).slice(0, 12);
        const consensus = crowded.map(t => {
            const s1 = t.sides[0];
            return {
                key: t.key, conditionId: t.conditionId, cat: t.cat,
                market: t.market, slug: t.slug, traderCount: t.traderCount, hedgedCount: t.hedgedCount,
                sides: t.sides.map(sideSummary),
                usdPct: Math.round(usdShare(t.sides) * 100),
                // what the leading side paid on average, and what the same side costs now
                paid: paidBy(s1),
                payNow: payNowFor(t.live, s1?.name) ?? (s1?.traders?.[0]?.curPrice ?? null),
                totalSize: t.totalSize, totalValue: t.totalValue,
                hoursLeft: t.hoursLeft, live: t.live,
            };
        });

        // 6. Trader positions, enriched with live tradability
        const traderPositions = {};
        for (const [w, plist] of Object.entries(positions)) {
            traderPositions[w] = plist.map(p => {
                const lm = p.conditionId ? liveMarkets[p.conditionId] : null;
                const hm = p.conditionId ? heldMarkets[p.conditionId] : null;
                const avg = num(p.avgPrice), cur = num(p.curPrice);
                return {
                    title: p.title || '?', outcome: p.outcome || '?',
                    size: num(p.size), avgPrice: r4(avg), curPrice: r4(cur),
                    currentValue: r2(num(p.currentValue)),
                    cashPnl: r2(num(p.cashPnl)), percentPnl: r2(num(p.percentPnl)),
                    slug: p.eventSlug || p.slug || '', conditionId: p.conditionId || '',
                    cat: hm?.cat || 'Other',
                    endDate: lm?.endDate || hm?.endDate || p.endDate || null,
                    hoursLeft: lm?.hoursLeft ?? (hm?.endMs ? (hm.endMs - nowMs) / 3600_000 : null),
                    bid: lm?.bestBid ?? null, ask: lm?.bestAsk ?? null,
                    spread: lm?.spread ?? null, vol24: lm?.vol24 ?? null,
                    oneHour: lm?.oneHour ?? null, oneDay: lm?.oneDay ?? null,
                };
            }).sort((a, b) => b.currentValue - a.currentValue);
        }

        // 7. PnL history
        const recorded = new Set();
        for (const [key, entries] of Object.entries(boards)) {
            const per = key.split('_')[0];
            for (const e of entries) {
                const w = e.proxyWallet || '';
                if (!w) continue;
                const hk = `${per}|${w}`;
                if (recorded.has(hk)) continue;
                recorded.add(hk);
                (pnlHistory[hk] ||= []).push({ ts: nowMs, pnl: num(e.pnl) });
                if (pnlHistory[hk].length > 240) pnlHistory[hk].shift();
            }
        }

        // 8. Price history per market (intraday sparklines)
        for (const [cid, lm] of Object.entries(liveMarkets)) {
            const px = lm.prices?.[0];
            if (px == null || !isFinite(px)) continue;
            (priceHistory[cid] ||= []).push(r4(px));
            if (priceHistory[cid].length > 240) priceHistory[cid].shift();
            priceSeen[cid] = nowMs;
        }
        // drop series for markets that left the live universe (keeps memory and the save file bounded)
        for (const cid of Object.keys(priceHistory))
            if (nowMs - (priceSeen[cid] || 0) > 6 * 3600_000) { delete priceHistory[cid]; delete priceSeen[cid]; }
        for (const hk of Object.keys(pnlHistory)) {
            const h = pnlHistory[hk];
            if (!h.length || nowMs - h[h.length - 1].ts > PNL_MAX_GAP) delete pnlHistory[hk];
        }

        // 9. Position change detection (rules documented at diffPositions)
        let myBook;
        try { myBook = await fetchMyBook(nowMs); }
        catch (e) {
            myBook = { ok: false, mine: [], value: null, activity: [],
                       base: { input: MY_ACCOUNT, wallet: _me.wallet, resolved: _me.resolved,
                               error: 'Personal tracking failed: ' + (e.message || e), userName: _me.userName,
                               positions: [], portfolioValue: null, signals: [], alerts: _myAlerts.slice(0, 60) } };
        }
        const manualRows = manualPositions(heldMarkets, pIdx);
        const myConds = new Set([...myBook.mine, ...manualRows].map(p => p.conditionId).filter(Boolean));

        const curSnap = buildSnapshot(wallets, positions, fetchOk, truncated, nowMs);
        const diff = await diffPositions(_snap, curSnap, rawKeys, nowMs, myConds);
        const diffedWallets = Object.keys(curSnap)
            .filter(w => _snap[w] && nowMs - _snap[w].ts <= SNAP_MAX_AGE);

        const newChanges = [];
        for (const e of diff.events) {
            const lm = e.cid ? liveMarkets[e.cid] : null;
            const base = {
                trader: wmeta[e.wallet]?.userName || (e.wallet.slice(0, 6) + '…' + e.wallet.slice(-4)),
                wallet: e.wallet, market: e.title, slug: e.slug, conditionId: e.cid,
                side: e.outcome, price: e.price, hoursLeft: lm?.hoursLeft ?? null, ts: nowMs,
            };
            const d = Math.abs(e.size - e.prevSize);
            if (e.kind === 'NEW' && e.size > 50)
                newChanges.push({ ...base, type: 'NEW', size: r2(e.size), notional: r2(e.size * e.price) });
            else if (e.kind === 'ADD' && e.size > e.prevSize * 1.15 && d > 200)
                newChanges.push({ ...base, type: 'ADD', size: r2(d), notional: r2(d * e.price) });
            else if (e.kind === 'TRIM' && e.size < e.prevSize * 0.85 && d > 200)       // partial reduction
                newChanges.push({ ...base, type: 'TRIM', size: r2(d), notional: r2(d * e.price) });
            else if (e.kind === 'EXIT' && e.prevSize > 50)                              // full close
                newChanges.push({ ...base, type: 'EXIT', size: r2(e.prevSize), notional: r2(e.prevSize * e.price) });
        }

        // Consensus flips. Counted only across wallets present in BOTH snapshots, so a
        // wallet joining or leaving the leaderboard cannot fake a flip, and a tie on
        // either side of the comparison is not treated as a consensus side.
        if (diffedWallets.length) {
            const was = leadBySide(_snap, diffedWallets), now = leadBySide(curSnap, diffedWallets);
            const byCid = {};
            for (const t of allTrades) if (t.conditionId) byCid[t.conditionId] = t;
            for (const [cid, n] of Object.entries(now)) {
                const p = was[cid], t = byCid[cid];
                if (!p || !t) continue;
                if (n.sides < 2 || p.sides < 2 || n.total < 3 || p.total < 3) continue;
                if (!n.strict || !p.strict || n.lead === p.lead) continue;
                if (t.hoursLeft != null && t.hoursLeft <= 0) continue;   // already past its end
                newChanges.push({
                    type: 'SHIFT', trader: '', wallet: '', market: t.market,
                    slug: t.slug, conditionId: cid, side: n.lead, prevSide: p.lead,
                    size: t.traderCount, price: 0, notional: t.totalValue,
                    hoursLeft: t.hoursLeft, ts: nowMs,
                });
            }
        }
        _snap = curSnap;

        if (newChanges.length) {
            newChanges.sort((a, b) => (b.notional || 0) - (a.notional || 0));
            for (const c of newChanges) c.id = ++_seq;
            liveChanges = [...newChanges, ...liveChanges].slice(0, 120);
        }

        // 10. Smart-money index by conditionId — who holds what, which side
        const smart = {};
        for (const t of allTrades) {
            if (!t.conditionId) continue;
            const top = t.sides[0];
            const cnt = t.sides.reduce((s, x) => s + x.count, 0);       // wallets on one side only
            const usd = t.sides.reduce((s, x) => s + x.value, 0);       // and their dollars
            const richest = [...t.sides].sort((a, b) => b.value - a.value)[0];
            smart[t.conditionId] = {
                traderCount: t.traderCount,
                topSide: cnt && top ? top.name : null,
                topSideCount: cnt && top ? top.count : 0,
                totalCount: cnt,
                value: t.totalValue,
                topSideUsd: cnt && top ? top.value : 0,
                usdPct: cnt && usd > 0 ? Math.round(top.value / usd * 100) : 0,
                usdSide: usd > 0 && richest ? richest.name : null,
                hedged: t.hedgedCount,
                sides: t.sides.map(sideSummary),
            };
        }

        /* ── 11. HOURLY PLAYS — the short-trade engine ────────────── */
        const hourlyPlays = [];
        for (const lm of Object.values(liveMarkets)) {
            const hl = lm.hoursLeft;
            if (hl == null || hl <= 0.15 || hl > 48) continue;        // resolving, but not already done
            if (!lm.acceptingOrders) continue;
            if (lm.liquidity < 3000) continue;                         // must be fillable
            if (lm.spread == null || lm.spread > 0.06) continue;       // must be cheap to enter
            if (lm.vol24 < 8000) continue;                             // must be active
            if (!lm.outcomes.length || !lm.prices.length) continue;

            const sm = smart[lm.conditionId];
            const mom = lm.oneHour == null ? 0 : lm.oneHour;
            const dayMom = lm.oneDay == null ? 0 : lm.oneDay;

            // pick the side to recommend
            let sideIdx = 0;
            if (sm?.topSide) {
                const i = lm.outcomes.findIndex(o => o === sm.topSide);
                if (i >= 0) sideIdx = i;
                else sideIdx = mom >= 0 ? 0 : 1;
            } else {
                sideIdx = mom >= 0 ? 0 : 1;
            }
            const sideName = lm.outcomes[sideIdx] ?? 'Yes';
            const px = lm.prices[sideIdx];
            if (px == null || px <= 0.02 || px >= 0.98) continue;      // no edge at the extremes

            // directional momentum for the chosen side
            const sideMom = sideIdx === 0 ? mom : -mom;
            const sideDayMom = sideIdx === 0 ? dayMom : -dayMom;

            // ── scoring ──
            // urgency: sooner = better for a short trade, peak 1–8h
            const urgency = hl <= 1 ? 0.75 : hl <= 3 ? 1 : hl <= 8 ? 0.92
                          : hl <= 16 ? 0.7 : hl <= 28 ? 0.5 : 0.32;
            // momentum: 1h move in the recommended direction
            const momScore = Math.max(0, Math.min(sideMom / 0.05, 1));
            // tradability: tight spread + deep book
            const spreadScore = Math.max(0, 1 - lm.spread / 0.06);
            const liqScore = Math.min(Math.log10(Math.max(lm.liquidity, 1)) / 5.2, 1);
            const volScore = Math.min(Math.log10(Math.max(lm.vol24, 1)) / 6, 1);
            // smart money confirmation: how many wallets, how lopsided by wallet count,
            // and how lopsided by dollars. Wallets on both sides are already excluded.
            let smScore = 0, agreePct = 0;
            const aligned = !!(sm && sm.totalCount > 0 && sm.topSide === sideName);
            if (sm && sm.totalCount > 0) {
                agreePct = Math.round(sm.topSideCount / sm.totalCount * 100);
                if (aligned)
                    smScore = Math.min(sm.topSideCount / 8, 1) *
                              (0.5 * sm.topSideCount / sm.totalCount + 0.5 * sm.usdPct / 100);
            }
            // the six components, in points, in the order the page draws them
            const parts = [urgency * 26, momScore * 22, spreadScore * 14,
                           liqScore * 12, volScore * 10, smScore * 16].map(x => Math.round(x * 10) / 10);
            const score = Math.round(
                urgency * 26 + momScore * 22 + spreadScore * 14 +
                liqScore * 12 + volScore * 10 + smScore * 16
            );
            if (score < 30) continue;

            // cost to trade right now
            const ask = lm.bestAsk, bid = lm.bestBid;
            // bid/ask are quoted for outcome 1. Buying outcome 2 means selling outcome 1
            // into the bid, so entry needs the bid; selling outcome 2 needs the ask.
            const entry = sideIdx === 0 ? ask : (bid != null ? r4(1 - bid) : null);
            const exitP = sideIdx === 0 ? bid : (ask != null ? r4(1 - ask) : null);
            const roundTrip = (entry != null && exitP != null) ? r4(entry - exitP) : lm.spread;
            // payoff if it resolves your way
            const upside = entry != null && entry > 0 ? r2((1 - entry) / entry * 100) : null;

            const why = [];
            if (hl <= 3) why.push(`Resolves in ${hl < 1 ? Math.round(hl * 60) + 'm' : hl.toFixed(1) + 'h'}`);
            else why.push(`Resolves in ${hl.toFixed(1)}h`);
            if (Math.abs(sideMom) >= 0.005)
                why.push(`${sideMom > 0 ? 'Up' : 'Down'} ${Math.abs(sideMom * 100).toFixed(1)}c in the last hour`);
            if (aligned && sm.topSideCount >= 2)
                why.push(`${sm.topSideCount} of ${sm.totalCount} top traders on ${sideName}, ${sm.usdPct}% of their dollars`);
            else if (sm && sm.topSide && sm.topSide !== sideName)
                why.push(`Note: smart money leans ${sm.topSide}`);
            if (lm.spread != null) why.push(`Spread ${(lm.spread * 100).toFixed(1)}c`);
            why.push(`24h volume ${lm.vol24 >= 1e6 ? '$' + (lm.vol24 / 1e6).toFixed(1) + 'M' : '$' + Math.round(lm.vol24 / 1000) + 'K'}`);

            // cautions, generated from the same numbers. "hard" ones keep a play out of Top candidate.
            const cautions = [];
            if (hl < 0.25) cautions.push({ k: 'time', hard: true, t: 'Closes in under 15 minutes' });
            if (upside != null && upside < 15) cautions.push({ k: 'pay', hard: true, t: 'Pays under 15 percent' });
            if ((roundTrip != null && roundTrip >= 0.04) || lm.spread >= 0.04)
                cautions.push({ k: 'spread', hard: true, t: 'Wide spread' });
            if (sideMom <= -0.01) cautions.push({ k: 'against', hard: true, t: 'Price is moving against this side' });
            if (sm && sm.totalCount >= 2 && !aligned)
                cautions.push({ k: 'split', hard: false, t: 'Top traders lean ' + sm.topSide });
            else if (sm && sm.totalCount >= 2 && (agreePct < 65 || (sm.usdSide && sm.usdSide !== sideName)))
                cautions.push({ k: 'split', hard: false, t: 'Top traders split' });
            else if (!sm || !sm.totalCount)
                cautions.push({ k: 'nosm', hard: false, t: 'No top traders positioned' });
            if (lm.liquidity < 10000) cautions.push({ k: 'thin', hard: false, t: 'Thin book' });

            hourlyPlays.push({
                parts, cautions, cat: lm.cat,
                smartUsdPct: sm?.usdPct || 0, smartUsd: sm?.topSideUsd || 0, smartHedged: sm?.hedged || 0,
                conditionId: lm.conditionId, market: lm.question,
                slug: lm.eventSlug || lm.slug, side: sideName, sideIdx,
                price: r4(px), entry, exit: exitP, spread: lm.spread,
                roundTripCost: roundTrip, upsidePct: upside,
                hoursLeft: r2(hl), endDate: lm.endDate,
                oneHour: lm.oneHour, oneDay: lm.oneDay,
                sideMom: r4(sideMom), sideDayMom: r4(sideDayMom),
                vol24: Math.round(lm.vol24), liquidity: Math.round(lm.liquidity),
                score, why,
                smartCount: sm?.topSideCount || 0, smartTotal: sm?.totalCount || 0,
                smartSide: sm?.topSide || null, smartValue: sm?.value || 0,
                agreePct,
                spark: (priceHistory[lm.conditionId] || []).slice(-60),
            });
        }
        hourlyPlays.sort((a, b) => b.score - a.score);

        /* ── 11b. TOP CANDIDATE — the best play that clears a minimum bar, or none ── */
        const TOP_BAR = 50;
        const cand = hourlyPlays.find(p => p.score >= TOP_BAR && p.entry != null && !p.cautions.some(c => c.hard));
        let top;
        if (cand) {
            const smLine = cand.smartSide === cand.side && cand.smartCount >= 2
                ? `${cand.smartCount} of ${cand.smartTotal} top traders hold this side, with ${cand.smartUsdPct}% of their dollars in the market.`
                : cand.smartTotal ? 'Top traders are in this market but not clearly on this side.'
                : 'No tracked top trader is in this market, so the score rests on timing, momentum and the order book.';
            top = {
                conditionId: cand.conditionId,
                verdict: `Buying ${cand.side} costs ${cents(cand.entry)} and pays +${Math.round(cand.upsidePct)}% if it resolves that way. ` +
                         `It closes in ${durTxt(cand.hoursLeft)}. ${smLine} ` +
                         `At this price it has to be right more than ${Math.round(cand.entry * 100)}% of the time to make money.`,
            };
        } else {
            const best = hourlyPlays[0];
            top = { none: true, bar: TOP_BAR, best: best ? {
                conditionId: best.conditionId, market: best.market, side: best.side, score: best.score,
                blockers: best.cautions.filter(c => c.hard).map(c => c.t),
            } : null };
        }

        /* ── 12. MOMENTUM MOVERS — biggest 1h moves worth watching ── */
        const momentumMovers = Object.values(liveMarkets)
            .filter(m => m.oneHour != null && Math.abs(m.oneHour) >= 0.01
                      && m.vol24 >= 20000 && m.liquidity >= 2000 && m.acceptingOrders
                      && (m.hoursLeft == null || m.hoursLeft > 0.1)
                      // exclude effectively-settled markets — a 95c move to 0 is not tradeable
                      && m.prices[0] != null && m.prices[0] > 0.03 && m.prices[0] < 0.97)
            .map(m => {
                const sm = smart[m.conditionId];
                const up = m.oneHour > 0;
                return {
                    conditionId: m.conditionId, market: m.question,
                    slug: m.eventSlug || m.slug,
                    outcome: m.outcomes[0] || 'Yes',
                    price: m.prices[0] ?? null,
                    oneHour: m.oneHour, oneDay: m.oneDay,
                    direction: up ? 'UP' : 'DOWN',
                    vol24: Math.round(m.vol24), liquidity: Math.round(m.liquidity),
                    spread: m.spread, hoursLeft: m.hoursLeft == null ? null : r2(m.hoursLeft),
                    smartCount: sm?.traderCount || 0, smartSide: sm?.topSide || null,
                    cat: m.cat,
                    spark: (priceHistory[m.conditionId] || []).slice(-60),
                };
            })
            .sort((a, b) => Math.abs(b.oneHour) - Math.abs(a.oneHour))
            .slice(0, 20);

        /* ── 13. RESOLVING SOON — where top traders have skin in ──── */
        const resolvingSoon = allTrades
            .filter(t => t.hoursLeft != null && t.hoursLeft > 0 && t.hoursLeft <= 24 && t.totalValue > 200)
            .map(t => {
                const lm = t.conditionId ? liveMarkets[t.conditionId] : null;
                const top = t.sides[0];
                return {
                    market: t.market, slug: t.slug, conditionId: t.conditionId,
                    hoursLeft: r2(t.hoursLeft), endDate: t.endDate,
                    traderCount: t.traderCount, totalValue: t.totalValue,
                    topSide: top?.name || '?', topCount: top?.count || 0,
                    totalCount: t.sides.reduce((s, x) => s + x.count, 0),
                    sides: t.sides.map(sideSummary),
                    usdPct: Math.round(usdShare(t.sides) * 100), cat: t.cat,
                    price: lm?.prices?.[0] ?? null, spread: lm?.spread ?? null,
                    oneHour: lm?.oneHour ?? null, vol24: lm ? Math.round(lm.vol24) : null,
                    spark: (priceHistory[t.conditionId] || []).slice(-60),
                };
            })
            .sort((a, b) => a.hoursLeft - b.hoursLeft)
            .slice(0, 20);

        /* ── 14. Conviction plays ─────────────────────────────────── */
        const walletStats = {};
        for (const [w, plist] of Object.entries(positions)) {
            const vals = plist.map(p => num(p.currentValue)).filter(v => v > 0);
            if (vals.length) walletStats[w] = { avg: vals.reduce((a, b) => a + b, 0) / vals.length, count: vals.length };
        }
        const convictionPlays = allTrades
            .filter(t => t.traderCount >= 1 && t.traderCount <= 2)
            .map(t => {
                let maxRatio = 0, flagReason = '';
                const traders = [];
                for (const s of t.sides) for (const tr of s.traders) {
                    const avg = walletStats[tr.wallet]?.avg || 1;
                    const ratio = avg > 0 ? tr.currentValue / avg : 1;
                    traders.push({
                        name: tr.name, wallet: tr.wallet, side: s.name, size: tr.size,
                        avgPrice: tr.avgPrice, curPrice: tr.curPrice,
                        currentValue: tr.currentValue, cashPnl: tr.cashPnl,
                        sizeRatio: Math.round(ratio * 10) / 10,
                    });
                    if (ratio > maxRatio) {
                        maxRatio = ratio;
                        flagReason = ratio >= 2 ? `${ratio.toFixed(1)}x this trader's average position`
                            : t.traderCount === 1 ? 'Solo position — no other top wallet here'
                            : `Only ${t.traderCount} top wallets in this market`;
                    }
                }
                if (maxRatio < 1.3 && t.totalValue < 500) return null;
                const score = Math.round(Math.min(maxRatio / 4, 1) * 50
                    + Math.min(Math.log10(Math.max(t.totalValue, 1)) / 4, 1) * 30
                    + (t.traderCount === 1 ? 20 : 10));
                return {
                    market: t.market, slug: t.slug, conditionId: t.conditionId,
                    traderCount: t.traderCount, totalValue: t.totalValue, cat: t.cat,
                    traders, score, flagReason, maxRatio: Math.round(maxRatio * 10) / 10,
                    hoursLeft: t.hoursLeft, live: t.live,
                    spark: (priceHistory[t.conditionId] || []).slice(-60),
                };
            })
            .filter(x => x && x.score >= 20)
            .sort((a, b) => b.score - a.score)
            .slice(0, 18);

        // 15. Day vs Week ROI
        const mkList = key => (enriched[key] || []).filter(e => e.vol > 0)
            .sort((a, b) => b.roi - a.roi).slice(0, 5)
            .map(e => ({
                userName: e.userName, wallet: e.wallet, pnl: e.pnl, vol: e.vol, roi: e.roi,
                exposure: e.exposure, unrealized: e.unrealized,
                topPositions: e.topPositions.slice(0, 2),
            }));
        const dayList = mkList('DAY_PNL'), weekList = mkList('WEEK_PNL');
        const dayW = new Set(dayList.map(e => e.wallet)), weekW = new Set(weekList.map(e => e.wallet));
        dayList.forEach(e => { e.inBoth = weekW.has(e.wallet); });
        weekList.forEach(e => { e.inBoth = dayW.has(e.wallet); });
        const overlap = dayList.filter(e => e.inBoth).length;

        // 16. Quick money (consensus, live-aware)
        const quickMoney = allTrades
            // must still be tradeable: a market past its close is not an opportunity,
            // even though the positions in it are legitimately open pending resolution
            .filter(t => t.traderCount - t.hedgedCount >= 3 && (t.hoursLeft == null || t.hoursLeft > 0))
            .map(t => {
                const s1 = t.sides[0];
                const own = (s1?.traders || []).filter(tr => !tr.hedged);
                if (!own.length) return null;
                const tot = t.sides.reduce((s, x) => s + x.count, 0);
                const con = tot > 0 ? s1.count / tot : 0;               // share of wallets
                if (con < 0.6) return null;
                const usd = usdShare(t.sides);                          // share of dollars
                const agree = (con + usd) / 2;
                const sc = Math.round(Math.min(tot / 12, 1) * 30 + Math.max(0, agree - 0.5) * 2 * 45
                    + Math.min(Math.log10(Math.max(t.totalValue, 1)) / 5, 1) * 25);
                const avgE = paidBy(s1) ?? own.reduce((s, tr) => s + tr.avgPrice, 0) / own.length;
                const avgC = own.reduce((s, tr) => s + tr.curPrice, 0) / own.length;
                return {
                    market: t.market, slug: t.slug, conditionId: t.conditionId, cat: t.cat,
                    side: s1.name, score: sc, traderCount: tot, hedgedCount: t.hedgedCount,
                    agreePct: Math.round(con * 100), usdPct: Math.round(usd * 100),
                    sideValue: s1.value, payNow: payNowFor(t.live, s1.name) ?? r4(avgC),
                    avgEntry: r4(avgE), avgCur: r4(avgC), totalValue: t.totalValue,
                    edge: avgE > 0 ? r2((avgC - avgE) / avgE * 100) : 0,
                    hoursLeft: t.hoursLeft, live: t.live,
                    spark: (priceHistory[t.conditionId] || []).slice(-60),
                };
            })
            .filter(Boolean).sort((a, b) => b.score - a.score).slice(0, 24);

        // 17. Sparklines
        const sparklines = { DAY: {}, WEEK: {}, ALL: {} };
        for (const [hk, hist] of Object.entries(pnlHistory)) {
            const i = hk.indexOf('|');
            const per = hk.slice(0, i), w = hk.slice(i + 1);
            if (sparklines[per]) sparklines[per][w] = hist.slice(-60).map(h => r2(h.pnl));
        }

        // 18. Tape — newest first for the ticker
        const tape = [...liveChanges].sort((a, b) => b.ts - a.ts).slice(0, 60);

        // 18b. Personal tracking (never allowed to fail the whole poll)
        let me;
        try {
            me = buildMe(myBook, manualRows, diff.events, positions, wmeta, allTrades, heldMarkets, nowMs);
        } catch (e) {
            me = { input: MY_ACCOUNT, wallet: _me.wallet, resolved: _me.resolved,
                   error: 'Personal tracking failed: ' + (e.message || e),
                   positions: [], portfolioValue: null, signals: [], alerts: [], manualCount: 0 };
        }

        // 18c. Scorecard: log what reached the default Trade Window, settle what has resolved
        const shownPlays = hourlyPlays.slice(0, TRADE_WINDOW_SHOWN);
        logPlays(shownPlays, nowMs);
        let scSettled = 0;
        try { scSettled = await settlePlays(nowMs); } catch { /* retried next poll */ }

        // 18d. Briefing: what closes soon, the biggest move, the largest new position
        const nextUp = [...shownPlays].sort((a, b) => a.hoursLeft - b.hoursLeft)[0];
        const bigNew = newChanges.find(c => c.type === 'NEW')
            || liveChanges.filter(c => c.type === 'NEW' && nowMs - c.ts < 30 * 60_000)
                          .sort((a, b) => b.notional - a.notional)[0] || null;
        const mv = momentumMovers[0];
        const briefing = {
            closing: nextUp ? { n: shownPlays.filter(p => p.hoursLeft <= 1).length, market: nextUp.market,
                                conditionId: nextUp.conditionId, hoursLeft: nextUp.hoursLeft } : null,
            move: mv ? { market: mv.market, conditionId: mv.conditionId, oneHour: mv.oneHour,
                         price: mv.price, outcome: mv.outcome } : null,
            flow: bigNew ? { trader: bigNew.trader, wallet: bigNew.wallet, market: bigNew.market,
                             conditionId: bigNew.conditionId, side: bigNew.side, notional: bigNew.notional,
                             ts: bigNew.ts } : null,
        };

        // 19. Stats
        const stats = {
            traders: wallets.length,
            positions: openCount,
            rawPositions: rawCount,
            settledFiltered: filteredOut,
            heldMarketsResolved: Object.keys(heldMarkets).length,
            markets: allTrades.length,
            liveMarkets: Object.keys(liveMarkets).length,
            hourly: shownPlays.length,            // what the Trade Window lists with default filters
            hourlyTotal: hourlyPlays.length,      // everything that passed the filters
            resolving24h: resolvingSoon.length,
            dayWeekOverlap: overlap,
            totalExposure: r2(Object.values(positions).flat()
                .reduce((s, p) => s + num(p.currentValue), 0)),
        };

        Object.assign(store, {
            leaderboards: enriched, topTrader, consensus, allTrades, traderPositions,
            sparklines, liveChanges, convictionPlays,
            dayVsWeek: { day: dayList, week: weekList, overlap },
            quickMoney, hourlyPlays: hourlyPlays.slice(0, TRADE_WINDOW_SENT), momentumMovers, resolvingSoon,
            marketPrices: {}, tape, stats, me, top, briefing, error: null,
            lastUpdated: new Date().toISOString(), pollFailed: false, initialized: true,
        });
        marketIndex = new Map(allTrades.map(t => [t.key, t]));
        traderMeta = tMeta; tokenIndex = tIdx; priceIndex = pIdx;
        liveNow = liveMarkets; heldNow = heldMarkets;
        _ctx = { myBook, positions, wmeta, heldMarkets };
        commit();

        const ts = new Date().toLocaleTimeString('en-US', { hour12: false });
        const meNote = me.resolved
            ? `me:${me.positions.length}pos/${me.alerts.length}alerts`
            : `me:unresolved`;
        console.log(`[${ts}] ok — ${wallets.length} traders · ${openCount} open pos ` +
            `(${filteredOut} settled filtered of ${rawCount}) · ${allTrades.length} mkts · ` +
            `${hourlyPlays.length} hourly · ${momentumMovers.length} movers · ` +
            `${newChanges.length} changes [${diffedWallets.length}/${wallets.length} wallets diffed` +
            `${fetchFailed ? `, ${fetchFailed} fetch failed` : ''}` +
            `${diff.checked ? `, ${diff.checked} exits checked` : ''}` +
            `${diff.skipped ? `, ${diff.skipped} unconfirmed` : ''}] · ${meNote}` +
            ` · scorecard ${scorecard.plays.length}${scSettled ? ` (${scSettled} settled)` : ''}`);

        await saveState(nowMs);      // once per poll, after the new data is already being served

    } catch (e) {
        const ts = new Date().toLocaleTimeString('en-US', { hour12: false });
        console.error(`[${ts}] poll failed —`, e.message || e);
        store.pollFailed = true;
        store.error = 'The last poll stopped with an error: ' + (e.message || e);
        commit();
    } finally {
        const ts = new Date().toLocaleTimeString('en-US', { hour12: false });
        console.log(`[${ts}] endpoints — ${formatStatusLog()}`);
        polling = false;
    }
}

/* ── Serving ───────────────────────────────────────────────────────
   /api/data?tab=<now|traders|consensus|markets|mine>   small summary for one tab
   /api/market?id=<market key>                          one market with full holder detail
   /api/trader?wallet=<0x…>                             one trader's book
   /api/depth?id=<conditionId>&o=<0|1>                  the public order book for one outcome
   /api/tape?min=<dollars>                              recent large public trades
   /api/find?q=<text>                                   markets and traders, for the command palette
   /api/health                                          last answer from each upstream host
   The two detail routes are fetched only when a row is opened. Passing
   &v=<rev> returns a tiny "unchanged" reply when nothing new has been polled. */
function commit() { store.rev++; payloadCache = new Map(); }

function pinOf(me) {
    if (!me?.positions?.length) return null;
    return {
        manual: me.manualCount || 0,
        value: me.portfolioValue != null ? me.portfolioValue
             : r2(me.positions.reduce((s, p) => s + p.currentValue, 0)),
        open: me.positions.length,
        pnl: r2(me.positions.reduce((s, p) => s + p.cashPnl, 0)),
        exiting: (me.signals || []).filter(s => s.flag === 'EXITING').length,
        alerts: (me.alerts || []).filter(a => a.type === 'EXIT' || a.type === 'TRIM').length,
    };
}
const basePayload = () => ({
    rev: store.rev, stats: store.stats, lastUpdated: store.lastUpdated,
    pollFailed: store.pollFailed, initialized: store.initialized, pin: pinOf(store.me),
    error: store.pollFailed ? store.error : null,
});
const TAB_FIELDS = {
    now: () => ({ hourlyPlays: store.hourlyPlays, top: store.top, briefing: store.briefing,
                  momentumMovers: store.momentumMovers,
                  resolvingSoon: store.resolvingSoon, liveChanges: store.liveChanges.slice(0, 60) }),
    traders: () => ({ leaderboards: store.leaderboards, sparklines: store.sparklines,
                      dayVsWeek: store.dayVsWeek }),
    consensus: () => ({ quickMoney: store.quickMoney, consensus: store.consensus,
                        convictionPlays: store.convictionPlays }),
    mine: () => ({ me: store.me }),
    score: () => ({ scorecard: scorecardSummary() }),
    map: () => ({ heat: heatRows() }),
};
function tabJson(tab) {
    if (!payloadCache.has(tab))
        payloadCache.set(tab, JSON.stringify({ ...basePayload(), tab, ...TAB_FIELDS[tab]() }));
    return payloadCache.get(tab);
}

/* Heatmap: the most-traded live markets, with enough to size and color a tile. */
function heatRows() {
    const now = Date.now();
    return Object.values(liveNow)
        .filter(m => m.vol24 > 0 && m.prices?.[0] != null && !(m.endMs && m.endMs <= now))
        .sort((a, b) => b.vol24 - a.vol24).slice(0, 180)
        .map(m => {
            const t = marketIndex.get(m.conditionId);
            return { k: m.conditionId, q: m.question, cat: m.cat, v: Math.round(m.vol24), l: Math.round(m.liquidity),
                     o: m.outcomes?.[0] || 'Yes', p: m.prices[0], h: m.oneHour, d: m.oneDay,
                     hl: m.endMs ? r2((m.endMs - now) / 3600_000) : null,
                     tc: t?.traderCount || 0, tv: t ? Math.round(t.totalValue) : 0 };
        });
}
/* The live quote for a market no tracked trader holds, in the same shape the
   holder rows carry, so the market panel can still show its book and chart. */
function liveOnly(id) {
    const lm = liveNow[id];
    if (!lm) return null;
    return {
        key: id, conditionId: id, market: lm.question, slug: lm.eventSlug || lm.slug, cat: lm.cat,
        traderCount: 0, hedgedCount: 0, sides: [], totalSize: 0, totalValue: 0, endDate: lm.endDate,
        hoursLeft: lm.endMs ? (lm.endMs - Date.now()) / 3600_000 : null,
        live: { bid: lm.bestBid, ask: lm.bestAsk, spread: lm.spread, vol24: lm.vol24, liquidity: lm.liquidity,
                oneHour: lm.oneHour, oneDay: lm.oneDay, outcomes: lm.outcomes, prices: lm.prices,
                acceptingOrders: lm.acceptingOrders },
    };
}
/* Other live markets in the same event (the other totals, spreads or candidates). */
function relatedMarkets(id) {
    const lm = liveNow[id];
    if (!lm?.eventSlug) return [];
    return Object.values(liveNow)
        .filter(m => m.eventSlug === lm.eventSlug && m.conditionId !== id)
        .sort((a, b) => b.vol24 - a.vol24).slice(0, 8)
        .map(m => ({ cid: m.conditionId, q: m.question, o: m.outcomes?.[0] || 'Yes', p: m.prices?.[0] ?? null,
                     h: m.oneHour, v: Math.round(m.vol24) }));
}
function searchTraders(text) {
    const s = String(text || '').trim().toLowerCase();
    if (s.length < 2) return [];
    const out = [];
    for (const [w, m] of traderMeta) {
        if (!(m.userName || '').toLowerCase().includes(s) && !String(w).toLowerCase().includes(s)) continue;
        out.push({ wallet: w, userName: m.userName || w, exposure: m.exposure ?? null,
                   positionCount: m.positionCount ?? null, src: m.src || null });
    }
    return out.sort((a, b) => (b.exposure || 0) - (a.exposure || 0)).slice(0, 8);
}

/* Markets are filtered, sorted and cut to one page here so the browser never
   has to hold all ~4,000 rows. Holder detail is left out; it loads per row. */
const isEnded = t => t.hoursLeft != null && t.hoursLeft <= 0;
const MARKET_SORTS = {
    traders: t => t.traderCount, value: t => t.totalValue, closing: t => t.hoursLeft,
    price: t => t.live?.prices?.[0], move: t => t.live?.oneHour, size: t => t.totalSize,
    market: t => t.market.toLowerCase(),
};
function marketsPage(q) {
    const text = (q.get('q') || '').trim().toLowerCase();
    const sort = MARKET_SORTS[q.get('sort')] ? q.get('sort') : 'traders';
    const d = q.get('dir') === 'asc' ? -1 : 1;                 // desc => larger value first
    const cat = q.get('cat') || '';
    const showEnded = q.get('ended') === '1';
    const limit = Math.max(1, Math.min(parseInt(q.get('limit'), 10) || 60, 600));
    let rows = store.allTrades || [];
    if (text) rows = rows.filter(t => t.market.toLowerCase().includes(text));
    const endedCount = rows.reduce((n, t) => n + (isEnded(t) ? 1 : 0), 0);
    if (!showEnded) rows = rows.filter(t => !isEnded(t));
    const cats = {};
    for (const t of rows) cats[t.cat] = (cats[t.cat] || 0) + 1;
    if (cat) rows = rows.filter(t => t.cat === cat);
    const val = MARKET_SORTS[sort];
    rows = [...rows].sort((a, b) => {
        const x = val(a), y = val(b);
        if (x == null || y == null) return (x == null) - (y == null);    // blanks always last
        return x < y ? d : x > y ? -d : 0;
    });
    return {
        total: rows.length, endedCount, showEnded, sort, dir: d === 1 ? 'desc' : 'asc', cat, cats, limit,
        rows: rows.slice(0, limit).map(t => ({
            key: t.key, market: t.market, cat: t.cat, traderCount: t.traderCount, hedgedCount: t.hedgedCount,
            sides: t.sides.slice(0, 4).map(sideSummary),
            totalSize: t.totalSize, totalValue: t.totalValue, hoursLeft: t.hoursLeft,
            price: t.live?.prices?.[0] ?? null, oneHour: t.live?.oneHour ?? null,
        })),
    };
}

/* Starred markets and followed traders, for the watchlist strip on every tab. */
function watchPayload(q) {
    const keys = (q.get('m') || '').split(',').filter(Boolean).slice(0, 40);
    const ws = (q.get('w') || '').split(',').filter(Boolean).slice(0, 40);
    const markets = keys.map(k => {
        const t = marketIndex.get(k), lm = liveNow[k], hm = heldNow[k];
        if (!t && !lm && !hm) return { key: k, known: false };
        const outcome = lm?.outcomes?.[0] || tokenIndex.get(k)?.outcomes?.[0] || null;
        return {
            key: k, known: true, market: lm?.question || t?.market || hm?.question || '?',
            outcome, price: lm?.prices?.[0] ?? (outcome ? priceIndex.get(k)?.[outcome] ?? null : null),
            oneHour: lm?.oneHour ?? null,
            hoursLeft: lm?.endMs ? (lm.endMs - Date.now()) / 3600_000 : (t?.hoursLeft ?? null),
            traderCount: t?.traderCount || 0,
        };
    });
    const wset = new Set(ws);
    const traders = ws.map(w => {
        const m = traderMeta.get(w);
        return m ? { wallet: w, known: true, userName: m.userName, exposure: m.exposure,
                     positionCount: m.positionCount }
                 : { wallet: w, known: false };
    });
    return { rev: store.rev, markets, traders,
             events: liveChanges.filter(c => wset.has(c.wallet)).slice(0, 40) };
}

/* Requests that change something must come from this page, not from another
   site open in the same browser. */
function sameOrigin(req) {
    const o = req.headers.origin;
    if (!o) return true;
    try { return new URL(o).host === req.headers.host; } catch { return false; }
}
function readBody(req) {
    return new Promise((resolve, reject) => {
        let s = '';
        req.on('data', d => { s += d; if (s.length > 20_000) { reject(new Error('too large')); req.destroy(); } });
        req.on('end', () => { try { resolve(JSON.parse(s || '{}')); } catch (e) { reject(e); } });
        req.on('error', reject);
    });
}

function sendJson(req, res, body, status = 200) {
    const text = typeof body === 'string' ? body : JSON.stringify(body);
    const head = { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' };
    if (text.length < 1400 || !/\bgzip\b/.test(req.headers['accept-encoding'] || '')) {
        res.writeHead(status, head); res.end(text); return;
    }
    zlib.gzip(text, (err, buf) => {
        if (err) { res.writeHead(status, head); res.end(text); return; }
        res.writeHead(status, { ...head, 'Content-Encoding': 'gzip', 'Vary': 'Accept-Encoding' });
        res.end(buf);
    });
}

const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1');
    const route = url.pathname, q = url.searchParams;

    if (route === '/favicon.ico') { res.writeHead(204); res.end(); return; }
    // Manual refresh — poll Polymarket now instead of waiting out the cycle.
    // poll() self-guards via the `polling` flag, so a burst of clicks is safe.
    if (route === '/api/refresh' && req.method === 'POST') {
        if (!sameOrigin(req)) { sendJson(req, res, { ok: false }, 403); return; }
        const wasPolling = polling;
        Promise.resolve(wasPolling ? null : poll()).then(() => {
            sendJson(req, res, { ok: true, alreadyPolling: wasPolling, lastUpdated: store.lastUpdated });
        }).catch(() => { res.writeHead(500); res.end('{"ok":false}'); });
        return;
    }

    if (route === '/api/data') {
        const tab = q.get('tab') || 'now';
        if (tab !== 'markets' && !TAB_FIELDS[tab]) { sendJson(req, res, { error: 'unknown tab' }, 400); return; }
        // nothing new since the copy the caller already has — skip the body
        if (q.get('v') && q.get('v') === String(store.rev)) {
            sendJson(req, res, { unchanged: true, rev: store.rev, lastUpdated: store.lastUpdated,
                                 pollFailed: store.pollFailed, initialized: store.initialized,
                                 error: store.pollFailed ? store.error : null });
            return;
        }
        if (tab === 'markets') sendJson(req, res, { ...basePayload(), tab, markets: marketsPage(q) });
        else sendJson(req, res, tabJson(tab));
        return;
    }
    if (route === '/api/market') {
        const id = q.get('id') || '', held = marketIndex.get(id) || null;
        sendJson(req, res, { rev: store.rev, market: held || liveOnly(id), held: !!held, related: relatedMarkets(id) });
        return;
    }
    if (route === '/api/trader') {
        const w = q.get('wallet') || '';
        sendJson(req, res, { rev: store.rev, wallet: w, meta: traderMeta.get(w) || null,
                             positions: store.traderPositions[w] || [],
                             events: liveChanges.filter(c => c.wallet === w).slice(0, 30) });
        return;
    }
    if (route === '/api/chart') {
        getChart(q.get('id') || '', q.get('o') === '1' ? 1 : 0, q.get('range') || '1d')
            .then(body => sendJson(req, res, body))
            .catch(e => sendJson(req, res, { error: String(e.message || e), points: [] }));
        return;
    }
    if (route === '/api/depth') {
        getDepth(q.get('id') || '', q.get('o') === '1' ? 1 : 0)
            .then(body => sendJson(req, res, body))
            .catch(e => sendJson(req, res, { error: String(e.message || e), bids: [], asks: [] }));
        return;
    }
    if (route === '/api/tape') {
        getTape(parseInt(q.get('min'), 10))
            .then(body => sendJson(req, res, body))
            .catch(e => sendJson(req, res, { error: String(e.message || e), trades: [] }));
        return;
    }
    if (route === '/api/health') { sendJson(req, res, healthPayload()); return; }
    if (route === '/api/find') {
        sendJson(req, res, { markets: searchMarkets(q.get('q')).slice(0, 8), traders: searchTraders(q.get('q')) });
        return;
    }
    if (route === '/api/watch') { sendJson(req, res, watchPayload(q)); return; }
    if (route === '/api/search') { sendJson(req, res, { results: searchMarkets(q.get('q')) }); return; }
    // Manual book: a local record of positions. It never talks to Polymarket.
    if (route === '/api/book' && (req.method === 'POST' || req.method === 'DELETE')) {
        if (!sameOrigin(req)) { sendJson(req, res, { error: 'forbidden' }, 403); return; }
        if (req.method === 'DELETE') {
            const r = removeManual(q.get('id'));
            sendJson(req, res, r, r.error ? 404 : 200); return;
        }
        if (!/^application\/json/.test(req.headers['content-type'] || '')) {
            sendJson(req, res, { error: 'expected JSON' }, 415); return;
        }
        readBody(req).then(b => { const r = addManual(b); sendJson(req, res, r, r.error ? 400 : 200); })
                     .catch(() => sendJson(req, res, { error: 'Could not read that entry.' }, 400));
        return;
    }
    if (route === '/' || route === '/index.html') {
        try {
            const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf-8');
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
            res.end(html);
        } catch { res.writeHead(500); res.end('Failed to read index.html'); }
        return;
    }
    res.writeHead(404); res.end('Not Found');
});

server.on('error', (e) => {
    if (e.code === 'EADDRINUSE')
        console.error(`\n  Port ${PORT} is already in use. Another copy of this server is probably running.\n`);
    else console.error(e);
    process.exit(1);
});

server.listen(PORT, '127.0.0.1', () => {
    console.log(`\n  POLYMARKET TERMINAL`);
    console.log(`  http://127.0.0.1:${PORT}`);
    console.log(`  Polling every ${POLL_INTERVAL / 1000}s`);
    loadState();
    console.log('');
    nextPollAt = Date.now() + POLL_INTERVAL;
    poll();
    setInterval(() => { nextPollAt = Date.now() + POLL_INTERVAL; poll(); }, POLL_INTERVAL);
});
