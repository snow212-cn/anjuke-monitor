#!/usr/bin/env node
/**
 * anjuke_monitor.js
 *
 * 目标：定时抓取指定小区二手房源列表，检测新增/下架（可选：字段变化），输出变更结果。
 *
 * - 默认使用 Node.js 18+ 的全局 fetch
 * - 支持通过 HAR 离线回放解析逻辑：--har <file>
 * - 通知能力预留：notify(diff, context)
 * - 可选外部通知模块：--notify <path> 或 AJ_NOTIFY_MODULE
 */

'use strict';

const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');

const DEFAULT_DEDUPE_FEATURE_FIELDS = [
  'community_id',
  'area_num',
  'room_num',
  'hall_num',
  'toilet_num',
  'floor_level_bucket',
  'total_floor'
];

// -------------------------
// CLI
// -------------------------

function parseArgs(argv) {
  const args = {
    harPath: null,
    noSave: false,
    noReport: false,
    configPath: null,
    target: null,
    notifyModule: null
  };

  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--har') {
      args.harPath = argv[++i] ?? null;
    } else if (a === '--no-save') {
      args.noSave = true;
    } else if (a === '--no-report') {
      args.noReport = true;
    } else if (a === '--config') {
      args.configPath = argv[++i] ?? null;
    } else if (a === '--target') {
      // 指定目标名称（用于多 targets 时只跑其中一个）
      args.target = argv[++i] ?? null;
    } else if (a === '--notify') {
      // 外部通知模块路径（CommonJS: module.exports = async function notify(diff, context) {})
      args.notifyModule = argv[++i] ?? null;
    }
  }
  return args;
}

// -------------------------
// Config
// -------------------------

function readJsonIfExists(filePath) {
  if (!filePath) return null;
  if (!fs.existsSync(filePath)) return null;
  const txt = fs.readFileSync(filePath, 'utf8');
  return JSON.parse(txt);
}

function envBool(name, defaultValue) {
  const v = process.env[name];
  if (v == null) return defaultValue;
  return v === '1' || v.toLowerCase() === 'true' || v.toLowerCase() === 'yes';
}

function envInt(name, defaultValue) {
  const v = process.env[name];
  if (v == null || v === '') return defaultValue;
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : defaultValue;
}

function envStr(name, defaultValue) {
  const v = process.env[name];
  return (v == null || v === '') ? defaultValue : v;
}

function envFloat(name, defaultValue) {
  const v = process.env[name];
  if (v == null || v === '') return defaultValue;
  const n = Number.parseFloat(v);
  return Number.isFinite(n) ? n : defaultValue;
}

function getDefaultConfig() {
  return {
    stateDir: './data',
    request: {
      baseUrl: 'https://mudanjiang.anjuke.com',
      timeoutMs: 20000,
      retries: 2,
      retryDelayMs: 800,
      headers: {
        accept: 'application/json, text/plain, */*, application/x-json, */*;q=0.01',
        // UA 不是必需，但很多站点会对缺少 UA 的请求更敏感
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
      }
    },
    diff: {
      enableUpdated: false,
      updatedFields: ['price', 'avg_price', 'area_num', 'room_num', 'hall_num', 'toilet_num', 'floor_level', 'orient', 'fitment_name', 'title'],
      maxItemsInNotification: 50
    },
    dedupe: {
      enabled: true,
      priceToleranceWan: 1,
      photoField: 'default_photo',
      featureFields: DEFAULT_DEDUPE_FEATURE_FIELDS.slice()
    },
    report: {
      enabled: true,
      saveListings: true,
      saveDiff: true,
      // listingsFormat/diffFormat 支持："csv" / "md" / "csv,md"
      listingsFormat: 'csv',
      diffFormat: 'md',
      // 默认不累积历史 diff 文件，只保留 latest（减少目录文件数）
      saveDiffHistory: false,
      // 自动清理旧版本/不再需要的报告文件（避免目录里越积越多）
      cleanupLegacy: true
    },
    targets: [
      {
        name: '东兴小区(东安)',
        cityId: 182,
        commId: 696590,
        entry: 51,
        pageSize: 20
      }
    ]
  };
}

function loadConfig(args) {
  const base = getDefaultConfig();

  // config file
  const configPath = args.configPath
    ? args.configPath
    : path.join(__dirname, 'config', 'config.json');
  const fileCfg = readJsonIfExists(configPath) || readJsonIfExists(path.join(__dirname, 'config', 'config.example.json')) || {};

  // env overrides (single target convenience)
  const envTarget = {
    name: envStr('AJ_TARGET_NAME', ''),
    cityId: envInt('AJ_CITY_ID', NaN),
    commId: envInt('AJ_COMM_ID', NaN),
    entry: envInt('AJ_ENTRY', NaN),
    pageSize: envInt('AJ_PAGE_SIZE', NaN),
    minLat: envFloat('AJ_MIN_LAT', NaN),
    minLng: envFloat('AJ_MIN_LNG', NaN),
    maxLat: envFloat('AJ_MAX_LAT', NaN),
    maxLng: envFloat('AJ_MAX_LNG', NaN)
  };

  const cfg = deepMerge(base, fileCfg);

  cfg.stateDir = envStr('AJ_STATE_DIR', cfg.stateDir);
  cfg.request.baseUrl = envStr('AJ_BASE_URL', cfg.request.baseUrl);
  cfg.diff.enableUpdated = envBool('AJ_ENABLE_UPDATE_DIFF', cfg.diff.enableUpdated);
  cfg.dedupe = cfg.dedupe || {};
  cfg.dedupe.enabled = envBool('AJ_DEDUPE_ENABLED', cfg.dedupe.enabled ?? true);
  cfg.dedupe.priceToleranceWan = envFloat('AJ_DEDUPE_PRICE_TOLERANCE_WAN', cfg.dedupe.priceToleranceWan ?? 1);
  cfg.dedupe.photoField = envStr('AJ_DEDUPE_PHOTO_FIELD', cfg.dedupe.photoField ?? 'default_photo');
  cfg.dedupe.featureFields = normalizeStringList(
    envStr('AJ_DEDUPE_FEATURE_FIELDS', ''),
    cfg.dedupe.featureFields ?? DEFAULT_DEDUPE_FEATURE_FIELDS
  );

  // report toggles
  cfg.report = cfg.report || {};
  cfg.report.enabled = envBool('AJ_REPORT_ENABLED', cfg.report.enabled ?? true);
  cfg.report.saveListings = envBool('AJ_REPORT_SAVE_LISTINGS', cfg.report.saveListings ?? true);
  cfg.report.saveDiff = envBool('AJ_REPORT_SAVE_DIFF', cfg.report.saveDiff ?? true);
  cfg.report.saveDiffHistory = envBool('AJ_REPORT_SAVE_DIFF_HISTORY', cfg.report.saveDiffHistory ?? false);
  cfg.report.cleanupLegacy = envBool('AJ_REPORT_CLEANUP_LEGACY', cfg.report.cleanupLegacy ?? true);
  cfg.report.listingsFormat = envStr('AJ_REPORT_LISTINGS_FORMAT', cfg.report.listingsFormat ?? 'csv');
  cfg.report.diffFormat = envStr('AJ_REPORT_DIFF_FORMAT', cfg.report.diffFormat ?? 'md');

  // if env target is complete, replace targets with one
  const envHasCommunityTarget = Number.isFinite(envTarget.cityId) && Number.isFinite(envTarget.commId);
  const envHasViewportTarget = Number.isFinite(envTarget.cityId)
    && Number.isFinite(envTarget.minLat)
    && Number.isFinite(envTarget.minLng)
    && Number.isFinite(envTarget.maxLat)
    && Number.isFinite(envTarget.maxLng);

  if (envHasCommunityTarget || envHasViewportTarget) {
    const fallbackName = envHasCommunityTarget
      ? `${envTarget.cityId}:${envTarget.commId}`
      : `${envTarget.cityId}:viewport`;
    cfg.targets = [
      normalizeTarget({
        name: envTarget.name || fallbackName,
        cityId: envTarget.cityId,
        entry: Number.isFinite(envTarget.entry) ? envTarget.entry : 51,
        pageSize: Number.isFinite(envTarget.pageSize) ? envTarget.pageSize : 20,
        commId: envHasCommunityTarget ? envTarget.commId : undefined,
        minLat: envHasViewportTarget ? envTarget.minLat : undefined,
        minLng: envHasViewportTarget ? envTarget.minLng : undefined,
        maxLat: envHasViewportTarget ? envTarget.maxLat : undefined,
        maxLng: envHasViewportTarget ? envTarget.maxLng : undefined
      })
    ];
  }

  cfg.targets = Array.isArray(cfg.targets) ? cfg.targets.map(normalizeTarget) : [];

  return cfg;
}

function deepMerge(a, b) {
  if (Array.isArray(a) || Array.isArray(b)) return (b !== undefined ? b : a);
  if (!isPlainObject(a) || !isPlainObject(b)) return (b !== undefined ? b : a);
  const out = { ...a };
  for (const k of Object.keys(b)) {
    out[k] = deepMerge(a[k], b[k]);
  }
  return out;
}

function isPlainObject(x) {
  return x != null && typeof x === 'object' && Object.getPrototypeOf(x) === Object.prototype;
}

function normalizeTarget(target) {
  const t = { ...(target || {}) };

  if (t.mode == null || t.mode === '') {
    if (Number.isFinite(Number(t.commId))) {
      t.mode = 'community';
    } else if (hasViewportBounds(t)) {
      t.mode = 'viewport';
    }
  }

  if (!t.mode) {
    throw new Error(`无法识别 target 模式: ${JSON.stringify(target)}`);
  }

  t.mode = String(t.mode).toLowerCase();
  if (t.mode !== 'community' && t.mode !== 'viewport') {
    throw new Error(`不支持的 target.mode: ${t.mode}`);
  }

  if (!Number.isFinite(Number(t.cityId))) {
    throw new Error(`target.cityId 无效: ${JSON.stringify(target)}`);
  }

  if (t.mode === 'community' && !Number.isFinite(Number(t.commId))) {
    throw new Error(`community 模式缺少有效 commId: ${JSON.stringify(target)}`);
  }

  if (t.mode === 'viewport' && !hasViewportBounds(t)) {
    throw new Error(`viewport 模式缺少有效经纬度范围: ${JSON.stringify(target)}`);
  }

  if (!t.name) {
    t.name = t.mode === 'community'
      ? `${t.cityId}:${t.commId}`
      : `${t.cityId}:viewport`;
  }

  return t;
}

function hasViewportBounds(target) {
  return ['minLat', 'minLng', 'maxLat', 'maxLng']
    .every((k) => Number.isFinite(Number(target?.[k])));
}

// -------------------------
// Network
// -------------------------

class HttpError extends Error {
  constructor(message, { status, url, bodyPreview } = {}) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
    this.url = url;
    this.bodyPreview = bodyPreview;
  }
}

async function fetchWithRetry(url, { headers, timeoutMs, retries, retryDelayMs } = {}) {
  const attemptMax = (retries ?? 0) + 1;
  let lastErr;
  for (let attempt = 1; attempt <= attemptMax; attempt++) {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(new Error('timeout')), timeoutMs ?? 20000);
    try {
      const resp = await fetch(url, {
        method: 'GET',
        headers,
        signal: ac.signal
      });
      if (!resp.ok) {
        const txt = await safeReadText(resp);
        throw new HttpError(`HTTP ${resp.status} for ${url}`, {
          status: resp.status,
          url,
          bodyPreview: txt.slice(0, 500)
        });
      }
      const json = await resp.json();
      clearTimeout(timer);
      return json;
    } catch (e) {
      clearTimeout(timer);
      lastErr = e;
      const shouldRetry = attempt < attemptMax;
      if (!shouldRetry) break;
      await sleep((retryDelayMs ?? 500) * attempt);
    }
  }
  throw lastErr;
}

async function safeReadText(resp) {
  try {
    return await resp.text();
  } catch {
    return '';
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// -------------------------
// Domain: Anjuke list
// -------------------------

function buildListUrl({ baseUrl, cityId, commId, entry, page, pageSize }) {
  const u = new URL('/esf-ajax/property/info/list', baseUrl);
  u.searchParams.set('city_id', String(cityId));
  u.searchParams.set('entry', String(entry ?? 51));
  u.searchParams.set('page', String(page));
  u.searchParams.set('page_size', String(pageSize ?? 20));
  if (commId != null && commId !== '') {
    u.searchParams.set('comm_id', String(commId));
  }
  return u.toString();
}

function buildTargetListUrl({ baseUrl, target, page, pageSize }) {
  const mode = getTargetMode(target);
  const u = new URL(buildListUrl({
    baseUrl,
    cityId: target.cityId,
    commId: mode === 'community' ? target.commId : undefined,
    entry: target.entry,
    page,
    pageSize
  }));

  if (mode === 'viewport') {
    u.searchParams.set('min_lat', String(target.minLat));
    u.searchParams.set('min_lng', String(target.minLng));
    u.searchParams.set('max_lat', String(target.maxLat));
    u.searchParams.set('max_lng', String(target.maxLng));
  }

  return u.toString();
}

function getTargetMode(target) {
  return String(target?.mode || '').toLowerCase();
}

/**
 * 解析接口响应 => 标准化房源数组
 *
 * HAR 示例中：data.list[].info.property.attribute 里包含 price/area 等字段；
 * 唯一标识推荐使用 data.list[].info.property.id (示例："7381368927")。
 */
function normalizeListResponse(json) {
  if (!json || json.status !== 'ok') {
    const msg = json && typeof json === 'object' ? (json.msg || 'unknown') : 'non-object';
    throw new Error(`Bad response status: ${msg}`);
  }

  const data = json.data || {};
  const list = Array.isArray(data.list) ? data.list : [];

  const items = [];
  for (const row of list) {
    const info = row && row.info;
    const prop = info && info.property;
    if (!prop) continue;

    const id = String(prop.id || '');
    if (!id) continue;

    const attr = prop.attribute || {};
    const community = info && info.community ? info.community : {};
    const broker = info && info.broker ? info.broker : {};
    const entityId = info?.entityId || prop.entityId || prop.house_id || '';

    // 选择一组“稳定且有用”的字段，便于通知和 diff
    items.push({
      id,
      entity_key: '',
      entity_id: entityId ? String(entityId) : '',
      dedupe_basis: '',
      duplicate_count: 1,
      source_ids: [id],
      broker_ids: Array.isArray(prop.broker_ids) ? prop.broker_ids.map((x) => String(x)) : [],
      broker_names: broker.name ? [String(broker.name)] : [],
      broker_companies: broker.company_name ? [String(broker.company_name)] : [],
      house_id: prop.house_id ? String(prop.house_id) : '',
      status: prop.status ? String(prop.status) : '',
      title: prop.title ? String(prop.title) : '',
      price: attr.price ? String(attr.price) : '',
      avg_price: attr.avg_price ? String(attr.avg_price) : '',
      area_num: attr.area_num ? String(attr.area_num) : '',
      room_num: attr.room_num ? String(attr.room_num) : '',
      hall_num: attr.hall_num ? String(attr.hall_num) : '',
      toilet_num: attr.toilet_num ? String(attr.toilet_num) : '',
      floor_level: attr.floor_level ? String(attr.floor_level) : '',
      total_floor: attr.total_floor ? String(attr.total_floor) : '',
      orient: attr.orient ? String(attr.orient) : '',
      fitment_name: attr.fitment_name ? String(attr.fitment_name) : '',
      default_photo: prop.default_photo ? String(prop.default_photo) : '',
      community_id: prop.community_id ? String(prop.community_id) : (community.id ? String(community.id) : ''),
      community_name: community.name ? String(community.name) : '',
      community_address: community.address ? String(community.address) : '',
      community_lat: community.lat ? String(community.lat) : '',
      community_lng: community.lng ? String(community.lng) : '',
      broker_id: broker.broker_id ? String(broker.broker_id) : '',
      broker_name: broker.name ? String(broker.name) : '',
      broker_company_name: broker.company_name ? String(broker.company_name) : '',
      broker_store_name: broker.store_name ? String(broker.store_name) : '',
      pc_url: prop.pc_url ? String(prop.pc_url) : '',
      tw_url: prop.tw_url ? String(prop.tw_url) : '',
      post_date: prop.post_date ? String(prop.post_date) : ''
    });
  }

  const total = Number.parseInt(data.total ?? String(items.length), 10);
  const hasMore = String(data.has_more ?? '0') === '1';

  return {
    total: Number.isFinite(total) ? total : items.length,
    hasMore,
    items
  };
}

async function fetchAllCommunityListings({ request, target }) {
  const pageSize = target.pageSize ?? 20;
  let page = 1;
  const all = [];
  const seen = new Set();

  while (true) {
    const url = buildTargetListUrl({
      baseUrl: request.baseUrl,
      target,
      page,
      pageSize
    });

    const json = await fetchWithRetry(url, {
      headers: request.headers,
      timeoutMs: request.timeoutMs,
      retries: request.retries,
      retryDelayMs: request.retryDelayMs
    });

    const { hasMore, items } = normalizeListResponse(json);

    for (const it of items) {
      if (seen.has(it.id)) continue;
      seen.add(it.id);
      all.push(it);
    }

    // 兜底：如果服务端说还有更多，继续；否则停止
    if (!hasMore) break;

    // 兜底：防止死循环
    if (items.length === 0) break;

    page++;
    if (page > 100) break; // hard cap
  }

  return all;
}

// -------------------------
// HAR offline playback
// -------------------------

function findHarEntryForUrl(harJson, urlContains) {
  const entries = harJson?.log?.entries;
  if (!Array.isArray(entries)) return null;

  // 从后往前找更接近“最新”
  for (let i = entries.length - 1; i >= 0; i--) {
    const e = entries[i];
    const reqUrl = e?.request?.url;
    if (typeof reqUrl !== 'string') continue;
    if (!reqUrl.includes(urlContains)) continue;

    const text = e?.response?.content?.text;
    if (typeof text !== 'string' || text.length === 0) continue;

    return e;
  }

  return null;
}

async function loadListingsFromHar(harPath) {
  const txt = await fsp.readFile(harPath, 'utf8');
  const har = JSON.parse(txt);

  const entry = findHarEntryForUrl(har, '/esf-ajax/property/info/list');
  if (!entry) {
    throw new Error('HAR 中未找到 /esf-ajax/property/info/list 的响应内容');
  }

  const bodyText = entry.response.content.text;

  // HAR 中该请求返回 content-encoding:gzip，但 devtools 已帮我们解码并放到 text
  const json = JSON.parse(bodyText);
  const { items } = normalizeListResponse(json);
  return items;
}

// -------------------------
// State & diff
// -------------------------

function stableStringify(obj) {
  // 简单稳定序列化：递归按 key 排序
  if (obj === null || typeof obj !== 'object') return JSON.stringify(obj);
  if (Array.isArray(obj)) return '[' + obj.map(stableStringify).join(',') + ']';
  const keys = Object.keys(obj).sort();
  return '{' + keys.map((k) => JSON.stringify(k) + ':' + stableStringify(obj[k])).join(',') + '}';
}

function sha256(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function normalizeKeyPart(value) {
  return String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '');
}

function normalizeNumericKeyPart(value, { digits = 2 } = {}) {
  const n = Number(value);
  if (!Number.isFinite(n)) return '';
  return n.toFixed(digits);
}

function normalizePhotoKey(url) {
  if (!url) return '';
  const raw = String(url).trim();
  if (!raw) return '';

  try {
    const u = new URL(raw);
    const pathname = u.pathname || '';
    const match = pathname.match(/\/([a-f0-9]{16,})\//i);
    if (match) return match[1].toLowerCase();
    return pathname.toLowerCase();
  } catch {
    return raw.toLowerCase();
  }
}

function normalizeFloorBucket(value) {
  const raw = String(value ?? '').trim();
  if (!raw) return '';
  const m = raw.match(/^([^\(]+)/);
  return normalizeKeyPart(m ? m[1] : raw);
}

function getConfiguredFeatureValue(it, fieldName, { photoField } = {}) {
  const field = String(fieldName || '').trim();
  if (!field) return '';

  if (field === 'floor_level_bucket') {
    return normalizeFloorBucket(it?.floor_level);
  }

  if (field === 'photo_key') {
    const keyField = photoField || 'default_photo';
    return normalizePhotoKey(it?.[keyField]);
  }

  if (field === 'area_num') {
    return normalizeNumericKeyPart(it?.area_num, { digits: 2 });
  }

  if (field === 'price' || field === 'avg_price') {
    return normalizeNumericKeyPart(it?.[field], { digits: 1 });
  }

  if (field === 'total_floor') {
    return normalizeNumericKeyPart(it?.total_floor, { digits: 0 });
  }

  return normalizeKeyPart(it?.[field]);
}

function buildListingFeatureCore(it, { featureFields, photoField } = {}) {
  const fields = normalizeStringList(featureFields, DEFAULT_DEDUPE_FEATURE_FIELDS);
  const pairs = [];

  for (const field of fields) {
    const value = getConfiguredFeatureValue(it, field, { photoField });
    if (!value) return null;
    pairs.push(`${field}=${value}`);
  }

  return pairs.length > 0 ? pairs : null;
}

function buildListingCandidateKeys(it, { featureFields, photoField } = {}) {
  const core = buildListingFeatureCore(it, { featureFields, photoField });
  const candidates = [];

  if (core) {
    candidates.push({
      type: 'feature_core',
      raw: `feature-core:${core.join('|')}`
    });
    const photoKey = photoField ? getConfiguredFeatureValue(it, 'photo_key', { photoField }) : '';
    if (photoKey) {
      candidates.push({
        type: 'feature_photo',
        raw: `feature-photo:${core.join('|')}|${photoKey}`
      });
    }
  }

  candidates.push({
    type: 'listing_id',
    raw: `listing:${String(it?.id || '')}`
  });

  return candidates;
}

function buildListingEntityInfo(it, { featureFields, photoField } = {}) {
  const candidates = buildListingCandidateKeys(it, { featureFields, photoField });
  const primary = candidates[0];

  return {
    key: `dedupe:${sha256(primary.raw).slice(0, 16)}`,
    basis: primary.type,
    candidates
  };
}

function getListingIdentity(it) {
  return String(it?.entity_key || it?.id || '');
}

function completenessScore(it) {
  const fields = [
    'title',
    'price',
    'avg_price',
    'area_num',
    'room_num',
    'hall_num',
    'toilet_num',
    'floor_level',
    'orient',
    'fitment_name',
    'pc_url',
    'default_photo',
    'broker_name',
    'community_name'
  ];
  return fields.reduce((sum, key) => sum + (it?.[key] ? 1 : 0), 0);
}

function compareListingsForKeep(a, b) {
  const postA = Number(a?.post_date || 0);
  const postB = Number(b?.post_date || 0);
  if (postA !== postB) return postB - postA;

  const completeA = completenessScore(a);
  const completeB = completenessScore(b);
  if (completeA !== completeB) return completeB - completeA;

  const priceA = Number(a?.price || Number.POSITIVE_INFINITY);
  const priceB = Number(b?.price || Number.POSITIVE_INFINITY);
  if (priceA !== priceB) return priceA - priceB;

  return String(a?.id || '').localeCompare(String(b?.id || ''));
}

function uniqSorted(arr) {
  return Array.from(new Set((arr || []).filter(Boolean).map((x) => String(x)))).sort((a, b) => a.localeCompare(b));
}

function pickEntityInfoForGroup(items, { priceToleranceWan } = {}) {
  const groupsByType = new Map();

  for (const it of items) {
    for (const candidate of it._dedupeCandidates || []) {
      if (candidate.type !== 'feature_photo') continue;
      const bucketKey = `${candidate.type}:${candidate.raw}`;
      if (!groupsByType.has(bucketKey)) {
        groupsByType.set(bucketKey, { type: candidate.type, raw: candidate.raw, ids: new Set() });
      }
      groupsByType.get(bucketKey).ids.add(String(it.id || ''));
    }
  }

  const ranked = [...groupsByType.values()]
    .filter((x) => x.ids.size > 1)
    .sort((a, b) => {
      if (b.ids.size !== a.ids.size) return b.ids.size - a.ids.size;
      return a.type.localeCompare(b.type);
    });

  if (ranked.length > 0) {
    const best = ranked[0];
    return {
      key: `dedupe:${sha256(best.raw).slice(0, 16)}`,
      basis: best.type
    };
  }

  const featureCore = items[0]?._featureCore || '';
  if (featureCore) {
    const priceValues = items
      .map((it) => Number(it._priceValue))
      .filter((x) => Number.isFinite(x))
      .sort((a, b) => a - b);

    if (priceValues.length > 0) {
      const basePrice = priceValues[0];
      const bucketWidth = Math.max(Number(priceToleranceWan) * 2 || 0, 1);
      const bucket = Math.round(basePrice / bucketWidth);
      return {
        key: `dedupe:${sha256(`feature-core:${featureCore}|bucket:${bucket}|width:${bucketWidth}`).slice(0, 16)}`,
        basis: Number(priceToleranceWan) > 0 ? 'feature_price_tolerance' : 'feature_core'
      };
    }

    return {
      key: `dedupe:${sha256(`feature-core:${featureCore}`).slice(0, 16)}`,
      basis: 'feature_core'
    };
  }

  const fallback = items[0]?._dedupeCandidates?.[0] || { raw: `listing:${String(items[0]?.id || '')}`, type: 'listing_id' };
  return {
    key: `dedupe:${sha256(fallback.raw).slice(0, 16)}`,
    basis: fallback.type
  };
}

function buildDisjointSet(size) {
  const parent = Array.from({ length: size }, (_, i) => i);

  const find = (x) => {
    let p = parent[x];
    while (p !== parent[p]) {
      parent[p] = parent[parent[p]];
      p = parent[p];
    }
    parent[x] = p;
    return p;
  };

  const union = (a, b) => {
    const ra = find(a);
    const rb = find(b);
    if (ra !== rb) parent[rb] = ra;
  };

  return { find, union };
}

function dedupeListings(items, { enabled, priceToleranceWan, featureFields, photoField } = {}) {
  const tolerance = Math.max(Number(priceToleranceWan) || 0, 0);
  const dedupeFeatureFields = normalizeStringList(featureFields, DEFAULT_DEDUPE_FEATURE_FIELDS);
  const dedupePhotoField = String(photoField ?? 'default_photo').trim();

  if (!enabled) {
    return (items || []).map((it) => {
      const entity = buildListingEntityInfo(it, {
        featureFields: dedupeFeatureFields,
        photoField: dedupePhotoField
      });
      return {
        ...it,
        entity_key: entity.key,
        dedupe_basis: entity.basis,
        duplicate_count: Number(it?.duplicate_count) || 1,
        source_ids: uniqSorted(it?.source_ids?.length ? it.source_ids : [it.id]),
        broker_ids: uniqSorted(it?.broker_ids || (it?.broker_id ? [it.broker_id] : [])),
        broker_names: uniqSorted(it?.broker_names || (it?.broker_name ? [it.broker_name] : [])),
        broker_companies: uniqSorted(it?.broker_companies || (it?.broker_company_name ? [it.broker_company_name] : []))
      };
    });
  }

  const prepared = (items || []).map((raw) => {
    const entity = buildListingEntityInfo(raw, {
      featureFields: dedupeFeatureFields,
      photoField: dedupePhotoField
    });
    const featureCoreCandidate = entity.candidates.find((x) => x.type === 'feature_core');
    return {
      ...raw,
      entity_key: entity.key,
      dedupe_basis: entity.basis,
      _dedupeCandidates: entity.candidates,
      _featureCore: featureCoreCandidate ? featureCoreCandidate.raw : '',
      _photoKey: dedupePhotoField ? getConfiguredFeatureValue(raw, 'photo_key', { photoField: dedupePhotoField }) : '',
      _priceValue: Number(raw?.price),
      source_ids: uniqSorted(raw?.source_ids?.length ? raw.source_ids : [raw.id]),
      broker_ids: uniqSorted(raw?.broker_ids || (raw?.broker_id ? [raw.broker_id] : [])),
      broker_names: uniqSorted(raw?.broker_names || (raw?.broker_name ? [raw.broker_name] : [])),
      broker_companies: uniqSorted(raw?.broker_companies || (raw?.broker_company_name ? [raw.broker_company_name] : []))
    };
  });

  const dsu = buildDisjointSet(prepared.length);
  const featureGroups = new Map();
  prepared.forEach((item, index) => {
    const key = item._featureCore || `listing:${String(item.id || '')}`;
    if (!featureGroups.has(key)) featureGroups.set(key, []);
    featureGroups.get(key).push(index);
  });

  for (const indexes of featureGroups.values()) {
    if (indexes.length <= 1) continue;

    const photoGroups = new Map();
    for (const idx of indexes) {
      const photoKey = prepared[idx]._photoKey;
      if (!photoKey) continue;
      if (!photoGroups.has(photoKey)) photoGroups.set(photoKey, []);
      photoGroups.get(photoKey).push(idx);
    }

    for (const photoIndexes of photoGroups.values()) {
      for (let i = 1; i < photoIndexes.length; i++) {
        dsu.union(photoIndexes[0], photoIndexes[i]);
      }
    }

    if (tolerance > 0) {
      const priced = indexes
        .map((idx) => ({ idx, price: prepared[idx]._priceValue }))
        .filter((x) => Number.isFinite(x.price))
        .sort((a, b) => a.price - b.price);

      for (let i = 0; i < priced.length; i++) {
        for (let j = i + 1; j < priced.length; j++) {
          const delta = Math.abs(priced[j].price - priced[i].price);
          if (delta > tolerance) break;
          dsu.union(priced[i].idx, priced[j].idx);
        }
      }
    }
  }

  const groups = new Map();
  prepared.forEach((item, index) => {
    const root = dsu.find(index);
    if (!groups.has(root)) groups.set(root, []);
    groups.get(root).push(item);
  });

  const deduped = [];
  for (const group of groups.values()) {
    const sorted = group.slice().sort(compareListingsForKeep);
    const keep = { ...sorted[0] };
    const entity = pickEntityInfoForGroup(group, { priceToleranceWan: tolerance });
    const sourceIds = uniqSorted(group.flatMap((it) => it.source_ids || [it.id]));
    const brokerIds = uniqSorted(group.flatMap((it) => it.broker_ids || []));
    const brokerNames = uniqSorted(group.flatMap((it) => it.broker_names || []));
    const brokerCompanies = uniqSorted(group.flatMap((it) => it.broker_companies || []));

    keep.entity_key = entity.key;
    keep.dedupe_basis = entity.basis;
    keep.source_ids = sourceIds;
    keep.broker_ids = brokerIds;
    keep.broker_names = brokerNames;
    keep.broker_companies = brokerCompanies;
    keep.duplicate_count = group.length;
    delete keep._dedupeCandidates;
    delete keep._featureCore;
    delete keep._photoKey;
    delete keep._priceValue;

    deduped.push(keep);
  }

  deduped.sort((a, b) => compareListingsForKeep(a, b));
  return deduped;
}

function getTargetFileSafeId(target) {
  if (getTargetMode(target) === 'community') {
    return `${safeFilePart(target.cityId)}_${safeFilePart(target.commId)}`;
  }

  const payload = {
    mode: getTargetMode(target),
    cityId: target.cityId,
    entry: target.entry ?? 51,
    minLat: Number(target.minLat),
    minLng: Number(target.minLng),
    maxLat: Number(target.maxLat),
    maxLng: Number(target.maxLng)
  };
  return `${safeFilePart(target.cityId)}_viewport_${sha256(stableStringify(payload)).slice(0, 12)}`;
}

function formatTargetMeta(target) {
  const base = [`cityId=${target.cityId}`, `mode=${getTargetMode(target) || 'community'}`];
  if (target.entry != null) base.push(`entry=${target.entry}`);

  if (getTargetMode(target) === 'community') {
    base.push(`commId=${target.commId}`);
  } else if (getTargetMode(target) === 'viewport') {
    base.push(`bounds=${target.minLat},${target.minLng} ~ ${target.maxLat},${target.maxLng}`);
  }

  return base.join(' ');
}

function buildStatePath(stateDir, target) {
  const safe = getTargetFileSafeId(target);
  return path.join(stateDir, `state_${safe}.json`);
}

async function loadPrevState(statePath) {
  try {
    const txt = await fsp.readFile(statePath, 'utf8');
    const obj = JSON.parse(txt);
    return obj;
  } catch {
    return null;
  }
}

async function saveState(statePath, stateObj) {
  await fsp.mkdir(path.dirname(statePath), { recursive: true });
  await fsp.writeFile(statePath, JSON.stringify(stateObj, null, 2), 'utf8');
}

function indexById(items) {
  const m = new Map();
  for (const it of items) m.set(getListingIdentity(it), it);
  return m;
}

function diffListings(prevItems, nextItems, { enableUpdated, updatedFields }) {
  const prevMap = indexById(prevItems);
  const nextMap = indexById(nextItems);

  const added = [];
  const removed = [];
  const updated = [];

  for (const [id, next] of nextMap.entries()) {
    if (!prevMap.has(id)) {
      added.push(next);
      continue;
    }

    if (enableUpdated) {
      const prev = prevMap.get(id);
      const changes = [];
      for (const f of updatedFields || []) {
        const a = prev?.[f] ?? '';
        const b = next?.[f] ?? '';
        if (String(a) !== String(b)) {
          changes.push({ field: f, from: String(a), to: String(b) });
        }
      }
      if (changes.length > 0) {
        updated.push({ id, changes, prev, next });
      }
    }
  }

  for (const [id, prev] of prevMap.entries()) {
    if (!nextMap.has(id)) removed.push(prev);
  }

  return { added, removed, updated };
}

// -------------------------
// Notify (placeholder)
// -------------------------

function resolveMaybeRelative(modulePath) {
  if (!modulePath) return null;
  // 兼容青龙：通常工作目录就是脚本目录；这里统一以 __dirname 为基准
  if (path.isAbsolute(modulePath)) return modulePath;
  return path.join(__dirname, modulePath);
}

function loadNotifyFromModule(modulePath) {
  if (!modulePath) return null;
  const full = resolveMaybeRelative(modulePath);
  // eslint-disable-next-line global-require, import/no-dynamic-require
  const mod = require(full);
  const fn = mod?.default ?? mod;
  if (typeof fn !== 'function') {
    throw new Error(`Notify module is not a function: ${modulePath}`);
  }
  return fn;
}

async function notify(diff, context) {
  // 默认通知：输出“人类可读”的文本（更适合青龙日志/通知内容）
  // 若你需要结构化 JSON 用于调试，可设置 AJ_NOTIFY_DEBUG_JSON=1

  const text = context?.text;
  if (text) {
    console.log(text);
  } else {
    const payload = {
      time: new Date().toISOString(),
      target: context?.target,
      summary: {
        added: diff.added.length,
        removed: diff.removed.length,
        updated: diff.updated.length
      },
      diff
    };
    console.log(JSON.stringify(payload, null, 2));
  }

  if (process.env.AJ_NOTIFY_DEBUG_JSON === '1') {
    const payload = {
      time: new Date().toISOString(),
      target: context?.target,
      summary: context?.summary || {
        added: diff.added.length,
        removed: diff.removed.length,
        updated: diff.updated.length
      },
      diff
    };
    console.log('\n[DEBUG_JSON]\n' + JSON.stringify(payload, null, 2));
  }
}

function truncateArray(arr, max) {
  if (!Array.isArray(arr)) return [];
  if (arr.length <= max) return arr;
  return arr.slice(0, max);
}

function normalizeStringList(v, fallback) {
  const raw = (v == null || v === '') ? fallback : v;
  const arr = Array.isArray(raw)
    ? raw
    : String(raw)
      .split(',')
      .map((x) => x.trim())
      .filter(Boolean);

  return Array.from(new Set(arr.map((x) => String(x).trim()).filter(Boolean)));
}

// -------------------------
// Report (human-readable files)
// -------------------------

function safeFilePart(s) {
  return String(s ?? '').replace(/[^a-zA-Z0-9._-]+/g, '_');
}

function normalizeFormatList(v, fallback) {
  const raw = (v == null || v === '') ? fallback : v;
  const arr = Array.isArray(raw)
    ? raw
    : String(raw)
      .split(',')
      .map((x) => x.trim())
      .filter(Boolean);
  const set = new Set(arr.map((x) => x.toLowerCase()));
  // 仅允许 csv / md
  const out = [];
  for (const x of set) {
    if (x === 'csv' || x === 'md') out.push(x);
  }
  return out.length > 0 ? out : [String(fallback || 'csv')];
}

function csvEscapeCell(s) {
  const t = String(s ?? '');
  if (/[\n\r",]/.test(t)) {
    return '"' + t.replace(/"/g, '""') + '"';
  }
  return t;
}

function withUtf8Bom(s) {
  // 兼容 Excel：UTF-8 BOM
  return '\ufeff' + s;
}

function formatIsoForFileName(date) {
  return (date instanceof Date ? date : new Date(date))
    .toISOString()
    .replace(/[:.]/g, '-')
    .replace('Z', 'Z');
}

function escapeMdText(s) {
  return String(s ?? '')
    .replace(/\|/g, '\\|')
    .replace(/\r?\n/g, ' ')
    .trim();
}

function mdLink(text, url) {
  const t = escapeMdText(text);
  if (!url) return t;
  // markdown link 中 ')' 需要转义，否则会截断
  const safeUrl = String(url).replace(/\)/g, '%29');
  return `[${t}](${safeUrl})`;
}

function formatEpochSeconds(sec) {
  const n = Number(sec);
  if (!Number.isFinite(n) || n <= 0) return '';
  // 注意：默认输出为本机时区时间（青龙一般是 Asia/Shanghai）
  return new Date(n * 1000).toLocaleString('zh-CN', { hour12: false });
}

function pickBestUrl(it) {
  return it?.pc_url || it?.tw_url || '';
}

function formatListingBrief(it) {
  const layout = [it.room_num, it.hall_num, it.toilet_num]
    .filter((x) => x !== '')
    .map((x, i) => (i === 0 ? `${x}室` : i === 1 ? `${x}厅` : `${x}卫`))
    .join('');

  const parts = [];
  if (it.price) parts.push(`${it.price}万`);
  if (it.area_num) parts.push(`${it.area_num}㎡`);
  if (layout) parts.push(layout);
  if (it.floor_level) parts.push(it.floor_level);
  if (it.orient) parts.push(it.orient);
  if (it.fitment_name) parts.push(it.fitment_name);
  if ((Number(it.duplicate_count) || 1) > 1) parts.push(`同房源${it.duplicate_count}条`);
  return parts.join(' | ');
}

function renderNotifyText(diff, { target, fetchedAt, listHash }) {
  const lines = [];

  const summary = `新增 ${diff.added.length} / 下架 ${diff.removed.length} / 变更 ${diff.updated.length}`;
  // lines.push(`【房源变更】${target.name}`);
  lines.push(summary);
  lines.push(`时间：${new Date(fetchedAt).toLocaleString('zh-CN', { hour12: false })}`);
  // lines.push(`目标：${formatTargetMeta(target)}`);

  const pushSection = (title, arr, renderLine) => {
    if (!arr || arr.length === 0) return;
    lines.push('');
    lines.push(`【${title}】`);
    arr.forEach((x, idx) => {
      lines.push(`${idx + 1}. ${renderLine(x)}`);
    });
  };

  const renderItemLine = (it) => {
    const name = it.title || it.id;
    const brief = formatListingBrief(it);
    const url = pickBestUrl(it);
    return `${name}${brief ? `\n   ${brief}` : ''}${url ? `\n   <a href=${url}>链接</a>\n` : ''}`;
  };

  pushSection('新增', diff.added, renderItemLine);
  pushSection('下架', diff.removed, renderItemLine);

  if (diff.updated && diff.updated.length > 0) {
    lines.push('');
    lines.push('【变更】');
    diff.updated.forEach((u, idx) => {
      const it = u.next || u.prev || {};
      const title = it.title || u.id;
      const url = pickBestUrl(it);
      const changes = (u.changes || []).map((c) => `${c.field}: ${c.from} -> ${c.to}`).join('; ');
      const brief = formatListingBrief(it);
      lines.push(`${idx + 1}. ${title}`);
      if (brief) lines.push(`   ${brief}`);
      if (changes) lines.push(`   ${changes}`);
      if (url) lines.push(`   ${url}`);
    });
  }

  return lines.join('\n');
}

function renderListingsMarkdown(items, { target, fetchedAt, count, listHash }) {
  const lines = [];
  lines.push(`# 房源列表 - ${escapeMdText(target.name)}`);
  lines.push('');
  lines.push(`- 目标：${formatTargetMeta(target)}`);
  lines.push(`- 抓取时间：${fetchedAt}`);
  lines.push(`- 房源数：${count}`);
  lines.push(`- 列表摘要（hash）：${listHash}`);
  lines.push('');

  const header = ['#', '实体键', '房源ID', '标题', '总价(万)', '单价', '面积', '户型', '楼层', '朝向', '装修', '重复数', '经纪人', '发布时间'];
  lines.push('|' + header.join('|') + '|');
  lines.push('|' + header.map(() => '---').join('|') + '|');

  items.forEach((it, idx) => {
    const layout = [it.room_num, it.hall_num, it.toilet_num]
      .filter((x) => x !== '')
      .map((x, i) => (i === 0 ? `${x}室` : i === 1 ? `${x}厅` : `${x}卫`))
      .join('');

    const row = [
      String(idx + 1),
      escapeMdText(getListingIdentity(it)),
      escapeMdText(it.id),
      mdLink(it.title || it.id, it.pc_url || it.tw_url),
      escapeMdText(it.price),
      escapeMdText(it.avg_price),
      escapeMdText(it.area_num),
      escapeMdText(layout),
      escapeMdText(it.floor_level || it.total_floor),
      escapeMdText(it.orient),
      escapeMdText(it.fitment_name),
      escapeMdText(it.duplicate_count),
      escapeMdText((it.broker_names || []).join(' / ')),
      escapeMdText(formatEpochSeconds(it.post_date))
    ];
    lines.push('|' + row.join('|') + '|');
  });

  lines.push('');
  return lines.join('\n');
}

function renderListingsCsv(items, { target, fetchedAt, count, listHash }) {
  const header = [
    'dedupe_key',
    'id',
    'title',
    'price_wan',
    'avg_price',
    'area',
    'room_num',
    'hall_num',
    'toilet_num',
    'floor_level',
    'total_floor',
    'orient',
    'fitment',
    'duplicate_count',
    'broker_names',
    'broker_companies',
    'post_date',
    'pc_url',
    'tw_url'
  ];

  const lines = [];
  // 元信息（用 # 开头，Excel 打开也能看见）
  // lines.push(`# target=${target.name} cityId=${target.cityId} commId=${target.commId} entry=${target.entry ?? ''}`);
  // lines.push(`# fetchedAt=${fetchedAt} count=${count} listHash=${listHash}`);
  lines.push(header.join(','));

  for (const it of items) {
    const row = [
      getListingIdentity(it),
      it.id,
      it.title,
      it.price,
      it.avg_price,
      it.area_num,
      it.room_num,
      it.hall_num,
      it.toilet_num,
      it.floor_level,
      it.total_floor,
      it.orient,
      it.fitment_name,
      it.duplicate_count,
      (it.broker_names || []).join(' / '),
      (it.broker_companies || []).join(' / '),
      formatEpochSeconds(it.post_date),
      it.pc_url,
      it.tw_url
    ].map(csvEscapeCell);
    lines.push(row.join(','));
  }

  return withUtf8Bom(lines.join('\n'));
}

function renderDiffMarkdown(diff, { target, fetchedAt, listHash }) {
  const lines = [];
  lines.push(`# 房源变更 - ${escapeMdText(target.name)}`);
  lines.push('');
  lines.push(`- 目标：${formatTargetMeta(target)}`);
  lines.push(`- 产生时间：${fetchedAt}`);
  lines.push(`- 列表摘要（hash）：${listHash}`);
  lines.push('');
  lines.push('## 摘要');
  lines.push('');
  lines.push(`- 新增：${diff.added.length}`);
  lines.push(`- 下架：${diff.removed.length}`);
  lines.push(`- 变更：${diff.updated.length}`);
  lines.push('');

  const renderSimpleTable = (title, arr) => {
    lines.push(`## ${title}（${arr.length}）`);
    lines.push('');
    if (arr.length === 0) {
      lines.push('- 无');
      lines.push('');
      return;
    }
    const header = ['实体键', '房源ID', '标题', '总价(万)', '面积', '户型', '楼层', '朝向', '装修', '重复数'];
    lines.push('|' + header.join('|') + '|');
    lines.push('|' + header.map(() => '---').join('|') + '|');
    arr.forEach((it) => {
      const layout = [it.room_num, it.hall_num, it.toilet_num]
        .filter((x) => x !== '')
        .map((x, i) => (i === 0 ? `${x}室` : i === 1 ? `${x}厅` : `${x}卫`))
        .join('');
      const row = [
        escapeMdText(getListingIdentity(it)),
        escapeMdText(it.id),
        mdLink(it.title || it.id, it.pc_url || it.tw_url),
        escapeMdText(it.price),
        escapeMdText(it.area_num),
        escapeMdText(layout),
        escapeMdText(it.floor_level || it.total_floor),
        escapeMdText(it.orient),
        escapeMdText(it.fitment_name),
        escapeMdText(it.duplicate_count)
      ];
      lines.push('|' + row.join('|') + '|');
    });
    lines.push('');
  };

  renderSimpleTable('新增', diff.added);
  renderSimpleTable('下架', diff.removed);

  lines.push(`## 变更（${diff.updated.length}）`);
  lines.push('');
  if (diff.updated.length === 0) {
    lines.push('- 无');
    lines.push('');
  } else {
    const header = ['实体键', '房源ID', '标题', '变化'];
    lines.push('|' + header.join('|') + '|');
    lines.push('|' + header.map(() => '---').join('|') + '|');
    diff.updated.forEach((u) => {
      const title = u.next?.title || u.prev?.title || u.id;
      const url = u.next?.pc_url || u.prev?.pc_url || u.next?.tw_url || u.prev?.tw_url || '';
      const changeText = (u.changes || [])
        .map((c) => `${c.field}: ${c.from} -> ${c.to}`)
        .join('; ');
      const row = [
        escapeMdText(u.id),
        escapeMdText(u.next?.id || u.prev?.id || ''),
        mdLink(title, url),
        escapeMdText(changeText)
      ];
      lines.push('|' + row.join('|') + '|');
    });
    lines.push('');
  }

  return lines.join('\n');
}

function renderDiffCsv(diff, { target, fetchedAt, listHash }) {
  const header = [
    'type',
    'dedupe_key',
    'id',
    'title',
    'price_wan',
    'avg_price',
    'area',
    'layout',
    'floor_level',
    'orient',
    'fitment',
    'duplicate_count',
    'changes',
    'pc_url',
    'tw_url',
    'fetchedAt',
    'listHash'
  ];

  const lines = [];
  lines.push(`# target=${target.name} ${formatTargetMeta(target)}`);
  lines.push(`# fetchedAt=${fetchedAt} listHash=${listHash}`);
  lines.push(header.join(','));

  const pushItem = (type, it, changesText) => {
    const layout = [it.room_num, it.hall_num, it.toilet_num]
      .filter((x) => x !== '')
      .map((x, i) => (i === 0 ? `${x}室` : i === 1 ? `${x}厅` : `${x}卫`))
      .join('');

    const row = [
      type,
      getListingIdentity(it),
      it.id,
      it.title,
      it.price,
      it.avg_price,
      it.area_num,
      layout,
      it.floor_level,
      it.orient,
      it.fitment_name,
      it.duplicate_count,
      changesText || '',
      it.pc_url,
      it.tw_url,
      fetchedAt,
      listHash
    ].map(csvEscapeCell);
    lines.push(row.join(','));
  };

  for (const it of diff.added) pushItem('added', it, '');
  for (const it of diff.removed) pushItem('removed', it, '');
  for (const u of diff.updated) {
    const it = u.next || u.prev || {};
    const changesText = (u.changes || []).map((c) => `${c.field}: ${c.from} -> ${c.to}`).join('; ');
    pushItem('updated', it, changesText);
  }

  return withUtf8Bom(lines.join('\n'));
}

function buildListingsReportPath(stateDir, target, format) {
  const safe = getTargetFileSafeId(target);
  const ext = (format === 'csv') ? 'csv' : 'md';
  return path.join(stateDir, `listings_${safe}.${ext}`);
}

function buildDiffReportPath(stateDir, target, date, { latest, format } = {}) {
  const safe = getTargetFileSafeId(target);
  const ext = (format === 'csv') ? 'csv' : 'md';
  if (latest) return path.join(stateDir, `diff_${safe}_latest.${ext}`);
  return path.join(stateDir, `diff_${safe}_${formatIsoForFileName(date)}.${ext}`);
}

function isRetryableFsError(e) {
  const code = e && typeof e === 'object' ? e.code : null;
  return code === 'EBUSY' || code === 'EPERM' || code === 'EACCES';
}

async function writeTextFileEnsureDir(filePath, content) {
  await fsp.mkdir(path.dirname(filePath), { recursive: true });

  // Windows 上如果文件正在被 Excel/WPS/预览占用，直接写入会报 EBUSY/EPERM。
  // 这里做简单重试，避免任务直接失败（青龙环境一般不会锁文件）。
  const maxAttempts = 6;
  let lastErr;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await fsp.writeFile(filePath, content, 'utf8');
      return;
    } catch (e) {
      lastErr = e;
      const shouldRetry = isRetryableFsError(e) && attempt < maxAttempts;
      if (!shouldRetry) break;
      await sleep(200 * attempt);
    }
  }

  throw lastErr;
}

async function safeUnlink(filePath) {
  try {
    await fsp.unlink(filePath);
  } catch {
    // ignore
  }
}

async function cleanupLegacyReportFiles(stateDir, target, { listingFormats, diffFormats, saveDiffHistory }) {
  await fsp.mkdir(stateDir, { recursive: true });

  let names;
  try {
    names = await fsp.readdir(stateDir);
  } catch {
    return;
  }

  const safe = getTargetFileSafeId(target);
  const listingsPrefix = `listings_${safe}.`;
  const diffPrefix = `diff_${safe}_`;
  const diffLatestPrefix = `diff_${safe}_latest.`;

  for (const name of names) {
    if (name.startsWith(listingsPrefix)) {
      const ext = String(name.split('.').pop() || '').toLowerCase();
      if (!listingFormats.includes(ext)) {
        await safeUnlink(path.join(stateDir, name));
      }
      continue;
    }

    if (name.startsWith(diffLatestPrefix)) {
      const ext = String(name.split('.').pop() || '').toLowerCase();
      if (!diffFormats.includes(ext)) {
        await safeUnlink(path.join(stateDir, name));
      }
      continue;
    }

    if (name.startsWith(diffPrefix)) {
      // diff_<safe>_<timestamp>.<ext>
      const ext = String(name.split('.').pop() || '').toLowerCase();
      if (!saveDiffHistory || !diffFormats.includes(ext)) {
        await safeUnlink(path.join(stateDir, name));
      }
    }
  }
}

// -------------------------
// Main
// -------------------------

async function runForTarget({ cfg, target, args, notifyFn }) {
  const stateDir = path.resolve(__dirname, cfg.stateDir);
  const statePath = buildStatePath(stateDir, target);

  const prevState = await loadPrevState(statePath);
  const prevItems = dedupeListings(Array.isArray(prevState?.items) ? prevState.items : [], {
    enabled: cfg.dedupe?.enabled ?? true,
    priceToleranceWan: cfg.dedupe?.priceToleranceWan,
    featureFields: cfg.dedupe?.featureFields,
    photoField: cfg.dedupe?.photoField
  });

  let nextItemsRaw;
  if (args.harPath) {
    nextItemsRaw = await loadListingsFromHar(args.harPath);
  } else {
    nextItemsRaw = await fetchAllCommunityListings({ request: cfg.request, target });
  }

  const nextItems = dedupeListings(nextItemsRaw, {
    enabled: cfg.dedupe?.enabled ?? true,
    priceToleranceWan: cfg.dedupe?.priceToleranceWan,
    featureFields: cfg.dedupe?.featureFields,
    photoField: cfg.dedupe?.photoField
  });

  // 为状态文件计算摘要，避免未来扩展时误判
  const normalizedForHash = nextItems
    .slice()
    .sort((a, b) => getListingIdentity(a).localeCompare(getListingIdentity(b)))
    .map((x) => ({ id: getListingIdentity(x) }));
  const listHash = sha256(stableStringify(normalizedForHash));

  // 生成“人类可读”的列表文件（csv/md）便于浏览
  const reportEnabled = (cfg.report?.enabled ?? true) && !args.noReport;
  const fetchedAtForReport = new Date().toISOString();

  const listingFormats = normalizeFormatList(cfg.report?.listingsFormat, 'csv');
  const diffFormats = normalizeFormatList(cfg.report?.diffFormat, 'md');

  if (reportEnabled && (cfg.report?.cleanupLegacy ?? true)) {
    await cleanupLegacyReportFiles(stateDir, target, {
      listingFormats,
      diffFormats,
      saveDiffHistory: !!cfg.report?.saveDiffHistory
    });
  }

  if (reportEnabled && (cfg.report?.saveListings ?? true)) {
    for (const fmt of listingFormats) {
      const content = fmt === 'md'
        ? renderListingsMarkdown(nextItems, {
          target,
          fetchedAt: fetchedAtForReport,
          count: nextItems.length,
          listHash
        })
        : renderListingsCsv(nextItems, {
          target,
          fetchedAt: fetchedAtForReport,
          count: nextItems.length,
          listHash
        });

      const reportPath = buildListingsReportPath(stateDir, target, fmt);
      try {
        await writeTextFileEnsureDir(reportPath, content);
      } catch (e) {
        console.warn(`[WARN] 写入列表报告失败: ${reportPath} (${e?.code || e?.message || e})`);
      }
    }
  }

  const diff = diffListings(prevItems, nextItems, {
    enableUpdated: !!cfg.diff.enableUpdated,
    updatedFields: cfg.diff.updatedFields
  });

  const changed = diff.added.length > 0 || diff.removed.length > 0 || (cfg.diff.enableUpdated && diff.updated.length > 0);

  if (changed) {
    const max = cfg.diff.maxItemsInNotification ?? 50;
    const limited = {
      added: truncateArray(diff.added, max),
      removed: truncateArray(diff.removed, max),
      updated: truncateArray(diff.updated, max)
    };

    // 发生变化时，额外保存“人类可读”的差异文件（csv/md）
    if (reportEnabled && (cfg.report?.saveDiff ?? true)) {
      for (const fmt of diffFormats) {
        const content = fmt === 'md'
          ? renderDiffMarkdown(limited, { target, fetchedAt: fetchedAtForReport, listHash })
          : renderDiffCsv(limited, { target, fetchedAt: fetchedAtForReport, listHash });

        // 默认只写 latest，避免产生过多文件
        const diffLatestPath = buildDiffReportPath(stateDir, target, new Date(), { latest: true, format: fmt });
        try {
          await writeTextFileEnsureDir(diffLatestPath, content);
        } catch (e) {
          console.warn(`[WARN] 写入 diff_latest 失败: ${diffLatestPath} (${e?.code || e?.message || e})`);
        }

        if (cfg.report?.saveDiffHistory) {
          const diffPath = buildDiffReportPath(stateDir, target, new Date(), { latest: false, format: fmt });
          try {
            await writeTextFileEnsureDir(diffPath, content);
          } catch (e) {
            console.warn(`[WARN] 写入 diff 历史失败: ${diffPath} (${e?.code || e?.message || e})`);
          }
        }
      }
    }

    const notifyText = renderNotifyText(limited, {
      target,
      fetchedAt: fetchedAtForReport,
      listHash
    });

    await notifyFn(limited, {
      target,
      fetchedAt: fetchedAtForReport,
      listHash,
      summary: {
        added: limited.added.length,
        removed: limited.removed.length,
        updated: limited.updated.length
      },
      text: notifyText
    });
  } else {
    // 无变化时也输出一行，便于青龙日志确认脚本在工作
    const rawCount = Array.isArray(nextItemsRaw) ? nextItemsRaw.length : nextItems.length;
    console.log(`[NOCHANGE] ${target.name} ${formatTargetMeta(target)} count=${nextItems.length} raw=${rawCount}`);
  }

  const nextState = {
    version: 2,
    updatedAt: new Date().toISOString(),
    target,
    listHash,
    rawCount: Array.isArray(nextItemsRaw) ? nextItemsRaw.length : nextItems.length,
    count: nextItems.length,
    items: nextItems
  };

  if (!args.noSave) {
    await saveState(statePath, nextState);
  }

  return { changed, diff, nextStatePath: statePath };
}

async function main() {
  const args = parseArgs(process.argv);
  const cfg = loadConfig(args);

  if (!Array.isArray(cfg.targets) || cfg.targets.length === 0) {
    throw new Error('未配置 targets');
  }

  const notifyModule = args.notifyModule || process.env.AJ_NOTIFY_MODULE || null;
  const notifyFn = notifyModule ? loadNotifyFromModule(notifyModule) : notify;

  const targets = args.target
    ? cfg.targets.filter((t) => t.name === args.target)
    : cfg.targets;

  if (targets.length === 0) {
    throw new Error(`未找到 target: ${args.target}`);
  }

  for (const t of targets) {
    await runForTarget({ cfg, target: t, args, notifyFn });
  }
}

main().catch((e) => {
  console.error('[ERROR]', e && e.stack ? e.stack : String(e));
  process.exitCode = 1;
});
