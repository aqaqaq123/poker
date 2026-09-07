"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 3000);
const DATA_FILE = process.env.DATA_FILE || "/data/state.json";
const HISTORY_FILE = process.env.HISTORY_FILE || path.join(path.dirname(DATA_FILE), "history.jsonl");
const PUB_DIR = path.join(__dirname, "..", "public");
const LOG_KEEP = 300;               // 内存/前端只保留最近300条动态（减小每次广播体积）；全量历史永久写入 HISTORY_FILE

// ========== 心跳/掉线配置 ==========
const HEARTBEAT_INTERVAL = 1000;    // 每1秒跑一次掉线检测（保证检测精度）
const HEARTBEAT_TIMEOUT = 10000;    // 10秒没收到任何消息判定掉线（前端每2秒发一次应用层心跳，给网络波动更多容忍）
const WS_PING_INTERVAL = 3000;      // 协议层 ping 每3秒发一次（应用层心跳已保活，仅辅助探测半开TCP连接，不必每秒发）
const OFFLINE_GRACE = 15000;         // 断开后15秒宽限期（允许重连恢复身份）
// 离线玩家正常倒计时，超时自动过牌（只剥夺本回合下注权，不弃牌）
const MSG_RATE_LIMIT = 20;           // 每秒最多20条消息
const MSG_RATE_WINDOW = 1000;        // 限流窗口1秒

// ========== 房间状态 ==========
function newRoom(){
  return {
    config: { sb: 1, bb: 2, turnSeconds: 60, addSeconds: 120, addCount: 5 },
    phase: "lobby", players: [], turn: null, order: [], result: null, logs: [],
    totalBuyin: 0, ownerId: null, dealerId: null, handsPlayed: 0,
    street: 0, round: 0, currentBet: 0, lastRaiseSize: 0, noRaise: false, lastBettorId: null,
    raiseCount: 0,   // 当前下注轮内"构成加注"的动作次数（用于计算 open/3-bet/4-bet 等叫法）
    idSeq: 1, pending: [],
  };
}

let room = (() => {
  try {
    const s = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    if (!s || !s.config) throw new Error("bad");
    s.phase = "lobby"; s.turn = null; s.order = []; s.result = null; s.pending = [];
    s.street = 0; s.round = 0; s.currentBet = 0; s.lastRaiseSize = 0; s.noRaise = false; s.lastBettorId = null; s.raiseCount = 0;
    s.players.forEach((p, i) => { p.roundBet = 0; p.folded = false; p.inHand = !p.resting; p.hasActed = true; if (!p.seat) p.seat = i + 1; });
    s.config = Object.assign({ sb: 1, bb: 2, turnSeconds: 60, addSeconds: 120, addCount: 5 }, s.config || {});
    return s;
  } catch (e) { return newRoom(); }
})();

// ========== 客户端连接（含心跳/限流状态）==========
const clients = new Map(); // ws -> { playerId, lastPong, msgTimes: [] }
function uid(){ return "p" + (room.idSeq++) + Math.random().toString(36).slice(2,6); }
function nextSeat(){ let mx = 0; room.players.forEach(p => { const s = +p.seat || 0; if (s > mx) mx = s; }); return mx + 1; }
function seatsSorted(){ return [...room.players].sort((a, b) => (+a.seat || 0) - (+b.seat || 0)); }

// ========== 防抖保存 ==========
let saveTimer = null;
let savePending = false;
function save(){
  savePending = true;
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    if (!savePending) return;
    savePending = false;
    try {
      const dir = path.dirname(DATA_FILE);
      if (!fs.existsSync(dir)) return;
      const data = JSON.stringify(room);
      fs.writeFile(DATA_FILE, data, (err) => {
        if (err) console.error("save error:", err.message);
      });
    } catch (e) { console.error("save exception:", e.message); }
  }, 500);
}
function saveSync(){
  try {
    const dir = path.dirname(DATA_FILE);
    if (!fs.existsSync(dir)) return;
    fs.writeFileSync(DATA_FILE, JSON.stringify(room));
  } catch (e) {}
}

// 全量历史：实时追加写入独立文件（永久保存，重置牌局/重启服务都不丢）；内存只留最近 LOG_KEEP 条用于前端显示
let historyStream = null;
function appendHistory(e){
  try {
    if (!historyStream) historyStream = fs.createWriteStream(HISTORY_FILE, { flags: "a" });
    historyStream.write(JSON.stringify(e) + "\n");
  } catch (err) { console.error("history write error:", err.message); }
}
function log(m){ const e = { t: Date.now(), m: m }; room.logs.push(e); if (room.logs.length > LOG_KEEP) room.logs = room.logs.slice(-LOG_KEEP); appendHistory(e); }
function pot(){ return room.players.reduce((s,p)=>s + p.totalInvested, 0); }
function findPlayer(id){ return room.players.find(p => p.id === id); }
function pname(id){ const p = findPlayer(id); return p ? p.name : "?"; }

// ========== 在线玩家 Set ==========
function getOnlineSet(){
  const set = new Set();
  const now = Date.now();
  for (const [ws, info] of clients) {
    // 必须同时满足：有playerId、连接打开、10秒内有消息
    if (info && info.playerId && ws.readyState === 1 && (now - info.lastPong) <= HEARTBEAT_TIMEOUT) {
      set.add(info.playerId);
    }
  }
  return set;
}
function onlineOf(pid, onlineSet){
  if (onlineSet) return onlineSet.has(pid);
  const now = Date.now();
  for (const [ws, info] of clients) {
    if (info && info.playerId === pid && ws.readyState === 1 && (now - info.lastPong) <= HEARTBEAT_TIMEOUT) return true;
  }
  return false;
}

function handleOffline(pid, onlineSet){
  if (!onlineOf(pid, onlineSet)) {
    const p = findPlayer(pid);
    if (p) log(p.name + " 已离线");
    // 找不到 p → 该玩家已被移出牌局，不再记"?"占位日志
    // 注意：不自动弃牌！掉线玩家的下注由超时检查处理（倒计时结束自动过牌，不弃牌）
    // 注意：不自动移交庄家！庄家离线后保持原庄家位，回来后继续操作
  }
}
const pendingOffline = new Map();
function scheduleOfflineCheck(pid){
  if (pendingOffline.has(pid)) return;
  pendingOffline.set(pid, setTimeout(() => {
    pendingOffline.delete(pid);
    handleOffline(pid, getOnlineSet());
    broadcast();   // 只改在线状态/日志，日志已实时写入历史文件，无需再写 state.json
  }, OFFLINE_GRACE));
}
function cancelOfflineCheck(pid){
  const t = pendingOffline.get(pid);
  if (t) { clearTimeout(t); pendingOffline.delete(pid); }
}

function sendTo(ws, obj){ if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function toast(ws, msg){ sendTo(ws, { type: "toast", msg }); }
function sendToPlayer(pid, obj){ for (const [ws, info] of clients) { if (info && info.playerId === pid && ws.readyState === 1) ws.send(JSON.stringify(obj)); } }

let reqSeq = 0; const reqTimers = new Map();
function scheduleReqTimeout(req){ const t = setTimeout(() => { reqTimers.delete(req.id); const idx = room.pending.findIndex(r => r.id === req.id); if (idx >= 0) { room.pending.splice(idx, 1); sendToPlayer(req.fromId, { type: "toast", msg: "交易请求超时未确认，已取消" }); if (req.toId) sendToPlayer(req.toId, { type: "toast", msg: "交易请求已超时" }); } }, 60000); reqTimers.set(req.id, t); }

// ========== 结算逻辑 ==========
function settle(entries){
  const act = entries.filter(e => e.bet > 0);
  const byId = {}; act.forEach(e => byId[e.id] = { id:e.id, name:e.name, bet:e.bet, rank:e.rank, seat:e.seat || 0, win:0 });
  let pool = act.map(e => ({ id:e.id, name:e.name, seat:e.seat || 0, rank:e.rank, left:e.bet }));
  const pots = [];
  while (pool.length) {
    const min = Math.min(...pool.map(p => p.left));
    const size = min * pool.length;
    const bestRank = Math.min(...pool.map(p => p.rank));
    const winners = pool.filter(p => p.rank === bestRank);
    /* 整数平分：每人基础赢取 = floor(size/人数)，余数按座位从小到大分配（座位越靠前越先得奇数筹码） */
    const base = Math.floor(size / winners.length);
    const remainder = size % winners.length;
    const sortedWinners = [...winners].sort((a,b) => (a.seat||0) - (b.seat||0));
    winners.forEach(w => byId[w.id].win += base);
    for (let i = 0; i < remainder; i++) { byId[sortedWinners[i].id].win += 1; }
    pots.push({ size, count: pool.length, winnerIds: winners.map(w=>w.id) });
    pool = pool.map(p => ({ id:p.id, name:p.name, seat:p.seat, rank:p.rank, left:p.left - min })).filter(p => p.left > 0);
  }
  let results = act.map(e => { const w = byId[e.id].win; return { id:e.id, name:e.name, bet:e.bet, rank:e.rank, win:w, net:w - e.bet }; });
  results.sort((a,b)=>a.rank - b.rank || a.bet - b.bet);
  return { pots, results };
}

function seatsFromDealer(){ const ps = seatsSorted(); const di = Math.max(0, ps.findIndex(p => p.id === room.dealerId)); const out = []; for (let k = 1; k <= ps.length; k++) out.push(ps[(di + k) % ps.length]); return out; }
function rotateDealer(){
  const ps = seatsSorted(); if (!ps.length) return;
  const di = Math.max(0, ps.findIndex(p => p.id === room.dealerId));
  // 休息玩家不再当庄：轮转时跳过 resting 的玩家
  for (let k = 1; k <= ps.length; k++) {
    const p = ps[(di + k) % ps.length];
    if (!p.resting) { room.dealerId = p.id; log("庄家（荷官）移至 " + pname(room.dealerId)); return; }
  }
}
// 查找休息/离线等场景下庄家的继任者：优先下一个「在线且不在休息」的玩家，退而求其次下一个「不在休息」的玩家
function nextDealerAfter(pid, onlineSet){
  const seated = seatsSorted();
  const idx = seated.findIndex(p => p.id === pid);
  if (idx < 0) return null;
  for (let i = 1; i <= seated.length; i++) { const p = seated[(idx + i) % seated.length]; if (!p.resting && onlineSet.has(p.id)) return p; }
  for (let i = 1; i <= seated.length; i++) { const p = seated[(idx + i) % seated.length]; if (!p.resting) return p; }
  return null;
}

// 切换玩家休息状态（支持玩家本人，也支持房主控制其他玩家）
function toggleRest(player){
  if (!player) return;
  player.resting = !player.resting;
  if (player.resting) {
    log(player.name + " 开始休息（本手视为弃牌）");
    if (player.inHand && !player.folded && player.stack > 0) { player.folded = true; player.hasActed = true; log(player.name + " 休息，本手弃牌"); }
    if (room.phase === "betting") afterAction();
    // 休息玩家不再当庄：若当前为庄家，移交给下一个在线且不在休息的玩家
    if (room.dealerId === player.id) {
      const nd = nextDealerAfter(player.id, getOnlineSet());
      if (nd) { room.dealerId = nd.id; log(player.name + " 休息，庄家移交给 " + nd.name); }
      else { log(player.name + " 休息，当前无其他玩家接任庄家"); }
    }
  } else {
    /* 对局过程中结束休息：本手仍不参与（明确置 inHand=false / folded=true），下一手 startHand 才自动恢复 */
    if (room.phase === "betting" || room.phase === "settling" || room.phase === "show") {
      player.inHand = false; player.folded = true; player.hasActed = true;
      log(player.name + " 结束休息，本手仍不参与，下一手恢复下注");
    } else {
      log(player.name + " 结束休息，下一手恢复下注");
    }
  }
}

function preflopQueue(){
  const order = seatsFromDealer().filter(p => p.inHand && !p.folded && p.stack > 0);
  if (order.length <= 2) { return [order[1], order[0]].filter(Boolean).map(p => p.id); }
  const rest = order.slice(2);
  return rest.concat([order[0], order[1]]).map(p => p.id);
}
function postflopQueue(){ return seatsFromDealer().filter(p => p.inHand && !p.folded && p.stack > 0).map(p => p.id); }
function actionQueue(){ return room.street === 0 ? preflopQueue() : postflopQueue(); }
function needToCallOf(p){ return Math.max(0, room.currentBet - p.roundBet); }

function advanceTurn(){
  if (room.phase !== "betting") return;
  const ps = seatsSorted(); let startIdx = -1;
  if (room.turn) startIdx = ps.findIndex(p => p.id === room.turn.playerId);
  const onlineSet = getOnlineSet();
  for (let k = 1; k <= ps.length; k++) {
    const p = ps[(startIdx + k + ps.length) % ps.length];
    if (p && p.inHand && !p.folded && p.stack > 0 && !p.hasActed) {
      // 掉线玩家也正常轮到他，倒计时结束自动过牌（不弃牌）
      room.turn = { playerId: p.id, deadline: Date.now() + room.config.turnSeconds * 1000 };
      return;
    }
  }
  room.turn = null; log("本轮下注结束，庄家可「进入下一轮下注」或「结束本手」");
}

function afterAction(){
  const alive = room.players.filter(p => p.inHand && !p.folded);
  if (alive.length <= 1) {
    room.phase = "settling"; room.turn = null;
    if (alive.length === 1) { room.order = [{ id: alive[0].id, rank: 1 }]; log(alive[0].name + " 胜出（其余玩家弃牌），自动结算"); doSettle(); }
    else { room.order = []; log("所有玩家弃牌，请庄家点选赢家"); }
    return;
  }
  const withChips = alive.filter(p => p.stack > 0);
  if (withChips.length === 0 && alive.length > 1) {
    log("所有玩家已全下，自动发牌至河牌摊牌");
    while (room.street < 3) {
      room.street++; room.round++; room.currentBet = 0; room.lastRaiseSize = room.config.bb; room.noRaise = false; room.lastBettorId = null; room.raiseCount = 0;
      room.players.forEach(p => { p.roundBet = 0; p.hasActed = false; p.lastActLabel = null; });
      log("自动进入 " + streetName() + "（所有人全下）");
    }
    endHand(); return;
  }
  const needAct = alive.filter(p => p.stack > 0);
  if (needAct.length === 0 || needAct.every(p => p.hasActed)) { room.turn = null; log("本轮下注结束，庄家可「进入下一轮下注」或「结束本手」"); return; }
  advanceTurn();
}

// ========== 超时检查（离线玩家正常倒计时，超时自动过牌不弃牌）==========
setInterval(() => {
  if (room.phase !== "betting" || !room.turn) return;
  const p = findPlayer(room.turn.playerId);
  if (!p) { room.turn = null; afterAction(); broadcast(); save(); return; }
  if (Date.now() >= room.turn.deadline) {
    const onlineSet = getOnlineSet();
    const isOnline = onlineSet.has(p.id);
    // 超时：自动过牌（只剥夺这一回合下注权，不弃牌，下一回合恢复）
    p.hasActed = true;
    if (isOnline) {
      if (needToCallOf(p) > 0) { p.folded = true; log(p.name + " 超时需跟注，自动弃牌"); }
      else { p.lastActLabel = "check"; log(p.name + " 超时，自动过牌"); }
    } else {
      // 掉线玩家：自动过牌，不弃牌，下一回合还能参与
      p.lastActLabel = "check"; log(p.name + " 掉线超时，自动过牌（本回合剥夺下注权，下一回合恢复）");
    }
    afterAction(); broadcast(); save();
  }
}, 500);

function startHand(){
  if (room.phase === "betting") return;
  room.handsPlayed++; room.phase = "betting"; room.result = null; room.order = [];
  room.street = 0; room.round = 0; room.currentBet = 0; room.lastRaiseSize = room.config.bb; room.noRaise = false; room.lastBettorId = null; room.raiseCount = 0;
  room.players.forEach(p => { p.totalInvested = 0; p.roundBet = 0; p.hasActed = false; p.folded = false; p.inHand = (p.stack > 0 && !p.resting); p.lastActionBet = 0; p.lastActLabel = null; });
  const order = seatsFromDealer().filter(p => p.inHand);
  if (order.length < 2) { log("至少需要 2 名有筹码且不休息的玩家才能开始"); room.phase = "lobby"; return; }
  let sbP, bbP;
  if (order.length === 2) { sbP = order[1]; bbP = order[0]; } else { sbP = order[0]; bbP = order[1]; }
  const paidSb = Math.min(sbP.stack, room.config.sb); sbP.stack -= paidSb; sbP.roundBet += paidSb; sbP.totalInvested += paidSb;
  const paidBb = Math.min(bbP.stack, room.config.bb); bbP.stack -= paidBb; bbP.roundBet += paidBb; bbP.totalInvested += paidBb;
  room.currentBet = Math.max(paidSb, paidBb);
  if (sbP.stack <= 0) sbP.hasActed = true;
  if (bbP.stack <= 0) bbP.hasActed = true;
  log("一手开始（庄家 " + pname(room.dealerId) + "）：" + sbP.name + " 小盲 " + paidSb + "，" + bbP.name + " 大盲 " + paidBb + "，底池 " + pot());
  const first = actionQueue().find(pid => !findPlayer(pid).hasActed);
  if (first) { room.turn = { playerId: first, deadline: Date.now() + room.config.turnSeconds * 1000 }; log("翻牌前：从 " + pname(first) + " 开始行动，大盲（" + bbP.name + "）最后"); }
  else { room.turn = null; log("本轮无玩家需要行动"); }
}

function nextRound(){
  if (room.phase !== "betting") return;
  const alive = room.players.filter(p => p.inHand && !p.folded);
  if (alive.length <= 1) { endHand(); return; }
  if (room.street >= 3) { endHand(); return; }
  const pending = room.players.filter(p => p.inHand && !p.folded && p.stack > 0 && !p.hasActed);
  if (pending.length > 0) { log("还有玩家未下注（" + pending.map(p=>p.name).join("、") + "），请等本轮下注完成"); return; }
  room.street++; room.round++; room.currentBet = 0; room.lastRaiseSize = room.config.bb; room.noRaise = false; room.lastBettorId = null; room.raiseCount = 0;
  room.players.forEach(p => { p.roundBet = 0; p.hasActed = false; p.lastActLabel = null; });
  const q = actionQueue(); const first = q[0];
  if (first) { room.turn = { playerId: first, deadline: Date.now() + room.config.turnSeconds * 1000 }; log("第 " + (room.round + 1) + " 轮下注开始（" + streetName() + "）：从 " + pname(first) + " 开始行动"); }
  else { room.turn = null; log("本轮无玩家需要行动"); }
}
function streetName(){ return ["翻前","翻后","转牌","河牌"][room.street] || ("第" + (room.street + 1) + "轮"); }
/* 根据当前下注轮（street）和本轮的加注次数，返回标准的专业加注叫法 */
function raiseLabel(street, raiseCount){
  if (street === 0) {  // 翻牌前：BB 视为第1个bet，open=第2个bet
    if (raiseCount === 1) return "open";   // 第一个加注 = 开池加注
    if (raiseCount === 2) return "3-bet";
    if (raiseCount === 3) return "4-bet";
    if (raiseCount === 4) return "5-bet";
    if (raiseCount >= 5) return (raiseCount + 1) + "-bet";
  } else {             // 翻牌后：第一个下注 = bet
    if (raiseCount === 1) return "bet";
    if (raiseCount === 2) return "raise";
    if (raiseCount === 3) return "3-bet";
    if (raiseCount === 4) return "4-bet";
    if (raiseCount >= 5) return raiseCount + "-bet";
  }
  return "raise";
}

function endHand(){
  if (room.phase !== "betting") return false;
  const pending = room.players.filter(p => p.inHand && !p.folded && p.stack > 0 && !p.hasActed);
  if (pending.length > 0) { log("还有玩家未下注（" + pending.map(p=>p.name).join("、") + "），不能结束本手"); return false; }
  room.phase = "settling"; room.turn = null;
  const alive = room.players.filter(p => p.inHand && !p.folded);
  if (alive.length === 1) { room.order = [{ id: alive[0].id, rank: 1 }]; log(alive[0].name + " 胜出（其余玩家弃牌），自动结算"); doSettle(); return true; }
  room.order = []; log("下注结束，请庄家点选赢家名次"); return true;
}

function doSettle(){
  const map = {}; room.order.forEach(o => map[o.id] = o.rank);
  const entries = room.players.filter(p => p.totalInvested > 0).map(p => ({ id: p.id, name: p.name, seat: p.seat || 0, bet: p.totalInvested, rank: map[p.id] || 999 }));
  const { pots, results } = settle(entries);
  /* 结算前记录每位玩家的筹码（用于计算变化） */
  const beforeStacks = {};
  room.players.forEach(p => { beforeStacks[p.id] = p.stack; });
  room.players.forEach(p => { const r = results.find(x => x.id === p.id); if (r) p.stack += r.win; });
  room.result = { pots, results }; room.phase = "show"; rotateDealer();
  /* 对局记录：体现庄家点选赢家后每位玩家的具体筹码额（投入 / 赢得 / 净盈亏） */
  const sumLine = results.length
    ? results.map(r => {
        const netStr = r.net >= 0 ? "+" + r.net : String(r.net);
        return r.name + " 赢得 " + r.win + "（投入 " + r.bet + "，净 " + netStr + "）";
      }).join("；")
    : "无";
  log("本手结算：" + sumLine);
  /* 结构化记录每手结算详情（完整历史页面渲染成玩家筹码变化表） */
  /* 只显示参与本手的玩家（totalInvested > 0），避免未参与玩家冗余显示 */
  const handDetails = room.players.filter(p => p.totalInvested > 0).map(p => {
    const r = results.find(x => x.id === p.id);
    const before = beforeStacks[p.id] || 0;
    const after = p.stack;
    const invested = p.totalInvested || 0;
    const won = r ? r.win : 0;
    const net = r ? r.net : (after - before);
    return { name: p.name, seat: p.seat, before: before, invested: invested, won: won, net: net, after: after };
  });
  appendHistory({ t: Date.now(), m: "本手结算：" + sumLine, type: "hand-settlement", handNo: room.handsPlayed, players: handDetails });
  log("结算完成，下一手由 " + pname(room.dealerId) + "（新庄家）点开始");
}

// ========== 状态构建缓存 ==========
let stateCache = null;
let stateCacheTime = 0;
function buildCommonState(onlineSet){
  const now = Date.now();
  if (stateCache && (now - stateCacheTime) < 50) return stateCache;
  const players = room.players.map(p => ({
    id: p.id, name: p.name, seat: p.seat || 0, stack: p.stack, inHand: p.inHand, folded: p.folded,
    roundBet: p.roundBet, totalInvested: p.totalInvested, hasActed: p.hasActed, lastActionBet: p.lastActionBet || 0,
    lastActLabel: p.lastActLabel || null,
    isTurn: !!(room.turn && room.turn.playerId === p.id), isDealer: p.id === room.dealerId, isOwner: p.id === room.ownerId,
    addLeft: p.addLeft, resting: p.resting, online: onlineSet.has(p.id), unreadTransfers: p.unreadTransfers || [],
  }));
  stateCache = {
    now, ownerId: room.ownerId, dealerId: room.dealerId,
    phase: room.phase, config: room.config, pot: pot(), street: room.street, round: room.round,
    currentBet: room.currentBet, lastRaiseSize: room.lastRaiseSize, noRaise: room.noRaise, lastBettorId: room.lastBettorId,
    turn: room.turn ? { playerId: room.turn.playerId, deadline: room.turn.deadline } : null,
    players, totalBuyin: room.totalBuyin, logs: room.logs,
  };
  stateCacheTime = now;
  return stateCache;
}
function invalidateStateCache(){ stateCache = null; }

function makeStateFor(ws, common, onlineSet){
  const info = clients.get(ws);
  const myId = info ? info.playerId : null;
  const isOwner = room.ownerId === myId;
  const isDealer = room.dealerId === myId;
  return {
    type: "state", now: common.now, selfId: myId,
    ownerId: common.ownerId, dealerId: common.dealerId, amOwner: isOwner, amDealer: isDealer,
    phase: common.phase, config: common.config, pot: common.pot, street: common.street, round: common.round,
    currentBet: common.currentBet, lastRaiseSize: common.lastRaiseSize, noRaise: common.noRaise, lastBettorId: common.lastBettorId,
    turn: common.turn,
    players: common.players,
    order: isDealer ? room.order : null,
    result: room.phase === "show" ? room.result : null,
    totalBuyin: common.totalBuyin, logs: common.logs,
    /* 双保险：发给当前玩家的待处理交易请求也随 state 广播，避免单独消息丢失导致看不到同意/拒绝弹窗 */
    myReqs: room.pending.filter(r => r.toId === myId).map(r => ({ id: r.id, kind: r.type, fromName: pname(r.fromId), amount: r.amount })),
  };
}

function broadcast(){
  invalidateStateCache();
  const onlineSet = getOnlineSet();
  const common = buildCommonState(onlineSet);
  for (const ws of clients.keys()) {
    if (ws.readyState === 1) {
      const state = makeStateFor(ws, common, onlineSet);
      ws.send(JSON.stringify(state));
    }
  }
}

// ========== 消息频率限制 ==========
function checkRateLimit(info){
  const now = Date.now();
  // 清理1秒前的记录
  info.msgTimes = info.msgTimes.filter(t => now - t < MSG_RATE_WINDOW);
  if (info.msgTimes.length >= MSG_RATE_LIMIT) return false;
  info.msgTimes.push(now);
  return true;
}

// ========== 消息处理 ==========
function handle(ws, raw){
  const info = clients.get(ws);
  if (!info) return;
  // 限流检查
  if (!checkRateLimit(info)) {
    console.warn("Rate limit exceeded for connection, dropping message");
    return;
  }
  let m; try { m = JSON.parse(raw); } catch (e) { return; }
  const myId = info.playerId;
  const isOwner = room.ownerId === myId; const isDealer = room.dealerId === myId;
  const me = myId ? findPlayer(myId) : null;
  switch (m.type) {
    case "hb": return; // 应用层心跳：直接返回，不触发 save 和 broadcast（性能优化）
    case "join": {
      const name = String(m.name || "玩家").slice(0, 8);
      let pl = room.players.find(p => p.name === name);
      if (pl) {
        pl.ip = ws.__ip || pl.ip;
        info.playerId = pl.id;
        // 同名玩家重连：以服务端筹码为准，不使用客户端 localStorage 的旧值覆盖
        // （转账、买卖、带入、结算等变化都发生在服务端，客户端值可能过时）
        const clientStack = Math.max(0, Math.floor(+m.stack || 0));
        if (clientStack > 0 && clientStack !== pl.stack) {
          log(name + " 重连，客户端带入 " + clientStack + " 与服务端 " + pl.stack + " 不一致，以服务端为准");
        }
        // 关键：关闭同一个 playerId 的旧连接（先收集再删除，避免遍历Map时删除的问题）
        const oldConns = [];
        for (const [oldWs, oldInfo] of clients) {
          if (oldWs !== ws && oldInfo.playerId === pl.id) {
            oldConns.push(oldWs);
          }
        }
        for (const oldWs of oldConns) {
          const oldInfo = clients.get(oldWs);
          // 先通知旧连接"你被顶替了，不要重连"，再关闭
          try { if(oldWs.readyState === 1) oldWs.send(JSON.stringify({type:"replaced"})); } catch(e) {}
          setTimeout(() => { try { oldWs.terminate(); } catch(e) {} }, 100);
          clients.delete(oldWs);
        }
        if (oldConns.length > 0) log(name + " 重连，关闭了 " + oldConns.length + " 个旧连接");
        // 重连: 取消离线检查，恢复 inHand
        cancelOfflineCheck(pl.id);
        if (room.phase === "betting" && !pl.folded && pl.stack > 0 && !pl.resting) {
          pl.inHand = true;
          log(name + " 重新连接，恢复本局下注资格");
        } else {
          log(name + " 重新连接（同昵称复用身份）");
        }
      }
      else {
        const amt = Math.max(0, Math.floor(+m.stack || 0)); const midHand = room.phase === "betting";
        pl = { id: uid(), name, ip: ws.__ip || "", stack: amt, seat: nextSeat(), totalInvested: 0, roundBet: 0, folded: false, hasActed: true, inHand: !midHand, addLeft: room.config.addCount, resting: false, initialBuyin: amt, boughtTotal: 0, soldTotal: 0, reloadTotal: 0, unreadTransfers: [], lastActLabel: null };
        if (midHand) pl.folded = true;
        room.players.push(pl); room.totalBuyin += amt; info.playerId = pl.id;
        log(name + " 加入牌局" + (amt ? "，带入 " + amt : "") + (midHand ? "（本手不参与，下一手开始下注）" : ""));
      }
      if (!room.ownerId) { room.ownerId = pl.id; room.dealerId = pl.id; log(name + " 成为房主与首任庄家"); }
      else if (!room.dealerId) { room.dealerId = pl.id; log(name + " 接任庄家"); }
      break;
    }
    case "config": {
      if (!isOwner) return; const c = room.config;
      const n = (v, d) => isFinite(+v) && +v > 0 ? Math.floor(+v) : d;
      c.sb = n(m.sb, c.sb); c.bb = n(m.bb, c.bb); c.turnSeconds = n(m.turnSeconds, c.turnSeconds); c.addSeconds = n(m.addSeconds, c.addSeconds); c.addCount = n(m.addCount, c.addCount);
      if (c.bb < c.sb) c.bb = c.sb;
      log("牌局配置已更新：小盲 " + c.sb + " / 大盲 " + c.bb + " / 限时 " + c.turnSeconds + "s / 加时卡 " + c.addSeconds + "s×" + c.addCount);
      break;
    }
    case "remove-player": {
      if (!isOwner) return; const i = room.players.findIndex(p => p.id === m.id); if (i < 0) return;
      const wasDealer = room.dealerId === m.id, wasOwner = room.ownerId === m.id;
      const removedName = room.players[i].name;
      log(removedName + " 被移出牌局"); room.players.splice(i, 1);
      /* 清理被移出玩家的残留状态，避免其旧连接/离线检查触发"？已离线/？连接已关闭"占位日志 */
      cancelOfflineCheck(m.id);
      for (const [ws, info] of clients) { if (info && info.playerId === m.id) { try { ws.close(); } catch(e){} } }
      if (room.players.length) {
        if (wasDealer || !findPlayer(room.dealerId)) rotateDealer();
        if (wasOwner || !findPlayer(room.ownerId)) { room.ownerId = room.players[0].id; log(room.players[0].name + " 接任房主"); }
        if (room.turn && !room.players.some(p => p.id === room.turn.playerId)) { afterAction(); }
      } else { room.ownerId = null; room.dealerId = null; room.phase = "lobby"; }
      break;
    }
    case "set-seat": {
      if (!isOwner) return; const p = findPlayer(String(m.id || "")); if (!p) return;
      const n = Math.floor(+m.seat); if (!(n >= 1 && n <= 999)) return; if (p.seat === n) return;
      const other = room.players.find(x => x.id !== p.id && x.seat === n);
      if (other) { const tmp = other.seat; other.seat = p.seat; p.seat = n; log(p.name + " 与 " + other.name + " 交换座位（" + n + "↔" + tmp + " 号）"); }
      else { p.seat = n; log(p.name + " 座位调整为 " + n + " 号"); }
      break;
    }
    case "set-owner": { const target = findPlayer(String(m.id || "")); if (!target) return; room.ownerId = target.id; log(target.name + "（" + (target.seat || "?") + " 号座）被任命为房主"); break; }
    case "set-dealer": {
      /* 房主手动任命庄家（荷官）：解决庄家掉线/休息后无人开牌、点赢家导致对局卡住的问题 */
      if (!isOwner) return;
      const target = findPlayer(String(m.id || "")); if (!target) return;
      room.dealerId = target.id;
      log(target.name + "（" + (target.seat || "?") + " 号座）被房主任命为庄家（荷官）");
      break;
    }
    case "reset": {
      if (!isOwner) return;
      const cfg = room.config;
      /* 生成结构化结算记录（完整历史页面渲染成汇总表） */
      if (room.players && room.players.length) {
        const sp = room.players.map(p => {
          const initial = p.initialBuyin || 0;
          const bought = p.boughtTotal || 0;
          const sold = p.soldTotal || 0;
          const reload = p.reloadTotal || 0;
          const finalStack = p.stack || 0;
          const netProfit = finalStack - initial - bought + sold - reload;
          return { name: p.name, seat: p.seat, initialBuyin: initial, bought: bought, sold: sold, reload: reload, finalStack: finalStack, netProfit: netProfit };
        });
        appendHistory({ t: Date.now(), m: "=== 牌局结算 · 重置牌局 ===", type: "settlement", players: sp,
          totalInitial: sp.reduce((s,p)=>s+p.initialBuyin,0),
          totalBought: sp.reduce((s,p)=>s+p.bought,0),
          totalSold: sp.reduce((s,p)=>s+p.sold,0),
          totalReload: sp.reduce((s,p)=>s+p.reload,0),
          totalFinal: sp.reduce((s,p)=>s+p.finalStack,0) });
        log("牌局结算完成，共 " + sp.length + " 位玩家，总剩余 " + sp.reduce((s,p)=>s+p.finalStack,0));
      }
      const oldLogs = room.logs;
      /* 重置前关闭历史文件流，避免文件句柄泄漏 */
      if (historyStream) { try { historyStream.end(); } catch(e) {} historyStream = null; }
      room = newRoom();
      room.config = cfg;
      room.logs = oldLogs;
      log("牌局已重置，回到初始状态（保留配置、保留历史记录），请刷新页面重新加入");
      break;
    }
    case "start-hand": if (isDealer) startHand(); break;
    case "next-round": if (isDealer) nextRound(); break;
    case "end-hand": { if (isDealer) { if (!endHand()) toast(ws, "还有玩家未下注，不能结束本手，请等本轮下注完成"); } break; }
    case "set-order": { if (!isDealer) return; room.order = Array.isArray(m.order) ? m.order.filter(o => findPlayer(o.id)) : []; break; }
    case "confirm-settle": if (isDealer) doSettle(); break;
    case "force-fold": {
      if (!isDealer) return;
      const target = findPlayer(m.playerId);
      if (!target) return;
      const olSet = getOnlineSet();
      if (olSet.has(target.id)) { toast(ws, "该玩家在线，不能强制操作"); return; }
      if (!target.inHand || target.folded) { toast(ws, "该玩家已不在牌局中"); return; }
      const mode = m.mode === "hand" ? "hand" : "round";
      if (mode === "hand") {
        // 本次弃牌：整手牌弃牌，本手不再参与
        target.folded = true;
        target.hasActed = true;
        log(target.name + " 被庄家强制本次弃牌（掉线，整手牌弃牌）");
      } else {
        // 本回合弃牌：只剥夺这一回合下注权，不弃牌，下一回合恢复
        target.hasActed = true;
        log(target.name + " 被庄家强制本回合弃牌（掉线，本回合剥夺下注权，下一回合恢复）");
      }
      if (room.turn && room.turn.playerId === target.id) { afterAction(); }
      break;
    }
    case "undo-bet": {
      if (!me || room.phase !== "betting") return;
      if ((me.lastActionBet || 0) <= 0) { toast(ws, "你本次没有可撤销的下注"); break; }
      if (room.lastBettorId !== me.id) { toast(ws, "已有其他玩家下注，无法撤销"); break; }
      const refund = me.lastActionBet || 0;
      /* 若撤销的是加注动作，回退本轮加注计数（保证 open/3-bet/4-bet 叫法正确） */
      const raiseLabels = new Set(["open","bet","raise","3-bet","4-bet","5-bet","6-bet","7-bet","8-bet","9-bet"]);
      if (raiseLabels.has(me.lastActLabel)) room.raiseCount = Math.max(0, room.raiseCount - 1);
      me.stack += refund; me.totalInvested -= refund; me.roundBet -= refund; me.hasActed = false; me.lastActionBet = 0; me.lastActLabel = null;
      let newCur = 0;
      for (const p of room.players) { if (p.roundBet > newCur) { newCur = p.roundBet; } }
      room.currentBet = newCur;
      /* 撤销后 lastBettorId 改为当前 roundBet 最高的玩家（而非 null），保证后续撤销判断正确 */
      let maxBet = 0, maxBettorId = null;
      for (const p of room.players) { if (p.roundBet > maxBet) { maxBet = p.roundBet; maxBettorId = p.id; } }
      room.lastBettorId = maxBettorId;
      /* 恢复加注前的 lastRaiseSize（如果是跟注撤销则用默认大盲） */
      room.lastRaiseSize = me._prevLastRaiseSize || room.config.bb;
      room.noRaise = false;
      me._prevLastRaiseSize = null;
      const savedDeadline = me._undoDeadline || (Date.now() + room.config.turnSeconds * 1000);
      room.turn = { playerId: me.id, deadline: savedDeadline };
      me._undoDeadline = null;
      log(me.name + " 撤销了本次下注，拿回 " + refund + "，请重新下注");
      break;
    }
    case "act": {
      if (!me || room.phase !== "betting" || !room.turn || room.turn.playerId !== me.id) return;
      if (!me.inHand || me.folded || me.stack <= 0) return;
      const action = m.action; const need = needToCallOf(me);
      if (action === "check") {
        if (need > 0) { toast(ws, "前面有玩家下注 " + room.currentBet + "，不能过牌，请跟注 / 加注 / 弃牌"); return; }
        me.hasActed = true; me.lastActLabel = "check"; log(me.name + " 过牌");
      } else if (action === "fold") { me.folded = true; me.hasActed = true; me.lastActLabel = "fold"; log(me.name + " 弃牌"); afterAction(); break; }
      else if (action === "call") { me._undoDeadline = room.turn ? room.turn.deadline : null; const pay = Math.min(me.stack, need); me.stack -= pay; me.roundBet += pay; me.totalInvested += pay; me.hasActed = true; me.lastActionBet = pay; me.lastActLabel = (room.street === 0 && room.raiseCount === 0) ? "limp" : "call"; room.lastBettorId = me.id; log(me.name + " 跟注 " + pay + (pay < need ? "（全下）" : "")); }
      else if (action === "allin") {
        me._undoDeadline = room.turn ? room.turn.deadline : null;
        const pay = me.stack; const prevBet = room.currentBet; const total = me.roundBet + pay;
        me.stack = 0; me.roundBet += pay; me.totalInvested += pay; me.hasActed = true; me.lastActionBet = pay; me.lastActLabel = "all-in";
        if (total > prevBet) {
          const raiseAmt = total - prevBet;
          /* 记录加注前的 lastRaiseSize，用于撤销时恢复 */
          me._prevLastRaiseSize = room.lastRaiseSize;
          if (raiseAmt >= room.lastRaiseSize) {
            room.currentBet = total; room.lastRaiseSize = raiseAmt; room.raiseCount++;
            room.players.forEach(p => { if (p !== me && p.inHand && !p.folded && p.stack > 0) p.hasActed = false; });
            log(me.name + " 全下 " + pay + "（构成加注到 " + total + "）");
          } else {
            room.currentBet = total; room.lastRaiseSize = raiseAmt; room.noRaise = true;
            room.players.forEach(p => { if (p !== me && p.inHand && !p.folded && p.stack > 0) p.hasActed = false; });
            log(me.name + " 全下 " + pay + "（到 " + total + "，不足最小加注额，本轮只能跟注/弃牌）");
          }
        } else { log(me.name + " 全下 " + pay + "（跟注不足）"); } room.lastBettorId = me.id;
      } else if (action === "raise") {
        me._undoDeadline = room.turn ? room.turn.deadline : null;
        if (room.noRaise) { toast(ws, "本轮因全下不足最小加注额，只能跟注或弃牌，不能再加注"); return; }
        const amount = Math.floor(+m.amount);
        if (!isFinite(amount) || amount <= 0 || amount > me.stack) { toast(ws, "无效下注金额"); return; }
        const total = me.roundBet + amount;
        if (total <= room.currentBet) { toast(ws, "下注不能低于前面玩家的下注 " + room.currentBet + "（全下除外）"); return; }
        const raiseAmt = total - room.currentBet;
        if (raiseAmt < room.lastRaiseSize) { toast(ws, "加注额至少 " + room.lastRaiseSize + "（最小加注额），当前只加了 " + raiseAmt + "，请加注到至少 " + (room.currentBet + room.lastRaiseSize)); return; }
        /* 记录加注前的 lastRaiseSize，用于撤销时恢复 */
        me._prevLastRaiseSize = room.lastRaiseSize;
        me.stack -= amount; me.roundBet += amount; me.totalInvested += amount; me.hasActed = true; me.lastActionBet = amount;
        room.currentBet = total; room.lastRaiseSize = raiseAmt; room.raiseCount++;
        me.lastActLabel = raiseLabel(room.street, room.raiseCount);
        room.players.forEach(p => { if (p !== me && p.inHand && !p.folded && p.stack > 0) p.hasActed = false; });
        room.lastBettorId = me.id; log(me.name + " 加注到 " + total + "（加注额 " + raiseAmt + "，最小加注额更新为 " + raiseAmt + "）");
      } else return;
      afterAction(); break;
    }
    case "rest": {
      if (!me) return;
      // 房主可控制其他玩家休息（带目标 playerId）
      if (m.playerId && m.playerId !== me.id) {
        if (!isOwner) { toast(ws, "只有房主可以控制其他玩家休息"); return; }
        const target = findPlayer(m.playerId);
        if (!target) return;
        toggleRest(target);
        break;
      }
      toggleRest(me);
      break;
    }
    case "add-time": { if (!me || room.phase !== "betting" || !room.turn || room.turn.playerId !== me.id) return; if (me.addLeft <= 0) { toast(ws, "加时卡已用完"); return; } me.addLeft--; room.turn.deadline += room.config.addSeconds * 1000; log(me.name + " 加时 +" + room.config.addSeconds + "s（剩 " + me.addLeft + " 张）"); break; }
    case "buy-chips": {
      if (!me) return; const other = findPlayer(m.fromId); const amt = Math.floor(+m.amount);
      if (!other || other.id === me.id || !(amt > 0)) { toast(ws, "无效的交易"); return; }
      if (other.stack < amt) { toast(ws, "对方筹码不足"); return; }
      const req = { id: "r" + (++reqSeq), type: "buy", fromId: me.id, toId: other.id, amount: amt };
      room.pending.push(req); log(me.name + " 请求向 " + other.name + " 购买 " + amt + " 筹码，等待确认");
      toast(ws, "已发送购买请求，等待 " + other.name + " 同意");
      sendToPlayer(other.id, { type: "request", id: req.id, kind: "buy", fromName: me.name, amount: amt });
      scheduleReqTimeout(req); break;
    }
    case "reload-chips": {
      if (!me) return; const amt = Math.floor(+m.amount); if (!(amt > 0)) return;
      const ownerP = findPlayer(room.ownerId);
      if (!ownerP || ownerP.id === me.id) {
        me.stack += amt; room.totalBuyin += amt; me.reloadTotal = (me.reloadTotal || 0) + amt;
        const reloadMsg = me.name + " 继续代入 " + amt + " 筹码";
        log(reloadMsg);
        appendHistory({ t: Date.now(), m: reloadMsg, type: "reload", player: me.name, amount: amt, balanceAfter: me.stack });
        break;
      }
      const req = { id: "r" + (++reqSeq), type: "reload", fromId: me.id, toId: ownerP.id, amount: amt };
      room.pending.push(req); log(me.name + " 请求代入 " + amt + " 筹码，等待房主 " + ownerP.name + " 确认");
      toast(ws, "已发送代入请求，等待房主同意");
      sendToPlayer(ownerP.id, { type: "request", id: req.id, kind: "reload", fromName: me.name, amount: amt });
      scheduleReqTimeout(req); break;
    }
    case "transfer-chips": {
      if (!me) return;
      const target = findPlayer(m.toId);
      const amt = Math.floor(+m.amount);
      if (!target || target.id === me.id || !(amt > 0)) { toast(ws, "无效的转账"); return; }
      if (me.stack < amt) { toast(ws, "你的筹码不足"); return; }
      me.stack -= amt; target.stack += amt;
      /* 记录未读转账通知，无论目标是否在线，上线后都会弹出提示 */
      target.unreadTransfers = target.unreadTransfers || [];
      target.unreadTransfers.push({ from: me.name, amount: amt, time: Date.now() });
      const transferMsg = me.name + " 向 " + target.name + " 手动转账 " + amt + " 筹码";
      log(transferMsg);
      appendHistory({ t: Date.now(), m: transferMsg, type: "transfer", player: me.name, amount: -amt, balanceAfter: me.stack });
      appendHistory({ t: Date.now(), m: target.name + " 收到 " + me.name + " 转账 " + amt + " 筹码", type: "transfer", player: target.name, amount: amt, balanceAfter: target.stack });
      toast(ws, "已向 " + target.name + " 转账 " + amt + " 筹码");
      /* 在线玩家额外发一个 toast 提示 */
      sendToPlayer(target.id, { type: "toast", msg: me.name + " 向你转账了 " + amt + " 筹码" });
      break;
    }
    case "clear-transfer-notif": {
      if (!me) return;
      me.unreadTransfers = [];
      break;
    }
    case "approve-request": {
      const req = room.pending.find(r => r.id === m.id); if (!req) return;
      room.pending = room.pending.filter(r => r.id !== req.id);
      if (reqTimers.has(req.id)) { clearTimeout(reqTimers.get(req.id)); reqTimers.delete(req.id); }
      const fromP = findPlayer(req.fromId), toP = findPlayer(req.toId);
      if (!fromP || !toP) { toast(ws, "请求对象已不在牌局"); return; }
      if (req.type === "buy") {
        if (toP.stack < req.amount) { toast(ws, "你的筹码不足，交易取消"); sendToPlayer(fromP.id, { type: "toast", msg: "卖家筹码不足，交易取消" }); break; }
        fromP.stack += req.amount; toP.stack -= req.amount; fromP.boughtTotal = (fromP.boughtTotal || 0) + req.amount; toP.soldTotal = (toP.soldTotal || 0) + req.amount;
        const buyMsg = fromP.name + " 向 " + toP.name + " 购买 " + req.amount + " 筹码，已完成";
        log(buyMsg);
        appendHistory({ t: Date.now(), m: buyMsg, type: "buy", player: fromP.name, amount: req.amount, balanceAfter: fromP.stack });
        appendHistory({ t: Date.now(), m: toP.name + " 卖出 " + req.amount + " 筹码给 " + fromP.name, type: "sell", player: toP.name, amount: -req.amount, balanceAfter: toP.stack });
        sendToPlayer(fromP.id, { type: "toast", msg: "卖家已同意，收到 " + req.amount + " 筹码" });
      } else {
        fromP.stack += req.amount; room.totalBuyin += req.amount; fromP.reloadTotal = (fromP.reloadTotal || 0) + req.amount;
        const reloadMsg = fromP.name + " 代入 " + req.amount + " 筹码，房主已同意";
        log(reloadMsg);
        appendHistory({ t: Date.now(), m: reloadMsg, type: "reload", player: fromP.name, amount: req.amount, balanceAfter: fromP.stack });
        sendToPlayer(fromP.id, { type: "toast", msg: "房主已同意，代入 " + req.amount + " 筹码" });
      }
      break;
    }
    case "reject-request": {
      const req = room.pending.find(r => r.id === m.id); if (!req) return;
      room.pending = room.pending.filter(r => r.id !== req.id);
      if (reqTimers.has(req.id)) { clearTimeout(reqTimers.get(req.id)); reqTimers.delete(req.id); }
      log(pname(req.fromId) + " 的交易请求被拒绝"); sendToPlayer(req.fromId, { type: "toast", msg: "你的交易请求被拒绝" }); break;
    }
  }
  save(); broadcast();
}

// ========== HTTP 服务器 + gzip ==========
const MIME = { ".html":"text/html; charset=utf-8", ".js":"text/javascript", ".css":"text/css", ".svg":"image/svg+xml", ".png":"image/png", ".ico":"image/x-icon", ".json":"application/json" };

const server = http.createServer((req, res) => {
  const url = decodeURIComponent((req.url || "/").split("?")[0]);
  // 调试接口：查看所有 WebSocket 连接状态
  if (url === "/debug") {
    const now = Date.now();
    const conns = [];
    for (const [ws, info] of clients) {
      conns.push({
        playerId: info.playerId,
        playerName: info.playerId ? (findPlayer(info.playerId) || {}).name : null,
        readyState: ws.readyState,
        lastPong: info.lastPong,
        lastPongAgo: now - info.lastPong,
        bufferedAmount: ws.bufferedAmount,
        ip: ws.__ip
      });
    }
    res.writeHead(200, {"Content-Type": "application/json; charset=utf-8"});
    res.end(JSON.stringify({
      now,
      totalConnections: clients.size,
      onlinePlayers: [...getOnlineSet()],
      heartbeatInterval: HEARTBEAT_INTERVAL,
      heartbeatTimeout: HEARTBEAT_TIMEOUT,
      connections: conns
    }, null, 2));
    return;
  }
  // 完整历史记录：表格化展示，结算汇总表，买卖/带入高亮
  if (url === "/history") {
    fs.readFile(HISTORY_FILE, "utf8", (err, txt) => {
      const rows = [];
      if (!err && txt) txt.split(/\r?\n/).forEach(line => { if (line.trim()) { try { rows.push(JSON.parse(line)); } catch (e) {} } });
      rows.reverse();
      const esc = x => String(x).replace(/[&<>]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;"}[c]));
      const fmtTime = t => { const d = new Date(t); const p = n => String(n).padStart(2,"0"); return (d.getMonth()+1)+"-"+p(d.getDate())+" "+p(d.getHours())+":"+p(d.getMinutes()); };
      const td = (text, opts={}) => {
        const style = opts.style || "";
        const align = opts.align ? "text-align:"+opts.align+";" : "";
        return "<td style=\'padding:6px 8px;border:1px solid #ddd;"+align+style+"\'>"+text+"</td>";
      };
      const renderSettlement = (o) => {
        if (!o.players || !o.players.length) return "";
        let h = "<tr><td colspan=\'5\' style=\'padding:0;border:none;\'>";
        h += "<div style=\'margin:8px 0;\'>";
        h += "<div style=\'font-weight:bold;color:#c82828;margin-bottom:6px;font-size:14px;\'>\u25bc 牌局结算汇总（重置牌局）</div>";
        h += "<table style=\'width:100%;border-collapse:collapse;font-size:12px;\'>";
        h += "<thead><tr style=\'background:#2d3732;color:#fff;\'>";
        ["玩家昵称","初始带入","买入","卖出","场外带入","最终剩余","净盈亏(注码)","转账(元)"].forEach(c => { h += "<th style=\'padding:6px;border:1px solid #ddd;\'>"+c+"</th>"; });
        h += "</tr></thead><tbody>";
        o.players.forEach(p => {
          const redStyle = "color:#c82828;background:#fff0f0;font-weight:bold;";
          h += "<tr>";
          h += td(esc(p.name), {style:redStyle, align:"center"});
          h += td(String(p.initialBuyin||0), {align:"center"});
          h += td(p.bought?("+"+p.bought):"0", {align:"center"});
          h += td(p.sold?("-"+p.sold):"0", {align:"center"});
          h += td(p.reload?("+"+p.reload):"0", {align:"center"});
          h += td(String(p.finalStack||0), {style:redStyle, align:"center"});
          const np = p.netProfit || 0;
          h += td((np>0?"+":"")+np, {style:np>0?"color:#2a8a2a;":np<0?"color:#c82828;":"", align:"center"});
          /* 转账列：本金=初始带入+场外带入，转账=最终剩余-本金，除以2为金额（1注码=0.5元） */
          const principal = (p.initialBuyin||0) + (p.reload||0);
          const transferChips = (p.finalStack||0) - principal;
          const transferYuan = (transferChips / 2).toFixed(1);
          const transferStyle = transferChips > 0 ? "color:#2a8a2a;background:#f0fff0;font-weight:bold;" : transferChips < 0 ? "color:#c82828;background:#fff0f0;font-weight:bold;" : "";
          const transferText = transferChips > 0 ? "+" + transferYuan + "（收）" : transferChips < 0 ? transferYuan + "（付）" : "0";
          h += td(transferText, {style:transferStyle, align:"center"});
          h += "</tr>";
        });
        const goldStyle = "color:#b48c28;background:#fffaf0;font-weight:bold;";
        const redStyle2 = "color:#c82828;background:#fff0f0;font-weight:bold;";
        h += "<tr>";
        h += td("合计", {style:redStyle2, align:"center"});
        h += td(String(o.totalInitial||0), {style:goldStyle, align:"center"});
        h += td(o.totalBought?("+"+o.totalBought):"0", {style:goldStyle, align:"center"});
        h += td(o.totalSold?("-"+o.totalSold):"0", {style:goldStyle, align:"center"});
        h += td(o.totalReload?("+"+o.totalReload):"0", {style:goldStyle, align:"center"});
        h += td(String(o.totalFinal||0), {style:redStyle2, align:"center"});
        const totalNet = (o.totalFinal||0) - (o.totalInitial||0) - (o.totalBought||0) + (o.totalSold||0) - (o.totalReload||0);
        h += td((totalNet>0?"+":"")+totalNet, {style:goldStyle, align:"center"});
        /* 合计转账 */
        const totalPrincipal = (o.totalInitial||0) + (o.totalReload||0);
        const totalTransfer = (o.totalFinal||0) - totalPrincipal;
        const totalTransferYuan = (totalTransfer / 2).toFixed(1);
        const totalTransferText = totalTransfer > 0 ? "+" + totalTransferYuan : totalTransfer < 0 ? totalTransferYuan : "0";
        h += td(totalTransferText, {style:goldStyle, align:"center"});
        h += "</tr>";
        h += "</tbody></table></div></td></tr>";
        return h;
      };
      /* 渲染每手结算详情表 */
      const renderHandSettlement = (o) => {
        if (!o.players || !o.players.length) return "";
        let h = "<tr><td colspan=\'5\' style=\'padding:0;border:none;\'>";
        h += "<div style=\'margin:8px 0;\'>";
        h += "<div style=\'font-weight:bold;color:#2a6a8a;margin-bottom:6px;font-size:13px;\'>\u25bc 第 " + (o.handNo||"?") + " 手结算详情</div>";
        h += "<table style=\'width:100%;border-collapse:collapse;font-size:11px;\'>";
        h += "<thead><tr style=\'background:#3a4a5a;color:#fff;\'>";
        ["玩家","结算前筹码","本手投入","赢得","净盈亏","结算后筹码"].forEach(c => { h += "<th style=\'padding:5px;border:1px solid #ddd;\'>"+c+"</th>"; });
        h += "</tr></thead><tbody>";
        o.players.forEach(p => {
          const netStyle = p.net > 0 ? "color:#2a8a2a;font-weight:bold;" : p.net < 0 ? "color:#c82828;font-weight:bold;" : "";
          h += "<tr>";
          h += td(esc(p.name), {align:"center"});
          h += td(String(p.before||0), {align:"center"});
          h += td(String(p.invested||0), {align:"center"});
          h += td("+"+(p.won||0), {style:p.won>0?"color:#2a8a2a;":"", align:"center"});
          h += td((p.net>0?"+":"")+(p.net||0), {style:netStyle, align:"center"});
          h += td(String(p.after||0), {style:"font-weight:bold;", align:"center"});
          h += "</tr>";
        });
        h += "</tbody></table></div></td></tr>";
        return h;
      };
      let body = "";
      body += "<table style=\'width:100%;border-collapse:collapse;font-size:13px;font-family:system-ui,sans-serif;\'>";
      body += "<thead><tr style=\'background:#2d3732;color:#fff;\'>";
      ["时间","事件","玩家","金额","操作后余额"].forEach(c => { body += "<th style=\'padding:8px;border:1px solid #ddd;text-align:center;\'>"+c+"</th>"; });
      body += "</tr></thead><tbody>";
      rows.forEach(o => {
        if (o.type === "settlement") { body += renderSettlement(o); return; }
        if (o.type === "hand-settlement") { body += renderHandSettlement(o); return; }
        const msg = o.m || "";
        const isBuy = msg.indexOf("购买") >= 0;
        const isReload = msg.indexOf("代入") >= 0 || msg.indexOf("继续代入") >= 0;
        const isTransfer = msg.indexOf("转账") >= 0;
        let rowStyle = "";
        let textColor = "#333";
        if (isBuy) { rowStyle = "background:#e6f0ff;"; textColor = "#1e50a0"; }
        else if (isReload) { rowStyle = "background:#fff5e6;"; textColor = "#b46414"; }
        else if (isTransfer) { rowStyle = "background:#f0e6ff;"; textColor = "#6a3aa0"; }
        /* 从消息中提取玩家名和金额（用于填充玩家/金额列） */
        let playerName = "";
        let amountStr = "";
        let balanceStr = "";
        if (o.player) playerName = o.player;
        if (o.amount != null) amountStr = (o.amount > 0 ? "+" : "") + o.amount;
        if (o.balanceAfter != null) balanceStr = String(o.balanceAfter);
        body += "<tr style=\'"+rowStyle+"\'>";
        body += td(fmtTime(o.t), {style:"color:#888;", align:"center"});
        body += td(esc(msg), {style:"color:"+textColor+";"});
        body += td(playerName ? esc(playerName) : "", {align:"center"});
        body += td(amountStr, {style:o.amount>0?"color:#2a8a2a;":o.amount<0?"color:#c82828;":"", align:"right"});
        body += td(balanceStr, {align:"center"});
        body += "</tr>";
      });
      body += "</tbody></table>";
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      res.end("<html><head><meta name=\'viewport\' content=\'width=device-width,initial-scale=1\'><title>完整牌局动态</title></head>" +
        "<body style=\'max-width:720px;margin:0 auto;padding:12px;background:#f5f5f5;\'>" +
        "<h3 style=\'position:sticky;top:0;background:#f5f5f5;padding:8px 0;margin:0 0 10px 0;font-size:18px;\'>完整牌局动态 · 共 " + rows.length + " 条（最新在上）</h3>" +
        "<div style=\'background:#fff;border-radius:8px;overflow-x:auto;\'>" + (body || "<p style=\'padding:12px;\'>暂无历史</p>") + "</div>" +
        "<div style=\'margin-top:12px;font-size:12px;color:#666;\'>" +
        "<span style=\'display:inline-block;width:14px;height:14px;background:#e6f0ff;border:1px solid #ddd;vertical-align:middle;margin-right:4px;\'></span>筹码买卖 " +
        "<span style=\'display:inline-block;width:14px;height:14px;background:#fff5e6;border:1px solid #ddd;vertical-align:middle;margin:0 4px 0 12px;\'></span>场外带入 " +
        "<span style=\'display:inline-block;width:14px;height:14px;background:#f0e6ff;border:1px solid #ddd;vertical-align:middle;margin:0 4px 0 12px;\'></span>手动转账 " +
        "<span style=\'display:inline-block;width:14px;height:14px;background:#e8f4f8;border:1px solid #ddd;vertical-align:middle;margin:0 4px 0 12px;\'></span>每手结算详情 " +
        "<span style=\'display:inline-block;width:14px;height:14px;background:#fff0f0;border:1px solid #ddd;vertical-align:middle;margin:0 4px 0 12px;\'></span>结算汇总（重置牌局）" +
        "<div style=\'margin-top:8px;color:#888;\'>注：1注码 = 0.5元，转账列金额已换算为元</div>" +
        "</div></body></html>");
    });
    return;
  }
  let file = url === "/" ? "/index.html" : url;
  const fp = path.normalize(path.join(PUB_DIR, file));
  if (!fp.startsWith(PUB_DIR)) { res.writeHead(403); res.end("forbidden"); return; }
  fs.readFile(fp, (err, data) => {
    if (err) { res.writeHead(404); res.end("not found"); return; }
    const ext = path.extname(fp).toLowerCase();
    const ct = MIME[ext] || "application/octet-stream";
    const headers = { "Content-Type": ct, "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0" };
    const acceptEncoding = req.headers["accept-encoding"] || "";
    const shouldGzip = acceptEncoding.includes("gzip") && (ext === ".html" || ext === ".js" || ext === ".css" || ext === ".json" || ext === ".svg");
    if (shouldGzip && data.length > 1024) {
      zlib.gzip(data, (err, compressed) => {
        if (err) { res.writeHead(200, headers); res.end(data); return; }
        headers["Content-Encoding"] = "gzip";
        headers["Content-Length"] = compressed.length;
        res.writeHead(200, headers);
        res.end(compressed);
      });
    } else {
      headers["Content-Length"] = data.length;
      res.writeHead(200, headers);
      res.end(data);
    }
  });
});

const wss = new WebSocketServer({ server });
wss.on("connection", (ws, req) => {
  const fwd = req.headers["x-forwarded-for"];
  const ip = (fwd ? String(fwd).split(",")[0].trim() : req.socket.remoteAddress || "").replace(/^::ffff:/, "");
  ws.__ip = ip;
  // 设置 TCP keepalive，5秒没响应就检测到连接断开（解决旧版Safari退出不发close帧的问题）
  try {
    if (ws._socket) {
      ws._socket.setKeepAlive(true, 5000);
      ws._socket.setTimeout(10000, () => { try { ws.terminate(); } catch(e) {} });
    }
  } catch(e) {}
  // 初始化客户端信息（含心跳和限流状态）
  clients.set(ws, { playerId: null, lastPong: Date.now(), lastPingSent: 0, msgTimes: [] });

  // 心跳: 收到pong更新时间
  ws.on("pong", () => {
    const info = clients.get(ws);
    if (info) info.lastPong = Date.now();
  });

  ws.on("error", (err) => {
    console.error("WebSocket error:", err.message);
    try { ws.close(); } catch (e) {}
  });

  ws.on("message", raw => {
    // 收到任何消息都更新最后活跃时间（应用层心跳，解决旧版Safari ping/pong兼容性问题）
    const info = clients.get(ws);
    if (info) info.lastPong = Date.now();
    try { handle(ws, raw.toString()); } catch (e) { console.error("handle error:", e.message); }
  });

  ws.on("close", () => {
    const info = clients.get(ws);
    clients.delete(ws);
    invalidateStateCache();
    if (info && info.playerId) {
      // 立即检查是否还有其他连接对应这个玩家，如果没有就标记离线
      const stillOnline = [...clients.values()].some(i => i.playerId === info.playerId);
      if (!stillOnline) {
        const p = findPlayer(info.playerId);
        if (p) log(p.name + " 连接已关闭");
        // 找不到 p → 已被移出牌局，不再记"?"占位日志
      }
      // 安排离线检查（15秒宽限期，允许重连）
      scheduleOfflineCheck(info.playerId);
    }
    broadcast();   // 仅连接状态变化，日志已实时入历史文件，无需写 state.json
  });
});

// ========== 心跳机制：每1秒检测一次；10秒没收到任何消息则判定掉线并断开；协议层 ping 每3秒发一次 ==========
setInterval(() => {
  const now = Date.now();
  const toRemove = [];
  for (const [ws, info] of clients) {
    // 连接已关闭或正在关闭，直接删除
    if (ws.readyState !== 1) {
      toRemove.push(ws);
      if (info.playerId) scheduleOfflineCheck(info.playerId);
      continue;
    }
    // 超过10秒没收到任何消息（含应用层心跳），判定掉线，主动断开
    if (now - info.lastPong > HEARTBEAT_TIMEOUT) {
      const who = info.playerId ? (findPlayer(info.playerId) || {}).name : "unknown";
      try { ws.terminate(); } catch (e) {}
      toRemove.push(ws);
      if (info.playerId) scheduleOfflineCheck(info.playerId);
      continue;
    }
    // 协议层 ping：每3秒发一次，失败则判定掉线（应用层 hb 已每2秒保活，这里只辅助探测半开连接）
    if (now - (info.lastPingSent || 0) >= WS_PING_INTERVAL) {
      info.lastPingSent = now;
      try {
        ws.ping();
      } catch (e) {
        try { ws.terminate(); } catch (e2) {}
        toRemove.push(ws);
        if (info.playerId) scheduleOfflineCheck(info.playerId);
        continue;
      }
    }
    // 额外检测：发送缓冲区堆积过多，说明连接已堵死
    try {
      if (ws.bufferedAmount > 1024 * 64) {
        try { ws.terminate(); } catch (e) {}
        toRemove.push(ws);
        if (info.playerId) scheduleOfflineCheck(info.playerId);
      }
    } catch(e) {}
  }
  // 批量删除无效连接（仅连接状态变化，不写 state.json）
  if (toRemove.length > 0) {
    for (const ws of toRemove) clients.delete(ws);
    invalidateStateCache();
    broadcast();
  }
}, HEARTBEAT_INTERVAL);

server.on("error", (err) => {
  if (err.code === "EADDRINUSE") {
    console.error("端口 " + PORT + " 已被占用，请检查是否有其他进程在运行，或修改 PORT 环境变量");
    process.exit(1);
  } else {
    console.error("服务器错误:", err.message);
  }
});

// 首次启用全量历史：把 state.json 中已有的最近日志导入 history.jsonl（部署前的历史不丢失）
try {
  if (!fs.existsSync(HISTORY_FILE) && room.logs && room.logs.length) {
    const dir = path.dirname(HISTORY_FILE);
    if (fs.existsSync(dir)) {
      const hws = fs.createWriteStream(HISTORY_FILE, { flags: "a" });
      room.logs.forEach(e => hws.write(JSON.stringify(e) + "\n"));
      hws.end();
      console.log("已导入 " + room.logs.length + " 条已有历史到 " + HISTORY_FILE);
    }
  }
} catch (e) { console.error("history import error:", e.message); }

server.listen(PORT, "0.0.0.0", () => console.log("Poker chip settler live server v9.5 (full history) on :" + PORT));

function gracefulShutdown(signal) {
  console.log("收到 " + signal + " 信号，正在保存状态并退出...");
  saveSync();
  try { if (historyStream) historyStream.end(); } catch (e) {}
  wss.close(() => {
    server.close(() => {
      console.log("服务器已关闭");
      process.exit(0);
    });
  });
  setTimeout(() => { console.log("强制退出"); process.exit(0); }, 5000);
}
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("uncaughtException", (err) => {
  console.error("未捕获异常:", err.message);
  saveSync();
});
