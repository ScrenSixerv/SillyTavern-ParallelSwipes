/**
 * 并行备选回复 (Parallel Swipes) — SillyTavern 扩展
 *
 * 酒馆自己发出一条回复请求时，用完全相同的请求体再并行发出 N-1 条，
 * 等酒馆那条完成后，把其余结果作为备选回复（swipe）追加到同一条消息上，
 * 用左右箭头即可切换。总耗时约等于最慢的一条，而不是 N 条相加。
 *
 * 只接管聊天补全（Chat Completion）接口的单角色聊天；群聊、续写、代写、静默请求不受影响。
 */

import {
    chat,
    eventSource,
    event_types,
    isGenerating,
    swipeState,
    streamingProcessor,
    name2,
    cleanUpMessage,
    ensureSwipes,
    syncMesToSwipe,
    syncSwipeToMes,
    updateMessageBlock,
    addOneMessage,
    updateSwipeCounter,
    refreshSwipeButtons,
    saveChatConditional,
    getRequestHeaders,
    getCurrentChatId,
    getGeneratingApi,
    getGeneratingModel,
    extractMessageFromData,
    saveSettingsDebounced,
} from '../../../../script.js';
import { extension_settings } from '../../../extensions.js';
import { selected_group } from '../../../group-chats.js';
import { getStreamingReply } from '../../../openai.js';
import { extractReasoningFromData, parseReasoningInSwipes, ReasoningType } from '../../../reasoning.js';
import { getRegexedString, regex_placement } from '../../regex/engine.js';
import { power_user } from '../../../power-user.js';
import { getTokenCountAsync } from '../../../tokenizers.js';
import { getMessageTimeStamp } from '../../../RossAscends-mods.js';
import EventSourceStream from '../../../sse-stream.js';
import { callGenericPopup, POPUP_TYPE } from '../../../popup.js';
import { SlashCommandParser } from '../../../slash-commands/SlashCommandParser.js';
import { SlashCommand } from '../../../slash-commands/SlashCommand.js';
import { ARGUMENT_TYPE, SlashCommandArgument } from '../../../slash-commands/SlashCommandArgument.js';

const MODULE = 'parallel_swipes';
const LOG = '[并行备选]';
const MAX_COUNT = 10;
const GENERATE_URL = '/api/backends/chat-completions/generate';
const MAIN_TYPES = ['normal', 'regenerate', 'swipe'];
const RETRYABLE_STATUS = [408, 409, 425, 429, 500, 502, 503, 504, 520, 522, 524, 529];
const RETRYABLE_TEXT = /429|rate.?limit|too many|overload|busy|timeout|timed out|temporar|unavailable|502|503|504|529/i;
const DEFAULTS = {
    enabled: true,
    count: 3,
    onSwipe: true,
    retry: true,
    timeoutSec: 0,
    graceSec: 120,
};
const SETTINGS_VERSION = 2;

/** 酒馆即将发出的主请求（GENERATION_STARTED 记下，CHAT_COMPLETION_SETTINGS_READY 认领） */
let pendingMain = null;
/** 进行中的并行会话 */
const sessions = new Set();
let sessionSeq = 0;
let lastGroupToast = 0;

// ─── 设置 ────────────────────────────────────────────────────────────────────

function clampCount(value) {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n)) return DEFAULTS.count;
    return Math.min(MAX_COUNT, Math.max(1, n));
}

/** 0 = 关闭；其余限制在 15–600 秒 */
function clampTimeout(value) {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n)) return DEFAULTS.timeoutSec;
    if (n <= 0) return 0;
    return Math.min(600, Math.max(15, n));
}

function clampGrace(value) {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n)) return DEFAULTS.graceSec;
    return Math.min(1800, Math.max(10, n));
}

function getSettings() {
    if (!extension_settings[MODULE] || typeof extension_settings[MODULE] !== 'object') {
        extension_settings[MODULE] = {};
    }
    const s = extension_settings[MODULE];
    if (s.settingsVersion !== SETTINGS_VERSION) {
        // 旧版默认的 90 秒"无内容超时"会误杀非真流式网关的长回复，升级时改为关闭
        if (s.timeoutSec === 90) s.timeoutSec = 0;
        s.settingsVersion = SETTINGS_VERSION;
    }
    for (const [key, value] of Object.entries(DEFAULTS)) {
        if (typeof s[key] !== typeof value) s[key] = value;
    }
    s.count = clampCount(s.count);
    s.timeoutSec = clampTimeout(s.timeoutSec);
    s.graceSec = clampGrace(s.graceSec);
    return s;
}

function setCount(value) {
    const s = getSettings();
    s.count = clampCount(value);
    saveSettingsDebounced();
    syncSettingsUI();
    return s.count;
}

function setEnabled(value) {
    const s = getSettings();
    s.enabled = Boolean(value);
    saveSettingsDebounced();
    syncSettingsUI();
}

// ─── 工具 ────────────────────────────────────────────────────────────────────

function sleep(ms, signal) {
    return new Promise((resolve) => {
        const timer = setTimeout(resolve, ms);
        signal?.addEventListener('abort', () => { clearTimeout(timer); resolve(); }, { once: true });
    });
}

function isEmptyReply(text) {
    const t = String(text ?? '').trim();
    return t === '' || t === '...';
}

function errorMessageFromPayload(payload) {
    const err = payload?.error ?? payload;
    if (typeof err === 'string') return err;
    return err?.message || err?.error?.message || payload?.message || JSON.stringify(payload).slice(0, 300);
}

function errorFromText(status, text) {
    let message = '';
    try {
        message = errorMessageFromPayload(JSON.parse(text));
    } catch {
        message = String(text || '').slice(0, 300);
    }
    const err = new Error(`HTTP ${status}${message ? `：${message}` : ''}`);
    err.retryable = RETRYABLE_STATUS.includes(status) || RETRYABLE_TEXT.test(message);
    return err;
}

function errorFromPayload(payload) {
    const message = errorMessageFromPayload(payload);
    const err = new Error(message);
    err.retryable = RETRYABLE_TEXT.test(message);
    return err;
}

/** 酒馆当前没在生成、没在切换、没在编辑消息 */
function isIdle() {
    if (isGenerating()) return false;
    if (swipeState !== 'none') return false;
    if (streamingProcessor && !streamingProcessor.isFinished) return false;
    if (document.querySelector('#curEditTextarea')) return false;
    return true;
}

// ─── 识别酒馆的主请求 ────────────────────────────────────────────────────────

function onGenerationStarted(type, _options, dryRun) {
    if (dryRun) return;
    if (!MAIN_TYPES.includes(type)) return;
    pendingMain = { type, at: Date.now() };
}

/**
 * CHAT_COMPLETION_SETTINGS_READY 也会被酒馆助手 generateRaw 等扩展请求触发，
 * 这里只认领酒馆自己 Generate() 发出的那一条。
 */
function isNativeMainRequest(data) {
    if (!pendingMain) return false;
    if (Date.now() - pendingMain.at > 10 * 60 * 1000) {
        pendingMain = null;
        return false;
    }
    if (data?.type !== pendingMain.type) return false;
    if (!isGenerating()) return false;
    if (data.stream) {
        const sp = streamingProcessor;
        if (!sp || sp.isFinished || sp.messageId !== -1 || sp.type !== pendingMain.type) return false;
    }
    return true;
}

function onSettingsReady(data) {
    try {
        if (!data || typeof data !== 'object') return;
        if (!isNativeMainRequest(data)) return;

        const type = pendingMain.type;
        pendingMain = null;

        const s = getSettings();
        if (!s.enabled || s.count <= 1) return;
        if (type === 'swipe' && !s.onSwipe) return;
        if (selected_group) {
            if (Date.now() - lastGroupToast > 60_000) {
                lastGroupToast = Date.now();
                toastr.info('群聊暂不支持并行备选回复，本次按普通方式生成。', '并行备选');
            }
            return;
        }

        // 多条回复由本扩展负责，酒馆自带的 Multi Swipe（n）不再随请求发出
        delete data.n;

        const session = createSession(type, s.count - 1);
        if (!session) return;

        // 等其它监听器也处理完请求体后再复制，保证并行请求与酒馆那条完全一致
        setTimeout(() => {
            try {
                const body = JSON.parse(JSON.stringify(data));
                delete body.n;
                startSession(session, body);
            } catch (err) {
                console.error(LOG, '复制请求失败', err);
                sessions.delete(session);
                renderBar();
            }
        }, 0);
    } catch (err) {
        console.error(LOG, err);
    }
}

// ─── 会话 ────────────────────────────────────────────────────────────────────

function createSession(type, extraCount) {
    const baseLen = chat.length;
    const baseLast = chat[baseLen - 1] ?? null;

    if (type === 'swipe' && (!baseLast || baseLast.is_user)) return null;

    const session = {
        id: ++sessionSeq,
        type,
        chatId: getCurrentChatId(),
        baseLen,
        baseLast,
        // swipe：目标就是当前最后一条；normal/regenerate：酒馆稍后在 baseLen 位置新建
        target: type === 'swipe' ? baseLast : null,
        slot: type === 'swipe' ? baseLast.swipe_id : 0,
        mainReceived: false,
        // 酒馆那条结束（成功、失败或被停止）的时间；之后并行的几条只再等 graceSec 秒
        mainDoneAt: 0,
        graceDeadline: 0,
        startedAt: new Date(),
        cancelled: false,
        phase: 'running',
        jobs: Array.from({ length: extraCount }, (_, index) => ({
            index,
            status: 'running',
            chars: 0,
            controller: new AbortController(),
            result: null,
            error: null,
        })),
    };
    sessions.add(session);
    renderBar();
    return session;
}

function startSession(session, body) {
    for (const job of session.jobs) {
        runJob(session, job, body);
    }
    sessionLoop(session);
}

function bindTarget(session) {
    if (session.target) return;
    const candidate = chat[session.baseLen];
    if (candidate && !candidate.is_user && !candidate.is_system && chat[session.baseLen - 1] === session.baseLast) {
        session.target = candidate;
    }
}

function onMessageReceived(messageId) {
    for (const session of sessions) {
        if (session.mainReceived) continue;
        bindTarget(session);
        if (session.target && chat[messageId] === session.target) {
            session.mainReceived = true;
        }
    }
}

async function sessionLoop(session) {
    let stable = 0;
    const deadline = Date.now() + 60 * 60 * 1000;

    while (true) {
        await sleep(250);

        if (session.cancelled) return finishSession(session, { reason: 'cancelled' });
        if (getCurrentChatId() !== session.chatId) return finishSession(session, { reason: 'chat_changed' });
        if (Date.now() > deadline) return finishSession(session, { reason: 'timeout' });

        bindTarget(session);

        const running = session.jobs.filter(job => job.status === 'running');

        if (!session.mainDoneAt && (session.mainReceived || !isGenerating())) {
            session.mainDoneAt = Date.now();
            session.graceDeadline = session.mainDoneAt + getSettings().graceSec * 1000;
            if (running.length) renderBar();
        }
        if (session.graceDeadline && running.length) {
            if (Date.now() > session.graceDeadline) {
                const expiring = running.filter(job => !job.graceExpired);
                for (const job of expiring) {
                    job.graceExpired = true;
                    job.controller.abort();
                }
                if (expiring.length) {
                    toastr.warning(
                        `酒馆那条完成后已等待 ${getSettings().graceSec} 秒，${expiring.length} 条未完成的并行回复已自动取消。`,
                        '并行备选',
                        { timeOut: 8000 },
                    );
                }
            } else {
                scheduleBarRender();
            }
        }

        if (running.length) {
            stable = 0;
            continue;
        }
        if (!session.jobs.some(job => job.status === 'done')) {
            return finishSession(session, { reason: 'all_failed' });
        }
        if (session.phase !== 'waiting') {
            session.phase = 'waiting';
            renderBar();
        }

        stable = isIdle() ? stable + 1 : 0;
        // 收到了主回复就只需短暂确认；主回复没出现（例如主请求失败）则多等一会儿
        const need = session.mainReceived ? 2 : 6;
        if (stable >= need) {
            let outcome;
            try {
                outcome = await applyResults(session);
            } catch (err) {
                console.error(LOG, '写入备选失败', err);
                outcome = { reason: 'error', error: err?.message || String(err) };
            }
            return finishSession(session, outcome);
        }
    }
}

function cancelSession(session, { discard }) {
    for (const job of session.jobs) {
        if (job.status === 'running') job.controller.abort();
    }
    if (discard) session.cancelled = true;
}

function abortRunningJobs() {
    for (const session of sessions) cancelSession(session, { discard: false });
}

function finishSession(session, outcome) {
    sessions.delete(session);
    for (const job of session.jobs) {
        if (job.status === 'running') job.controller.abort();
    }
    renderBar();

    const done = session.jobs.filter(j => j.status === 'done').length;
    const failed = session.jobs.filter(j => j.status === 'failed');
    const aborted = session.jobs.filter(j => j.status === 'aborted');
    // 已经单独弹过提示的失败不再重复列原因
    const unnotified = failed.filter(j => !j.notified);
    const reasons = jobs => [...new Set(jobs.map(j => j.error))].join('；');
    const failText = unnotified.length ? `\n失败 ${unnotified.length} 条：${reasons(unnotified)}` : '';
    const skipped = failed.length + aborted.length;

    switch (outcome.reason) {
        case 'applied':
            toastr.success(
                `已加入 ${outcome.added} 条备选回复（这条消息共 ${outcome.total} 条）${skipped ? `，另有 ${skipped} 条已取消` : ''}，用左右箭头切换。${failText}`,
                '并行备选',
                { timeOut: skipped ? 8000 : 3500 },
            );
            break;
        case 'created':
            toastr.warning(
                `酒馆自己的那条没有生成出来，已用并行结果新建回复（共 ${outcome.total} 条）。${failText}`,
                '并行备选',
                { timeOut: 8000 },
            );
            break;
        case 'all_failed':
            if (unnotified.length) {
                toastr.error(`并行请求全部失败：${reasons(unnotified)}`, '并行备选', { timeOut: 10000 });
            } else if (failed.length) {
                toastr.warning('并行回复都没有完成，已全部取消，这次只有酒馆自己那条。', '并行备选', { timeOut: 8000 });
            } else if (aborted.length) {
                toastr.info('已停止并行请求，这次没有加入备选回复。', '并行备选');
            }
            break;
        case 'chat_changed':
            if (done) toastr.info('已切换聊天，未写入的并行备选回复已丢弃。', '并行备选');
            break;
        case 'missing_target':
            toastr.warning('要追加的那条消息已被删除或改动，并行备选回复已丢弃。', '并行备选');
            break;
        case 'timeout':
            toastr.warning('等待超时，并行备选回复已丢弃。', '并行备选');
            break;
        case 'error':
            toastr.error(`写入备选回复时出错：${outcome.error}`, '并行备选');
            break;
    }
}

// ─── 单条并行请求 ────────────────────────────────────────────────────────────

async function runJob(session, job, body) {
    const maxAttempts = getSettings().retry ? 2 : 1;
    try {
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                const raw = await requestOnce(job, body);
                const result = await processResult(session, raw);
                if (isEmptyReply(result.text)) {
                    throw Object.assign(new Error('模型返回了空回复'), { retryable: false });
                }
                job.result = result;
                job.status = 'done';
                return;
            } catch (err) {
                if (job.controller.signal.aborted) {
                    markAborted(job);
                    return;
                }
                if (err?.retryable && attempt < maxAttempts) {
                    console.warn(LOG, `第 ${job.index + 2} 条请求失败，稍后重试：`, err);
                    toastr.info(
                        `第 ${job.index + 2} 条并行回复失败，正在重试一次：${err?.message || err}`,
                        '并行备选',
                        { timeOut: 5000 },
                    );
                    job.chars = 0;
                    await sleep(1500 * attempt, job.controller.signal);
                    if (job.controller.signal.aborted) {
                        markAborted(job);
                        return;
                    }
                    continue;
                }
                console.warn(LOG, `第 ${job.index + 2} 条请求失败：`, err);
                job.status = 'failed';
                job.error = err?.message || String(err);
                job.notified = true;
                toastr.warning(
                    `第 ${job.index + 2} 条并行回复失败，已自动取消：${job.error}`,
                    '并行备选',
                    { timeOut: 8000 },
                );
                return;
            }
        }
    } finally {
        renderBar();
    }
}

/** 被停止的请求：酒馆那条完成后等待超时算失败（要提示原因），用户主动停止算放弃 */
function markAborted(job) {
    if (job.graceExpired) {
        job.status = 'failed';
        job.error = `酒馆那条完成后又等了 ${getSettings().graceSec} 秒仍未完成，已放弃`;
        job.notified = true;
        console.warn(LOG, `第 ${job.index + 2} 条请求：${job.error}`);
    } else {
        job.status = 'aborted';
    }
}

/**
 * 每次尝试单独一个 AbortController：用户停止或等待超时（job.controller）会连带停止它；
 * 若开启了"无内容超时"，超过设定秒数没有收到任何内容也会停止它。
 * 超时不自动重试：长回复重来一遍大概率还是超时，还要再计一次费。
 */
async function requestOnce(job, body) {
    const timeoutMs = getSettings().timeoutSec * 1000;
    const attempt = new AbortController();
    const onJobAbort = () => attempt.abort();
    job.controller.signal.addEventListener('abort', onJobAbort, { once: true });

    let timedOut = false;
    let timer = null;
    const touch = () => {
        if (!timeoutMs) return;
        clearTimeout(timer);
        timer = setTimeout(() => {
            timedOut = true;
            attempt.abort();
        }, timeoutMs);
    };
    touch();

    try {
        const response = await fetch(GENERATE_URL, {
            method: 'POST',
            headers: getRequestHeaders(),
            body: JSON.stringify(body),
            signal: attempt.signal,
        });

        if (!response.ok) {
            const text = await response.text().catch(() => '');
            throw errorFromText(response.status, text);
        }

        if (body.stream) {
            return await readStream(response, job, touch);
        }

        const data = await response.json();
        if (data?.error) throw errorFromPayload(data);
        return {
            text: extractMessageFromData(data, 'openai'),
            reasoning: extractReasoningFromData(data),
        };
    } catch (err) {
        if (timedOut && !job.controller.signal.aborted) {
            const sec = Math.round(timeoutMs / 1000);
            throw Object.assign(new Error(`超过 ${sec} 秒没有收到内容（服务商可能繁忙或卡住）`), { retryable: false });
        }
        throw err;
    } finally {
        clearTimeout(timer);
        job.controller.signal.removeEventListener('abort', onJobAbort);
    }
}

async function readStream(response, job, touch) {
    const reader = response.body.pipeThrough(new EventSourceStream()).getReader();
    const state = { reasoning: '', images: [], signature: '', toolSignatures: {} };
    let text = '';

    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            const rawData = value?.data;
            if (rawData === '[DONE]') break;

            let parsed;
            try {
                parsed = JSON.parse(rawData);
            } catch {
                continue;
            }
            if (parsed?.error) throw errorFromPayload(parsed);

            text += getStreamingReply(parsed, state) || '';
            const chars = text.length + (state.reasoning?.length || 0);
            // 只有真正收到新内容才算"有进展"，空的心跳包不重置超时；
            // 关闭"显示思考"时思考内容不计入 chars，但模型仍在思考，也算有进展
            const delta = parsed?.choices?.[0]?.delta;
            if (chars > job.chars || delta?.reasoning_content || delta?.reasoning) touch();
            job.chars = chars;
            scheduleBarRender();
        }
    } finally {
        reader.releaseLock?.();
    }

    return { text, reasoning: state.reasoning };
}

/** 按酒馆处理正常回复的同一套规则清理文本（停止词、正则、去空白、思维链解析等） */
async function processResult(session, raw) {
    let text = cleanUpMessage({
        getMessage: String(raw?.text ?? ''),
        isImpersonate: false,
        isContinue: false,
        displayIncompleteSentences: false,
    });

    let reasoning = getRegexedString(String(raw?.reasoning ?? ''), regex_placement.REASONING);
    if (power_user.trim_spaces) reasoning = reasoning.trim();

    const info = {
        send_date: getMessageTimeStamp(),
        gen_started: session.startedAt,
        gen_finished: new Date(),
        extra: {
            api: getGeneratingApi(),
            model: getGeneratingModel(),
            reasoning,
            reasoning_duration: null,
            parallel_swipe: true,
        },
    };
    if (reasoning) info.extra.reasoning_type = ReasoningType.Model;

    const swipes = [text];
    parseReasoningInSwipes(swipes, [info], null);
    text = swipes[0];

    if (power_user.message_token_count_enabled) {
        try {
            info.extra.token_count = await getTokenCountAsync((info.extra.reasoning || '') + text, 0);
        } catch {
            // 计数失败不影响结果
        }
    }

    return { text, info };
}

// ─── 写回聊天 ────────────────────────────────────────────────────────────────

async function applyResults(session) {
    const results = session.jobs.filter(j => j.status === 'done').map(j => j.result);
    if (!results.length) return { reason: 'all_failed' };
    if (getCurrentChatId() !== session.chatId) return { reason: 'chat_changed' };

    bindTarget(session);
    const target = session.target;
    const messageId = target ? chat.indexOf(target) : -1;

    if (messageId === -1) {
        // 酒馆那条完全没生成出来（例如非流式请求报错），聊天也没被别的东西改动过 → 用并行结果新建回复
        const untouched = session.type !== 'swipe'
            && chat.length === session.baseLen
            && chat[chat.length - 1] === session.baseLast;
        if (!untouched) return { reason: 'missing_target' };
        return await createMessageFromResults(results);
    }

    ensureSwipes(target);
    syncMesToSwipe(messageId);

    const queue = [...results];
    let replacedCurrent = false;

    // 酒馆那条是空的（报错或被停止）→ 用第一条并行结果顶替它，而不是留一条空白备选
    const slot = session.slot;
    if (Number.isInteger(slot) && typeof target.swipes[slot] === 'string' && isEmptyReply(target.swipes[slot])) {
        const first = queue.shift();
        target.swipes[slot] = first.text;
        target.swipe_info[slot] = structuredClone(first.info);
        if (target.swipe_id === slot) {
            syncSwipeToMes(messageId, slot);
            replacedCurrent = true;
        }
    }

    for (const r of queue) {
        target.swipes.push(r.text);
        target.swipe_info.push(structuredClone(r.info));
    }

    if (replacedCurrent) {
        updateMessageBlock(messageId, target);
        await eventSource.emit(event_types.MESSAGE_UPDATED, messageId);
    }
    await updateSwipeCounter(messageId);
    refreshSwipeButtons(true);
    await saveChatConditional();

    return { reason: 'applied', added: results.length, total: target.swipes.length };
}

async function createMessageFromResults(results) {
    const [first] = results;
    const message = {
        name: name2,
        is_user: false,
        send_date: first.info.send_date,
        mes: first.text,
        gen_started: first.info.gen_started,
        gen_finished: first.info.gen_finished,
        extra: structuredClone(first.info.extra),
        swipe_id: 0,
        swipes: results.map(r => r.text),
        swipe_info: results.map(r => structuredClone(r.info)),
    };

    chat.push(message);
    const messageId = chat.length - 1;
    await eventSource.emit(event_types.MESSAGE_RECEIVED, messageId, 'normal');
    addOneMessage(message);
    await eventSource.emit(event_types.CHARACTER_MESSAGE_RENDERED, messageId, 'normal');
    await saveChatConditional();

    return { reason: 'created', total: message.swipes.length };
}

// ─── 进度条 ──────────────────────────────────────────────────────────────────

let barRenderPending = false;

function scheduleBarRender() {
    if (barRenderPending) return;
    barRenderPending = true;
    requestAnimationFrame(() => {
        barRenderPending = false;
        renderBar();
    });
}

function renderBar() {
    if (!sessions.size) {
        $('#pswipe_bar').remove();
        return;
    }

    let running = 0;
    let done = 0;
    let failed = 0;
    let chars = 0;
    let waiting = false;
    let graceLeft = null;
    for (const session of sessions) {
        let sessionRunning = 0;
        for (const job of session.jobs) {
            if (job.status === 'running') {
                running++;
                sessionRunning++;
                chars += job.chars;
            } else if (job.status === 'done') {
                done++;
            } else if (job.status === 'failed' || job.status === 'aborted') {
                failed++;
            }
        }
        if (session.phase === 'waiting') waiting = true;
        if (sessionRunning && session.graceDeadline) {
            const left = Math.max(0, Math.ceil((session.graceDeadline - Date.now()) / 1000));
            graceLeft = graceLeft === null ? left : Math.min(graceLeft, left);
        }
    }

    let text;
    if (running) {
        text = `并行备选生成中：完成 ${done} · 进行中 ${running}${failed ? ` · 已取消 ${failed}` : ''}${chars ? `（已收到约 ${chars} 字）` : ''}`;
        if (graceLeft !== null) text += ` · 酒馆那条已完成，最多再等 ${graceLeft} 秒`;
    } else if (waiting) {
        text = `并行备选已就绪 ${done} 条${failed ? `（已取消 ${failed} 条）` : ''}，等酒馆这条完成后加入…`;
    } else {
        text = '并行备选准备中…';
    }

    let $bar = $('#pswipe_bar');
    if (!$bar.length) {
        $bar = $(`
            <div id="pswipe_bar">
                <i class="fa-solid fa-layer-group pswipe_spinner fa-beat-fade"></i>
                <span class="pswipe_text"></span>
                <div class="menu_button pswipe_stop" title="停止还在进行的并行请求（已完成的仍会加入）">停止</div>
            </div>
        `);
        $bar.find('.pswipe_stop').on('click', () => abortRunningJobs());
        const $form = $('#form_sheld');
        if ($form.length) $form.before($bar);
        else $('#sheld').append($bar);
    }
    $bar.find('.pswipe_text').text(text);
    $bar.find('.pswipe_stop').toggle(running > 0);
}

// ─── 设置界面 ────────────────────────────────────────────────────────────────

function syncSettingsUI() {
    const s = getSettings();
    $('#pswipe_enabled').prop('checked', s.enabled);
    $('#pswipe_count').val(s.count);
    $('#pswipe_on_swipe').prop('checked', s.onSwipe);
    $('#pswipe_retry').prop('checked', s.retry);
    $('#pswipe_timeout').val(s.timeoutSec);
    $('#pswipe_grace').val(s.graceSec);
    $('#pswipe_wand_label').text(s.enabled ? `并行回复：${s.count} 条` : '并行回复：已关闭');
}

function addSettingsPanel() {
    if ($('#pswipe_settings').length) return;
    const html = `
        <div id="pswipe_settings" class="pswipe_settings">
            <div class="inline-drawer">
                <div class="inline-drawer-toggle inline-drawer-header">
                    <b>并行备选回复</b>
                    <div class="inline-drawer-icon fa-solid fa-circle-chevron-down down"></div>
                </div>
                <div class="inline-drawer-content">
                    <label class="checkbox_label" for="pswipe_enabled">
                        <input type="checkbox" id="pswipe_enabled">
                        <span>启用</span>
                    </label>
                    <div class="pswipe_row">
                        <label for="pswipe_count">每次生成的回复总数</label>
                        <input type="number" id="pswipe_count" class="text_pole" min="1" max="${MAX_COUNT}" step="1">
                    </div>
                    <label class="checkbox_label" for="pswipe_on_swipe">
                        <input type="checkbox" id="pswipe_on_swipe">
                        <span>右滑生成新回复时也并行</span>
                    </label>
                    <label class="checkbox_label" for="pswipe_retry">
                        <input type="checkbox" id="pswipe_retry">
                        <span>遇到限流/网关错误自动重试一次</span>
                    </label>
                    <div class="pswipe_row">
                        <label for="pswipe_grace">酒馆那条完成后，最多再等几秒</label>
                        <input type="number" id="pswipe_grace" class="text_pole" min="10" max="1800" step="10">
                    </div>
                    <div class="pswipe_row">
                        <label for="pswipe_timeout">多少秒没收到新内容就放弃（0 = 不限）</label>
                        <input type="number" id="pswipe_timeout" class="text_pole" min="0" max="600" step="5">
                    </div>
                    <small>
                        回复总数包含酒馆自己那条，例如填 3 = 酒馆 1 条 + 并行 2 条。每条都单独计费。<br>
                        酒馆那条还在生成时，并行的几条不会被判超时；酒馆那条结束后，没完成的再等上面设的秒数，超过就放弃。<br>
                        「没收到新内容就放弃」只适合真正逐字输出的服务商；如果回复是最后一次性整段出现的，请保持 0。<br>
                        也可以在魔杖菜单里点「并行回复」，或输入 <code>/pswipe 3</code>、<code>/pswipe off</code> 随时调整。<br>
                        只对聊天补全接口的单角色聊天生效；启用后酒馆自带的「每次生成多个备选」(n) 不再随请求发送。
                    </small>
                </div>
            </div>
        </div>
    `;
    const $container = $('#extensions_settings2').length ? $('#extensions_settings2') : $('#extensions_settings');
    $container.append(html);

    $('#pswipe_enabled').on('input', function () { setEnabled($(this).prop('checked')); });
    $('#pswipe_count').on('change', function () { setCount($(this).val()); });
    $('#pswipe_on_swipe').on('input', function () {
        getSettings().onSwipe = $(this).prop('checked');
        saveSettingsDebounced();
    });
    $('#pswipe_retry').on('input', function () {
        getSettings().retry = $(this).prop('checked');
        saveSettingsDebounced();
    });
    $('#pswipe_timeout').on('change', function () {
        getSettings().timeoutSec = clampTimeout($(this).val());
        saveSettingsDebounced();
        syncSettingsUI();
    });
    $('#pswipe_grace').on('change', function () {
        getSettings().graceSec = clampGrace($(this).val());
        saveSettingsDebounced();
        syncSettingsUI();
    });
}

async function openCountPopup() {
    const s = getSettings();
    const value = await callGenericPopup(
        `每次生成的回复总数（1–${MAX_COUNT}，含酒馆自己那条；填 1 或 0 = 关闭）`,
        POPUP_TYPE.INPUT,
        String(s.enabled ? s.count : 1),
    );
    if (value === null || value === false || value === undefined) return;
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n)) {
        toastr.warning('请输入数字。', '并行备选');
        return;
    }
    if (n <= 1) {
        setEnabled(false);
        toastr.info('并行备选回复已关闭。', '并行备选');
    } else {
        setCount(n);
        setEnabled(true);
        toastr.info(`之后每次生成 ${getSettings().count} 条回复。`, '并行备选');
    }
}

function addWandMenuEntry() {
    if ($('#pswipe_wand').length) return true;
    const $menu = $('#extensionsMenu');
    if (!$menu.length) return false;
    const $entry = $(`
        <div id="pswipe_wand" class="list-group-item flex-container flexGap5 interactable" tabindex="0" title="设置每次并行生成几条回复">
            <i class="fa-solid fa-layer-group extensionsMenuExtensionButton"></i>
            <span id="pswipe_wand_label"></span>
        </div>
    `);
    $entry.on('click', async () => {
        $menu.hide();
        await openCountPopup();
    });
    $menu.append($entry);
    syncSettingsUI();
    return true;
}

function registerSlashCommand() {
    SlashCommandParser.addCommandObject(SlashCommand.fromProps({
        name: 'pswipe',
        callback: (_args, value) => {
            const raw = String(value ?? '').trim().toLowerCase();
            if (!raw) {
                const s = getSettings();
                return s.enabled ? String(s.count) : '0';
            }
            if (['off', '0', '1', 'false'].includes(raw)) {
                setEnabled(false);
                toastr.info('并行备选回复已关闭。', '并行备选');
                return '0';
            }
            if (['on', 'true'].includes(raw)) {
                setEnabled(true);
                return String(getSettings().count);
            }
            const n = Math.floor(Number(raw));
            if (!Number.isFinite(n)) {
                toastr.warning('用法：/pswipe 3 或 /pswipe off', '并行备选');
                return '';
            }
            setCount(n);
            setEnabled(true);
            toastr.info(`之后每次生成 ${getSettings().count} 条回复。`, '并行备选');
            return String(getSettings().count);
        },
        unnamedArgumentList: [
            SlashCommandArgument.fromProps({
                description: '回复总数（2–10），或 on / off；不填则返回当前数量',
                typeList: [ARGUMENT_TYPE.STRING],
                isRequired: false,
            }),
        ],
        helpString: '设置并行备选回复的数量。例如 <code>/pswipe 3</code> 表示每次生成共 3 条回复，<code>/pswipe off</code> 关闭。',
        returns: '当前回复总数（0 表示关闭）',
    }));
}

// ─── 初始化 ──────────────────────────────────────────────────────────────────

jQuery(() => {
    getSettings();
    addSettingsPanel();
    syncSettingsUI();

    let attempts = 0;
    const tryWand = () => {
        if (!addWandMenuEntry() && attempts++ < 40) setTimeout(tryWand, 250);
    };
    tryWand();

    try {
        registerSlashCommand();
    } catch (err) {
        console.warn(LOG, '注册 /pswipe 命令失败', err);
    }

    eventSource.on(event_types.GENERATION_STARTED, onGenerationStarted);
    eventSource.makeLast(event_types.CHAT_COMPLETION_SETTINGS_READY, onSettingsReady);
    eventSource.on(event_types.MESSAGE_RECEIVED, onMessageReceived);
    // 主请求没走到发送就结束了（例如被斜杠命令中断），别让记号残留去误认后面的扩展请求
    eventSource.on(event_types.GENERATION_ENDED, () => { pendingMain = null; });

    // 酒馆的停止按钮（不带参数）→ 一并停止并行请求；酒馆助手按 ID 停止自己的请求时会带参数，忽略
    eventSource.on(event_types.GENERATION_STOPPED, (...args) => {
        if (args.length === 0 || args[0] === undefined) abortRunningJobs();
    });

    eventSource.on(event_types.CHAT_CHANGED, () => {
        pendingMain = null;
        for (const session of sessions) cancelSession(session, { discard: true });
    });

    console.info(LOG, '扩展已加载');
});
