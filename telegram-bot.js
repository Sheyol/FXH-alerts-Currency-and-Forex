const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');
const os = require('os');
const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '.env') });

const supabase = createClient(
  process.env.SUPABASE_URL || '',
  process.env.SUPABASE_SERVICE_ROLE_KEY || ''
);

async function telegramApi(method, payload, tokenOverride) {
  const token =
    tokenOverride ||
    process.env.MONITOR_BOT_TOKEN ||
    process.env.TELEGRAM_BOT_TOKEN ||
    '';
  if (!token) return null;

  const relayUrl = process.env.TELEGRAM_RELAY_URL || null;
  const relaySecret = process.env.TELEGRAM_RELAY_SECRET;

  let url;
  let body;
  const headers = { 'Content-Type': 'application/json' };

  if (relayUrl) {
    url = relayUrl;
    body = JSON.stringify({ method, payload, token });
    if (relaySecret) headers['x-relay-secret'] = relaySecret;
    if (process.env.SUPABASE_SERVICE_ROLE_KEY) {
      headers['Authorization'] = `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`;
    }
  } else {
    url = `https://api.telegram.org/bot${token}/${method}`;
    body = JSON.stringify(payload);
  }

  try {
    const res = await fetch(url, { method: 'POST', headers, body });
    const parsed = await res.json();
    if (!parsed.ok) {
      console.error(`[Telegram ${method}] API error:`, parsed.description || JSON.stringify(parsed));
      return null;
    }
    return parsed.result;
  } catch (err) {
    const cause = err.cause ? ` | cause: ${err.cause.code || err.cause.message}` : '';
    const via = relayUrl ? 'relay' : 'direct';
    console.error(`[Telegram ${method}] request failed via ${via}: ${err.message}${cause}`);
    return null;
  }
}

class MonitorService {
  constructor() {
    this.botToken =
      process.env.MONITOR_BOT_TOKEN ||
      process.env.TELEGRAM_BOT_TOKEN ||
      '';
    this.developerChatId =
      process.env.DEVELOPER_CHAT_ID ||
      process.env.TELEGRAM_CHAT_ID ||
      '';
    this.secretToken =
      process.env.MONITOR_SECRET_TOKEN ||
      '';

    this.authorizedIds = new Set(
      [this.developerChatId.toString().trim()].filter(Boolean)
    );

    // Sessions and Lockouts (Identical to FXH Synthetic Bot)
    this.sessions = new Map(); // Chat ID -> { authenticated: bool, authenticatedAt: number, attempts: number, lockedAt: number }
    this.MAX_AUTH_ATTEMPTS = 3;
    this.SESSION_TTL_MS = 8 * 60 * 60 * 1000; // 8 hours
    this.LOCKOUT_DURATION_MS = 5 * 60 * 1000; // 5 minutes

    // Throttling for outgoing alerts
    this.recentAlerts = new Map();
    this.THROTTLE_MS = 5 * 60 * 1000; // 5 mins

    // System stats hook references
    this.startTime = Date.now();
    this.forexShard = null;
    this.cryptoShard = null;
    this.supabaseService = null;
    this.fcmSuccessCount = 0;
    this.fcmFailCount = 0;
    this.staleTokenCount = 0;
    this.alertsFiredToday = 0;
    this.alertsFiredThisHour = 0;
    this.alertsFailedToday = 0;
    this.processingTimes = [];

    this.schedulesActive = false;
    this.isPolling = false;
    this.lastUpdateId = 0;
  }

  init({ forexShard, cryptoShard, supabaseService }) {
    this.forexShard = forexShard;
    this.cryptoShard = cryptoShard;
    this.supabaseService = supabaseService;

    if (!this.botToken || !this.developerChatId) {
      console.warn('Telegram Monitor Bot credentials not fully set.');
      return;
    }

    const appUrl =
      process.env.APP_URL ||
      process.env.RENDER_EXTERNAL_URL ||
      (process.env.VERCEL_PROJECT_PRODUCTION_URL
        ? `https://${process.env.VERCEL_PROJECT_PRODUCTION_URL}`
        : null);
    if (appUrl) {
      this._registerWebhook(appUrl);
    } else if (!process.env.VERCEL) {
      // Localhost / non-webhook mode: delete any stale webhook & start long-polling
      telegramApi('deleteWebhook', {}, this.botToken).then(() => {
        if (!this.isPolling) {
          this.isPolling = true;
          this._pollLoop();
          console.log('[TelegramBot] Initialized in long-polling mode with strict FXH Lock.');
        }
      });
    }

    this.startSchedules();
  }

  async _registerWebhook(appUrl) {
    const webhookUrl = `${appUrl.replace(/\/$/, '')}/telegram-webhook`;
    const webhookSecret = process.env.TELEGRAM_WEBHOOK_SECRET;
    const result = await telegramApi(
      'setWebhook',
      {
        url: webhookUrl,
        ...(webhookSecret ? { secret_token: webhookSecret } : {}),
      },
      this.botToken
    );
    if (result) {
      console.log(`[Telegram] Webhook registered at ${webhookUrl}`);
    }
  }

  async _pollLoop() {
    while (this.isPolling) {
      try {
        const updates = await telegramApi(
          'getUpdates',
          { offset: this.lastUpdateId + 1, timeout: 20 },
          this.botToken
        );
        if (Array.isArray(updates)) {
          for (const update of updates) {
            this.lastUpdateId = update.update_id;
            await this.processUpdate(update);
          }
        }
      } catch (_) {
        await new Promise((r) => setTimeout(r, 5000));
      }
      await new Promise((r) => setTimeout(r, 800));
    }
  }

  async processUpdate(update) {
    if (update && update.message) {
      await this._handleIncoming(update.message);
    }
  }

  async sendMessage(chatId, text, options = {}) {
    return telegramApi(
      'sendMessage',
      {
        chat_id: chatId,
        text,
        ...options,
      },
      this.botToken
    );
  }

  async deleteMessage(chatId, messageId) {
    return telegramApi(
      'deleteMessage',
      {
        chat_id: chatId,
        message_id: messageId,
      },
      this.botToken
    );
  }

  startSchedules() {
    if (this.schedulesActive) return;
    this.schedulesActive = true;
    this.healthInterval = setInterval(() => this._runHealthCheck(), 5 * 60 * 1000);
    this.hourlyInterval = setInterval(() => this._runHourlySummary(), 60 * 60 * 1000);
    this._scheduleDailyReport();
  }

  async send(message, options = {}) {
    const { level = 'INFO', throttleKey = null } = options;

    if (throttleKey) {
      const lastSent = this.recentAlerts.get(throttleKey);
      if (lastSent && Date.now() - lastSent < this.THROTTLE_MS) {
        return;
      }
      this.recentAlerts.set(throttleKey, Date.now());
    }

    const emoji =
      {
        INFO: 'ℹ️',
        WARN: '⚠️',
        ERROR: '🔴',
        CRITICAL: '🚨',
      }[level] || 'ℹ️';

    const timestamp = new Date().toUTCString();
    const formattedMessage =
      `${emoji} *FXH ${level}*\n` +
      `\`${timestamp}\`\n` +
      `─────────────────────\n` +
      `${message}`;

    await this.sendMessage(this.developerChatId, formattedMessage, {
      parse_mode: 'Markdown',
    });
  }

  async info(message, throttleKey = null) {
    await this.send(message, { level: 'INFO', throttleKey });
  }

  async warn(message, throttleKey = null) {
    await this.send(message, { level: 'WARN', throttleKey });
  }

  async error(message, throttleKey = null) {
    await this.send(message, { level: 'ERROR', throttleKey });
  }

  async critical(message) {
    await this.send(message, { level: 'CRITICAL' });
  }

  async notifyAlertTriggered({ alert, currentPrice, displayName, category }) {
    this.alertsFiredToday++;
    this.alertsFiredThisHour++;
    this.fcmSuccessCount++;
    const dirEmoji = (alert.alert_type || 'ABOVE') === 'ABOVE' ? '📈' : '📉';
    const noteLine = alert.alert_note ? `\n📝 *Note:* ${alert.alert_note}` : '';
    const msg =
      `🚨 *FXH ALERT TRIGGERED*\n` +
      `${dirEmoji} *Asset:* ${displayName} (\`${alert.symbol}\`)\n` +
      `🏷 *Category:* ${category}\n` +
      `🎯 *Target:* ${alert.alert_type} \`${alert.target_price}\`\n` +
      `⚡ *Live Price:* \`${currentPrice}\`${noteLine}`;
    await this.sendMessage(this.developerChatId, msg, { parse_mode: 'Markdown' });
  }

  async _handleIncoming(msg) {
    const chatId = msg.chat.id.toString();
    const text = msg.text ? msg.text.trim() : '';

    if (!this.authorizedIds.has(chatId)) {
      await this._reportUnauthorizedAccess(msg);
      return;
    }

    // Process Auth Commands (unprotected by session check)
    if (text.startsWith('/auth')) {
      const parts = text.split(/\s+/);
      const token = parts[1];
      await this._handleAuth(msg, token);
      return;
    }

    // Wrap all other commands with strict auth check
    await this._requireAuth(chatId, async () => {
      if (text === '/status') {
        await this._handleStatus(msg);
      } else if (text === '/revenue') {
        await this._handleRevenue(msg);
      } else if (text === '/alerts') {
        await this._handleAlerts(msg);
      } else if (text === '/users') {
        await this._handleUsers(msg);
      } else if (text === '/shards') {
        await this._handleShards(msg);
      } else if (text.startsWith('/restart')) {
        const parts = text.split(/\s+/);
        await this._handleRestart(msg, parts[1]);
      } else if (text.startsWith('/maintenance')) {
        const parts = text.split(/\s+/);
        await this._handleMaintenance(msg, parts[1]);
      } else if (text === '/system') {
        await this._handleSystem(msg);
      } else if (text === '/database') {
        await this._handleDatabase(msg);
      } else if (text === '/force_sync') {
        await this._handleForceSync(msg);
      } else if (text === '/check_exploits') {
        await this._handleCheckExploits(msg);
      } else if (text === '/help' || text === '/start') {
        await this._handleHelp(msg);
      }
    });
  }

  async _isSessionAuthenticated(chatId) {
    const session = this.sessions.get(chatId);
    if (session?.authenticated) {
      if (Date.now() - session.authenticatedAt > this.SESSION_TTL_MS) {
        this.sessions.delete(chatId);
      } else {
        session.authenticatedAt = Date.now();
        return true;
      }
    }

    // Persist 8-hour FXH Lock session across Vercel serverless invocations via Supabase
    try {
      const { data: profile } = await supabase
        .from('profiles')
        .select('id')
        .eq('username', 'jasiel.abah')
        .single();
      if (profile) {
        const { data: userData } = await supabase.auth.admin.getUserById(profile.id);
        const expiresAt = Number(userData?.user?.user_metadata?.fxh_fc_session_until || 0);
        if (expiresAt > Date.now()) {
          this.sessions.set(chatId, {
            authenticated: true,
            authenticatedAt: Date.now(),
            attempts: 0,
            lockedAt: 0,
          });
          return true;
        }
      }
    } catch (_) {}

    return false;
  }

  async _requireAuth(chatId, callback) {
    if (await this._isSessionAuthenticated(chatId)) {
      await callback();
    } else {
      await this.sendMessage(
        chatId,
        `🔒 *Session expired or not authenticated.*\n` +
          `Send: \`/auth <secret_token>\` or request a verification code via database using: \`/auth request_db\``,
        { parse_mode: 'Markdown' }
      );
    }
  }

  async _handleAuth(msg, token) {
    const chatId = msg.chat.id.toString();
    const session = this.sessions.get(chatId) || {
      authenticated: false,
      attempts: 0,
      lockedAt: 0,
    };

    // Check lockout status
    if (session.lockedAt && Date.now() - session.lockedAt < this.LOCKOUT_DURATION_MS) {
      const remainingSecs = Math.ceil(
        (this.LOCKOUT_DURATION_MS - (Date.now() - session.lockedAt)) / 1000
      );
      await this.sendMessage(
        chatId,
        `🔒 Locked out. Please try again in ${remainingSecs} seconds.`
      );
      return;
    }

    // Option B: Request Verification Code via Database
    if (token === 'request_db') {
      const code = Math.floor(100000 + Math.random() * 900000).toString();

      session.pendingCode = code;
      session.pendingCodeExpiresAt = Date.now() + 10 * 60 * 1000;
      this.sessions.set(chatId, session);

      try {
        const { data: profile, error: selectError } = await supabase
          .from('profiles')
          .select('id')
          .eq('username', 'jasiel.abah')
          .single();

        if (selectError || !profile) {
          throw new Error(selectError ? selectError.message : 'Developer profile not found');
        }

        const { error: updateError } = await supabase
          .from('profiles')
          .update({ trader_type: `MONITOR_AUTH:${code}` })
          .eq('id', profile.id);

        if (updateError) throw updateError;

        await supabase.auth.admin.updateUserById(profile.id, {
          user_metadata: { monitor_verification_code: code },
        });

        await this.sendMessage(
          chatId,
          `🔑 *Verification code written to Supabase.*\n` +
            `Please check the \`profiles\` table (row \`jasiel.abah\` -> \`trader_type\`) or the Auth User metadata console.\n` +
            `Enter: \`/auth <code>\` to unlock your session. Code expires in 10 minutes.`,
          { parse_mode: 'Markdown' }
        );
      } catch (err) {
        console.error('Option B Database write failed:', err.message);
        await this.sendMessage(
          chatId,
          `❌ Failed to write verification code to Supabase: \`${err.message}\``,
          { parse_mode: 'Markdown' }
        );
      }
      return;
    }

    let isValid = false;

    // 1. Check against local secret token
    if (token && this.secretToken) {
      if (token.length === this.secretToken.length) {
        isValid = crypto.timingSafeEqual(
          Buffer.from(token),
          Buffer.from(this.secretToken)
        );
      }
    }

    // 2. Check Option B memory code
    if (!isValid && session.pendingCode && Date.now() < session.pendingCodeExpiresAt) {
      if (token === session.pendingCode) {
        isValid = true;
        session.pendingCode = null;
        session.pendingCodeExpiresAt = null;

        try {
          const { data: profile } = await supabase
            .from('profiles')
            .select('id')
            .eq('username', 'jasiel.abah')
            .single();
          if (profile) {
            await supabase
              .from('profiles')
              .update({ trader_type: 'retail' })
              .eq('id', profile.id);
            await supabase.auth.admin.updateUserById(profile.id, {
              user_metadata: { monitor_verification_code: null },
            });
          }
        } catch (_) {}
      }
    }

    // 3. Check Option B database code directly (surviving backend restarts)
    if (!isValid && token && token.length === 6 && /^\d+$/.test(token)) {
      try {
        const { data: profile } = await supabase
          .from('profiles')
          .select('id, trader_type')
          .eq('username', 'jasiel.abah')
          .single();
        if (profile && profile.trader_type === `MONITOR_AUTH:${token}`) {
          isValid = true;
          await supabase
            .from('profiles')
            .update({ trader_type: 'retail' })
            .eq('id', profile.id);
          await supabase.auth.admin.updateUserById(profile.id, {
            user_metadata: { monitor_verification_code: null },
          });
        }
      } catch (_) {}
    }

    if (isValid) {
      session.authenticated = true;
      session.authenticatedAt = Date.now();
      session.attempts = 0;
      session.lockedAt = 0;
      this.sessions.set(chatId, session);

      try {
        const { data: profile } = await supabase
          .from('profiles')
          .select('id')
          .eq('username', 'jasiel.abah')
          .single();
        if (profile) {
          await supabase.auth.admin.updateUserById(profile.id, {
            user_metadata: { fxh_fc_session_until: Date.now() + this.SESSION_TTL_MS },
          });
        }
      } catch (_) {}

      // Delete the user's /auth message so the secret token doesn't stay visible in chat history
      try {
        await this.deleteMessage(chatId, msg.message_id);
      } catch (_) {}

      await this.sendMessage(
        chatId,
        `✅ *Authenticated Successfully*\n` +
          `Session is valid for 8 hours.\n` +
          `Use the menu below or type \`/help\` to see commands.`,
        {
          parse_mode: 'Markdown',
          reply_markup: {
            keyboard: [
              [{ text: '/status' }, { text: '/revenue' }, { text: '/alerts' }],
              [{ text: '/users' }, { text: '/shards' }, { text: '/system' }],
              [{ text: '/database' }, { text: '/force_sync' }, { text: '/check_exploits' }],
              [{ text: '/help' }],
            ],
            resize_keyboard: true,
            one_time_keyboard: false,
          },
        }
      );
    } else {
      session.attempts++;
      if (session.attempts >= this.MAX_AUTH_ATTEMPTS) {
        session.lockedAt = Date.now();
        await this.sendMessage(
          chatId,
          `🔒 Too many failed attempts. Locked out for 5 minutes.`
        );
      } else {
        await this.sendMessage(
          chatId,
          `❌ Invalid token. \`${this.MAX_AUTH_ATTEMPTS - session.attempts}\` attempts remaining.`,
          { parse_mode: 'Markdown' }
        );
      }
      this.sessions.set(chatId, session);
    }
  }

  async _sendAndAutoDelete(chatId, text, deleteAfterMs = 60000) {
    try {
      const sent = await this.sendMessage(chatId, text, { parse_mode: 'Markdown' });
      if (sent && sent.message_id) {
        setTimeout(async () => {
          try {
            await this.deleteMessage(chatId, sent.message_id);
          } catch (_) {}
        }, deleteAfterMs);
      }
      return sent;
    } catch (err) {
      console.error('Send and auto-delete failed:', err.message);
    }
  }

  _getCPUUsage() {
    const cpus = os.cpus();
    let user = 0,
      nice = 0,
      sys = 0,
      idle = 0,
      irq = 0;
    for (const cpu of cpus) {
      user += cpu.times.user;
      nice += cpu.times.nice;
      sys += cpu.times.sys;
      idle += cpu.times.idle;
      irq += cpu.times.irq;
    }
    const total = user + nice + sys + idle + irq;
    if (total === 0) return 0;
    return (((total - idle) / total) * 100).toFixed(1);
  }

  async _reportUnauthorizedAccess(msg) {
    const chatId = msg.chat.id;
    const from = msg.from || {};
    const text = msg.text || '';

    const report =
      `🚨 *Unauthorized Bot Access Attempt*\n` +
      `─────────────────────\n` +
      `Chat ID: \`${chatId}\`\n` +
      `User ID: \`${from.id || 'Unknown'}\`\n` +
      `Username: \`@${from.username || 'Unknown'}\`\n` +
      `Message: \`${text}\`\n` +
      `Time: \`${new Date().toISOString()}\`\n` +
      `─────────────────────\n` +
      `_Message ignored._`;

    try {
      await this.sendMessage(this.developerChatId, report, { parse_mode: 'Markdown' });
    } catch (_) {}
  }

  async _handleStatus(msg) {
    const chatId = msg.chat.id;

    try {
      if (this.supabaseService) {
        await this.supabaseService.syncActiveAlerts();
      }

      const nowIso = new Date().toISOString();
      const startOfTodayIso = new Date(new Date().setUTCHours(0, 0, 0, 0)).toISOString();

      const { count: totalUsers } = await supabase
        .from('profiles')
        .select('*', { count: 'exact', head: true });
      const { count: proUsers } = await supabase
        .from('profiles')
        .select('*', { count: 'exact', head: true })
        .or(`is_pro.eq.true,pro_expires_at.gt.${nowIso}`);
      const { data: rawActiveRows } = await supabase
        .from('user_alerts')
        .select('symbol')
        .eq('status', 'active')
        .neq('symbol', 'DELETE_ACCOUNT_REQUEST');
      const dbActiveRows = (rawActiveRows || []).filter((r) => {
        const s = (r.symbol || '').trim().toUpperCase();
        return (
          s.startsWith('FRX') ||
          s.startsWith('OTC_') ||
          s.endsWith('=F') ||
          s === 'DX-Y.NYB' ||
          s.endsWith('USDT') ||
          s.endsWith('USD')
        );
      });
      const { data: rawTriggeredToday } = await supabase
        .from('user_alerts')
        .select('symbol')
        .eq('status', 'triggered')
        .gte('triggered_at', startOfTodayIso);
      const triggeredTodayCount = (rawTriggeredToday || []).filter((r) => {
        const s = (r.symbol || '').trim().toUpperCase();
        return (
          s.startsWith('FRX') ||
          s.startsWith('OTC_') ||
          s.endsWith('=F') ||
          s === 'DX-Y.NYB' ||
          s.endsWith('USDT') ||
          s.endsWith('USD')
        );
      }).length;

      const stats = this.supabaseService
        ? this.supabaseService.getStats()
        : { activeAlertsCount: 0, activeSymbolsCount: 0 };
      const activeAlertCount = dbActiveRows ? dbActiveRows.length : stats.activeAlertsCount;
      const activeSymbols = dbActiveRows
        ? new Set(dbActiveRows.map((r) => r.symbol)).size
        : stats.activeSymbolsCount;

      const onlineShards =
        (this.forexShard?.isConnected ? 1 : 0) + (this.cryptoShard?.isConnected ? 1 : 0);
      const shardCount = 2;

      const rssMB = (process.memoryUsage().rss / 1024 / 1024).toFixed(1);

      const uptimeSecs = process.uptime();
      const days = Math.floor(uptimeSecs / (3600 * 24));
      const hours = Math.floor((uptimeSecs % (3600 * 24)) / 3600);
      const mins = Math.floor((uptimeSecs % 3600) / 60);
      const uptimeStr = `${days}d ${hours}h ${mins}m`;

      const response =
        `🟢 *FXH Forex & Crypto System Status*\n` +
        `─────────────────────\n` +
        `👤 Users: \`${totalUsers || 0}\` total | \`${proUsers || 0}\` Pro\n` +
        `🔔 Active alerts: \`${activeAlertCount}\` alerts across \`${activeSymbols}\` symbols\n` +
        `🎯 Triggered today: \`${Math.max(this.alertsFiredToday, triggeredTodayCount || 0)}\` alerts\n` +
        `📡 Shards: \`${onlineShards}/${shardCount}\` online (Deriv FX + Binance Crypto)\n` +
        `💾 Node RSS: \`${rssMB}MB\` / \`450MB\` cap\n` +
        `⏱️ Uptime: \`${uptimeStr}\``;

      await this.sendMessage(chatId, response, { parse_mode: 'Markdown' });
    } catch (err) {
      await this.sendMessage(chatId, `❌ Failed to retrieve status: \`${err.message}\``, {
        parse_mode: 'Markdown',
      });
    }
  }

  async _handleRevenue(msg) {
    const chatId = msg.chat.id;

    try {
      const { count: proUsers } = await supabase
        .from('profiles')
        .select('*', { count: 'exact', head: true })
        .eq('is_pro', true);

      const mrr = (proUsers || 0) * 5.99;
      const arr = mrr * 12;

      const response =
        `💰 *Revenue Snapshot*\n` +
        `─────────────────────\n` +
        `Active Pro Subs: \`${proUsers || 0}\`\n` +
        `Estimated MRR: \`$${mrr.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}\`\n` +
        `Estimated ARR: \`$${arr.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}\`\n` +
        `─────────────────────\n` +
        `_This data will auto-delete in 60s for device security._`;

      await this._sendAndAutoDelete(chatId, response, 60000);
      try {
        await this.deleteMessage(chatId, msg.message_id);
      } catch (_) {}
    } catch (err) {
      await this.sendMessage(chatId, `❌ Failed to calculate revenue: \`${err.message}\``, {
        parse_mode: 'Markdown',
      });
    }
  }

  async _handleAlerts(msg) {
    const chatId = msg.chat.id;

    try {
      const startOfTodayIso = new Date(new Date().setUTCHours(0, 0, 0, 0)).toISOString();

      const { data: allRows } = await supabase
        .from('user_alerts')
        .select('symbol, status, triggered_at')
        .neq('symbol', 'DELETE_ACCOUNT_REQUEST');
      const fxRows = (allRows || []).filter((r) => {
        const s = (r.symbol || '').trim().toUpperCase();
        return (
          s.startsWith('FRX') ||
          s.startsWith('OTC_') ||
          s.endsWith('=F') ||
          s === 'DX-Y.NYB' ||
          s.endsWith('USDT') ||
          s.endsWith('USD')
        );
      });

      const activeCount = fxRows.filter((r) => r.status === 'active').length;
      const pausedCount = fxRows.filter((r) => r.status === 'paused').length;
      const dbTriggeredToday = fxRows.filter(
        (r) => r.status === 'triggered' && r.triggered_at && r.triggered_at >= startOfTodayIso
      ).length;
      const dbTriggeredTotal = fxRows.filter((r) => r.status === 'triggered').length;

      const firedToday = Math.max(this.alertsFiredToday, dbTriggeredToday || 0);
      const fcmTotal = this.fcmSuccessCount + this.fcmFailCount;
      const fcmSuccessRateStr =
        fcmTotal > 0
          ? `${((this.fcmSuccessCount / fcmTotal) * 100).toFixed(1)}%`
          : 'N/A (0 pushes since boot)';

      const avgProcessingStr =
        this.processingTimes.length > 0
          ? `${Math.round(this.processingTimes.reduce((a, b) => a + b, 0) / this.processingTimes.length)}ms`
          : 'N/A (no server triggers yet)';

      const response =
        `🔔 *Alert Engine Stats*\n` +
        `─────────────────────\n` +
        `Active in DB: \`${activeCount || 0}\` | Paused: \`${pausedCount || 0}\`\n` +
        `Fired today: \`${firedToday}\` alerts (Total history: \`${dbTriggeredTotal || 0}\`)\n` +
        `Failed today: \`${this.alertsFailedToday}\` alerts\n` +
        `Avg processing: \`${avgProcessingStr}\`\n` +
        `FCM success rate: \`${fcmSuccessRateStr}\` (success: \`${this.fcmSuccessCount}\`, fail: \`${this.fcmFailCount}\`)\n` +
        `Stale FCM tokens cleaned: \`${this.staleTokenCount}\``;

      await this.sendMessage(chatId, response, { parse_mode: 'Markdown' });
    } catch (err) {
      await this.sendMessage(chatId, `❌ Failed to fetch alert stats: \`${err.message}\``, {
        parse_mode: 'Markdown',
      });
    }
  }

  async _handleUsers(msg) {
    const chatId = msg.chat.id;

    try {
      const { count: total } = await supabase
        .from('profiles')
        .select('*', { count: 'exact', head: true });
      const { count: pro } = await supabase
        .from('profiles')
        .select('*', { count: 'exact', head: true })
        .eq('is_pro', true);
      const conversionRate = total > 0 ? ((pro / total) * 100).toFixed(1) : '0.0';

      const {
        data: { users },
      } = await supabase.auth.admin.listUsers();
      const startOfToday = new Date().setHours(0, 0, 0, 0);
      const newToday = (users || []).filter(
        (u) => new Date(u.created_at).getTime() >= startOfToday
      ).length;
      const newThisWeek = (users || []).filter(
        (u) => new Date(u.created_at).getTime() >= Date.now() - 7 * 24 * 3600 * 1000
      ).length;

      const response =
        `👤 *User Statistics*\n` +
        `─────────────────────\n` +
        `Total registered: \`${total || 0}\`\n` +
        `New today: \`+${newToday}\` users\n` +
        `New this week: \`+${newThisWeek}\` users\n` +
        `Pro subscribers: \`${pro || 0}\`\n` +
        `Free users: \`${(total || 0) - (pro || 0)}\`\n` +
        `Conversion rate: \`${conversionRate}%\``;

      await this.sendMessage(chatId, response, { parse_mode: 'Markdown' });
    } catch (err) {
      await this.sendMessage(chatId, `❌ Failed to fetch user stats: \`${err.message}\``, {
        parse_mode: 'Markdown',
      });
    }
  }

  async _handleShards(msg) {
    const chatId = msg.chat.id;

    const fxIcon = this.forexShard?.isConnected ? '🟢' : '🔴';
    const fxStatus = this.forexShard?.isConnected ? 'ONLINE' : 'OFFLINE';
    const fxWeekend = this.forexShard?.isForexWeekendClosed() ? ' (Weekend Closed)' : '';
    const fxSubs = this.forexShard
      ? Array.from(this.forexShard.subscribedSymbols).join(', ') || 'none'
      : 'none';

    const cryptoIcon = this.cryptoShard?.isConnected ? '🟢' : '🔴';
    const cryptoStatus = this.cryptoShard?.isConnected ? 'ONLINE (24/7)' : 'OFFLINE';
    const cryptoSubs = this.cryptoShard
      ? Array.from(this.cryptoShard.targetSymbols).join(', ') || 'none'
      : 'none';

    const response =
      `📡 *WebSocket Shards*\n` +
      `─────────────────────\n` +
      `${fxIcon} *Shard 0 (Deriv Forex & Metals)*: ${fxStatus}${fxWeekend}\n   Subscriptions: \`${fxSubs}\`\n\n` +
      `${cryptoIcon} *Shard 1 (Binance Crypto 24/7)*: ${cryptoStatus}\n   Subscriptions: \`${cryptoSubs}\``;

    await this.sendMessage(chatId, response, { parse_mode: 'Markdown' });
  }

  async _handleRestart(msg, shardIdStr) {
    const chatId = msg.chat.id;
    const shardIndex = parseInt(shardIdStr, 10);

    if (shardIndex !== 0 && shardIndex !== 1) {
      await this.sendMessage(
        chatId,
        `❌ Invalid Shard ID. Use \`/restart 0\` (Deriv Forex) or \`/restart 1\` (Binance Crypto).`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    await this.sendMessage(chatId, `🔄 *Attempting restart of Shard ${shardIndex}...*`, {
      parse_mode: 'Markdown',
    });

    try {
      if (shardIndex === 0 && this.forexShard) {
        this.forexShard.start();
      } else if (shardIndex === 1 && this.cryptoShard) {
        this.cryptoShard.start();
      }
      await this.sendMessage(
        chatId,
        `✅ *Shard ${shardIndex} reconnection triggered.*`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      await this.sendMessage(chatId, `❌ Shard restart failed: \`${err.message}\``, {
        parse_mode: 'Markdown',
      });
    }
  }

  async _handleMaintenance(msg, mode) {
    const chatId = msg.chat.id;

    if (mode !== 'on' && mode !== 'off') {
      await this.sendMessage(chatId, '❌ Usage: `/maintenance on` or `/maintenance off`', {
        parse_mode: 'Markdown',
      });
      return;
    }

    try {
      const { error } = await supabase
        .from('app_settings')
        .update({
          minimum_version: mode === 'on' ? '99.99.99' : '1.0.0',
          update_url:
            mode === 'on'
              ? 'https://fxhalerts.com/maintenance'
              : 'https://fxhalerts.com/update',
        })
        .eq('id', 1);

      if (error) throw error;

      await this.sendMessage(
        chatId,
        `${mode === 'on' ? '🔴' : '🟢'} *Maintenance mode toggled ${mode.toUpperCase()}*\n` +
          `Supabase App Settings \`minimum_version\` updated. App redirection active.`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      await this.sendMessage(chatId, `❌ Failed to toggle maintenance: \`${err.message}\``, {
        parse_mode: 'Markdown',
      });
    }
  }

  async _handleSystem(msg) {
    const chatId = msg.chat.id;
    const cpus = os.cpus();
    const loadAvg = os.loadavg();
    const freeMem = os.freemem();
    const totalMem = os.totalmem();
    const heap = process.memoryUsage();

    const response =
      `🖥️ *OS / System Metrics*\n` +
      `─────────────────────\n` +
      `*Platform:* \`${os.platform()} (${os.arch()})\`\n` +
      `*CPU Cores:* \`${cpus.length}\` @ \`${cpus[0]?.speed || 0}MHz\`\n` +
      `*Load Average (1/5/15m):* \`${loadAvg.map((l) => l.toFixed(2)).join(', ')}\`\n` +
      `*Free Memory:* \`${(freeMem / 1024 / 1024).toFixed(0)} MB\` / \`${(totalMem / 1024 / 1024).toFixed(0)} MB\`\n` +
      `*Node RSS:* \`${(heap.rss / 1024 / 1024).toFixed(1)} MB\`\n` +
      `*Heap Used:* \`${(heap.heapUsed / 1024 / 1024).toFixed(1)} MB\` / \`${(heap.heapTotal / 1024 / 1024).toFixed(1)} MB\``;

    await this.sendMessage(chatId, response, { parse_mode: 'Markdown' });
  }

  async _handleDatabase(msg) {
    const chatId = msg.chat.id;
    const start = Date.now();
    try {
      const { error } = await supabase.from('app_settings').select('id').limit(1);
      if (error) throw error;
      const latency = Date.now() - start;
      await this.sendMessage(
        chatId,
        `🗄️ *Supabase DB Ping*\n─────────────────────\nLatency: \`${latency}ms\`\nStatus: \`Healthy\``,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      await this.sendMessage(
        chatId,
        `❌ *Supabase DB Error*\n─────────────────────\nError: \`${err.message}\``,
        { parse_mode: 'Markdown' }
      );
    }
  }

  async _handleForceSync(msg) {
    const chatId = msg.chat.id;
    await this.sendMessage(chatId, `🔄 *Force syncing user alerts from Supabase...*`, {
      parse_mode: 'Markdown',
    });
    try {
      if (this.supabaseService) {
        await this.supabaseService.syncActiveAlerts();
      }
      const stats = this.supabaseService
        ? this.supabaseService.getStats()
        : { activeSymbolsCount: 0, activeAlertsCount: 0 };
      await this.sendMessage(
        chatId,
        `✅ *Alert pool synced.* Watcher is watching \`${stats.activeAlertsCount}\` alerts across \`${stats.activeSymbolsCount}\` symbols.`,
        { parse_mode: 'Markdown' }
      );
    } catch (err) {
      await this.sendMessage(chatId, `❌ Sync failed: \`${err.message}\``, {
        parse_mode: 'Markdown',
      });
    }
  }

  async _handleHelp(msg) {
    const response =
      `🤖 *FXH Forex & Crypto Monitor Commands*\n` +
      `─────────────────────\n` +
      `*Query (read-only):*\n` +
      `\`/status\` — Full system health snapshot\n` +
      `\`/revenue\` — MRR, ARR metrics (auto-delete)\n` +
      `\`/alerts\` — Alert engine performance\n` +
      `\`/users\` — User growth and conversion\n` +
      `\`/shards\` — WebSocket shards & assets list\n` +
      `\`/system\` — Detailed OS memory/CPU statistics\n` +
      `\`/database\` — Supabase DB round-trip ping latency\n` +
      `\n` +
      `*Action (requires active session):*\n` +
      `\`/restart [0|1]\` — Re-initialize a specific WebSocket shard\n` +
      `\`/maintenance [on|off]\` — Toggle maintenance mode redirects\n` +
      `\`/force_sync\` — Force reload alerts from Supabase database\n` +
      `\`/check_exploits\` — Scan for system exploits, anomalies, and unreviewed security flags\n` +
      `\`/help\` — Print this help message`;

    await this.sendMessage(msg.chat.id, response, {
      parse_mode: 'Markdown',
      reply_markup: {
        keyboard: [
          [{ text: '/status' }, { text: '/revenue' }, { text: '/alerts' }],
          [{ text: '/users' }, { text: '/shards' }, { text: '/system' }],
          [{ text: '/database' }, { text: '/force_sync' }, { text: '/check_exploits' }],
          [{ text: '/help' }],
        ],
        resize_keyboard: true,
        one_time_keyboard: false,
      },
    });
  }

  async _handleCheckExploits(msg) {
    const chatId = msg.chat.id;
    await this.sendMessage(chatId, `🔍 *Scanning system for exploits and anomalies...*`, {
      parse_mode: 'Markdown',
    });

    try {
      const { data: highPointsUsers, error: pointsErr } = await supabase
        .from('profiles')
        .select('username, points')
        .gt('points', 5000)
        .order('points', { ascending: false })
        .limit(10);

      if (pointsErr) throw pointsErr;

      const { data: unreviewedEvents, error: eventsErr } = await supabase
        .from('security_events')
        .select('event_type, user_id, ip_address, created_at')
        .eq('reviewed', false)
        .order('created_at', { ascending: false })
        .limit(10);

      if (eventsErr) throw eventsErr;

      let response = `🛡️ *Exploit & Security Scan Report*\n`;
      response += `📅 \`${new Date().toUTCString()}\`\n`;
      response += `─────────────────────\n\n`;

      response += `💰 *High Points Users (>5000 pts):*\n`;
      if (highPointsUsers && highPointsUsers.length > 0) {
        highPointsUsers.forEach((u) => {
          response += `• \`@${u.username || 'unknown'}\`: \`${u.points} pts\`\n`;
        });
      } else {
        response += `_No users with abnormally high points._\n`;
      }
      response += `\n`;

      response += `🚨 *Unreviewed Security Events (last 10):*\n`;
      if (unreviewedEvents && unreviewedEvents.length > 0) {
        unreviewedEvents.forEach((e) => {
          const timeStr = new Date(e.created_at).toLocaleTimeString();
          response += `• \`[${timeStr}]\` *${e.event_type}*\n  IP: \`${e.ip_address || 'unknown'}\` | User: \`${e.user_id || 'anonymous'}\`\n`;
        });
      } else {
        response += `_No pending unreviewed security events. Clean sheet!_\n`;
      }

      await this.sendMessage(chatId, response, { parse_mode: 'Markdown' });
    } catch (err) {
      await this.sendMessage(
        chatId,
        `❌ *Failed to complete security scan:* \`${err.message}\``,
        { parse_mode: 'Markdown' }
      );
    }
  }

  async _runHealthCheck() {
    const cpuUsage = parseFloat(this._getCPUUsage());
    if (cpuUsage > 85.0) {
      await this.warn(
        `🖥️ *High CPU Usage*\nCPU Usage: \`${cpuUsage}%\`\nThreshold: \`85%\``,
        'high_cpu'
      );
    }

    const rssMB = process.memoryUsage().rss / 1024 / 1024;
    if (rssMB > 400) {
      await this.warn(
        `💾 *High Memory Usage*\nNode RSS: \`${rssMB.toFixed(0)}MB\` / \`450MB\` limit`,
        'high_memory'
      );
    }
  }

  async _runHourlySummary() {
    if (this.alertsFiredThisHour > 0) {
      const fcmTotal = this.fcmSuccessCount + this.fcmFailCount;
      const fcmSuccessRate =
        fcmTotal > 0 ? ((this.fcmSuccessCount / fcmTotal) * 100).toFixed(1) : '100.0';

      await this.info(
        `📊 *Hourly Alert Summary*\n` +
          `─────────────────────\n` +
          `Fired this hour: \`${this.alertsFiredThisHour}\` alerts\n` +
          `FCM Success Rate: \`${fcmSuccessRate}%\` (total today: \`${this.alertsFiredToday}\`)\n` +
          `Status: All systems operational.`
      );
      this.alertsFiredThisHour = 0;
    }
  }

  _scheduleDailyReport() {
    const now = new Date();
    const nextReport = new Date(
      Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 8, 0, 0, 0)
    );
    if (now.getTime() >= nextReport.getTime()) {
      nextReport.setUTCDate(nextReport.getUTCDate() + 1);
    }
    const delay = nextReport.getTime() - now.getTime();
    setTimeout(() => {
      this._runDailyReport().catch(() => {});
      this.dailyInterval = setInterval(() => {
        this._runDailyReport().catch(() => {});
      }, 24 * 60 * 60 * 1000);
    }, delay);
  }

  async _runDailyReport() {
    try {
      const { count: totalUsers } = await supabase
        .from('profiles')
        .select('*', { count: 'exact', head: true });
      const { count: proUsers } = await supabase
        .from('profiles')
        .select('*', { count: 'exact', head: true })
        .eq('is_pro', true);
      const conversionRate =
        totalUsers > 0 ? ((proUsers / totalUsers) * 100).toFixed(1) : '0.0';
      const mrr = (proUsers || 0) * 5.99;
      const arr = mrr * 12;

      await this.info(
        `📊 *FXH Forex & Crypto Daily Report*\n` +
          `📅 ${new Date().toDateString()}\n` +
          `─────────────────────\n` +
          `👤 *USERS*: \`${totalUsers || 0}\` total | \`${proUsers || 0}\` Pro (\`${conversionRate}%\`)\n` +
          `💰 *REVENUE*: MRR \`$${mrr.toFixed(2)}\` | ARR \`$${arr.toFixed(2)}\`\n` +
          `🔔 *ALERTS FIRED TODAY*: \`${this.alertsFiredToday}\``
      );
    } catch (_) {}
  }
}

module.exports = new MonitorService();
