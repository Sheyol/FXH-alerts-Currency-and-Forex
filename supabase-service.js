const { createClient } = require('@supabase/supabase-js');
const firebaseService = require('./firebase-service');
const telegramBot = require('./telegram-bot');

class SupabaseService {
  constructor() {
    const url = process.env.SUPABASE_URL || '';
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY || '';
    this.client = url && key ? createClient(url, key) : null;

    // Map<symbol, Map<alertId, alertObj>>
    this.alertsBySymbol = new Map();
    // 50ms Atomic Trigger Flush Queue
    this.flushQueue = new Map();
    this.inFlightTriggers = new Set();
    this.onSymbolsChangedCallbacks = [];

    const timer = setInterval(() => this._processFlushQueue(), 50);
    if (typeof timer.unref === 'function') timer.unref();
  }

  onSymbolsChanged(cb) {
    this.onSymbolsChangedCallbacks.push(cb);
  }

  _notifySymbolsChanged() {
    const allSymbols = Array.from(this.alertsBySymbol.keys());
    const forexSymbols = allSymbols.filter((s) => s.startsWith('frx') || s.startsWith('OTC_'));
    const cryptoSymbols = allSymbols.filter((s) => !s.startsWith('frx') && !s.startsWith('OTC_'));
    for (const cb of this.onSymbolsChangedCallbacks) {
      cb({ forexSymbols, cryptoSymbols });
    }
  }

  async startSync() {
    if (!this.client) {
      console.warn('[SupabaseService] Missing SUPABASE_URL or key; running without DB sync.');
      return;
    }

    await this.syncActiveAlerts();
    // 30-second periodic reconciliation sync
    setInterval(() => this.syncActiveAlerts(), 30000);

    // Realtime subscription for instant alert additions/edits/cancellations
    this.client
      .channel('public:user_alerts')
      .on(
        'postgres_changes',
        { event: '*', schema: 'public', table: 'user_alerts' },
        () => this.syncActiveAlerts()
      )
      .subscribe();
  }

  async syncActiveAlerts() {
    if (!this.client) return;
    try {
      const { data, error } = await this.client
        .from('user_alerts')
        .select('*')
        .eq('status', 'active');

      if (error) throw error;

      const nextMap = new Map();
      for (const alert of data || []) {
        if (!alert.symbol) continue;
        const sym = alert.symbol.trim().toUpperCase();
        // Isolate Forex, Gold, Metals & Crypto alerts from sister app synthetic index alerts
        const isForexCrypto = sym.startsWith('FRX') || sym.startsWith('OTC_') || sym.endsWith('USDT') || sym.endsWith('USD');
        if (!isForexCrypto) continue;
        if (alert.app_type && alert.app_type !== 'forex_crypto') continue;

        if (!nextMap.has(alert.symbol)) {
          nextMap.set(alert.symbol, new Map());
        }
        nextMap.get(alert.symbol).set(alert.id, alert);
      }

      this.alertsBySymbol = nextMap;
      this._notifySymbolsChanged();
    } catch (err) {
      console.error('[SupabaseService] syncActiveAlerts error:', err.message);
    }
  }

  checkTick(symbol, currentPrice, meta = {}) {
    const symbolAlerts = this.alertsBySymbol.get(symbol);
    if (!symbolAlerts || symbolAlerts.size === 0) return;

    for (const [alertId, alert] of symbolAlerts.entries()) {
      if (this.inFlightTriggers.has(alertId)) continue;

      const target = parseFloat(alert.target_price);
      if (isNaN(target)) continue;

      const type = (alert.alert_type || 'ABOVE').toUpperCase();
      let crossed = false;
      if (type === 'ABOVE' && currentPrice >= target) crossed = true;
      else if (type === 'BELOW' && currentPrice <= target) crossed = true;
      else if (type === 'EQUALS' && Math.abs(currentPrice - target) <= target * 0.0001) crossed = true;

      if (crossed) {
        this.inFlightTriggers.add(alertId);
        symbolAlerts.delete(alertId);
        this.flushQueue.set(alertId, { alert, currentPrice, meta });
      }
    }
  }

  async flushNow() {
    return this._processFlushQueue();
  }

  async _processFlushQueue() {
    if (this.flushQueue.size === 0) return;

    const batch = Array.from(this.flushQueue.values());
    this.flushQueue.clear();

    for (const item of batch) {
      const { alert, currentPrice, meta } = item;
      const isAutoReset = (alert.alert_note || '').includes('[Auto-Reset]');
      const nextStatus = isAutoReset ? 'active' : 'triggered';

      try {
        if (this.client) {
          // 1. Atomic UPDATE status='triggered' (or re-arm if Auto-Reset)
          const { data: updated, error } = await this.client
            .from('user_alerts')
            .update({
              status: nextStatus,
              triggered_at: new Date().toISOString(),
            })
            .eq('id', alert.id)
            .eq('status', 'active')
            .select()
            .maybeSingle();

          if (error || !updated) {
            this.inFlightTriggers.delete(alert.id);
            continue;
          }
        }

        // 2. Dispatch High-Priority Data-Only FCM Push
        await firebaseService.sendHighPriorityAlertPush({
          fcmToken: alert.fcm_token,
          alert,
          currentPrice,
          displayName: meta.displayName || alert.symbol,
          category: meta.category || 'Forex Majors',
        });

        // 3. Notify Admin Telegram Bot
        await telegramBot.notifyAlertTriggered({
          alert,
          currentPrice,
          displayName: meta.displayName || alert.symbol,
          category: meta.category || 'Forex Majors',
        });
      } catch (err) {
        console.error(`[SupabaseService] Failed to flush alert ${alert.id}:`, err.message);
      } finally {
        setTimeout(() => this.inFlightTriggers.delete(alert.id), 10000);
      }
    }
  }

  getStats() {
    let totalAlerts = 0;
    for (const map of this.alertsBySymbol.values()) {
      totalAlerts += map.size;
    }
    return {
      activeSymbolsCount: this.alertsBySymbol.size,
      activeAlertsCount: totalAlerts,
    };
  }
}

module.exports = new SupabaseService();
