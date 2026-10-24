const WebSocket = require('ws');

const DERIV_WS_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';

/**
 * 100% Free Forex, Gold, Silver & Commodities Shard via Deriv Public WebSocket.
 * Includes the Weekend Market Hours Guard (Friday 21:00 UTC -> Sunday 21:00 UTC).
 */
class DerivForexShard {
  constructor(supabaseService) {
    this.supabaseService = supabaseService;
    this.ws = null;
    this.isConnected = false;
    this.subscribedSymbols = new Set();
    this.targetSymbols = new Set([
      'frxEURUSD',
      'frxGBPUSD',
      'frxUSDJPY',
      'frxGBPJPY',
      'frxXAUUSD',
      'frxXAGUSD',
      'frxBROUSD',
    ]);
    this.lastTickAt = new Map();
    this.cachedSymbols = [];

    // Stale-data checker every 10s with Weekend Market Hours Guard
    setInterval(() => this._checkStaleFeeds(), 10000);
  }

  /**
   * Section 4.3: Weekend Market Hours Guard for Forex
   * Forex & Gold close from Friday 21:00 UTC to Sunday 21:00 UTC.
   */
  isForexWeekendClosed(now = new Date()) {
    const day = now.getUTCDay(); // 0 = Sun, 5 = Fri, 6 = Sat
    const hour = now.getUTCHours();
    if (day === 6) return true; // Saturday all day
    if (day === 5 && hour >= 21) return true; // Friday >= 21:00 UTC
    if (day === 0 && hour < 21) return true; // Sunday < 21:00 UTC
    return false;
  }

  start() {
    this._connect();
  }

  updateSubscriptions(symbols = []) {
    for (const s of symbols) {
      if (s && s.startsWith('frx')) {
        this.targetSymbols.add(s);
      }
    }
    this._syncSubscriptions();
  }

  _connect() {
    try {
      if (this.ws) {
        try {
          this.ws.terminate();
        } catch (_) {}
      }

      this.ws = new WebSocket(DERIV_WS_URL);

      this.ws.on('open', () => {
        this.isConnected = true;
        this.subscribedSymbols.clear();
        console.log('[DerivForexShard] Connected to Deriv Public WS');
        // Fetch active_symbols brief for /symbols endpoint
        this.ws.send(JSON.stringify({ active_symbols: 'brief' }));
        this._syncSubscriptions();
      });

      this.ws.on('message', (raw) => {
        try {
          const msg = JSON.parse(raw);
          if (msg.msg_type === 'active_symbols' && Array.isArray(msg.active_symbols)) {
            this.cachedSymbols = msg.active_symbols.filter(
              (s) => s.market === 'forex' || s.market === 'commodities'
            );
          } else if (msg.msg_type === 'tick' && msg.tick) {
            const symbol = msg.tick.symbol;
            const quote = parseFloat(msg.tick.quote);
            if (!isNaN(quote)) {
              this.lastTickAt.set(symbol, Date.now());
              this.supabaseService.checkTick(symbol, quote, {
                displayName: this._formatDisplayName(symbol),
                category: symbol.includes('XAU') || symbol.includes('XAG')
                  ? 'Gold & Metals'
                  : 'Forex Majors',
              });
            }
          }
        } catch (err) {
          console.error('[DerivForexShard] Parse error:', err.message);
        }
      });

      this.ws.on('close', () => {
        this.isConnected = false;
        setTimeout(() => this._connect(), 3000);
      });

      this.ws.on('error', (err) => {
        console.error('[DerivForexShard] WS error:', err.message);
      });
    } catch (err) {
      console.error('[DerivForexShard] Connection failed:', err.message);
      setTimeout(() => this._connect(), 5000);
    }
  }

  _syncSubscriptions() {
    if (!this.ws || !this.isConnected || this.ws.readyState !== WebSocket.OPEN) return;

    for (const symbol of this.targetSymbols) {
      if (!this.subscribedSymbols.has(symbol)) {
        this.ws.send(JSON.stringify({ ticks: symbol, subscribe: 1 }));
        this.subscribedSymbols.add(symbol);
      }
    }
  }

  _checkStaleFeeds() {
    if (this.isForexWeekendClosed()) {
      // Skip stale warnings for frx* symbols when the Forex market is closed for the weekend
      return;
    }

    const now = Date.now();
    for (const symbol of this.subscribedSymbols) {
      const last = this.lastTickAt.get(symbol) || 0;
      if (last > 0 && now - last > 120000) {
        console.warn(`[DerivForexShard] Stale feed detected for ${symbol}, re-subscribing...`);
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
          this.ws.send(JSON.stringify({ ticks: symbol, subscribe: 1 }));
        }
      }
    }
  }

  _formatDisplayName(symbol) {
    if (symbol === 'frxXAUUSD') return 'XAU/USD (Gold)';
    if (symbol === 'frxXAGUSD') return 'XAG/USD (Silver)';
    if (symbol === 'frxBROUSD') return 'BRENT/USD (Oil)';
    if (symbol.startsWith('frx') && symbol.length === 9) {
      return `${symbol.slice(3, 6)}/${symbol.slice(6, 9)}`;
    }
    return symbol;
  }
}

module.exports = DerivForexShard;
