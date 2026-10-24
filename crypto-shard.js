const WebSocket = require('ws');

// Uses data-stream.binance.vision by default to work globally without US/EU geo-restrictions and with zero API keys
const BINANCE_WS_URL =
  process.env.BINANCE_WS_URL || 'wss://data-stream.binance.vision/ws';

/**
 * 100% Free 24/7 Crypto Stream via Binance Public WebSocket.
 */
class CryptoShard {
  constructor(supabaseService) {
    this.supabaseService = supabaseService;
    this.ws = null;
    this.isConnected = false;
    this.subscribedStreams = new Set();
    this.targetSymbols = new Set([
      'BTCUSDT',
      'ETHUSDT',
      'SOLUSDT',
      'BNBUSDT',
      'XRPUSDT',
      'ADAUSDT',
      'DOGEUSDT',
      'AVAXUSDT',
      'LINKUSDT',
      'TONUSDT',
    ]);
    this.lastTickAt = new Map();
  }

  start() {
    this._connect();
  }

  updateSubscriptions(symbols = []) {
    for (const s of symbols) {
      if (s && !s.startsWith('frx')) {
        this.targetSymbols.add(s.toUpperCase());
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

      this.ws = new WebSocket(BINANCE_WS_URL);

      this.ws.on('open', () => {
        this.isConnected = true;
        this.subscribedStreams.clear();
        console.log('[CryptoShard] Connected to Binance Public WS');
        this._syncSubscriptions();
      });

      this.ws.on('message', (raw) => {
        try {
          const msg = JSON.parse(raw);
          if (msg.e === '24hrMiniTicker' && msg.s && msg.c) {
            const symbol = msg.s.toUpperCase();
            const currentPrice = parseFloat(msg.c);
            if (!isNaN(currentPrice)) {
              this.lastTickAt.set(symbol, Date.now());
              const isMajor = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT'].includes(symbol);
              this.supabaseService.checkTick(symbol, currentPrice, {
                displayName: symbol.replace('USDT', '/USDT'),
                category: isMajor ? 'Crypto Majors' : 'Altcoins',
              });
            }
          }
        } catch (err) {
          console.error('[CryptoShard] Parse error:', err.message);
        }
      });

      this.ws.on('close', () => {
        this.isConnected = false;
        setTimeout(() => this._connect(), 3000);
      });

      this.ws.on('error', (err) => {
        console.error('[CryptoShard] WS error:', err.message);
      });
    } catch (err) {
      console.error('[CryptoShard] Connection failed:', err.message);
      setTimeout(() => this._connect(), 5000);
    }
  }

  _syncSubscriptions() {
    if (!this.ws || !this.isConnected || this.ws.readyState !== WebSocket.OPEN) return;

    const paramsToSubscribe = [];
    for (const sym of this.targetSymbols) {
      const streamName = `${sym.toLowerCase()}@miniTicker`;
      if (!this.subscribedStreams.has(streamName)) {
        paramsToSubscribe.push(streamName);
        this.subscribedStreams.add(streamName);
      }
    }

    if (paramsToSubscribe.length > 0) {
      this.ws.send(
        JSON.stringify({
          method: 'SUBSCRIBE',
          params: paramsToSubscribe,
          id: Date.now(),
        })
      );
    }
  }
}

module.exports = CryptoShard;
