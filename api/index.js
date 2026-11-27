const WebSocket = require('ws');
const supabaseService = require('../supabase-service');
const telegramBot = require('../telegram-bot');

const DERIV_WS_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';
const BINANCE_TICKER_URLS = [
  'https://data-api.binance.vision/api/v3/ticker/price',
  'https://api.binance.com/api/v3/ticker/price',
];

const DEFAULT_FOREX_TARGETS = new Set([
  'frxEURUSD',
  'frxGBPUSD',
  'frxUSDJPY',
  'frxGBPJPY',
  'frxXAUUSD',
  'frxXAGUSD',
  'frxBROUSD',
]);

const DEFAULT_CRYPTO_TARGETS = new Set([
  'BTCUSDT',
  'ETHUSDT',
  'SOLUSDT',
  'BNBUSDT',
  'XRPUSDT',
]);

function isForexWeekendClosed(now = new Date()) {
  const day = now.getUTCDay();
  const hour = now.getUTCHours();
  if (day === 6) return true;
  if (day === 5 && hour >= 21) return true;
  if (day === 0 && hour < 21) return true;
  return false;
}

function formatDisplayName(symbol) {
  if (symbol === 'frxXAUUSD') return 'XAU/USD (Gold)';
  if (symbol === 'frxXAGUSD') return 'XAG/USD (Silver)';
  if (symbol === 'frxBROUSD') return 'BRENT/USD (Oil)';
  if (symbol.startsWith('frx') && symbol.length === 9) {
    return `${symbol.slice(3, 6)}/${symbol.slice(6, 9)}`;
  }
  if (symbol.endsWith('USDT')) {
    return `${symbol.slice(0, -4)}/USDT`;
  }
  return symbol;
}

function categorizeSymbol(symbol) {
  if (symbol.includes('XAU') || symbol.includes('XAG')) return 'Gold & Metals';
  if (symbol.startsWith('frx') || symbol.startsWith('OTC_')) return 'Forex Majors';
  return 'Crypto Majors';
}

const forexShardProxy = {
  isConnected: true,
  subscribedSymbols: new Set(DEFAULT_FOREX_TARGETS),
  isForexWeekendClosed: () => isForexWeekendClosed(),
  start: () => {},
};

const cryptoShardProxy = {
  isConnected: true,
  targetSymbols: new Set(DEFAULT_CRYPTO_TARGETS),
  start: () => {},
};

telegramBot.forexShard = forexShardProxy;
telegramBot.cryptoShard = cryptoShardProxy;
telegramBot.supabaseService = supabaseService;

let webhookRegisteredForHost = null;

async function ensureTelegramWebhook(req) {
  const hostHeader = req.headers['x-forwarded-host'] || req.headers.host || '';
  const prodHost = process.env.VERCEL_PROJECT_PRODUCTION_URL || hostHeader;
  if (!prodHost || prodHost.includes('localhost') || prodHost.includes('127.0.0.1')) {
    return;
  }
  const baseUrl = prodHost.startsWith('http') ? prodHost : `https://${prodHost}`;
  if (webhookRegisteredForHost === baseUrl) return;
  webhookRegisteredForHost = baseUrl;
  await telegramBot._registerWebhook(baseUrl);
}

async function fetchBinancePrices(symbols) {
  const quotes = new Map();
  if (symbols.length === 0) return quotes;
  const wanted = new Set(symbols.map((s) => s.toUpperCase()));
  wanted.add('PAXGUSDT');

  for (const url of BINANCE_TICKER_URLS) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
      if (!res.ok) continue;
      const list = await res.json();
      if (Array.isArray(list)) {
        for (const item of list) {
          const sym = (item.symbol || '').toUpperCase();
          if (wanted.has(sym)) {
            const p = parseFloat(item.price);
            if (!isNaN(p) && p > 0) quotes.set(sym, p);
          }
        }
        break;
      }
    } catch (_) {}
  }
  return quotes;
}

function fetchDerivLatestPrices(symbols, timeoutMs = 4500) {
  return new Promise((resolve) => {
    const quotes = new Map();
    if (symbols.length === 0) return resolve(quotes);

    let settled = false;
    const finish = (ws) => {
      if (settled) return;
      settled = true;
      try {
        ws.close();
      } catch (_) {}
      resolve(quotes);
    };

    try {
      const ws = new WebSocket(DERIV_WS_URL);
      const timer = setTimeout(() => finish(ws), timeoutMs);

      ws.on('open', () => {
        for (const sym of symbols) {
          ws.send(
            JSON.stringify({
              ticks_history: sym,
              adjust_start_time: 1,
              count: 1,
              end: 'latest',
              style: 'ticks',
            })
          );
        }
      });

      ws.on('message', (raw) => {
        try {
          const msg = JSON.parse(raw.toString());
          const sym = msg?.echo_req?.ticks_history;
          if (msg.msg_type === 'history' && sym) {
            const prices = msg?.history?.prices;
            if (Array.isArray(prices) && prices.length > 0) {
              const p = parseFloat(prices[prices.length - 1]);
              if (!isNaN(p) && p > 0) quotes.set(sym, p);
            } else {
              quotes.set(sym, NaN);
            }
          } else if (msg.error && sym) {
            quotes.set(sym, NaN);
          }

          if (quotes.size >= symbols.length) {
            clearTimeout(timer);
            finish(ws);
          }
        } catch (_) {}
      });

      ws.on('error', () => {
        clearTimeout(timer);
        finish(ws);
      });
    } catch (_) {
      resolve(quotes);
    }
  });
}

async function runServerlessPriceSweep() {
  await supabaseService.syncActiveAlerts();
  const activeSymbols = Array.from(supabaseService.alertsBySymbol.keys());

  const derivSymbols = activeSymbols.filter(
    (s) => s.startsWith('frx') || s.startsWith('OTC_')
  );
  const cryptoSymbols = activeSymbols.filter(
    (s) => !s.startsWith('frx') && !s.startsWith('OTC_')
  );

  for (const s of derivSymbols) forexShardProxy.subscribedSymbols.add(s);
  for (const s of cryptoSymbols) cryptoShardProxy.targetSymbols.add(s);

  const weekendClosed = isForexWeekendClosed();
  const needBinance =
    cryptoSymbols.length > 0 || (weekendClosed && derivSymbols.includes('frxXAUUSD'));

  const [binanceQuotes, derivQuotes] = await Promise.all([
    needBinance ? fetchBinancePrices(cryptoSymbols) : Promise.resolve(new Map()),
    !weekendClosed && derivSymbols.length > 0
      ? fetchDerivLatestPrices(derivSymbols)
      : Promise.resolve(new Map()),
  ]);

  let evaluatedCount = 0;

  for (const sym of cryptoSymbols) {
    const price = binanceQuotes.get(sym.toUpperCase());
    if (price && !isNaN(price)) {
      evaluatedCount++;
      supabaseService.checkTick(sym, price, {
        displayName: formatDisplayName(sym),
        category: categorizeSymbol(sym),
      });
    }
  }

  for (const sym of derivSymbols) {
    let price = derivQuotes.get(sym);
    if ((!price || isNaN(price)) && sym === 'frxXAUUSD') {
      price = binanceQuotes.get('PAXGUSDT');
    }
    if (price && !isNaN(price)) {
      evaluatedCount++;
      supabaseService.checkTick(sym, price, {
        displayName: formatDisplayName(sym),
        category: categorizeSymbol(sym),
      });
    }
  }

  const pendingBeforeFlush = supabaseService.flushQueue.size;
  await supabaseService.flushNow();

  return {
    evaluatedSymbols: evaluatedCount,
    triggeredNow: pendingBeforeFlush,
    forexWeekendClosed: weekendClosed,
  };
}

module.exports = async (req, res) => {
  try {
    const urlPath = (req.url || '/').split('?')[0];

    if (req.method === 'POST' && urlPath.startsWith('/telegram-webhook')) {
      const expectedSecret = process.env.TELEGRAM_WEBHOOK_SECRET;
      if (expectedSecret) {
        const providedSecret = req.headers['x-telegram-bot-api-secret-token'];
        if (providedSecret !== expectedSecret) {
          return res.status(401).json({ error: 'Unauthorized' });
        }
      }
      await telegramBot.processUpdate(req.body);
      return res.status(200).json({ ok: true });
    }

    if (urlPath.startsWith('/symbols')) {
      return res.status(200).json({
        forex_and_metals: Array.from(forexShardProxy.subscribedSymbols),
        crypto: Array.from(cryptoShardProxy.targetSymbols),
      });
    }

    await ensureTelegramWebhook(req);
    const sweep = await runServerlessPriceSweep();

    return res.status(200).json({
      status: 'ok',
      mode: 'vercel-serverless',
      ...sweep,
      ...supabaseService.getStats(),
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error('[Vercel Watcher] Error:', err.message);
    return res.status(500).json({ status: 'error', message: err.message });
  }
};
