require('dotenv').config();
const express = require('express');
const supabaseService = require('./supabase-service');
const DerivForexShard = require('./deriv-forex-shard');
const CryptoShard = require('./crypto-shard');
const telegramBot = require('./telegram-bot');

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 7860;

const forexShard = new DerivForexShard(supabaseService);
const cryptoShard = new CryptoShard(supabaseService);

supabaseService.onSymbolsChanged(({ forexSymbols, cryptoSymbols }) => {
  forexShard.updateSubscriptions(forexSymbols);
  cryptoShard.updateSubscriptions(cryptoSymbols);
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    forexConnected: forexShard.isConnected,
    forexWeekendClosed: forexShard.isForexWeekendClosed(),
    cryptoConnected: cryptoShard.isConnected,
    ...supabaseService.getStats(),
  });
});

app.get('/symbols', (req, res) => {
  res.json({
    forex_and_metals: forexShard.cachedSymbols,
    crypto: Array.from(cryptoShard.targetSymbols),
  });
});

app.post('/telegram-webhook', (req, res) => {
  const expectedSecret = process.env.TELEGRAM_WEBHOOK_SECRET;
  if (expectedSecret) {
    const providedSecret = req.get('X-Telegram-Bot-Api-Secret-Token');
    if (providedSecret !== expectedSecret) {
      return res.sendStatus(401);
    }
  }
  telegramBot.processUpdate(req.body).catch((err) =>
    console.error('[telegram-webhook] error:', err.message)
  );
  res.sendStatus(200);
});

app.listen(PORT, async () => {
  console.log(`[FXH Watcher] Listening on port ${PORT}`);
  forexShard.start();
  cryptoShard.start();
  await supabaseService.startSync();
  telegramBot.init({ forexShard, cryptoShard, supabaseService });
});

// 10-minute self-ping keep-alive for Render Free Tier (auto-detects RENDER_EXTERNAL_URL)
const SELF_PING_URL =
  process.env.SELF_PING_URL ||
  (process.env.RENDER_EXTERNAL_URL ? `${process.env.RENDER_EXTERNAL_URL}/health` : null);
if (SELF_PING_URL) {
  setInterval(() => {
    fetch(SELF_PING_URL).catch((err) =>
      console.warn('[KeepAlive] Self-ping error:', err.message)
    );
  }, 10 * 60 * 1000);
}
