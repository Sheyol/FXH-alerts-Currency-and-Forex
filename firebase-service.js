const admin = require('firebase-admin');

class FirebaseService {
  constructor() {
    this.initialized = false;
    this._init();
  }

  _init() {
    try {
      const rawJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
      if (rawJson && rawJson !== '{}') {
        const serviceAccount = JSON.parse(rawJson);
        admin.initializeApp({
          credential: admin.credential.cert(serviceAccount),
        });
        this.initialized = true;
        console.log('[FirebaseService] Initialized with service account.');
      } else {
        console.warn('[FirebaseService] FIREBASE_SERVICE_ACCOUNT_JSON not set; running in dry-run log mode.');
      }
    } catch (err) {
      console.error('[FirebaseService] Initialization failed:', err.message);
    }
  }

  /**
   * Mandatory Mobile Alarm Rule #1:
   * Send DATA-ONLY High-Priority FCM Pushes!
   * Never send a top-level `notification: { title, body }` object to Android,
   * otherwise Android's system tray swallows the push when the app is killed
   * and never invokes `firebaseMessagingBackgroundHandler`.
   */
  async sendHighPriorityAlertPush({ fcmToken, alert, currentPrice, displayName, category }) {
    if (!fcmToken) return false;

    const payload = {
      token: fcmToken,
      data: {
        type: 'PRICE_ALERT_TRIGGERED',
        alert_id: String(alert.id),
        symbol: String(alert.symbol),
        display_name: String(displayName || alert.symbol),
        category: String(category || 'Forex Majors'),
        target_price: String(alert.target_price),
        current_price: String(currentPrice),
        alert_type: String(alert.alert_type || 'ABOVE'),
        sound_key: String(alert.sound_key || 'default'),
        title: `🚨 ${displayName || alert.symbol} TARGET HIT!`,
        body: `${displayName || alert.symbol} crossed ${alert.alert_type} ${alert.target_price} (Live: ${currentPrice})${alert.alert_note ? ' • ' + alert.alert_note : ''}`,
      },
      android: {
        priority: 'high',
        ttl: 0,
      },
      apns: {
        headers: {
          'apns-priority': '10',
          'apns-push-type': 'background',
        },
        payload: {
          aps: {
            'content-available': 1,
          },
        },
      },
    };

    if (!this.initialized) {
      console.log('[FirebaseService][DRY-RUN] High-priority data push:', JSON.stringify(payload.data));
      return true;
    }

    try {
      await admin.messaging().send(payload);
      return true;
    } catch (err) {
      console.error(`[FirebaseService] Push failed for alert ${alert.id}:`, err.message);
      return false;
    }
  }
}

module.exports = new FirebaseService();
