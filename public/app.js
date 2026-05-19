'use strict';

const WS_URL = `ws://${window.location.host}`;
const RECONNECT_DELAY_MS = 3000;

// ── Couleurs et configs par symbole ────────────────────────────────
const SYMBOL_CONFIG = {
  'BTC/USDT': {
    color:   '#F7931A',
    icon:    'https://cdn.jsdelivr.net/gh/atomiclabs/cryptocurrency-icons@master/svg/color/btc.svg',
    label:   'BTC / USDT',
    source:  'Binance · Temps réel',
    iconBg:  'rgba(247,147,26,0.15)',
    glowBg:  'radial-gradient(circle at 80% 20%, rgba(247,147,26,0.12), transparent 60%)'
  },
  'ETH/USDT': {
    color:   '#627EEA',
    icon:    'https://cdn.jsdelivr.net/gh/atomiclabs/cryptocurrency-icons@master/svg/color/eth.svg',
    label:   'ETH / USDT',
    source:  'Binance · Temps réel',
    iconBg:  'rgba(98,126,234,0.15)',
    glowBg:  'radial-gradient(circle at 80% 20%, rgba(98,126,234,0.12), transparent 60%)'
  },
  'BTC-USD': {
    color:   '#3B9EFF',
    icon:    'https://cdn.jsdelivr.net/gh/atomiclabs/cryptocurrency-icons@master/svg/color/btc.svg',
    label:   'BTC / USD',
    source:  'Coinbase · Temps réel',
    iconBg:  'rgba(59,158,255,0.15)',
    glowBg:  'radial-gradient(circle at 80% 20%, rgba(59,158,255,0.12), transparent 60%)'
  }
};

// ── État global ────────────────────────────────────────────────────
let currentSymbol = 'BTC/USDT';
let allData       = { symbols: {}, alerts: [] };
let priceChart    = null;
let wsConnection  = null;
let reconnectTimer = null;

// ── Formatage ──────────────────────────────────────────────────────
const fmt = {
  price:   (v) => v != null ? `$${Number(v).toLocaleString('fr-FR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '—',
  volume:  (v) => {
    if (v == null) return '—';
    if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`;
    if (v >= 1_000)     return `$${(v / 1_000).toFixed(1)}K`;
    return `$${v.toFixed(2)}`;
  },
  pct:     (v) => v != null ? `${v >= 0 ? '+' : ''}${(v * 100).toFixed(3)}%` : '—',
  integer: (v) => v != null ? Number(v).toLocaleString('fr-FR') : '—',
  date:    (d) => {
    if (!d) return '—';
    return new Date(d).toLocaleString('fr-FR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }
};

// ── Graphique Chart.js ─────────────────────────────────────────────
function initChart() {
  const canvas = document.getElementById('priceChart');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');

  const gradient = ctx.createLinearGradient(0, 0, 0, 300);
  gradient.addColorStop(0, 'rgba(247,147,26,0.25)');
  gradient.addColorStop(1, 'rgba(247,147,26,0)');

  priceChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [{
        label: 'Prix moyen',
        data: [],
        borderColor: '#F7931A',
        borderWidth: 2.5,
        pointRadius: 3,
        pointHoverRadius: 6,
        pointBackgroundColor: '#F7931A',
        pointBorderColor: '#070B14',
        pointBorderWidth: 2,
        fill: true,
        backgroundColor: gradient,
        tension: 0.4,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 500, easing: 'easeInOutQuart' },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(13,19,33,0.95)',
          borderColor: 'rgba(255,255,255,0.1)',
          borderWidth: 1,
          titleColor: '#8892A4',
          bodyColor: '#F0F4FF',
          padding: 12,
          titleFont: { family: 'Inter', size: 11, weight: '500' },
          bodyFont:  { family: 'JetBrains Mono', size: 14, weight: '600' },
          callbacks: { label: (c) => ` ${fmt.price(c.raw)}` }
        }
      },
      scales: {
        x: {
          grid: { color: 'rgba(255,255,255,0.04)' },
          ticks: { color: '#4A5568', font: { family: 'JetBrains Mono', size: 11 }, maxTicksLimit: 10, maxRotation: 0 }
        },
        y: {
          position: 'right',
          grid: { color: 'rgba(255,255,255,0.04)' },
          ticks: { color: '#4A5568', font: { family: 'JetBrains Mono', size: 11 }, callback: (v) => `$${(v/1000).toFixed(1)}K` }
        }
      }
    }
  });
}

// Mettre à jour la couleur du graphique selon le symbole sélectionné
function updateChartColor(color) {
  if (!priceChart) return;
  const canvas = document.getElementById('priceChart');
  const ctx = canvas.getContext('2d');

  const hexToRgb = (hex) => {
    const r = parseInt(hex.slice(1,3),16);
    const g = parseInt(hex.slice(3,5),16);
    const b = parseInt(hex.slice(5,7),16);
    return `${r},${g},${b}`;
  };

  const rgb = hexToRgb(color);
  const gradient = ctx.createLinearGradient(0, 0, 0, 300);
  gradient.addColorStop(0, `rgba(${rgb},0.25)`);
  gradient.addColorStop(1, `rgba(${rgb},0)`);

  priceChart.data.datasets[0].borderColor = color;
  priceChart.data.datasets[0].pointBackgroundColor = color;
  priceChart.data.datasets[0].backgroundColor = gradient;
  priceChart.update('none');
}

// ── Rendu du symbole actif ─────────────────────────────────────────
function renderSymbol(symbol) {
  const config = SYMBOL_CONFIG[symbol];
  if (!config) return;

  const symData = allData.symbols[symbol] || { stats: [], aggregates: [] };

  // Mettre à jour les couleurs du graphique
  updateChartColor(config.color);

  // Mettre à jour les icônes et labels
  const iconImg = document.getElementById('kpi-icon-img');
  if (iconImg) iconImg.src = config.icon;

  const priceLabel = document.getElementById('kpi-price-label');
  if (priceLabel) priceLabel.textContent = `Prix ${config.label}`;

  const priceSource = document.getElementById('kpi-price-source');
  if (priceSource) priceSource.textContent = config.source;

  const chartIcon = document.getElementById('chart-crypto-icon');
  if (chartIcon) chartIcon.src = config.icon;

  const chartTitle = document.getElementById('chart-title');
  if (chartTitle) chartTitle.textContent = `Prix Moyen Mobile — ${config.label}`;

  const chartDot = document.getElementById('chart-legend-dot');
  if (chartDot) chartDot.style.background = config.color;

  const vwapBadge = document.getElementById('vwap-symbol-badge');
  if (vwapBadge) vwapBadge.textContent = symbol;

  const priceGlow = document.getElementById('kpi-price-glow');
  if (priceGlow) priceGlow.style.background = config.glowBg;

  // Mettre à jour le chart-card data-symbol
  const chartCard = document.getElementById('chart-card');
  if (chartCard) chartCard.setAttribute('data-symbol', symbol);

  // Mettre à jour le graphique avec les données du symbole sélectionné
  renderStats(symData.stats);

  // Mettre à jour les fenêtres VWAP
  renderAggregates(symData.aggregates);
}

// ── Rendu graphique (stats) ────────────────────────────────────────
function renderStats(data) {
  const placeholder = document.getElementById('chart-placeholder');

  if (!Array.isArray(data) || data.length === 0) {
    if (placeholder) placeholder.classList.add('visible');
    if (priceChart) { priceChart.data.labels = []; priceChart.data.datasets[0].data = []; priceChart.update(); }
    setKPI('kpi-price-val', '—');
    setKPI('kpi-volume-val', '—');
    setKPI('kpi-trades-val', '—');
    return;
  }

  if (placeholder) placeholder.classList.remove('visible');

  const sorted = [...data].sort((a, b) => a.timeLabel.localeCompare(b.timeLabel));
  const labels = sorted.map(d => d.timeLabel);
  const prices = sorted.map(d => d.prixMoyen);

  if (priceChart) {
    priceChart.data.labels = labels;
    priceChart.data.datasets[0].data = prices;
    priceChart.update('active');
  }

  const latest = sorted[sorted.length - 1];
  setKPI('kpi-price-val', fmt.price(latest?.prixMoyen));
  setKPI('kpi-volume-val', fmt.volume(sorted.reduce((acc, d) => acc + (d.volumeMinute || 0), 0)));
  setKPI('kpi-trades-val', fmt.integer(latest?.nombreTrades));
}

// ── Rendu VWAP ─────────────────────────────────────────────────────
function renderAggregates(data) {
  const list = document.getElementById('vwap-list');
  if (!list) return;

  if (!Array.isArray(data) || data.length === 0) {
    list.innerHTML = '<div class="vwap-placeholder">Aucune donnée VWAP disponible</div>';
    return;
  }

  const config  = SYMBOL_CONFIG[currentSymbol] || {};
  const order   = { '1m': 1, '5m': 2, '15m': 3, '1h': 4 };
  const sorted  = [...data].sort((a, b) => (order[a.fenetre || a._id] || 99) - (order[b.fenetre || b._id] || 99));

  list.innerHTML = sorted.map(item => {
    const fenetre   = item.fenetre || item._id || '—';
    const vwap      = item.vwapArrondi ?? item.dernierVwap;
    const variation = item.variationPourcentage;
    const volume    = item.volumeFenetreUsd;
    const tendance  = item.tendance || (variation >= 0 ? 'HAUSSE' : 'BAISSE');
    const isHausse  = tendance === 'HAUSSE';
    const pctClass  = variation != null ? (variation >= 0 ? 'pos' : 'neg') : '';

    return `
      <div class="vwap-item">
        <div class="vwap-item-head">
          <span class="vwap-window-tag">${fenetre}</span>
          <span class="vwap-trend ${isHausse ? 'hausse' : 'baisse'}">${tendance}</span>
        </div>
        <div class="vwap-price">${fmt.price(vwap)}</div>
        <div class="vwap-meta">
          <span class="vwap-vol">Vol: ${fmt.volume(volume)}</span>
          <span class="vwap-variation ${pctClass}">${fmt.pct(variation)}</span>
        </div>
      </div>
    `;
  }).join('');
}

// ── Rendu alertes ──────────────────────────────────────────────────
function renderAlerts(data) {
  const tbody = document.getElementById('alerts-tbody');
  const badge = document.getElementById('total-alerts-badge');
  if (!tbody) return;

  const totalCount = Array.isArray(data) ? data.reduce((acc, d) => acc + (d.nombreAlertes || 0), 0) : 0;
  if (badge) badge.textContent = `${fmt.integer(totalCount)} occurrences`;
  setKPI('kpi-anomalies-val', fmt.integer(totalCount));

  if (!Array.isArray(data) || data.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-row">Aucune anomalie détectée</td></tr>';
    return;
  }

  tbody.innerHTML = data.map(item => `
    <tr>
      <td>
        <div class="alert-type-tag type-${item.typeAlerte}">
          <span class="alert-type-dot"></span>
          ${(item.typeAlerte || '—').replace(/_/g, ' ')}
        </div>
      </td>
      <td><span class="symbol-tag">${item.symbole || '—'}</span></td>
      <td><span class="count-val">${fmt.integer(item.nombreAlertes)}</span></td>
      <td><span class="risk-badge risk-${item.niveauRisque}">${item.niveauRisque || 'FAIBLE'}</span></td>
      <td><span class="date-val">${fmt.date(item.derniereAlerte)}</span></td>
    </tr>
  `).join('');
}

// ── Mise à jour des prix dans les tabs ─────────────────────────────
function updateTabPrices() {
  for (const [symbol, symData] of Object.entries(allData.symbols)) {
    const el = document.getElementById(`tab-price-${CSS.escape(symbol)}`);
    if (!el) continue;
    const stats = symData.stats || [];
    if (stats.length > 0) {
      const sorted = [...stats].sort((a, b) => a.timeLabel.localeCompare(b.timeLabel));
      const latest = sorted[sorted.length - 1];
      el.textContent = fmt.price(latest?.prixMoyen);
    }
  }
}

// ── Changement de symbole (déclenché par les tabs) ─────────────────
window.switchSymbol = function(symbol) {
  if (currentSymbol === symbol) return;
  currentSymbol = symbol;

  // Mettre à jour les tabs actifs
  document.querySelectorAll('.symbol-tab').forEach(tab => {
    tab.classList.toggle('active', tab.dataset.symbol === symbol);
  });

  // Re-rendre toutes les sections avec les données du nouveau symbole
  renderSymbol(symbol);
};

// ── Helpers UI ─────────────────────────────────────────────────────
function setKPI(id, value) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = value;
  el.classList.remove('flash-update');
  void el.offsetWidth;
  el.classList.add('flash-update');
}

function updateLastUpdateTime() {
  const el = document.getElementById('last-update-time');
  if (el) el.textContent = new Date().toLocaleTimeString('fr-FR');
}

function setWsStatus(state) {
  const dot  = document.getElementById('ws-dot');
  const pill = document.getElementById('ws-pill');
  if (!dot || !pill) return;
  dot.className  = 'status-dot';
  pill.className = 'status-pill';
  if (state === 'connected')  { dot.classList.add('active');     pill.classList.add('connected'); }
  if (state === 'connecting') { dot.classList.add('connecting'); }
}

// ── Traitement des données reçues ──────────────────────────────────
function handleData(payload) {
  if (!payload) return;
  allData = payload;

  // Mettre à jour les prix dans les tabs
  updateTabPrices();

  // Re-rendre le symbole actuellement affiché
  renderSymbol(currentSymbol);

  // Mettre à jour les alertes (globales, indépendantes du symbole)
  renderAlerts(payload.alerts);

  updateLastUpdateTime();
}

// ── WebSocket ──────────────────────────────────────────────────────
function connectWebSocket() {
  setWsStatus('connecting');
  wsConnection = new WebSocket(WS_URL);

  wsConnection.onopen = () => {
    console.log('[Dashboard] WebSocket connecté.');
    setWsStatus('connected');
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  };

  wsConnection.onmessage = (event) => {
    try {
      const message = JSON.parse(event.data);
      if (message.type === 'INIT' || message.type === 'UPDATE_ALL') {
        handleData(message.payload);
      }
    } catch (err) {
      console.error('[Dashboard] Erreur parsing message:', err.message);
    }
  };

  wsConnection.onerror = () => setWsStatus('connecting');

  wsConnection.onclose = () => {
    console.warn('[Dashboard] WebSocket déconnecté. Reconnexion dans 3s...');
    setWsStatus('connecting');
    reconnectTimer = setTimeout(connectWebSocket, RECONNECT_DELAY_MS);
  };
}

// ── Démarrage ──────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initChart();
  connectWebSocket();
  console.log('[Dashboard] CryptoMonitor multi-crypto initialisé.');
});
