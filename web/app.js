// Cymbal Retail Operations Studio Client Application
// Built on Google Cloud ADK 2.0 & BigQuery Agent Analytics

let currentView = 'chat';
let currentRightTab = 'sql';
let currentSessionId = 'default';
let agentFilter = 'auto';
let isProcessing = false;

// 7 Verified Operational Scenarios
const SCENARIOS = [
  {
    id: 'UC 1.1a',
    name: 'ERR-PAY-4001 EMV Freeze SOP',
    domain: 'POS Hardware Troubleshooting',
    gateway: 'POS Runbook RAG (BigQuery Vector Search)',
    dispatch: 'Single-Tool (RAG)',
    prompt: 'What is the immediate field recovery protocol when a cashier encounters an ERR-PAY-4001 EMV contactless payment freeze, and how do we ensure the customer is not double-charged?',
    validator: (res) => {
      const calls = res.tool_calls || [];
      const hasRag = calls.some(c => c.name === 'pos_troubleshooting_rag_tool');
      const text = res.final_text || '';
      const hasLink = text.includes('https://') || text.includes('storage.cloud.google.com') || text.includes('ERR-PAY-4001');
      return hasRag && hasLink;
    }
  },
  {
    id: 'UC 1.1c',
    name: 'Ford F-150 Maintenance Refusal',
    domain: 'Safety & Out-of-Scope Guardrails',
    gateway: 'POS Runbook RAG (Certified Refusal)',
    dispatch: 'Single-Tool (RAG Refusal)',
    prompt: 'How do I replace the engine oil on a Ford F-150 truck?',
    validator: (res) => {
      const calls = res.tool_calls || [];
      const hasRag = calls.some(c => c.name === 'pos_troubleshooting_rag_tool');
      const text = res.final_text || '';
      return hasRag && (text.includes('cannot find certified') || text.includes('warranty or repair rules') || text.includes('technical repository'));
    }
  },
  {
    id: 'UC 1.2a',
    name: 'Critical Stockout Risk (<20h)',
    domain: 'Inventory Operations & Supply Chain',
    gateway: 'BigQuery Conversational Data Agent',
    dispatch: 'Single-Tool (Gold Ledger)',
    prompt: 'Which high-velocity SKU has less than 20 hours of inventory remaining, and what is its estimated stockout time?',
    validator: (res) => {
      const calls = res.tool_calls || [];
      const hasAnalytics = calls.some(c => c.name === 'cymbal_analytics_tool');
      const text = res.final_text || '';
      return hasAnalytics && (text.includes('SKU-') || text.includes('Stockout') || text.includes('hours') || text.includes('inventory'));
    }
  },
  {
    id: 'UC 1.3',
    name: 'CASH_1190 Real-Time Telemetry',
    domain: 'Loss Prevention & Audit',
    gateway: 'Cloud Bigtable Sub-second Telemetry',
    dispatch: 'Single-Tool (Bigtable MCP)',
    prompt: 'Fetch the real-time 1-hour rolling metrics for cashier CASH_1190 at store STORE_048.',
    validator: (res) => {
      const calls = res.tool_calls || [];
      const hasBt = calls.some(c => c.name === 'read_cashier_realtime_alerts');
      const text = res.final_text || '';
      return hasBt && (text.includes('CASH_1190') || text.includes('STORE_048') || text.includes('Discount') || text.includes('Risk Score'));
    }
  },
  {
    id: 'UC 2.1a',
    name: 'Lifetime Warranty Verification',
    domain: 'Policy & Customer Service',
    gateway: 'BigQuery Conversational Data Agent',
    dispatch: 'Single-Tool (Warranty Docs)',
    prompt: 'Does our store return policy provide a lifetime replacement guarantee on power tools, and what is the maximum refund without manager approval?',
    validator: (res) => {
      const calls = res.tool_calls || [];
      const hasAnalytics = calls.some(c => c.name === 'cymbal_analytics_tool');
      const text = res.final_text || '';
      return hasAnalytics && (text.includes('warranty') || text.includes('refund') || text.includes('manager') || text.includes('policy'));
    }
  },
  {
    id: 'UC 2.2',
    name: 'Dual Cashier Baseline Comparison',
    domain: 'Cashier Anomaly Investigation',
    gateway: 'Bigtable Real-Time + BigQuery Gold Baseline',
    dispatch: 'Parallel Dispatch (Turn 1)',
    prompt: "Compare cashier CASH_1190's live discount metrics against their historical 7-day baseline to evaluate promo abuse risk.",
    validator: (res) => {
      const calls = res.tool_calls || [];
      const hasBt = calls.some(c => c.name === 'read_cashier_realtime_alerts');
      const hasAnalytics = calls.some(c => c.name === 'cymbal_analytics_tool');
      return hasBt && hasAnalytics;
    }
  },
  {
    id: 'UC 2.3',
    name: 'Cross-Cloud Promo Abuse Audit',
    domain: 'Multi-Cloud Fraud Investigation',
    gateway: 'BigQuery Gold Alerts + Federated AWS S3',
    dispatch: 'Sequential Multi-Turn Audit',
    prompt: 'Identify the cashier with the highest anomaly alerts in GCP, and cross-reference their transaction logs in AWS S3.',
    validator: (res) => {
      const calls = res.tool_calls || [];
      return calls.some(c => c.name === 'cymbal_analytics_tool');
    }
  }
];

document.addEventListener('DOMContentLoaded', () => {
  if (window.lucide) {
    window.lucide.createIcons();
  }
  renderMatrixTable();
});

// View Switching: Operations Studio vs Architecture vs Test Matrix
function switchView(viewName) {
  currentView = viewName;
  const views = ['chat', 'arch', 'matrix'];
  
  views.forEach(v => {
    const el = document.getElementById(`view-${v}`);
    const btn = document.getElementById(`nav-btn-${v}`);
    if (el) {
      if (v === viewName) {
        el.classList.remove('hidden');
      } else {
        el.classList.add('hidden');
      }
    }
    if (btn) {
      if (v === viewName) {
        btn.className = 'px-3.5 py-1.5 rounded-lg flex items-center gap-1.5 transition bg-white text-blue-700 shadow-xs font-semibold';
      } else {
        btn.className = 'px-3.5 py-1.5 rounded-lg flex items-center gap-1.5 transition text-slate-600 hover:text-slate-900 font-semibold';
      }
    }
  });

  if (window.lucide) {
    window.lucide.createIcons();
  }
}

// Right Pane Tab Switching
function switchRightTab(tabName) {
  currentRightTab = tabName;
  const tabs = ['sql', 'chart', 'sop', 'trace'];
  
  tabs.forEach(t => {
    const content = document.getElementById(`rtab-content-${t}`);
    const btn = document.getElementById(`rtab-btn-${t}`);
    if (content) {
      if (t === tabName) {
        content.classList.remove('hidden');
      } else {
        content.classList.add('hidden');
      }
    }
    if (btn) {
      if (t === tabName) {
        btn.className = 'px-3 py-1.5 rounded-lg flex items-center gap-1.5 bg-blue-50 text-blue-700 border border-blue-200 transition font-semibold';
      } else {
        btn.className = 'px-3 py-1.5 rounded-lg flex items-center gap-1.5 text-slate-600 hover:text-slate-900 border border-transparent transition';
      }
    }
  });

  if (tabName === 'chart' && window.Plotly) {
    setTimeout(() => {
      const container = document.getElementById('plotly-container');
      if (container && container.data) {
        window.Plotly.Plots.resize(container);
      }
    }, 50);
  }

  if (window.lucide) {
    window.lucide.createIcons();
  }
}

// Agent Filter Selection
function setAgentFilter(filter) {
  agentFilter = filter;
  const filters = ['auto', 'analytics', 'rag', 'bigtable', 'audit'];
  
  filters.forEach(f => {
    const btn = document.getElementById(`filter-${f}`);
    if (btn) {
      if (f === filter) {
        btn.className = 'px-2 py-1 rounded-lg border flex items-center gap-1 transition bg-blue-600 text-white border-blue-600 shadow-xs whitespace-nowrap text-[11px]';
      } else {
        btn.className = 'px-2 py-1 rounded-lg border border-slate-200 text-slate-600 bg-white hover:bg-slate-100 flex items-center gap-1 transition whitespace-nowrap text-[11px]';
      }
    }
  });
}

// Start Fresh Conversation Session
async function startNewSession() {
  try {
    const res = await fetch('/api/sessions/new', { method: 'POST' });
    if (res.ok) {
      const data = await res.json();
      currentSessionId = data.session_id || `sess_${Date.now().toString(16)}`;
    } else {
      currentSessionId = `sess_${Date.now().toString(16)}`;
    }
  } catch (e) {
    currentSessionId = `sess_${Date.now().toString(16)}`;
  }

  document.getElementById('session-id-label').innerText = currentSessionId;
  
  // Reset chat messages to welcome message
  const chatMessages = document.getElementById('chat-messages');
  chatMessages.innerHTML = `
    <div class="flex items-start gap-3 chat-bubble">
      <div class="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center text-white flex-shrink-0 shadow-xs">
        <i data-lucide="bot" class="w-4 h-4"></i>
      </div>
      <div class="bg-slate-100 p-4 rounded-2xl rounded-tl-none max-w-[90%] text-sm text-slate-800 shadow-xs leading-relaxed">
        <div class="flex items-center gap-2 mb-1.5">
          <span class="font-bold text-slate-900">Cymbal Retail Operations Coordinator</span>
          <span class="text-[10px] bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded font-mono font-medium">New Session</span>
        </div>
        <p class="mb-2 text-slate-700">
          Started a new clean session (<code>#${currentSessionId}</code>). Session memory and temporal cashier caches have been initialized.
        </p>
      </div>
    </div>
  `;

  // Reset Right Pane
  document.getElementById('sql-code-display').innerText = '-- Generated GoogleSQL queries will appear here automatically.';
  document.getElementById('sop-content-display').innerText = 'No active hardware SOP loaded.';
  document.getElementById('sop-error-badge').innerText = 'No active SOP';
  document.getElementById('gcs-links-container').innerHTML = '<div class="text-xs text-slate-400 italic">No manual links in current session context.</div>';
  document.getElementById('trace-json-container').innerHTML = '<div class="text-slate-400 p-3 bg-slate-900 rounded-lg text-slate-300">Awaiting multi-agent tool execution trace...</div>';
  document.getElementById('dispatch-protocol-badge').innerText = 'IDLE';
  document.getElementById('status-chip').innerText = 'Idle';

  const chartContainer = document.getElementById('plotly-container');
  if (chartContainer) {
    chartContainer.innerHTML = 'Run an operational query (e.g. Stockout Risk or Cashier Baseline) to render interactive metrics.';
  }

  if (window.lucide) {
    window.lucide.createIcons();
  }
}

// Starter Prompt Handler
function useStarter(idx) {
  if (SCENARIOS[idx]) {
    const input = document.getElementById('prompt-input');
    input.value = SCENARIOS[idx].prompt;
    autoResize(input);
    if (currentView !== 'chat') {
      switchView('chat');
    }
    submitMessage();
  }
}

// Auto-expand textarea
function autoResize(el) {
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 128) + 'px';
}

function handleKeyDown(event) {
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault();
    submitMessage();
  }
}

// Send Message Flow
async function submitMessage() {
  const input = document.getElementById('prompt-input');
  const text = input.value.trim();
  if (!text || isProcessing) return;

  isProcessing = true;
  input.value = '';
  input.style.height = 'auto';
  
  const sendBtn = document.getElementById('send-btn');
  const sendIcon = document.getElementById('send-icon');
  sendBtn.disabled = true;
  document.getElementById('status-chip').innerText = 'Executing...';

  // Append user bubble
  appendUserMessage(text);

  // Append loading bubble
  const loadingBubbleId = 'loading-' + Date.now();
  appendLoadingBubble(loadingBubbleId);

  try {
    const response = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: text,
        session_id: currentSessionId,
        agent_override: agentFilter
      })
    });

    const data = await response.json();
    removeElement(loadingBubbleId);

    if (response.ok) {
      appendAssistantResponse(data);
      updateRightPane(data);
      document.getElementById('status-chip').innerText = `${data.latency_ms || 0}ms`;
    } else {
      appendErrorMessage(data.detail || 'An unexpected error occurred while communicating with the agent.');
      document.getElementById('status-chip').innerText = 'Error';
    }
  } catch (err) {
    removeElement(loadingBubbleId);
    appendErrorMessage('Network or server connection error: ' + err.message);
    document.getElementById('status-chip').innerText = 'Error';
  } finally {
    isProcessing = false;
    sendBtn.disabled = false;
    if (window.lucide) {
      window.lucide.createIcons();
    }
  }
}

function appendUserMessage(text) {
  const chat = document.getElementById('chat-messages');
  const div = document.createElement('div');
  div.className = 'flex items-start justify-end gap-3 chat-bubble';
  div.innerHTML = `
    <div class="bg-blue-600 text-white p-3.5 rounded-2xl rounded-tr-none max-w-[85%] text-sm shadow-xs leading-relaxed">
      ${escapeHtml(text)}
    </div>
    <div class="w-8 h-8 rounded-lg bg-slate-200 text-slate-700 flex items-center justify-center flex-shrink-0 text-xs font-bold">
      You
    </div>
  `;
  chat.appendChild(div);
  chat.scrollTop = chat.scrollHeight;
}

function appendLoadingBubble(id) {
  const chat = document.getElementById('chat-messages');
  const div = document.createElement('div');
  div.id = id;
  div.className = 'flex items-start gap-3 chat-bubble';
  div.innerHTML = `
    <div class="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center text-white flex-shrink-0 shadow-xs">
      <i data-lucide="bot" class="w-4 h-4"></i>
    </div>
    <div class="bg-slate-100 p-4 rounded-2xl rounded-tl-none text-sm text-slate-600 shadow-xs flex items-center gap-2">
      <div class="w-2 h-2 rounded-full bg-blue-600 animate-bounce"></div>
      <div class="w-2 h-2 rounded-full bg-blue-600 animate-bounce [animation-delay:-.15s]"></div>
      <div class="w-2 h-2 rounded-full bg-blue-600 animate-bounce [animation-delay:-.3s]"></div>
      <span class="text-xs text-slate-500 font-mono ml-1">Orchestrating tools across BQ & Bigtable...</span>
    </div>
  `;
  chat.appendChild(div);
  chat.scrollTop = chat.scrollHeight;
  if (window.lucide) window.lucide.createIcons();
}

function appendAssistantResponse(data) {
  const chat = document.getElementById('chat-messages');
  const div = document.createElement('div');
  div.className = 'flex items-start gap-3 chat-bubble';

  const renderedMarkdown = window.marked ? window.marked.parse(data.final_text || '') : escapeHtml(data.final_text || '');
  
  // Format tool execution pills
  const toolCalls = data.tool_calls || [];
  let toolsHtml = '';
  if (toolCalls.length > 0) {
    toolsHtml = `
      <div class="mt-3 pt-2.5 border-t border-slate-200/80 flex flex-wrap items-center gap-1.5 text-[10.5px] font-mono">
        <span class="text-slate-400 font-sans font-medium mr-1">Executed Gateways:</span>
        ${toolCalls.map(tc => `
          <span class="px-2 py-0.5 rounded bg-white border border-slate-300 text-slate-700 flex items-center gap-1 shadow-2xs">
            <i data-lucide="check" class="w-3 h-3 text-emerald-600"></i> ${tc.name}
          </span>
        `).join('')}
        <span class="ml-auto px-1.5 py-0.5 rounded bg-slate-200 text-slate-700 text-[10px] font-bold">
          ${data.dispatch_mode || 'STANDARD'}
        </span>
      </div>
    `;
  }

  div.innerHTML = `
    <div class="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center text-white flex-shrink-0 shadow-xs">
      <i data-lucide="bot" class="w-4 h-4"></i>
    </div>
    <div class="bg-slate-100 p-4 rounded-2xl rounded-tl-none max-w-[90%] text-sm text-slate-800 shadow-xs leading-relaxed">
      <div class="flex items-center gap-2 mb-2">
        <span class="font-bold text-slate-900 text-xs">Cymbal Operations Agent</span>
        <span class="text-[10px] px-1.5 py-0.2 rounded bg-emerald-100 text-emerald-800 font-mono">Grounded</span>
        <span class="text-[10px] text-slate-400 font-mono ml-auto">${data.latency_ms || 0}ms</span>
      </div>
      <div class="prose prose-sm text-slate-800 max-w-none text-xs leading-relaxed">
        ${renderedMarkdown}
      </div>
      ${toolsHtml}
    </div>
  `;
  chat.appendChild(div);
  chat.scrollTop = chat.scrollHeight;
}

function appendErrorMessage(msg) {
  const chat = document.getElementById('chat-messages');
  const div = document.createElement('div');
  div.className = 'flex items-start gap-3 chat-bubble';
  div.innerHTML = `
    <div class="w-8 h-8 rounded-lg bg-rose-600 flex items-center justify-center text-white flex-shrink-0 shadow-xs">
      <i data-lucide="alert-triangle" class="w-4 h-4"></i>
    </div>
    <div class="bg-rose-50 border border-rose-200 p-4 rounded-2xl rounded-tl-none max-w-[90%] text-sm text-rose-800 shadow-xs">
      <div class="font-bold text-xs mb-1 text-rose-900">Execution Error</div>
      <p class="text-xs leading-relaxed">${escapeHtml(msg)}</p>
    </div>
  `;
  chat.appendChild(div);
  chat.scrollTop = chat.scrollHeight;
}

function removeElement(id) {
  const el = document.getElementById(id);
  if (el) el.remove();
}

// Update Right Pane Tabs Content
function updateRightPane(data) {
  // 1. GoogleSQL Tab
  const sqlEl = document.getElementById('sql-code-display');
  if (data.generated_sql && data.generated_sql.trim()) {
    sqlEl.innerText = data.generated_sql.trim();
  } else {
    const hasBt = (data.tool_calls || []).some(c => c.name === 'read_cashier_realtime_alerts');
    if (hasBt) {
      sqlEl.innerText = `-- Cloud Bigtable Direct Key-Value Lookup (Sub-second RPC)\n-- Table: operations-db:cashier_realtime_alerts\n-- Row Key Prefix: STORE_048#CASH_1190\n-- Columns Retrieved: [hourly_discount_usd, anomaly_risk_score, last_event_ts]`;
    } else {
      sqlEl.innerText = '-- No relational GoogleSQL executed for this inquiry (resolved via vector RAG or local cache).';
    }
  }

  // 2. Visualizer Tab (Plotly)
  const chartContainer = document.getElementById('plotly-container');
  if (data.plotly_spec && data.plotly_spec.data) {
    if (window.Plotly) {
      chartContainer.innerHTML = '';
      window.Plotly.newPlot('plotly-container', data.plotly_spec.data, data.plotly_spec.layout, {
        responsive: true,
        displayModeBar: false
      });
      document.getElementById('chart-type-badge').innerText = data.plotly_spec.title || 'Live Metric';
      if (data.dispatch_mode === 'PARALLEL_DISPATCH' || (data.message && data.message.includes('stockout'))) {
        switchRightTab('chart');
      }
    }
  }

  // 3. Hardware SOP Tab
  if (data.sop_data) {
    document.getElementById('sop-content-display').innerText = data.sop_data;
    document.getElementById('sop-error-badge').innerText = data.sop_error_code || 'SOP Loaded';
    if (data.dispatch_mode === 'SINGLE_TOOL_RAG') {
      switchRightTab('sop');
    }
  }

  // GCS Manual Links
  const linksContainer = document.getElementById('gcs-links-container');
  if (data.gcs_links && data.gcs_links.length > 0) {
    linksContainer.innerHTML = data.gcs_links.map(link => `
      <a href="${link}" target="_blank" class="flex items-center justify-between p-2.5 rounded-lg bg-blue-50/60 border border-blue-200 text-blue-700 hover:bg-blue-100 transition text-xs font-semibold">
        <span class="flex items-center gap-1.5 truncate">
          <i data-lucide="file-text" class="w-3.5 h-3.5 flex-shrink-0"></i>
          <span class="truncate">${link.split('/').pop()}</span>
        </span>
        <i data-lucide="external-link" class="w-3.5 h-3.5 flex-shrink-0 ml-2"></i>
      </a>
    `).join('');
  } else {
    linksContainer.innerHTML = '<div class="text-xs text-slate-400 italic">No manual links in current session context.</div>';
  }

  // 4. ADK Tool Trace Tab
  document.getElementById('dispatch-protocol-badge').innerText = data.dispatch_mode || 'STANDARD';
  const traceContainer = document.getElementById('trace-json-container');
  const traceData = {
    turn_index: data.turn_index || 1,
    dispatch_mode: data.dispatch_mode,
    tool_calls: data.tool_calls || [],
    tool_responses: data.tool_responses || [],
    latency_ms: data.latency_ms
  };
  traceContainer.innerHTML = `
    <pre class="bg-slate-900 text-emerald-400 p-3 rounded-lg overflow-x-auto text-[11px] leading-relaxed custom-scrollbar max-h-96">${escapeHtml(JSON.stringify(traceData, null, 2))}</pre>
  `;
}

function copySQL() {
  const sql = document.getElementById('sql-code-display').innerText;
  navigator.clipboard.writeText(sql).then(() => {
    const btn = document.getElementById('copy-sql-btn');
    btn.innerHTML = '<i data-lucide="check" class="w-3.5 h-3.5 text-emerald-600"></i> <span class="text-emerald-600 font-semibold">Copied!</span>';
    if (window.lucide) window.lucide.createIcons();
    setTimeout(() => {
      btn.innerHTML = '<i data-lucide="copy" class="w-3.5 h-3.5"></i> <span>Copy SQL</span>';
      if (window.lucide) window.lucide.createIcons();
    }, 2000);
  });
}

// Render Operational Test Matrix Table
function renderMatrixTable() {
  const tbody = document.getElementById('matrix-table-body');
  if (!tbody) return;

  tbody.innerHTML = SCENARIOS.map((sc, idx) => `
    <tr class="hover:bg-slate-50 transition" id="matrix-row-${idx}">
      <td class="p-3 font-mono font-bold text-slate-900 text-xs">${sc.id}</td>
      <td class="p-3 font-medium text-slate-800 text-xs">${sc.name}</td>
      <td class="p-3 text-xs text-slate-600">
        <span class="px-2 py-0.5 rounded bg-slate-100 border border-slate-200 font-mono text-[10px]">${sc.dispatch}</span>
      </td>
      <td class="p-3 text-xs text-slate-500 max-w-md">${escapeHtml(sc.prompt)}</td>
      <td class="p-3 text-center" id="matrix-status-${idx}">
        <span class="px-2 py-0.5 rounded-full bg-slate-100 text-slate-600 border border-slate-200 text-[10px] font-mono font-medium">READY</span>
      </td>
      <td class="p-3 text-right">
        <button onclick="runSingleMatrixTest(${idx})" id="matrix-btn-${idx}" class="px-2.5 py-1 rounded bg-blue-50 text-blue-700 hover:bg-blue-100 border border-blue-200 text-[11px] font-semibold transition">
          Run
        </button>
      </td>
    </tr>
  `).join('');
  if (window.lucide) window.lucide.createIcons();
}

async function runSingleMatrixTest(idx) {
  const sc = SCENARIOS[idx];
  const statusEl = document.getElementById(`matrix-status-${idx}`);
  const btnEl = document.getElementById(`matrix-btn-${idx}`);

  statusEl.innerHTML = '<span class="px-2 py-0.5 rounded-full bg-amber-50 text-amber-700 border border-amber-200 text-[10px] font-mono font-medium animate-pulse">RUNNING</span>';
  btnEl.disabled = true;

  try {
    const res = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: sc.prompt,
        session_id: `test_${sc.id.replace(/[^a-zA-Z0-9]/g, '')}`,
        agent_override: 'auto'
      })
    });
    const data = await res.json();
    const passed = sc.validator(data);

    if (passed) {
      statusEl.innerHTML = '<span class="px-2 py-0.5 rounded-full bg-emerald-50 text-emerald-700 border border-emerald-200 text-[10px] font-mono font-bold">PASS</span>';
    } else {
      statusEl.innerHTML = '<span class="px-2 py-0.5 rounded-full bg-rose-50 text-rose-700 border border-rose-200 text-[10px] font-mono font-bold">FAIL</span>';
    }
  } catch (e) {
    statusEl.innerHTML = '<span class="px-2 py-0.5 rounded-full bg-rose-50 text-rose-700 border border-rose-200 text-[10px] font-mono font-bold">ERROR</span>';
  } finally {
    btnEl.disabled = false;
  }
}

async function runAllMatrixTests() {
  const btn = document.getElementById('run-all-tests-btn');
  btn.disabled = true;
  btn.innerHTML = '<i data-lucide="loader-2" class="w-3.5 h-3.5 animate-spin"></i> Running Matrix...';
  if (window.lucide) window.lucide.createIcons();

  for (let i = 0; i < SCENARIOS.length; i++) {
    await runSingleMatrixTest(i);
  }

  btn.disabled = false;
  btn.innerHTML = '<i data-lucide="play" class="w-3.5 h-3.5"></i> Run All 7 Scenarios';
  if (window.lucide) window.lucide.createIcons();
}

function escapeHtml(text) {
  if (!text) return '';
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}
