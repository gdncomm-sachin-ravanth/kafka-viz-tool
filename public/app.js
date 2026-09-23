const state = {
  config: { kafkaHome: '', environments: [] },
  currentEnv: null,
  topics: [],
  selectedTopic: null,
  messages: [], // loaded batch for the selected topic (accumulates as older pages load)
  nextCursor: null, // per-partition offset to resume from for "Load older messages"
  hasMore: false,
  selectedMessageIndex: null,
  selectedMessageValue: null, // raw (pretty-printed if JSON) text of the currently viewed message
  detailsOpen: false,
  detailsLoadedForTopic: null
};

const el = (id) => document.getElementById(id);

// Kafka's own convention: internal topics (__consumer_offsets,
// __transaction_state, etc.) are prefixed with a double underscore. The
// server enforces the same rule on purge/delete - this just drives the UI.
const isInternalTopic = (topic) => topic.startsWith('__');

// ---------- theme ----------

const THEME_KEY = 'kvt-theme';

function getStoredTheme() {
  try {
    return localStorage.getItem(THEME_KEY);
  } catch (e) {
    return null;
  }
}

function currentTheme() {
  const stored = getStoredTheme();
  if (stored === 'light' || stored === 'dark') return stored;
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  el('icon-sun').hidden = theme !== 'dark';
  el('icon-moon').hidden = theme !== 'light';
  el('theme-toggle').title = theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme';
}

applyTheme(currentTheme());

el('theme-toggle').addEventListener('click', () => {
  const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  try {
    localStorage.setItem(THEME_KEY, next);
  } catch (e) {}
  applyTheme(next);
});

// ---------- topic pane resize ----------

const TOPIC_PANE_WIDTH_KEY = 'kvt-topic-pane-width';
const TOPIC_PANE_MIN_WIDTH = 260;
const TOPIC_PANE_MAX_WIDTH = 640;

(function initTopicPaneWidth() {
  let stored;
  try {
    stored = Number(localStorage.getItem(TOPIC_PANE_WIDTH_KEY));
  } catch (e) {}
  if (stored && stored >= TOPIC_PANE_MIN_WIDTH && stored <= TOPIC_PANE_MAX_WIDTH) {
    el('topic-pane').style.width = `${stored}px`;
  }
})();

(function setupTopicPaneResizer() {
  const resizer = el('topic-pane-resizer');
  const pane = el('topic-pane');

  resizer.addEventListener('mousedown', (e) => {
    e.preventDefault();
    resizer.classList.add('dragging');
    document.body.classList.add('resizing-pane');
    const startX = e.clientX;
    const startWidth = pane.getBoundingClientRect().width;

    function onMouseMove(ev) {
      const width = Math.min(
        TOPIC_PANE_MAX_WIDTH,
        Math.max(TOPIC_PANE_MIN_WIDTH, startWidth + (ev.clientX - startX))
      );
      pane.style.width = `${width}px`;
    }

    function onMouseUp() {
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      resizer.classList.remove('dragging');
      document.body.classList.remove('resizing-pane');
      try {
        localStorage.setItem(TOPIC_PANE_WIDTH_KEY, String(Math.round(pane.getBoundingClientRect().width)));
      } catch (e) {}
    }

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  });
})();

// ---------- toast ----------

let toastTimer = null;
function toast(msg, isErr = false) {
  const t = el('toast');
  t.textContent = msg;
  t.className = 'toast show' + (isErr ? ' err' : '');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { t.className = 'toast'; }, 3500);
}

async function api(path, options) {
  const res = await fetch(path, options);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `Request failed (${res.status})`);
  return data;
}

// ---------- tabs ----------

function switchToPage(pageName) {
  document.querySelectorAll('.tab').forEach((t) => t.classList.toggle('active', t.dataset.page === pageName));
  document.querySelectorAll('.page').forEach((p) => p.classList.remove('active'));
  el(`page-${pageName}`).classList.add('active');
}

document.querySelectorAll('.tab').forEach((tab) => {
  tab.addEventListener('click', () => switchToPage(tab.dataset.page));
});

// ---------- config / environments ----------

async function loadConfig() {
  state.config = await api('/api/config');
  if (!state.currentEnv && state.config.environments.length) {
    state.currentEnv = state.config.environments[0].name;
  }
  renderEnvSelect();
  renderEnvList();
  el('kafka-home-input').value = state.config.kafkaHome;
}

function renderEnvSelect() {
  const sel = el('env-select');
  sel.innerHTML = '';
  state.config.environments.forEach((e) => {
    const opt = document.createElement('option');
    opt.value = e.name;
    opt.textContent = e.name;
    if (e.name === state.currentEnv) opt.selected = true;
    sel.appendChild(opt);
  });
}

function renderEnvList() {
  const list = el('env-list');
  list.innerHTML = '';
  state.config.environments.forEach((e) => {
    const li = document.createElement('li');
    li.innerHTML = `
      <div>
        <div class="env-name">${escapeHtml(e.name)}</div>
        <div class="env-servers">${escapeHtml(e.bootstrapServers)}</div>
      </div>
      <button class="env-delete" data-name="${escapeHtml(e.name)}">Remove</button>
    `;
    list.appendChild(li);
  });
  list.querySelectorAll('.env-delete').forEach((btn) => {
    btn.addEventListener('click', async () => {
      await api(`/api/environments/${encodeURIComponent(btn.dataset.name)}`, { method: 'DELETE' });
      await loadConfig();
      toast(`Removed environment ${btn.dataset.name}`);
    });
  });
}

el('env-select').addEventListener('change', (e) => {
  state.currentEnv = e.target.value;
  state.selectedTopic = null;
  loadTopics();
  resetMessagePane();
});

el('open-settings').addEventListener('click', () => el('settings-modal').classList.add('open'));
el('close-settings').addEventListener('click', () => el('settings-modal').classList.remove('open'));
el('settings-modal').addEventListener('click', (e) => {
  if (e.target.id === 'settings-modal') el('settings-modal').classList.remove('open');
});

el('save-kafka-home').addEventListener('click', async () => {
  try {
    await api('/api/config/kafka-home', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ kafkaHome: el('kafka-home-input').value.trim() })
    });
    toast('Kafka home saved');
  } catch (err) {
    toast(err.message, true);
  }
});

el('add-env').addEventListener('click', async () => {
  const name = el('new-env-name').value.trim();
  const servers = el('new-env-servers').value.trim();
  if (!name || !servers) return toast('Name and bootstrap servers are required', true);
  try {
    await api('/api/environments', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, bootstrapServers: servers })
    });
    el('new-env-name').value = '';
    el('new-env-servers').value = '';
    await loadConfig();
    toast(`Added environment ${name}`);
  } catch (err) {
    toast(err.message, true);
  }
});

// ---------- topics ----------

async function loadTopics() {
  if (!state.currentEnv) return;
  const list = el('topic-list');
  list.innerHTML = '<li class="muted" style="cursor:default">Loading…</li>';
  try {
    const data = await api(`/api/topics?env=${encodeURIComponent(state.currentEnv)}`);
    state.topics = data.topics;
    renderTopicList();
  } catch (err) {
    list.innerHTML = '';
    toast(err.message, true);
  }
}

function renderTopicList() {
  const filter = el('topic-filter').value.toLowerCase();
  const list = el('topic-list');
  list.innerHTML = '';
  state.topics
    .filter((t) => t.toLowerCase().includes(filter))
    .forEach((t) => {
      const li = document.createElement('li');
      li.title = t;
      if (t === state.selectedTopic) li.classList.add('selected');
      if (isInternalTopic(t)) {
        li.classList.add('internal-topic');
        li.innerHTML = `<span class="internal-badge" title="Kafka internal topic - purge/delete disabled">internal</span>${escapeHtml(t)}`;
      } else {
        li.textContent = t;
      }
      li.addEventListener('click', () => selectTopic(t));
      list.appendChild(li);
    });
}

el('topic-filter').addEventListener('input', renderTopicList);
el('refresh-topics').addEventListener('click', loadTopics);
el('refresh-messages').addEventListener('click', () => selectTopic(state.selectedTopic));

el('load-older-btn').addEventListener('click', async () => {
  const topic = state.selectedTopic;
  if (!topic || !state.nextCursor) return;

  const btn = el('load-older-btn');
  btn.disabled = true;
  btn.textContent = 'Loading…';

  try {
    const query = new URLSearchParams({
      env: state.currentEnv,
      topic,
      limit: '50',
      cursor: JSON.stringify(state.nextCursor)
    });
    const data = await api(`/api/messages?${query.toString()}`);
    if (state.selectedTopic !== topic) return;

    // Cursor pages are disjoint and strictly older than what's loaded, so
    // this is a plain append, not a merge-and-dedupe.
    state.messages = state.messages.concat(data.messages).sort((a, b) => b.timestamp - a.timestamp);
    state.nextCursor = data.nextCursor;
    state.hasMore = data.hasMore;
    renderMessages();
    populatePartitionFilter();
    applyFilters();
    const oldest = state.messages[state.messages.length - 1];
    el('loaded-until').textContent = oldest
      ? `Showing ${state.messages.length} messages, loaded back to ${new Date(oldest.timestamp).toLocaleString()}`
      : 'No messages found on this topic yet';
  } catch (err) {
    toast(err.message, true);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Load older messages';
    btn.hidden = !state.hasMore;
  }
});

// ---------- new topic ----------

function openNewTopicModal() {
  el('new-topic-name').value = '';
  el('new-topic-partitions').value = '1';
  el('new-topic-replication').value = '1';
  el('new-topic-error').textContent = '';
  el('new-topic-modal').classList.add('open');
  el('new-topic-name').focus();
}

function closeNewTopicModal() {
  el('new-topic-modal').classList.remove('open');
}

el('new-topic-btn').addEventListener('click', openNewTopicModal);
el('close-new-topic').addEventListener('click', closeNewTopicModal);
el('cancel-new-topic').addEventListener('click', closeNewTopicModal);
el('new-topic-modal').addEventListener('click', (e) => {
  if (e.target.id === 'new-topic-modal') closeNewTopicModal();
});

el('create-new-topic').addEventListener('click', async () => {
  const topic = el('new-topic-name').value.trim();
  const partitions = el('new-topic-partitions').value;
  const replicationFactor = el('new-topic-replication').value;
  const errorEl = el('new-topic-error');
  errorEl.textContent = '';

  errorEl.style.color = 'var(--danger)';
  if (!topic) return errorEl.textContent = 'Topic name is required';
  if (!state.currentEnv) return errorEl.textContent = 'Select an environment first';

  const btn = el('create-new-topic');
  btn.disabled = true;
  btn.textContent = 'Creating…';
  try {
    await api('/api/topics', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ env: state.currentEnv, topic, partitions, replicationFactor })
    });
    toast(`Created topic ${topic} (${partitions} partition(s))`);
    closeNewTopicModal();
    await loadTopics();
    selectTopic(topic);
  } catch (err) {
    errorEl.textContent = err.message;
  } finally {
    btn.disabled = false;
    btn.textContent = 'Create';
  }
});

const FILTER_IDS = ['message-search', 'filter-partition', 'filter-key', 'filter-date-from', 'filter-date-to', 'clear-filters'];

function setFiltersEnabled(enabled) {
  FILTER_IDS.forEach((id) => { el(id).disabled = !enabled; });
}

function resetFilters() {
  el('message-search').value = '';
  el('filter-partition').innerHTML = '<option value="">All partitions</option>';
  el('filter-key').value = '';
  el('filter-date-from').value = '';
  el('filter-date-to').value = '';
}

function resetMessagePane() {
  state.selectedTopic = null;
  state.messages = [];
  state.nextCursor = null;
  state.hasMore = false;
  state.selectedMessageIndex = null;
  state.selectedMessageValue = null;
  el('current-topic-name').textContent = 'Select a topic';
  el('loaded-until').textContent = '';
  el('message-stream').innerHTML = '<p class="empty-hint">Pick a topic on the left to load its most recent messages.</p>';
  el('message-detail').innerHTML = '<p class="empty-hint">Select a message to see its full payload.</p>';
  el('detail-search').value = '';
  el('detail-search').disabled = true;
  el('copy-message-btn').disabled = true;
  resetFilters();
  setFiltersEnabled(false);
  el('refresh-messages').disabled = true;
  el('view-details-btn').disabled = true;
  el('publish-to-topic-btn').disabled = true;
  el('purge-topic-btn').disabled = true;
  el('delete-topic-btn').disabled = true;
  el('load-older-btn').hidden = true;
}

async function selectTopic(topic) {
  state.selectedTopic = topic;
  state.selectedMessageIndex = null;
  state.selectedMessageValue = null;
  renderTopicList();
  el('current-topic-name').textContent = topic;
  el('message-detail').innerHTML = '<p class="empty-hint">Select a message to see its full payload.</p>';
  el('detail-search').value = '';
  el('detail-search').disabled = true;
  el('copy-message-btn').disabled = true;
  resetFilters();
  setFiltersEnabled(false);
  el('view-details-btn').disabled = true;
  el('publish-to-topic-btn').disabled = true;
  el('purge-topic-btn').disabled = true;
  el('delete-topic-btn').disabled = true;

  await loadMessages(topic);
}

// Purge/delete stay disabled for internal topics (__consumer_offsets etc.)
// even once everything else has finished loading - the server rejects
// those requests anyway, so this just keeps the buttons honest.
function setDestructiveButtonsEnabled(topic) {
  const allowed = !isInternalTopic(topic);
  el('purge-topic-btn').disabled = !allowed;
  el('delete-topic-btn').disabled = !allowed;
  const reason = allowed ? '' : 'Internal Kafka topics can’t be purged or deleted here.';
  el('purge-topic-btn').title = reason;
  el('delete-topic-btn').title = reason;
}

// Loads (replacing whatever's currently shown) the newest messages for a
// topic. Used for the initial load and Reload.
async function loadMessages(topic) {
  el('refresh-messages').disabled = true;
  el('load-older-btn').hidden = true;
  el('loaded-until').textContent = 'Loading recent messages…';
  el('message-stream').innerHTML = '<div class="loading-hint"><span class="spinner"></span> Loading recent messages…</div>';

  const query = new URLSearchParams({ env: state.currentEnv, topic, limit: '50' });

  try {
    const data = await api(`/api/messages?${query.toString()}`);
    // A reset (env switch, topic reselect, delete) may have happened while
    // this request was in flight - don't let a stale response re-enable
    // controls for a topic that's no longer selected.
    if (state.selectedTopic !== topic) return;

    state.messages = data.messages;
    state.nextCursor = data.nextCursor;
    state.hasMore = data.hasMore;
    renderMessages();
    populatePartitionFilter();
    el('loaded-until').textContent = data.loadedUntil
      ? `Showing ${data.messages.length} messages, loaded back to ${new Date(data.loadedUntil).toLocaleString()}`
      : 'No messages found on this topic yet';
    setFiltersEnabled(true);
    el('refresh-messages').disabled = false;
    el('view-details-btn').disabled = false;
    el('publish-to-topic-btn').disabled = false;
    setDestructiveButtonsEnabled(topic);
    el('load-older-btn').hidden = !state.hasMore;
  } catch (err) {
    if (state.selectedTopic !== topic) return;

    el('loaded-until').textContent = '';
    el('message-stream').innerHTML = `<p class="empty-hint">${escapeHtml(err.message)}</p>`;
    // details/publish/purge/delete can still work even if message load failed
    el('view-details-btn').disabled = false;
    el('publish-to-topic-btn').disabled = false;
    setDestructiveButtonsEnabled(topic);
    toast(err.message, true);
  }
}

function renderMessages() {
  const stream = el('message-stream');
  stream.innerHTML = '';
  if (!state.messages.length) {
    stream.innerHTML = '<p class="empty-hint">No messages found on this topic yet.</p>';
    return;
  }
  state.messages.forEach((m, idx) => {
    const row = document.createElement('div');
    row.className = 'msg-row';
    row.dataset.searchBlob = `${(m.key || '')} ${m.value}`.toLowerCase();
    row.dataset.partition = m.partition;
    row.dataset.key = (m.key || '').toLowerCase();
    row.dataset.timestamp = m.timestamp;
    if (idx === state.selectedMessageIndex) row.classList.add('selected');
    const time = new Date(m.timestamp).toLocaleString();
    row.innerHTML = `
      <div class="msg-meta">
        <span>${time}</span>
        <span>p${m.partition} · o${m.offset}</span>
        ${m.key ? `<span class="msg-key">${escapeHtml(m.key)}</span>` : ''}
      </div>
      <div class="msg-preview">${escapeHtml(m.value)}</div>
    `;
    row.addEventListener('click', () => selectMessage(idx));
    stream.appendChild(row);
  });
}

function selectMessage(idx) {
  state.selectedMessageIndex = idx;
  document.querySelectorAll('.msg-row').forEach((row, i) => {
    row.classList.toggle('selected', i === idx);
  });
  const m = state.messages[idx];
  const detail = el('message-detail');
  el('detail-search').value = '';
  if (!m) {
    state.selectedMessageValue = null;
    detail.innerHTML = '<p class="empty-hint">Select a message to see its full payload.</p>';
    el('detail-search').disabled = true;
    el('copy-message-btn').disabled = true;
    return;
  }
  state.selectedMessageValue = prettyIfJson(m.value);
  el('detail-search').disabled = false;
  el('copy-message-btn').disabled = false;
  renderMessageDetail(m);
}

// Re-renders just the payload body, highlighting any text that matches the
// detail-search box - the meta line (timestamp/partition/key) stays as-is.
function renderMessageDetail(m) {
  const detail = el('message-detail');
  detail.innerHTML = `
    <div class="detail-meta">
      <span>${new Date(m.timestamp).toLocaleString()}</span>
      <span>partition ${m.partition}</span>
      <span>offset ${m.offset}</span>
      ${m.key ? `<span class="msg-key">key: ${escapeHtml(m.key)}</span>` : '<span>no key</span>'}
    </div>
    <div class="msg-value">${highlightMatches(state.selectedMessageValue, el('detail-search').value)}</div>
  `;
}

function highlightMatches(text, query) {
  const escaped = escapeHtml(text);
  const q = query.trim();
  if (!q) return escaped;
  const pattern = q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); // escape regex metacharacters
  return escaped.replace(new RegExp(pattern, 'gi'), (hit) => `<mark class="search-hit">${hit}</mark>`);
}

el('detail-search').addEventListener('input', () => {
  const idx = state.selectedMessageIndex;
  const m = idx === null ? null : state.messages[idx];
  if (m) renderMessageDetail(m);
});

// Falls back to the legacy execCommand approach for contexts where the
// async Clipboard API is unavailable or blocked (e.g. no user-gesture
// context, or a browser that doesn't grant clipboard-write by default).
function legacyCopyToClipboard(text) {
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.style.position = 'fixed';
  textarea.style.opacity = '0';
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  let ok = false;
  try {
    ok = document.execCommand('copy');
  } catch {
    ok = false;
  }
  document.body.removeChild(textarea);
  return ok;
}

el('copy-message-btn').addEventListener('click', async () => {
  if (state.selectedMessageValue === null) return;
  try {
    await navigator.clipboard.writeText(state.selectedMessageValue);
    toast('Copied message to clipboard');
  } catch {
    if (legacyCopyToClipboard(state.selectedMessageValue)) {
      toast('Copied message to clipboard');
    } else {
      toast('Could not copy to clipboard', true);
    }
  }
});

function prettyIfJson(value) {
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
}

function populatePartitionFilter() {
  const sel = el('filter-partition');
  const current = sel.value;
  const partitions = Array.from(new Set(state.messages.map((m) => m.partition))).sort((a, b) => a - b);
  sel.innerHTML = '<option value="">All partitions</option>' +
    partitions.map((p) => `<option value="${p}">Partition ${p}</option>`).join('');
  if (partitions.some((p) => String(p) === current)) sel.value = current;
}

function applyFilters() {
  const q = el('message-search').value.toLowerCase();
  const partition = el('filter-partition').value;
  const keyFilter = el('filter-key').value.toLowerCase();
  // Date-only inputs ("2026-09-16") - anchor to local midnight rather than
  // UTC, and treat "to" as the end of that day so it's inclusive.
  const fromDate = el('filter-date-from').value;
  const toDate = el('filter-date-to').value;
  const fromMs = fromDate ? new Date(`${fromDate}T00:00:00`).getTime() : null;
  const toMs = toDate ? new Date(`${toDate}T23:59:59.999`).getTime() : null;

  document.querySelectorAll('.msg-row').forEach((row) => {
    const ts = Number(row.dataset.timestamp);
    let visible = true;
    if (q && !row.dataset.searchBlob.includes(q)) visible = false;
    if (visible && partition && row.dataset.partition !== partition) visible = false;
    if (visible && keyFilter && !row.dataset.key.includes(keyFilter)) visible = false;
    if (visible && fromMs !== null && ts < fromMs) visible = false;
    if (visible && toMs !== null && ts > toMs) visible = false;
    row.classList.toggle('hidden', !visible);
  });
}

['message-search', 'filter-key'].forEach((id) => el(id).addEventListener('input', applyFilters));
['filter-partition', 'filter-date-from', 'filter-date-to'].forEach((id) => el(id).addEventListener('change', applyFilters));

el('clear-filters').addEventListener('click', () => {
  resetFilters();
  populatePartitionFilter();
  applyFilters();
});

// ---------- topic delete / purge ----------

// Opens a modal requiring the user to type the topic name back exactly,
// resolving true/false. The modal overlay itself already blocks interaction
// with the rest of the page while it's open.
function confirmByTypingTopicName(topic, title, message) {
  return new Promise((resolve) => {
    const modal = el('confirm-name-modal');
    const input = el('confirm-name-input');
    const confirmBtn = el('confirm-name-confirm');
    const cancelBtn = el('confirm-name-cancel');
    const closeBtn = el('confirm-name-close');

    el('confirm-name-title').textContent = title;
    el('confirm-name-message').textContent = message;
    input.value = '';
    confirmBtn.disabled = true;
    modal.classList.add('open');
    input.focus();

    function cleanup(result) {
      modal.classList.remove('open');
      input.removeEventListener('input', onInput);
      input.removeEventListener('keydown', onKeydown);
      confirmBtn.removeEventListener('click', onConfirm);
      cancelBtn.removeEventListener('click', onCancel);
      closeBtn.removeEventListener('click', onCancel);
      modal.removeEventListener('click', onOverlayClick);
      resolve(result);
    }

    function onInput() { confirmBtn.disabled = input.value !== topic; }
    function onConfirm() { if (input.value === topic) cleanup(true); }
    function onCancel() { cleanup(false); }
    function onOverlayClick(e) { if (e.target === modal) cleanup(false); }
    function onKeydown(e) {
      if (e.key === 'Enter' && input.value === topic) cleanup(true);
      if (e.key === 'Escape') cleanup(false);
    }

    input.addEventListener('input', onInput);
    input.addEventListener('keydown', onKeydown);
    confirmBtn.addEventListener('click', onConfirm);
    cancelBtn.addEventListener('click', onCancel);
    closeBtn.addEventListener('click', onCancel);
    modal.addEventListener('click', onOverlayClick);
  });
}

// Full-page, non-dismissable overlay - blocks every other control while a
// purge/delete request is in flight.
function showBusy(message) {
  el('busy-message').textContent = message;
  el('busy-overlay').classList.add('open');
}
function hideBusy() {
  el('busy-overlay').classList.remove('open');
}

el('publish-to-topic-btn').addEventListener('click', async () => {
  const topic = state.selectedTopic;
  if (!topic) return;

  switchToPage('publish');
  el('publish-topic').value = topic;
  await refreshPublishPartitions();
});

el('purge-topic-btn').addEventListener('click', async () => {
  const topic = state.selectedTopic;
  if (!topic) return;

  const confirmed = await confirmByTypingTopicName(
    topic,
    'Purge all messages',
    `This deletes ALL messages in every partition of "${topic}". This cannot be undone.`
  );
  if (!confirmed) return;

  showBusy(`Purging messages in "${topic}"…`);
  try {
    const data = await api(
      `/api/topics/${encodeURIComponent(topic)}/purge?env=${encodeURIComponent(state.currentEnv)}`,
      { method: 'POST' }
    );
    toast(`Purged ${data.partitionCount} partition(s) on ${topic}`);
    if (data.warning) toast(data.warning, true);
    await selectTopic(topic);
  } catch (err) {
    toast(err.message, true);
  } finally {
    hideBusy();
  }
});

el('delete-topic-btn').addEventListener('click', async () => {
  const topic = state.selectedTopic;
  if (!topic) return;

  const confirmed = await confirmByTypingTopicName(
    topic,
    'Delete topic',
    `This permanently deletes topic "${topic}" and all its data.`
  );
  if (!confirmed) return;

  showBusy(`Deleting topic "${topic}"…`);
  try {
    await api(
      `/api/topics/${encodeURIComponent(topic)}?env=${encodeURIComponent(state.currentEnv)}`,
      { method: 'DELETE' }
    );
    toast(`Deleted topic ${topic}`);
    resetMessagePane();
    if (el('topic-filter').value) el('topic-filter').value = '';
    loadTopics();
  } catch (err) {
    toast(err.message, true);
  } finally {
    hideBusy();
  }
});

// ---------- topic details (modal) ----------

el('view-details-btn').addEventListener('click', async () => {
  const topic = state.selectedTopic;
  if (!topic) return;

  el('topic-details-modal-title').textContent = `Topic details — ${topic}`;
  el('topic-details-content').innerHTML = '<div class="loading-hint"><span class="spinner"></span> Loading topic details…</div>';
  el('topic-details-modal').classList.add('open');

  try {
    const data = await api(
      `/api/topics/${encodeURIComponent(topic)}/details?env=${encodeURIComponent(state.currentEnv)}`
    );
    renderTopicDetails(data);
  } catch (err) {
    el('topic-details-content').innerHTML = `<p class="empty-hint">${escapeHtml(err.message)}</p>`;
    toast(err.message, true);
  }
});

el('close-topic-details').addEventListener('click', () => el('topic-details-modal').classList.remove('open'));
el('topic-details-modal').addEventListener('click', (e) => {
  if (e.target.id === 'topic-details-modal') el('topic-details-modal').classList.remove('open');
});

function renderTopicDetails(data) {
  const panel = el('topic-details-content');
  const groupCount = data.consumerGroups.length;
  const consumerCount = data.consumerGroups.reduce((s, g) => s + g.consumerIds.length, 0);

  let html = '<div class="details-grid">';
  html += statCard(data.partitionCount, 'Partitions');
  html += statCard(data.replicationFactor, 'Replication factor');
  html += statCard(data.totalMessages, 'Total messages (approx)');
  html += statCard(groupCount, 'Consumer groups');
  html += statCard(consumerCount, 'Active consumers');
  html += '</div>';

  html += '<div class="details-section-title">Partitions</div>';
  html += '<table class="details-table"><thead><tr><th>Partition</th><th>Leader</th><th>Replicas</th><th>ISR</th><th>Earliest</th><th>Latest</th><th>Messages</th></tr></thead><tbody>';
  data.partitions.forEach((p) => {
    html += `<tr>
      <td>${p.partition}</td>
      <td>${p.leader}</td>
      <td>${p.replicas.join(',')}</td>
      <td>${p.isr.join(',')}</td>
      <td>${p.earliestOffset ?? '-'}</td>
      <td>${p.latestOffset ?? '-'}</td>
      <td>${p.messageCount ?? '-'}</td>
    </tr>`;
  });
  html += '</tbody></table>';

  html += '<div class="details-section-title">Consumer groups</div>';
  if (!data.consumerGroups.length) {
    html += '<p class="muted">No consumer groups have committed offsets on this topic.</p>';
  } else {
    data.consumerGroups.forEach((g) => {
      html += `<div class="group-block">
        <div class="group-name">${escapeHtml(g.groupId)}</div>
        <div class="group-meta">
          total lag ${g.totalLag} ·
          ${g.consumerIds.length ? `${g.consumerIds.length} active consumer(s)` : 'no active consumers'}
        </div>
        ${g.consumerIds.length ? `<div class="config-kv">consumer IDs: <span>${g.consumerIds.map(escapeHtml).join(', ')}</span></div>` : ''}
      </div>`;
    });
  }

  const configEntries = Object.entries(data.config || {});
  html += '<div class="details-section-title">Config overrides</div>';
  if (!configEntries.length) {
    html += '<p class="muted">No topic-level overrides — using cluster defaults.</p>';
  } else {
    configEntries.forEach(([k, v]) => {
      html += `<div class="config-kv">${escapeHtml(k)}: <span>${escapeHtml(v)}</span></div>`;
    });
  }

  panel.innerHTML = html;
}

function statCard(value, label) {
  return `<div class="details-stat"><div class="stat-value">${value ?? '-'}</div><div class="stat-label">${label}</div></div>`;
}

// ---------- publish ----------

let publishPartitionManual = false;

function setPublishPartitionMode(manual) {
  publishPartitionManual = manual;
  el('publish-partition-select').style.display = manual ? 'none' : '';
  el('publish-partition-input').style.display = manual ? '' : 'none';
  el('publish-partition-toggle').textContent = manual ? 'Choose from list' : 'Enter manually';
}

async function refreshPublishPartitions() {
  const topic = el('publish-topic').value.trim();
  const hint = el('publish-partition-hint');
  const sel = el('publish-partition-select');
  sel.innerHTML = '<option value="">No preference</option>';
  hint.textContent = '';

  if (!topic || !state.currentEnv) return;

  try {
    const data = await api(
      `/api/topics/${encodeURIComponent(topic)}/partitions?env=${encodeURIComponent(state.currentEnv)}`
    );
    data.partitions.forEach((p) => {
      const opt = document.createElement('option');
      opt.value = p;
      opt.textContent = `Partition ${p}`;
      sel.appendChild(opt);
    });
    setPublishPartitionMode(false);
    hint.textContent = `${data.partitions.length} partition(s) found for this topic.`;
  } catch {
    // Topic doesn't exist yet (or couldn't be described) - fall back to manual entry.
    setPublishPartitionMode(true);
    hint.textContent = "Topic not found yet - enter a partition number manually, or leave blank.";
  }
}

el('publish-topic').addEventListener('blur', refreshPublishPartitions);
el('publish-partition-toggle').addEventListener('click', () => setPublishPartitionMode(!publishPartitionManual));
setPublishPartitionMode(false);

el('publish-json').addEventListener('input', (e) => {
  const status = el('publish-json-status');
  if (!e.target.value.trim()) { status.textContent = ''; return; }
  try {
    JSON.parse(e.target.value);
    status.textContent = 'Valid JSON';
    status.style.color = 'var(--accent)';
  } catch (err) {
    status.textContent = 'Invalid JSON';
    status.style.color = 'var(--danger)';
  }
});

el('publish-btn').addEventListener('click', async () => {
  const topic = el('publish-topic').value.trim();
  const message = el('publish-json').value;
  const resultBox = el('publish-result');
  resultBox.textContent = '';
  resultBox.className = 'publish-result';

  if (!state.currentEnv) return toast('Select an environment first', true);
  if (!topic) return toast('Topic is required', true);
  try {
    JSON.parse(message);
  } catch (err) {
    return toast('Message is not valid JSON', true);
  }

  const key = el('publish-key').value.trim();
  const partitionValue = publishPartitionManual
    ? el('publish-partition-input').value
    : el('publish-partition-select').value;
  const partition = partitionValue !== '' ? Number(partitionValue) : undefined;

  const btn = el('publish-btn');
  btn.disabled = true;
  btn.textContent = 'Publishing…';
  try {
    const data = await api('/api/publish', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ env: state.currentEnv, topic, message, key: key || undefined, partition })
    });
    resultBox.textContent = `Published to ${topic} on ${state.currentEnv}`
      + (data.sentKey ? ` with key "${data.sentKey}"` : '');
    resultBox.className = 'publish-result ok';
    if (data.warning) toast(data.warning, true);
  } catch (err) {
    resultBox.textContent = err.message;
    resultBox.className = 'publish-result err';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Publish';
  }
});

// ---------- utils ----------

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ---------- init ----------

(async function init() {
  await loadConfig();
  if (state.currentEnv) loadTopics();
})();
