const state = {
  config: { kafkaHome: '', environments: [] },
  currentEnv: null,
  topics: [],
  selectedTopic: null,
  messages: [], // last loaded batch for the selected topic
  selectedMessageIndex: null,
  detailsOpen: false,
  detailsLoadedForTopic: null
};

const el = (id) => document.getElementById(id);

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

document.querySelectorAll('.tab').forEach((tab) => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach((t) => t.classList.remove('active'));
    document.querySelectorAll('.page').forEach((p) => p.classList.remove('active'));
    tab.classList.add('active');
    el(`page-${tab.dataset.page}`).classList.add('active');
  });
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
      li.textContent = t;
      li.title = t;
      if (t === state.selectedTopic) li.classList.add('selected');
      li.addEventListener('click', () => selectTopic(t));
      list.appendChild(li);
    });
}

el('topic-filter').addEventListener('input', renderTopicList);
el('refresh-topics').addEventListener('click', loadTopics);
el('refresh-messages').addEventListener('click', () => selectTopic(state.selectedTopic));

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
  state.selectedMessageIndex = null;
  el('current-topic-name').textContent = 'Select a topic';
  el('loaded-until').textContent = '';
  el('message-stream').innerHTML = '<p class="empty-hint">Pick a topic on the left to load its most recent messages.</p>';
  el('message-detail').innerHTML = '<p class="empty-hint">Select a message to see its full payload.</p>';
  resetFilters();
  setFiltersEnabled(false);
  el('refresh-messages').disabled = true;
  el('view-details-btn').disabled = true;
  el('purge-topic-btn').disabled = true;
  el('delete-topic-btn').disabled = true;
}

async function selectTopic(topic) {
  state.selectedTopic = topic;
  state.selectedMessageIndex = null;
  renderTopicList();
  el('current-topic-name').textContent = topic;
  el('loaded-until').textContent = 'Loading recent messages…';
  el('message-stream').innerHTML = '<div class="loading-hint"><span class="spinner"></span> Loading recent messages…</div>';
  el('message-detail').innerHTML = '<p class="empty-hint">Select a message to see its full payload.</p>';
  resetFilters();
  setFiltersEnabled(false);
  el('refresh-messages').disabled = true;
  el('view-details-btn').disabled = true;
  el('purge-topic-btn').disabled = true;
  el('delete-topic-btn').disabled = true;

  try {
    const data = await api(
      `/api/messages?env=${encodeURIComponent(state.currentEnv)}&topic=${encodeURIComponent(topic)}&limit=50`
    );
    state.messages = data.messages;
    renderMessages();
    populatePartitionFilter();
    el('loaded-until').textContent = data.loadedUntil
      ? `Showing ${data.messages.length} messages, loaded back to ${new Date(data.loadedUntil).toLocaleString()}`
      : 'No messages found on this topic yet';
    setFiltersEnabled(true);
    el('refresh-messages').disabled = false;
    el('view-details-btn').disabled = false;
    el('purge-topic-btn').disabled = false;
    el('delete-topic-btn').disabled = false;
  } catch (err) {
    el('loaded-until').textContent = '';
    el('message-stream').innerHTML = `<p class="empty-hint">${escapeHtml(err.message)}</p>`;
    // details/purge/delete can still work even if message load failed
    el('view-details-btn').disabled = false;
    el('purge-topic-btn').disabled = false;
    el('delete-topic-btn').disabled = false;
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
  if (!m) {
    detail.innerHTML = '<p class="empty-hint">Select a message to see its full payload.</p>';
    return;
  }
  detail.innerHTML = `
    <div class="detail-meta">
      <span>${new Date(m.timestamp).toLocaleString()}</span>
      <span>partition ${m.partition}</span>
      <span>offset ${m.offset}</span>
      ${m.key ? `<span class="msg-key">key: ${escapeHtml(m.key)}</span>` : '<span>no key</span>'}
    </div>
    <div class="msg-value">${escapeHtml(prettyIfJson(m.value))}</div>
  `;
}

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
  const fromMs = el('filter-date-from').value ? new Date(el('filter-date-from').value).getTime() : null;
  const toMs = el('filter-date-to').value ? new Date(el('filter-date-to').value).getTime() : null;

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

el('purge-topic-btn').addEventListener('click', async () => {
  const topic = state.selectedTopic;
  if (!topic) return;
  if (!confirm(`Delete ALL messages in "${topic}"?\n\nThis purges every partition and cannot be undone.`)) return;

  const btn = el('purge-topic-btn');
  btn.disabled = true;
  btn.textContent = 'Purging…';
  try {
    const data = await api(
      `/api/topics/${encodeURIComponent(topic)}/purge?env=${encodeURIComponent(state.currentEnv)}`,
      { method: 'POST' }
    );
    toast(`Purged ${data.partitionCount} partition(s) on ${topic}`);
    if (data.warning) toast(data.warning, true);
    selectTopic(topic);
  } catch (err) {
    toast(err.message, true);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Purge messages';
  }
});

el('delete-topic-btn').addEventListener('click', async () => {
  const topic = state.selectedTopic;
  if (!topic) return;
  const typed = prompt(`This permanently deletes topic "${topic}" and all its data.\nType the topic name to confirm:`);
  if (typed === null) return;
  if (typed !== topic) return toast('Topic name did not match - cancelled', true);

  const btn = el('delete-topic-btn');
  btn.disabled = true;
  btn.textContent = 'Deleting…';
  try {
    await api(
      `/api/topics/${encodeURIComponent(topic)}?env=${encodeURIComponent(state.currentEnv)}`,
      { method: 'DELETE' }
    );
    toast(`Deleted topic ${topic}`);
    resetMessagePane();
    loadTopics();
  } catch (err) {
    toast(err.message, true);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Delete topic';
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
