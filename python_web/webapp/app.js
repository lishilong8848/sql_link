const state = {
  session: null,
  slotDrafts: {},
  fields: [],
  rows: [],
  total: 0,
  page: 1,
  pageSize: 50,
  selectedKeys: new Set(),
  batchRows: [],
  nextBatchRowId: 1,
};

const elements = {
  connectionView: document.getElementById("connectionView"),
  workspaceView: document.getElementById("workspaceView"),
  slotGrid: document.getElementById("slotGrid"),
  workspaceTitle: document.getElementById("workspaceTitle"),
  workspaceMeta: document.getElementById("workspaceMeta"),
  totalCount: document.getElementById("totalCount"),
  selectedCount: document.getElementById("selectedCount"),
  pageInfo: document.getElementById("pageInfo"),
  querySummary: document.getElementById("querySummary"),
  startTime: document.getElementById("startTime"),
  endTime: document.getElementById("endTime"),
  queryButton: document.getElementById("queryButton"),
  resetButton: document.getElementById("resetButton"),
  selectPageButton: document.getElementById("selectPageButton"),
  clearSelectionButton: document.getElementById("clearSelectionButton"),
  openBatchButton: document.getElementById("openBatchButton"),
  logoutButton: document.getElementById("logoutButton"),
  pageSizeSelect: document.getElementById("pageSizeSelect"),
  prevPageButton: document.getElementById("prevPageButton"),
  nextPageButton: document.getElementById("nextPageButton"),
  eventTableBody: document.getElementById("eventTableBody"),
  detailModal: document.getElementById("detailModal"),
  detailContent: document.getElementById("detailContent"),
  batchModal: document.getElementById("batchModal"),
  batchRows: document.getElementById("batchRows"),
  addBatchRowButton: document.getElementById("addBatchRowButton"),
  submitBatchButton: document.getElementById("submitBatchButton"),
  batchWarning: document.getElementById("batchWarning"),
  toast: document.getElementById("toast"),
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || "GET",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    body: options.body ? JSON.stringify(options.body) : undefined,
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.message || "请求失败");
  }
  return data;
}

function showToast(message, isError = false) {
  elements.toast.textContent = message;
  elements.toast.style.background = isError ? "rgba(160, 32, 32, 0.96)" : "rgba(15, 31, 39, 0.94)";
  elements.toast.classList.remove("hidden");
  window.clearTimeout(showToast.timer);
  showToast.timer = window.setTimeout(() => {
    elements.toast.classList.add("hidden");
  }, 3200);
}

function nowLocalInputValue() {
  const date = new Date();
  const timezoneOffsetMs = date.getTimezoneOffset() * 60 * 1000;
  return new Date(date.getTime() - timezoneOffsetMs).toISOString().slice(0, 16);
}

function monthStartLocalInputValue() {
  const date = new Date();
  date.setDate(1);
  date.setHours(0, 0, 0, 0);
  const timezoneOffsetMs = date.getTimezoneOffset() * 60 * 1000;
  return new Date(date.getTime() - timezoneOffsetMs).toISOString().slice(0, 16);
}

function toUnixSeconds(localDateTime) {
  return Math.floor(new Date(localDateTime).getTime() / 1000);
}

function formatTimestamp(value) {
  const timestamp = Number(value);
  if (!Number.isFinite(timestamp) || timestamp <= 0) {
    return "-";
  }
  const date = new Date(timestamp * 1000);
  return date.toLocaleString("zh-CN", { hour12: false });
}

function formatCell(field, value) {
  if (field === "event_time" || field.endsWith("_time")) {
    return formatTimestamp(value);
  }
  return value === null || value === undefined || value === "" ? "-" : String(value);
}

function rowKey(row) {
  return `${row._table}:${row.guid}`;
}

function getEditableFields() {
  return state.fields.filter((field) => field.editable);
}

function ensureDrafts() {
  for (let slotId = 1; slotId <= 5; slotId += 1) {
    const slot = state.session?.slots?.find((item) => item.slotId === slotId);
    if (!state.slotDrafts[slotId]) {
      state.slotDrafts[slotId] = {
        label: `数据库 ${slotId}`,
        host: "",
        port: 3306,
        database: "",
        user: "",
        password: "",
      };
    }
    if (slot?.connection) {
      state.slotDrafts[slotId] = { ...slot.connection };
    }
  }
}

function renderConnections() {
  ensureDrafts();
  const cards = [];
  for (let slotId = 1; slotId <= 5; slotId += 1) {
    const slot = state.session?.slots?.find((item) => item.slotId === slotId);
    const draft = state.slotDrafts[slotId];
    const status =
      state.session?.activeConnectionId === slotId
        ? "当前使用中"
        : slot?.connection
          ? "已保存"
          : "待配置";

    cards.push(`
      <article class="slot-card">
        <div class="slot-head">
          <div>
            <p class="eyebrow">卡槽 ${slotId}</p>
            <h2>${escapeHtml(draft.label || `数据库 ${slotId}`)}</h2>
          </div>
          <span class="slot-status">${status}</span>
        </div>
        <div class="slot-form">
          ${renderSlotInput(slotId, "label", "连接名称", draft.label)}
          ${renderSlotInput(slotId, "host", "数据库 IP", draft.host)}
          <div class="slot-form-row split">
            ${renderSlotInput(slotId, "database", "数据库名", draft.database)}
            ${renderSlotInput(slotId, "port", "端口", draft.port, "number")}
          </div>
          ${renderSlotInput(slotId, "user", "账号", draft.user)}
          ${renderSlotInput(slotId, "password", "密码", draft.password, "password")}
        </div>
        <div class="slot-actions">
          <button class="btn" data-slot-action="test" data-slot-id="${slotId}">测试连接</button>
          <button class="btn" data-slot-action="save" data-slot-id="${slotId}">保存卡槽</button>
          <button class="btn btn-primary" data-slot-action="enter" data-slot-id="${slotId}">进入数据库</button>
          <button class="btn btn-danger" data-slot-action="clear" data-slot-id="${slotId}">清空</button>
        </div>
      </article>
    `);
  }
  elements.slotGrid.innerHTML = cards.join("");
}

function renderSlotInput(slotId, field, label, value, type = "text") {
  return `
    <label class="slot-form-row">
      <span>${label}</span>
      <input
        type="${type}"
        data-slot-id="${slotId}"
        data-slot-field="${field}"
        value="${escapeHtml(value)}"
      />
    </label>
  `;
}

function syncView() {
  const active = state.session?.activeConnection;
  const connected = Boolean(active);
  elements.connectionView.classList.toggle("hidden", connected);
  elements.workspaceView.classList.toggle("hidden", !connected);
  if (connected) {
    elements.workspaceTitle.textContent = active.label;
    elements.workspaceMeta.textContent = `当前数据库：${active.host} / ${active.database}。默认显示本月前 50 条，可按时间范围跨月查询并对当前页勾选记录批量修改。`;
  }
}

function renderWorkspace() {
  const totalPages = Math.max(1, Math.ceil(state.total / state.pageSize));
  elements.totalCount.textContent = String(state.total);
  elements.selectedCount.textContent = String(state.selectedKeys.size);
  elements.pageInfo.textContent = `第 ${state.page} / ${totalPages} 页`;
  elements.pageSizeSelect.value = String(state.pageSize);
  elements.querySummary.textContent = `查询区间：${elements.startTime.value || "-"} 至 ${elements.endTime.value || "-"}，按 event_time 倒序`;
  renderTable();
}

function renderTable() {
  if (!state.rows.length) {
    elements.eventTableBody.innerHTML = `
      <tr>
        <td colspan="16" class="muted">当前查询范围内没有记录，或对应月份表不存在。</td>
      </tr>
    `;
    return;
  }

  const html = state.rows
    .map((row) => {
      const key = rowKey(row);
      return `
        <tr>
          <td class="compact"><input type="checkbox" data-row-check="${key}" ${state.selectedKeys.has(key) ? "checked" : ""} /></td>
          <td>${escapeHtml(row._table)}</td>
          <td>${escapeHtml(formatCell("guid", row.guid))}</td>
          <td>${escapeHtml(formatCell("event_time", row.event_time))}</td>
          <td>${escapeHtml(formatCell("sys_type", row.sys_type))}</td>
          <td>${escapeHtml(formatCell("resource_id", row.resource_id))}</td>
          <td>${escapeHtml(formatCell("device_type", row.device_type))}</td>
          <td>${escapeHtml(formatCell("content", row.content))}</td>
          <td>${escapeHtml(formatCell("event_level", row.event_level))}</td>
          <td>${escapeHtml(formatCell("event_type", row.event_type))}</td>
          <td>${escapeHtml(formatCell("is_recover", row.is_recover))}</td>
          <td>${escapeHtml(formatCell("is_confirm", row.is_confirm))}</td>
          <td>${escapeHtml(formatCell("is_accept", row.is_accept))}</td>
          <td>${escapeHtml(formatCell("confirm_description", row.confirm_description))}</td>
          <td>${escapeHtml(formatCell("accept_description", row.accept_description))}</td>
          <td><button class="btn" data-detail-key="${key}">详情</button></td>
        </tr>
      `;
    })
    .join("");

  elements.eventTableBody.innerHTML = html;
}

function openModal(modal) {
  modal.classList.remove("hidden");
}

function closeModal(modal) {
  modal.classList.add("hidden");
}

function openDetailModal(key) {
  const row = state.rows.find((item) => rowKey(item) === key);
  if (!row) {
    return;
  }
  const html = Object.keys(row)
    .sort()
    .map(
      (field) => `
        <div class="detail-row">
          <div class="detail-key">${escapeHtml(field)}</div>
          <div>${escapeHtml(formatCell(field, row[field]))}</div>
        </div>
      `,
    )
    .join("");
  elements.detailContent.innerHTML = html;
  openModal(elements.detailModal);
}

function resetDefaultRange() {
  elements.startTime.value = monthStartLocalInputValue();
  elements.endTime.value = nowLocalInputValue();
}

async function loadSession() {
  state.session = await api("/api/session/state");
  ensureDrafts();
  renderConnections();
  syncView();
}

async function loadFields() {
  const response = await api("/api/event/fields");
  state.fields = response.columns || [];
}

async function loadEvents() {
  const result = await api("/api/events/query", {
    method: "POST",
    body: {
      startTime: toUnixSeconds(elements.startTime.value),
      endTime: toUnixSeconds(elements.endTime.value),
      page: state.page,
      pageSize: state.pageSize,
    },
  });
  state.rows = result.rows || [];
  state.total = Number(result.total || 0);
  state.selectedKeys.clear();
  renderWorkspace();
}

function getSlotPayload(slotId) {
  const draft = state.slotDrafts[slotId];
  return {
    label: draft.label,
    host: draft.host,
    port: Number(draft.port),
    database: draft.database,
    user: draft.user,
    password: draft.password,
  };
}

async function handleSlotAction(slotId, action) {
  const payload = getSlotPayload(slotId);
  if (action === "test") {
    await api("/api/session/connections/test", { method: "POST", body: payload });
    showToast(`卡槽 ${slotId} 数据库连接成功。`);
    return;
  }

  if (action === "clear") {
    if (!window.confirm(`确认清空卡槽 ${slotId} 吗？`)) {
      return;
    }
    state.session = await api(`/api/session/connections/${slotId}`, { method: "DELETE" });
    state.slotDrafts[slotId] = {
      label: `数据库 ${slotId}`,
      host: "",
      port: 3306,
      database: "",
      user: "",
      password: "",
    };
    renderConnections();
    syncView();
    showToast(`卡槽 ${slotId} 已清空。`);
    return;
  }

  state.session = await api("/api/session/connections", {
    method: "POST",
    body: { slotId, ...payload },
  });

  if (action === "save") {
    renderConnections();
    showToast(`卡槽 ${slotId} 已保存。`);
    return;
  }

  state.session = await api("/api/session/active-connection", {
    method: "POST",
    body: { slotId },
  });
  state.page = 1;
  state.pageSize = 50;
  resetDefaultRange();
  await loadFields();
  await loadEvents();
  syncView();
  showToast(`已进入 ${payload.label}。`);
}

function createBatchRow() {
  return {
    id: state.nextBatchRowId++,
    field: "",
    value: "",
    setNull: false,
  };
}

function openBatchModal() {
  if (!state.selectedKeys.size) {
    showToast("请先勾选当前页需要修改的记录。", true);
    return;
  }
  if (!state.batchRows.length) {
    state.batchRows = [createBatchRow()];
  }
  renderBatchRows();
  openModal(elements.batchModal);
}

function updateBatchWarning() {
  const warnings = state.batchRows
    .map((row) => getEditableFields().find((field) => field.name === row.field))
    .filter((field) => field && field.warning)
    .map((field) => field.warning);
  elements.batchWarning.textContent = [...new Set(warnings)].join("\n");
}

function renderBatchRows() {
  const fieldOptions = getEditableFields()
    .map((field) => `<option value="${escapeHtml(field.name)}">${escapeHtml(`${field.name} (${field.dbType})`)}</option>`)
    .join("");

  elements.batchRows.innerHTML = state.batchRows
    .map((row) => {
      const field = getEditableFields().find((item) => item.name === row.field);
      const inputControl =
        field?.inputKind === "textarea"
          ? `<textarea data-batch-field="value" data-batch-id="${row.id}" ${row.setNull ? "disabled" : ""}>${escapeHtml(row.value)}</textarea>`
          : `<input data-batch-field="value" data-batch-id="${row.id}" type="${field?.inputKind === "number" ? "number" : "text"}" value="${escapeHtml(row.value)}" ${row.setNull ? "disabled" : ""} />`;

      return `
        <div class="batch-row">
          <select data-batch-field="field" data-batch-id="${row.id}">
            <option value="">选择字段</option>
            ${fieldOptions}
          </select>
          ${inputControl}
          <label class="field">
            <span>空值</span>
            <input data-batch-field="setNull" data-batch-id="${row.id}" type="checkbox" ${row.setNull ? "checked" : ""} />
          </label>
          <button class="btn btn-danger" data-batch-remove="${row.id}">删除</button>
        </div>
      `;
    })
    .join("");

  state.batchRows.forEach((row) => {
    const select = elements.batchRows.querySelector(`select[data-batch-id="${row.id}"]`);
    if (select) {
      select.value = row.field;
    }
  });

  updateBatchWarning();
}

function buildBatchUpdates() {
  const updates = {};
  const picked = new Set();
  for (const row of state.batchRows) {
    if (!row.field) {
      throw new Error("请为每一行选择字段。");
    }
    if (picked.has(row.field)) {
      throw new Error(`字段 ${row.field} 只能选择一次。`);
    }
    const field = getEditableFields().find((item) => item.name === row.field);
    if (!field) {
      throw new Error(`字段 ${row.field} 不存在。`);
    }
    if (row.setNull) {
      if (!field.nullable) {
        throw new Error(`字段 ${row.field} 不允许设置为 NULL。`);
      }
      updates[row.field] = null;
      picked.add(row.field);
      continue;
    }
    if (field.inputKind === "number") {
      if (String(row.value).trim() === "") {
        throw new Error(`字段 ${row.field} 需要填写数字。`);
      }
      updates[row.field] = Number(row.value);
    } else {
      updates[row.field] = String(row.value ?? "");
    }
    picked.add(row.field);
  }
  return updates;
}

async function submitBatchUpdates() {
  const updates = buildBatchUpdates();
  const warningFields = getEditableFields().filter((field) => field.warning && updates[field.name] !== undefined);
  if (warningFields.length) {
    const confirmed = window.confirm(warningFields.map((field) => field.warning).join("\n"));
    if (!confirmed) {
      return;
    }
  }

  const targets = state.rows
    .filter((row) => state.selectedKeys.has(rowKey(row)))
    .map((row) => ({
      table: row._table,
      guid: row.guid,
    }));

  const result = await api("/api/events/batch-update", {
    method: "POST",
    body: { targets, updates },
  });
  closeModal(elements.batchModal);
  state.batchRows = [];
  const summary = result.warning
    ? `批量修改完成，成功 ${result.affectedRows} 条，跳过 ${result.skippedCount || 0} 条。${result.warning}`
    : `批量修改成功，影响 ${result.affectedRows} 条记录。`;
  showToast(summary, Boolean(result.warning && !result.affectedRows));
  await loadEvents();
}

function bindEvents() {
  elements.slotGrid.addEventListener("input", (event) => {
    const target = event.target;
    const slotId = Number(target.dataset.slotId);
    const field = target.dataset.slotField;
    if (!slotId || !field) {
      return;
    }
    state.slotDrafts[slotId][field] = target.type === "number" ? Number(target.value) : target.value;
  });

  elements.slotGrid.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-slot-action]");
    if (!button) {
      return;
    }
    try {
      await handleSlotAction(Number(button.dataset.slotId), button.dataset.slotAction);
    } catch (error) {
      showToast(error.message, true);
    }
  });

  elements.queryButton.addEventListener("click", async () => {
    try {
      state.page = 1;
      await loadEvents();
    } catch (error) {
      showToast(error.message, true);
    }
  });

  elements.resetButton.addEventListener("click", async () => {
    try {
      state.page = 1;
      resetDefaultRange();
      await loadEvents();
    } catch (error) {
      showToast(error.message, true);
    }
  });

  elements.selectPageButton.addEventListener("click", () => {
    state.rows.forEach((row) => state.selectedKeys.add(rowKey(row)));
    renderWorkspace();
  });

  elements.clearSelectionButton.addEventListener("click", () => {
    state.selectedKeys.clear();
    renderWorkspace();
  });

  elements.openBatchButton.addEventListener("click", () => {
    try {
      openBatchModal();
    } catch (error) {
      showToast(error.message, true);
    }
  });

  elements.logoutButton.addEventListener("click", async () => {
    try {
      await api("/api/session/logout-current", { method: "POST" });
      state.session = await api("/api/session/state");
      state.rows = [];
      state.total = 0;
      state.selectedKeys.clear();
      syncView();
      renderConnections();
      showToast("已退出当前数据库。");
    } catch (error) {
      showToast(error.message, true);
    }
  });

  elements.pageSizeSelect.addEventListener("change", async () => {
    try {
      state.pageSize = Number(elements.pageSizeSelect.value);
      state.page = 1;
      await loadEvents();
    } catch (error) {
      showToast(error.message, true);
    }
  });

  elements.prevPageButton.addEventListener("click", async () => {
    if (state.page <= 1) return;
    try {
      state.page -= 1;
      await loadEvents();
    } catch (error) {
      showToast(error.message, true);
    }
  });

  elements.nextPageButton.addEventListener("click", async () => {
    const totalPages = Math.max(1, Math.ceil(state.total / state.pageSize));
    if (state.page >= totalPages) return;
    try {
      state.page += 1;
      await loadEvents();
    } catch (error) {
      showToast(error.message, true);
    }
  });

  elements.eventTableBody.addEventListener("click", (event) => {
    const detailButton = event.target.closest("[data-detail-key]");
    if (detailButton) {
      openDetailModal(detailButton.dataset.detailKey);
    }
  });

  elements.eventTableBody.addEventListener("change", (event) => {
    const checkbox = event.target.closest("[data-row-check]");
    if (!checkbox) {
      return;
    }
    const key = checkbox.dataset.rowCheck;
    if (checkbox.checked) {
      state.selectedKeys.add(key);
    } else {
      state.selectedKeys.delete(key);
    }
    renderWorkspace();
  });

  document.querySelectorAll("[data-close]").forEach((button) => {
    button.addEventListener("click", () => {
      closeModal(document.getElementById(button.dataset.close));
    });
  });

  elements.addBatchRowButton.addEventListener("click", () => {
    state.batchRows.push(createBatchRow());
    renderBatchRows();
  });

  elements.batchRows.addEventListener("input", (event) => {
    const target = event.target;
    const id = Number(target.dataset.batchId);
    const field = target.dataset.batchField;
    const row = state.batchRows.find((item) => item.id === id);
    if (!row || !field) {
      return;
    }
    row[field] = target.type === "checkbox" ? target.checked : target.value;
    if (field === "setNull") {
      renderBatchRows();
    }
  });

  elements.batchRows.addEventListener("change", (event) => {
    const target = event.target;
    const id = Number(target.dataset.batchId);
    const field = target.dataset.batchField;
    const row = state.batchRows.find((item) => item.id === id);
    if (!row || !field) {
      return;
    }
    row[field] = target.type === "checkbox" ? target.checked : target.value;
    if (field === "field" || field === "setNull") {
      renderBatchRows();
    } else {
      updateBatchWarning();
    }
  });

  elements.batchRows.addEventListener("click", (event) => {
    const button = event.target.closest("[data-batch-remove]");
    if (!button) {
      return;
    }
    if (state.batchRows.length === 1) {
      return;
    }
    state.batchRows = state.batchRows.filter((item) => item.id !== Number(button.dataset.batchRemove));
    renderBatchRows();
  });

  elements.submitBatchButton.addEventListener("click", async () => {
    try {
      await submitBatchUpdates();
    } catch (error) {
      showToast(error.message, true);
    }
  });
}

async function bootstrap() {
  resetDefaultRange();
  bindEvents();
  try {
    await loadSession();
    if (state.session?.activeConnectionId) {
      await loadFields();
      await loadEvents();
      syncView();
    }
  } catch (error) {
    showToast(error.message, true);
  }
}

bootstrap();
