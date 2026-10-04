<script setup>
import { computed, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  Bell,
  CircleCheckFilled,
  Connection,
  Delete,
  EditPen,
  Plus,
  Refresh,
  SwitchButton,
  Upload,
} from '@element-plus/icons-vue'
import { nodes } from './api'
import { useClusterStore } from './stores/cluster'
import { useOrdersStore } from './stores/orders'

const orders = useOrdersStore()
const cluster = useClusterStore()
const selectedNode = ref('node_a')
const dialogVisible = ref(false)
const editingRecord = ref(null)
const search = ref('')
const activeFilter = ref('active')
const formRef = ref()
const form = reactive({ item_name: '', price: 0 })
let refreshTimer

const selectedNodeInfo = computed(() => nodes.find((node) => node.id === selectedNode.value))
const visibleOrders = computed(() => {
  const query = search.value.trim().toLowerCase()
  return orders.items.filter((order) => {
    const matchesFilter = activeFilter.value === 'all'
      || (activeFilter.value === 'active' && !order.is_deleted)
      || (activeFilter.value === 'deleted' && order.is_deleted)
    const matchesSearch = !query
      || order.item_name.toLowerCase().includes(query)
      || order.record_id.toLowerCase().includes(query)
    return matchesFilter && matchesSearch
  })
})

const activeCount = computed(() => orders.items.filter((order) => !order.is_deleted).length)
const deletedCount = computed(() => orders.items.filter((order) => order.is_deleted).length)
const manifestChecksum = computed(() => cluster.manifest?.checksum || 'Waiting for manifest')

function resetForm() {
  form.item_name = ''
  form.price = 0
  editingRecord.value = null
}

function openCreate() {
  resetForm()
  dialogVisible.value = true
}

function openEdit(order) {
  editingRecord.value = order
  form.item_name = order.item_name
  form.price = order.price
  dialogVisible.value = true
}

async function loadWorkspace(showError = false) {
  try {
    await Promise.all([
      orders.fetchOrders(selectedNode.value),
      cluster.refresh(selectedNode.value),
    ])
  } catch (error) {
    if (showError) ElMessage.error(error.message)
  }
}

async function saveOrder() {
  try {
    await formRef.value.validate()
    if (editingRecord.value) {
      await orders.update(selectedNode.value, editingRecord.value.record_id, form)
      ElMessage.success('Order updated')
    } else {
      await orders.create(selectedNode.value, form)
      ElMessage.success('Order created')
    }
    dialogVisible.value = false
    await cluster.refresh(selectedNode.value)
  } catch (error) {
    if (error !== false) ElMessage.error(error.message)
  }
}

async function removeOrder(order) {
  try {
    await ElMessageBox.confirm(
      `Delete “${order.item_name}”? The record will remain as a tombstone.`,
      'Confirm deletion',
      { type: 'warning', confirmButtonText: 'Delete', cancelButtonText: 'Cancel' },
    )
    await orders.remove(selectedNode.value, order.record_id)
    await cluster.refresh(selectedNode.value)
    ElMessage.success('Order deleted')
  } catch (error) {
    if (error !== 'cancel' && error !== 'close') ElMessage.error(error.message)
  }
}

async function runSync(replayAll = false) {
  try {
    const result = await cluster.sync(selectedNode.value, replayAll)
    await orders.fetchOrders(selectedNode.value)
    ElMessage.success(`${result.published_events?.length || 0} event(s) published`)
  } catch (error) {
    ElMessage.error(error.message)
  }
}

function formatDate(date) {
  return date ? date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '--:--'
}

watch(selectedNode, () => loadWorkspace(true))
watch(() => orders.includeDeleted, () => orders.fetchOrders(selectedNode.value))

onMounted(async () => {
  await loadWorkspace(true)
  refreshTimer = window.setInterval(() => cluster.refresh(selectedNode.value), 15000)
})

onUnmounted(() => window.clearInterval(refreshTimer))

const rules = {
  item_name: [{ required: true, message: 'Enter an item name', trigger: 'blur' }],
  price: [{ required: true, message: 'Enter a price', trigger: 'change' }],
}
</script>

<template>
  <div class="app-shell">
    <header class="topbar">
      <div class="brand-lockup">
        <div class="brand-mark"><Connection /></div>
        <div>
          <div class="brand-name">Relay / Ops</div>
          <div class="brand-caption">Distributed order control</div>
        </div>
      </div>
      <div class="topbar-meta">
        <span class="live-dot"></span>
        <span>LOCAL CLUSTER</span>
        <span class="topbar-divider"></span>
        <span class="mono">API v1</span>
      </div>
    </header>

    <main class="workspace">
      <section class="intro-row">
        <div>
          <p class="eyebrow">Operator console / 29 Sep 2026</p>
          <h1>Keep the cluster in motion.</h1>
          <p class="intro-copy">One calm surface for orders, replication health, and the events waiting at the edge.</p>
        </div>
        <div class="node-picker-wrap">
          <span class="field-label">Active node</span>
          <el-select v-model="selectedNode" class="node-picker" size="large">
            <el-option v-for="node in nodes" :key="node.id" :label="`${node.label} · :${node.port}`" :value="node.id">
              <div class="node-option"><span :class="['node-dot', node.tone]"></span>{{ node.label }}<span class="option-port">:{{ node.port }}</span></div>
            </el-option>
          </el-select>
        </div>
      </section>

      <section class="status-strip">
        <div class="status-lead">
          <div :class="['status-symbol', cluster.isOnline ? 'online' : 'offline']"><CircleCheckFilled v-if="cluster.isOnline" /><SwitchButton v-else /></div>
          <div>
            <div class="status-title">{{ selectedNodeInfo?.label }} <span class="status-pill">{{ cluster.isOnline ? 'HEALTHY' : 'OFFLINE' }}</span></div>
            <div class="status-subtitle">Last checked {{ formatDate(cluster.lastUpdated) }} · auto-refresh every 15 sec</div>
          </div>
        </div>
        <div class="status-actions">
          <el-button text :icon="Refresh" :loading="cluster.loading" @click="loadWorkspace(true)">Refresh</el-button>
          <el-button type="primary" :icon="Upload" :loading="cluster.syncing" @click="runSync()">Sync events</el-button>
        </div>
      </section>

      <section class="metrics-grid">
        <article class="metric-card metric-primary">
          <div class="metric-top"><span>Visible orders</span><span class="metric-index">01</span></div>
          <div class="metric-value">{{ activeCount }}</div>
          <div class="metric-foot"><span>Across this node</span><span class="metric-accent">{{ deletedCount }} tombstones</span></div>
        </article>
        <article class="metric-card">
          <div class="metric-top"><span>Pending outbox</span><span class="metric-index">02</span></div>
          <div class="metric-value">{{ cluster.status?.pending_outbox ?? '—' }}</div>
          <div class="metric-foot"><span>Waiting to publish</span><span class="metric-dot"></span></div>
        </article>
        <article class="metric-card">
          <div class="metric-top"><span>Processed events</span><span class="metric-index">03</span></div>
          <div class="metric-value">{{ cluster.status?.processed_events ?? '—' }}</div>
          <div class="metric-foot"><span>Inbox acknowledgements</span><span class="metric-check">✓</span></div>
        </article>
        <article class="metric-card metric-manifest">
          <div class="metric-top"><span>Manifest checksum</span><span class="metric-index">04</span></div>
          <div class="checksum">{{ manifestChecksum }}</div>
          <div class="metric-foot"><span>{{ cluster.manifest?.count ?? 0 }} records in canonical view</span></div>
        </article>
      </section>

      <section class="orders-section">
        <div class="section-heading">
          <div>
            <p class="eyebrow">Business records</p>
            <h2>Orders <span>{{ visibleOrders.length }}</span></h2>
          </div>
          <el-button type="primary" :icon="Plus" @click="openCreate">New order</el-button>
        </div>

        <div class="table-toolbar">
          <el-input v-model="search" class="search-input" placeholder="Search by item or record ID" clearable />
          <div class="filter-tabs">
            <button :class="{ active: activeFilter === 'active' }" @click="activeFilter = 'active'">Active <b>{{ activeCount }}</b></button>
            <button :class="{ active: activeFilter === 'all' }" @click="activeFilter = 'all'">All <b>{{ orders.items.length }}</b></button>
            <button :class="{ active: activeFilter === 'deleted' }" @click="activeFilter = 'deleted'">Tombstones <b>{{ deletedCount }}</b></button>
          </div>
        </div>

        <el-alert v-if="orders.error || cluster.error" :title="orders.error || cluster.error" type="error" show-icon :closable="false" class="error-alert" />

        <div class="table-shell">
          <el-table v-loading="orders.loading" :data="visibleOrders" empty-text="No orders on this node yet" class="orders-table">
            <el-table-column label="Order" min-width="260">
              <template #default="{ row }">
                <div class="order-name"><span :class="['order-bullet', row.is_deleted ? 'muted' : '']"></span>{{ row.item_name }}</div>
                <div class="record-id mono">{{ row.record_id }}</div>
              </template>
            </el-table-column>
            <el-table-column label="Price" width="130">
              <template #default="{ row }"><span class="price">${{ row.price.toLocaleString() }}</span></template>
            </el-table-column>
            <el-table-column label="Version" width="120">
              <template #default="{ row }"><span class="version-badge">v{{ row.version }}</span></template>
            </el-table-column>
            <el-table-column label="Origin / writer" min-width="190">
              <template #default="{ row }"><span class="node-label">{{ row.origin_node }}</span><span class="writer"> → {{ row.updated_by_node }}</span></template>
            </el-table-column>
            <el-table-column label="State" width="130">
              <template #default="{ row }"><el-tag :type="row.is_deleted ? 'info' : 'success'" effect="plain">{{ row.is_deleted ? 'Tombstone' : 'Active' }}</el-tag></template>
            </el-table-column>
            <el-table-column label="" width="112" align="right">
              <template #default="{ row }">
                <div class="row-actions" v-if="!row.is_deleted">
                  <el-button text :icon="EditPen" title="Edit order" @click="openEdit(row)" />
                  <el-button text type="danger" :icon="Delete" title="Delete order" @click="removeOrder(row)" />
                </div>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </section>

      <footer class="footer-note">
        <span><Bell /> Events are persisted through the outbox before publication.</span>
        <span class="mono">{{ selectedNodeInfo?.id }} / local operator mode</span>
      </footer>
    </main>

    <el-dialog v-model="dialogVisible" :title="editingRecord ? 'Edit order' : 'Create order'" width="440px" @closed="resetForm">
      <el-form ref="formRef" :model="form" :rules="rules" label-position="top" @submit.prevent="saveOrder">
        <el-form-item label="Item name" prop="item_name"><el-input v-model="form.item_name" placeholder="e.g. Event relay license" /></el-form-item>
        <el-form-item label="Price" prop="price"><el-input-number v-model="form.price" :min="0" :precision="0" controls-position="right" class="full-width" /></el-form-item>
        <div class="dialog-note"><Connection /> This write will create a new outbox event on {{ selectedNodeInfo?.label }}.</div>
      </el-form>
      <template #footer><el-button @click="dialogVisible = false">Cancel</el-button><el-button type="primary" :loading="orders.saving" @click="saveOrder">{{ editingRecord ? 'Save changes' : 'Create order' }}</el-button></template>
    </el-dialog>
  </div>
</template>
