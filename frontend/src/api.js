const nodeBases = {
  node_a: '/api/node-a',
  node_b: '/api/node-b',
  node_c: '/api/node-c',
}

async function request(nodeId, path, options = {}) {
  const headers = new Headers(options.headers || {})
  headers.set('Accept', 'application/json')

  const apiKey = import.meta.env.VITE_API_KEY
  if (apiKey) {
    headers.set('X-API-Key', apiKey)
  }

  if (options.body && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json')
  }

  const response = await fetch(`${nodeBases[nodeId]}${path}`, {
    ...options,
    headers,
  })

  const contentType = response.headers.get('content-type') || ''
  const payload = contentType.includes('application/json')
    ? await response.json()
    : await response.text()

  if (!response.ok) {
    const detail = typeof payload === 'object' && payload?.detail
      ? payload.detail
      : `Request failed with HTTP ${response.status}`
    throw new Error(detail)
  }

  return payload
}

export const api = {
  listOrders: (nodeId, includeDeleted = false) =>
    request(nodeId, `/orders/?include_deleted=${includeDeleted}`),
  createOrder: (nodeId, order) =>
    request(nodeId, '/orders/', { method: 'POST', body: JSON.stringify(order) }),
  updateOrder: (nodeId, recordId, order) =>
    request(nodeId, `/orders/${recordId}`, { method: 'PUT', body: JSON.stringify(order) }),
  deleteOrder: (nodeId, recordId) =>
    request(nodeId, `/orders/${recordId}`, { method: 'DELETE' }),
  health: (nodeId) => request(nodeId, '/health'),
  status: (nodeId) => request(nodeId, '/status'),
  manifest: (nodeId) => request(nodeId, '/manifest'),
  sync: (nodeId, replayAll = false) =>
    request(nodeId, `/sync?replay_all=${replayAll}`, { method: 'POST' }),
}

export const nodes = [
  { id: 'node_a', label: 'Node A', port: 8000, tone: 'coral' },
  { id: 'node_b', label: 'Node B', port: 8001, tone: 'teal' },
  { id: 'node_c', label: 'Node C', port: 8002, tone: 'gold' },
]
