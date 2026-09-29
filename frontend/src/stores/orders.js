import { defineStore } from 'pinia'
import { api } from '../api'

export const useOrdersStore = defineStore('orders', {
  state: () => ({
    items: [],
    includeDeleted: false,
    loading: false,
    saving: false,
    error: '',
  }),
  actions: {
    async fetchOrders(nodeId) {
      this.loading = true
      this.error = ''
      try {
        this.items = await api.listOrders(nodeId, this.includeDeleted)
      } catch (error) {
        this.error = error.message
        throw error
      } finally {
        this.loading = false
      }
    },
    async create(nodeId, order) {
      this.saving = true
      try {
        const result = await api.createOrder(nodeId, order)
        await this.fetchOrders(nodeId)
        return result
      } finally {
        this.saving = false
      }
    },
    async update(nodeId, recordId, order) {
      this.saving = true
      try {
        const result = await api.updateOrder(nodeId, recordId, order)
        await this.fetchOrders(nodeId)
        return result
      } finally {
        this.saving = false
      }
    },
    async remove(nodeId, recordId) {
      this.saving = true
      try {
        const result = await api.deleteOrder(nodeId, recordId)
        await this.fetchOrders(nodeId)
        return result
      } finally {
        this.saving = false
      }
    },
  },
})
