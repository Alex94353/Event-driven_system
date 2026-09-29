import { defineStore } from 'pinia'
import { api } from '../api'

export const useClusterStore = defineStore('cluster', {
  state: () => ({
    status: null,
    health: null,
    manifest: null,
    loading: false,
    syncing: false,
    error: '',
    lastUpdated: null,
  }),
  getters: {
    isOnline: (state) => state.health?.status === 'healthy',
  },
  actions: {
    async refresh(nodeId) {
      this.loading = true
      this.error = ''
      try {
        const [health, status, manifest] = await Promise.all([
          api.health(nodeId),
          api.status(nodeId),
          api.manifest(nodeId),
        ])
        this.health = health
        this.status = status
        this.manifest = manifest
        this.lastUpdated = new Date()
      } catch (error) {
        this.error = error.message
        this.health = null
        this.status = null
        this.manifest = null
        throw error
      } finally {
        this.loading = false
      }
    },
    async sync(nodeId, replayAll = false) {
      this.syncing = true
      try {
        const result = await api.sync(nodeId, replayAll)
        await this.refresh(nodeId)
        return result
      } finally {
        this.syncing = false
      }
    },
  },
})
