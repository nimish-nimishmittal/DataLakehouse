import axios from 'axios';

const API_BASE = 'http://localhost:8000/api';

const api = axios.create({
  baseURL: API_BASE,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const dashboardAPI = {
  getMetrics: () => api.get('/dashboard/metrics'),
  getFiles: (params) => api.get('/files', { params }),
  getFileDetails: (id) => api.get(`/files/${id}`),
  deleteFile: (id) => api.delete(`/files/${id}`),
  uploadFile: (formData) => api.post('/upload', formData, {
    headers: { 'Content-Type': 'multipart/form-data' }
  }),
  search: (query) => api.get('/search', { params: { query } }),
  getStorageStats: () => api.get('/stats/storage'),
  getProcessingStats: () => api.get('/stats/processing'),
  healthCheck: () => api.get('/health'),
};

export default api;