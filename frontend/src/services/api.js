// src/services/api.js
import axios from 'axios';

const API_BASE = 'http://localhost:8000/api';

const api = axios.create({
  baseURL: API_BASE,
  headers: {
    'Content-Type': 'application/json',
  },
});

api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

export const dashboardAPI = {
  login: (credentials) => api.post('/auth/login', credentials),
  register: (credentials) => api.post('/auth/register', credentials),
  getMetrics: () => api.get('/dashboard/metrics'),
  getFiles: (params = {}) => api.get('/files', { params }),
  uploadFile: (formData) => api.post('/upload', formData, {
    headers: { 'Content-Type': 'multipart/form-data' }
  }),
  deleteFile: (id) => api.delete(`/files/${id}`),
  getHealth: () => api.get('/health'),
  getJobs: () => api.get('/jobs'),
  getSystemStats: () => api.get('/admin/system/stats'),
  getDiagnostics: () => api.get('/admin/diagnostics'),
  downloadFile: (id) => api.get(`/files/download/${id}`, { responseType: 'blob' }),
  triggerJob: (id) => api.post(`/jobs/trigger/${id}`),
  searchDocuments: (query) => api.get(`/search?query=${query}`),

  // User Management
  getUsers: () => api.get('/admin/users'),
  addUser: (userData) => api.post('/admin/users', userData),
  deleteUser: (userId) => api.delete(`/admin/users/${userId}`),
  getAuditLogs: () => api.get('/admin/audit-logs'),
};

export default api;