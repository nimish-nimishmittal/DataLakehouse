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
  apiBaseUrl: API_BASE,
  login: (credentials) => api.post('/auth/login', credentials),
  // register: (credentials) => api.post('/auth/register', credentials),
  getMetrics: () => api.get('/dashboard/metrics'),

  // Legacy flat file listing
  getFiles: (params = {}) => api.get('/files/flat', { params }),

  // File Browser APIs
  getBuckets: () => api.get('/buckets'),
  browseFolder: (bucket, prefix = '', search = '') => {
    const params = { bucket };
    if (prefix) params.prefix = prefix;
    if (search) params.search = search;
    return api.get('/files', { params });
  },

  // uploadFile: (formData) => api.post('/upload', formData),
  uploadFile: (formData) =>
  api.post('/upload', formData, {
    headers: {
      'Content-Type': undefined,
    },
  }),
  deleteFile: (id) => api.delete(`/files/${id}`),
  getFileDetails: (id) => api.get(`/files/${id}`),
  getHealth: () => api.get('/health'),
  getJobs: () => api.get('/jobs'),
  getSystemStats: () => api.get('/admin/system/stats'),
  getDiagnostics: () => api.get('/admin/diagnostics'),
  downloadFile: (id) => api.get(`/files/download/${id}`, { responseType: 'blob' }),
  previewFile: (id) => api.get(`/files/preview/${id}`),
  triggerJob: (id) => api.post(`/jobs/trigger/${id}`),
  searchDocuments: (query) => api.get(`/search?query=${query}`),

  // User Management
  getUsers: () => api.get('/admin/users'),
  addUser: (userData) => api.post('/admin/users', userData),
  deleteUser: (userId) => api.delete(`/admin/users/${userId}`),
  getAuditLogs: () => api.get('/admin/audit-logs'),
};


// ── Search API — points to python-etl (port 5000, same JWT token) ────────
// These endpoints live on the Flask uploader service (search_api.py blueprint)
// not on backend-api, so we need a separate axios instance.
const searchAxios = axios.create({
  baseURL: 'http://localhost:5000',
  headers: { 'Content-Type': 'application/json' },
});

searchAxios.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

export const searchAPI = {
  // POST /search  { query, top_k, table }
  // table can be: null (all), "table_name" (one), or ["t1","t2"] (many)
  semanticSearch: (query, topK = 10, table = null) =>
    searchAxios.post('/search', {
      query,
      top_k: topK,
      ...(table !== null ? { table } : {}),
    }),

  // GET /search/tables  — all indexed tables with row counts + dim coverage
  getEmbedTables: () => searchAxios.get('/search/tables'),

  // GET /search/status  — queue stats + total embedding counts
  getEmbedStatus: () => searchAxios.get('/search/status'),
};


// ── RAG API — python-etl Flask (port 5000) ────────────────────────────────
// NOTE: /rag/query is a streaming SSE endpoint — use native fetch() in RAG.jsx
//       (axios does not support ReadableStream natively).
//       These helpers cover the non-streaming endpoints only.
export const ragAPI = {
  // GET /rag/status — ollama_rag health + whether gemma3:270m is loaded
  getStatus: () => searchAxios.get('/rag/status'),

  // GET /rag/models — list models available on ollama_rag container
  getModels: () => searchAxios.get('/rag/models'),
};
// Streaming query is called directly via fetch() in RAG.jsx:
//   fetch('http://localhost:5000/rag/query', { method:'POST', ... })
//   then response.body.getReader() → ReadableStream tokens

export default api;