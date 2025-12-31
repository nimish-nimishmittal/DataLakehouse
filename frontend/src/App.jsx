import React, { useState, useEffect } from 'react';
import { Upload, Database, FileText, TrendingUp, Search, X, RefreshCw, Download, Trash2, AlertCircle, CheckCircle, Clock, HardDrive } from 'lucide-react';

// Mock API for demo - Replace with actual API calls
const API_BASE = 'http://localhost:8000/api';

const Dashboard = () => {
  const [metrics, setMetrics] = useState(null);
  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploadProgress, setUploadProgress] = useState(null);
  const [view, setView] = useState('dashboard'); // dashboard, files, search

  // Simulated data for demo
  useEffect(() => {
    setTimeout(() => {
      setMetrics({
        total_documents: 1247,
        processed_today: 89,
        raw_pending: 12,
        total_storage_gb: 45.8,
        files_by_format: [
          { file_format: 'pdf', count: 423 },
          { file_format: 'csv', count: 356 },
          { file_format: 'json', count: 245 },
          { file_format: 'docx', count: 156 },
          { file_format: 'image', count: 67 }
        ],
        processing_trend: [
          { date: '2025-12-24', count: 145 },
          { date: '2025-12-25', count: 123 },
          { date: '2025-12-26', count: 167 },
          { date: '2025-12-27', count: 189 },
          { date: '2025-12-28', count: 156 },
          { date: '2025-12-29', count: 178 },
          { date: '2025-12-30', count: 89 }
        ]
      });
      
      setFiles([
        { catalog_id: 1, object_name: 'raw/pdf/annual_report_2024.pdf', file_format: 'pdf', object_size: 2457600, created_at: '2025-12-30T10:30:00', text_extracted: true },
        { catalog_id: 2, object_name: 'raw/structured/sales_data.csv', file_format: 'csv', object_size: 1024000, created_at: '2025-12-30T09:15:00', text_extracted: false },
        { catalog_id: 3, object_name: 'raw/structured/employees.json', file_format: 'json', object_size: 512000, created_at: '2025-12-30T08:45:00', text_extracted: false }
      ]);
      
      setLoading(false);
    }, 500);
  }, []);

  const formatBytes = (bytes) => {
    if (!bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
  };

  const formatDate = (dateString) => {
    const date = new Date(dateString);
    return date.toLocaleString();
  };

  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    setUploadProgress({ name: file.name, progress: 0 });
    
    // Simulate upload progress
    for (let i = 0; i <= 100; i += 10) {
      await new Promise(resolve => setTimeout(resolve, 200));
      setUploadProgress({ name: file.name, progress: i });
    }
    
    setTimeout(() => {
      setUploadProgress(null);
      alert('File uploaded successfully!');
    }, 500);
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center">
        <div className="text-white text-xl">Loading dashboard...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
      {/* Header */}
      <header className="bg-slate-800/50 backdrop-blur-sm border-b border-slate-700 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Database className="w-8 h-8 text-blue-400" />
              <div>
                <h1 className="text-2xl font-bold text-white">Data Lakehouse</h1>
                <p className="text-sm text-slate-400">Admin Dashboard</p>
              </div>
            </div>
            
            <div className="flex items-center gap-4">
              <button
                onClick={() => setView('dashboard')}
                className={`px-4 py-2 rounded-lg transition ${
                  view === 'dashboard' ? 'bg-blue-600 text-white' : 'text-slate-300 hover:bg-slate-700'
                }`}
              >
                Dashboard
              </button>
              <button
                onClick={() => setView('files')}
                className={`px-4 py-2 rounded-lg transition ${
                  view === 'files' ? 'bg-blue-600 text-white' : 'text-slate-300 hover:bg-slate-700'
                }`}
              >
                Files
              </button>
              <button
                onClick={() => setView('search')}
                className={`px-4 py-2 rounded-lg transition ${
                  view === 'search' ? 'bg-blue-600 text-white' : 'text-slate-300 hover:bg-slate-700'
                }`}
              >
                Search
              </button>
              
              <label className="cursor-pointer">
                <input
                  type="file"
                  className="hidden"
                  onChange={handleFileUpload}
                />
                <div className="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg transition">
                  <Upload className="w-4 h-4" />
                  <span>Upload</span>
                </div>
              </label>
            </div>
          </div>
        </div>
      </header>

      {/* Upload Progress */}
      {uploadProgress && (
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="bg-slate-800 rounded-lg p-4 border border-slate-700">
            <div className="flex items-center justify-between mb-2">
              <span className="text-white text-sm">Uploading: {uploadProgress.name}</span>
              <span className="text-blue-400 text-sm">{uploadProgress.progress}%</span>
            </div>
            <div className="w-full bg-slate-700 rounded-full h-2">
              <div
                className="bg-blue-600 h-2 rounded-full transition-all duration-300"
                style={{ width: `${uploadProgress.progress}%` }}
              />
            </div>
          </div>
        </div>
      )}

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-6 py-8">
        {view === 'dashboard' && (
          <>
            {/* Metrics Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
              <MetricCard
                title="Total Documents"
                value={metrics.total_documents.toLocaleString()}
                icon={<FileText className="w-6 h-6" />}
                color="blue"
              />
              <MetricCard
                title="Processed Today"
                value={metrics.processed_today}
                icon={<CheckCircle className="w-6 h-6" />}
                color="green"
              />
              <MetricCard
                title="Pending in Raw"
                value={metrics.raw_pending}
                icon={<Clock className="w-6 h-6" />}
                color="yellow"
              />
              <MetricCard
                title="Total Storage"
                value={`${metrics.total_storage_gb} GB`}
                icon={<HardDrive className="w-6 h-6" />}
                color="purple"
              />
            </div>

            {/* Charts Row */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
              {/* Processing Trend */}
              <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
                <h3 className="text-lg font-semibold text-white mb-4">Processing Trend (7 Days)</h3>
                <div className="space-y-2">
                  {metrics.processing_trend.map((day, idx) => (
                    <div key={idx} className="flex items-center gap-3">
                      <span className="text-slate-400 text-sm w-24">{day.date}</span>
                      <div className="flex-1 bg-slate-700 rounded-full h-6 relative">
                        <div
                          className="bg-gradient-to-r from-blue-500 to-blue-600 h-6 rounded-full flex items-center justify-end pr-2"
                          style={{ width: `${(day.count / 200) * 100}%` }}
                        >
                          <span className="text-white text-xs font-medium">{day.count}</span>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Files by Format */}
              <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
                <h3 className="text-lg font-semibold text-white mb-4">Files by Format</h3>
                <div className="space-y-3">
                  {metrics.files_by_format.map((format, idx) => (
                    <div key={idx} className="flex items-center justify-between p-3 bg-slate-700/50 rounded-lg">
                      <div className="flex items-center gap-3">
                        <div className={`w-3 h-3 rounded-full ${
                          ['bg-blue-500', 'bg-green-500', 'bg-yellow-500', 'bg-purple-500', 'bg-pink-500'][idx % 5]
                        }`} />
                        <span className="text-slate-300 font-medium uppercase">{format.file_format}</span>
                      </div>
                      <span className="text-white font-semibold">{format.count}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Recent Activity */}
            <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
              <h3 className="text-lg font-semibold text-white mb-4">Recent Activity</h3>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-slate-700">
                      <th className="text-left text-slate-400 font-medium py-3 px-4">File Name</th>
                      <th className="text-left text-slate-400 font-medium py-3 px-4">Format</th>
                      <th className="text-left text-slate-400 font-medium py-3 px-4">Size</th>
                      <th className="text-left text-slate-400 font-medium py-3 px-4">Status</th>
                      <th className="text-left text-slate-400 font-medium py-3 px-4">Date</th>
                    </tr>
                  </thead>
                  <tbody>
                    {files.slice(0, 5).map((file) => (
                      <tr key={file.catalog_id} className="border-b border-slate-700/50 hover:bg-slate-700/30 transition">
                        <td className="py-3 px-4 text-white">{file.object_name.split('/').pop()}</td>
                        <td className="py-3 px-4">
                          <span className="px-2 py-1 bg-blue-600/20 text-blue-400 rounded text-xs uppercase">
                            {file.file_format}
                          </span>
                        </td>
                        <td className="py-3 px-4 text-slate-300">{formatBytes(file.object_size)}</td>
                        <td className="py-3 px-4">
                          {file.text_extracted ? (
                            <span className="flex items-center gap-1 text-green-400 text-sm">
                              <CheckCircle className="w-4 h-4" /> Processed
                            </span>
                          ) : (
                            <span className="flex items-center gap-1 text-yellow-400 text-sm">
                              <Clock className="w-4 h-4" /> Pending
                            </span>
                          )}
                        </td>
                        <td className="py-3 px-4 text-slate-400 text-sm">{formatDate(file.created_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        )}

        {view === 'files' && (
          <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-xl font-semibold text-white">All Files</h3>
              <button className="flex items-center gap-2 px-4 py-2 bg-slate-700 hover:bg-slate-600 text-white rounded-lg transition">
                <RefreshCw className="w-4 h-4" />
                Refresh
              </button>
            </div>
            
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-slate-700">
                    <th className="text-left text-slate-400 font-medium py-3 px-4">File Name</th>
                    <th className="text-left text-slate-400 font-medium py-3 px-4">Format</th>
                    <th className="text-left text-slate-400 font-medium py-3 px-4">Size</th>
                    <th className="text-left text-slate-400 font-medium py-3 px-4">Status</th>
                    <th className="text-left text-slate-400 font-medium py-3 px-4">Created</th>
                    <th className="text-left text-slate-400 font-medium py-3 px-4">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {files.map((file) => (
                    <tr key={file.catalog_id} className="border-b border-slate-700/50 hover:bg-slate-700/30 transition">
                      <td className="py-3 px-4 text-white">{file.object_name}</td>
                      <td className="py-3 px-4">
                        <span className="px-2 py-1 bg-blue-600/20 text-blue-400 rounded text-xs uppercase">
                          {file.file_format}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-slate-300">{formatBytes(file.object_size)}</td>
                      <td className="py-3 px-4">
                        {file.text_extracted ? (
                          <span className="flex items-center gap-1 text-green-400 text-sm">
                            <CheckCircle className="w-4 h-4" /> Extracted
                          </span>
                        ) : (
                          <span className="flex items-center gap-1 text-slate-400 text-sm">
                            <Clock className="w-4 h-4" /> N/A
                          </span>
                        )}
                      </td>
                      <td className="py-3 px-4 text-slate-400 text-sm">{formatDate(file.created_at)}</td>
                      <td className="py-3 px-4">
                        <div className="flex gap-2">
                          <button className="p-2 hover:bg-slate-600 rounded text-slate-400 hover:text-white transition">
                            <Download className="w-4 h-4" />
                          </button>
                          <button className="p-2 hover:bg-red-600/20 rounded text-slate-400 hover:text-red-400 transition">
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {view === 'search' && (
          <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
            <h3 className="text-xl font-semibold text-white mb-6">Search Documents</h3>
            
            <div className="relative mb-6">
              <Search className="absolute left-4 top-1/2 transform -translate-y-1/2 text-slate-400 w-5 h-5" />
              <input
                type="text"
                placeholder="Search across all documents..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full pl-12 pr-4 py-3 bg-slate-700 border border-slate-600 rounded-lg text-white placeholder-slate-400 focus:outline-none focus:border-blue-500"
              />
            </div>

            <div className="text-center text-slate-400 py-12">
              <Search className="w-16 h-16 mx-auto mb-4 opacity-50" />
              <p>Enter a search query to find documents</p>
            </div>
          </div>
        )}
      </main>
    </div>
  );
};

const MetricCard = ({ title, value, icon, color }) => {
  const colors = {
    blue: 'from-blue-600 to-blue-700',
    green: 'from-green-600 to-green-700',
    yellow: 'from-yellow-600 to-yellow-700',
    purple: 'from-purple-600 to-purple-700'
  };

  return (
    <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
      <div className="flex items-center justify-between mb-4">
        <div className={`p-3 rounded-lg bg-gradient-to-br ${colors[color]}`}>
          {icon}
        </div>
        <TrendingUp className="w-5 h-5 text-green-400" />
      </div>
      <h3 className="text-slate-400 text-sm font-medium mb-1">{title}</h3>
      <p className="text-white text-3xl font-bold">{value}</p>
    </div>
  );
};

export default Dashboard;