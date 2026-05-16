// src/components/FileBrowser.jsx
import React, { useState, useEffect, useContext } from 'react';
import {
  Folder, FileText, Image as ImageIcon, FileCode, Table as TableIcon,
  File as FileIcon, ChevronRight, Home, RefreshCw, Search,
  Database, HardDrive, Calendar, User, Trash2, Download, Info,
  ArrowLeft, Layers, FileSpreadsheet, FileJson, FileType2,
  ChevronLeft, X
} from 'lucide-react';
import { toast } from 'react-toastify';
import { dashboardAPI } from '../services/api';
import AuthContext from '../context/AuthContext';
import FilePreviewModal from './FilePreviewModal';

const FileBrowser = () => {
  const { auth } = useContext(AuthContext);
  const isAdmin = auth.user?.role === 'admin';

  // Navigation state
  const [view, setView] = useState('buckets'); // 'buckets', 'browser'
  const [currentBucket, setCurrentBucket] = useState(null);
  const [currentPrefix, setCurrentPrefix] = useState('');
  const [breadcrumbs, setBreadcrumbs] = useState([]);

  // Data state
  const [buckets, setBuckets] = useState([]);
  const [folders, setFolders] = useState([]);
  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState('');

  // Selection state
  const [selectedItem, setSelectedItem] = useState(null);
  const [selectedFileDetails, setSelectedFileDetails] = useState(null);

  // Preview modal state
  const [previewFile, setPreviewFile] = useState(null);

  // Fetch buckets on mount
  useEffect(() => {
    fetchBuckets();
  }, []);

  // Fetch folder contents when bucket/prefix changes
  useEffect(() => {
    if (currentBucket) {
      fetchFolderContents();
    }
  }, [currentBucket, currentPrefix]);

  const fetchBuckets = async () => {
    setLoading(true);
    try {
      const res = await dashboardAPI.getBuckets();
      setBuckets(res.data.buckets || []);
    } catch (err) {
      console.error('Failed to fetch buckets:', err);
      toast.error('Failed to load buckets');
    } finally {
      setLoading(false);
    }
  };

  const fetchFolderContents = async () => {
    setLoading(true);
    try {
      const res = await dashboardAPI.browseFolder(currentBucket, currentPrefix, searchQuery);
      setFolders(res.data.folders || []);
      setFiles(res.data.files || []);
      setBreadcrumbs(res.data.breadcrumbs || []);
    } catch (err) {
      console.error('Failed to fetch folder contents:', err);
      toast.error('Failed to load folder contents');
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = (e) => {
    e.preventDefault();
    if (currentBucket) {
      fetchFolderContents();
    }
  };

  const enterBucket = (bucket) => {
    setCurrentBucket(bucket.bucket_name);
    setCurrentPrefix('');
    setView('browser');
    setSelectedItem(null);
    setSelectedFileDetails(null);
  };

  const enterFolder = (folder) => {
    setCurrentPrefix(folder.path);
    setSelectedItem(null);
    setSelectedFileDetails(null);
  };

  const navigateToBreadcrumb = (index) => {
    if (index === -1) {
      // Go back to buckets view
      setView('buckets');
      setCurrentBucket(null);
      setCurrentPrefix('');
      setBreadcrumbs([]);
    } else {
      const crumb = breadcrumbs[index];
      if (crumb.is_bucket) {
        setCurrentPrefix('');
      } else {
        setCurrentPrefix(crumb.path);
      }
    }
    setSelectedItem(null);
    setSelectedFileDetails(null);
  };

  const goBack = () => {
    if (currentPrefix) {
      // Go up one level
      const parts = currentPrefix.replace(/\/$/, '').split('/');
      parts.pop();
      const newPrefix = parts.length > 0 ? parts.join('/') + '/' : '';
      setCurrentPrefix(newPrefix);
    } else {
      // Go back to buckets
      setView('buckets');
      setCurrentBucket(null);
    }
    setSelectedItem(null);
    setSelectedFileDetails(null);
  };

  const selectFile = async (file) => {
    setSelectedItem({ type: 'file', ...file });
    setSelectedFileDetails(file);

    // Fetch full details if needed
    try {
      const res = await dashboardAPI.getFileDetails(file.catalog_id);
      setSelectedFileDetails(res.data);
    } catch (err) {
      console.error('Failed to fetch file details:', err);
    }
  };

  const previewFileOnDoubleClick = async (file) => {
    setPreviewFile(file);
  };

  const closePreviewModal = () => {
    setPreviewFile(null);
  };

  const selectFolder = (folder) => {
    setSelectedItem({ type: 'folder', ...folder });
    setSelectedFileDetails(null);
  };

  const handleDelete = async () => {
    if (!selectedItem || selectedItem.type !== 'file') return;

    const fileName = selectedItem.object_name || selectedItem.display_name;
    if (!window.confirm(`Are you sure you want to delete "${fileName}"?`)) return;

    try {
      await dashboardAPI.deleteFile(selectedItem.catalog_id);
      toast.success('File deleted successfully');
      setSelectedItem(null);
      setSelectedFileDetails(null);
      fetchFolderContents();
    } catch (err) {
      toast.error('Failed to delete file');
    }
  };

  const handleDownload = async () => {
    if (!selectedItem || selectedItem.type !== 'file') return;

    try {
      const response = await dashboardAPI.downloadFile(selectedItem.catalog_id);
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', selectedItem.display_name || selectedItem.object_name.split('/').pop());
      document.body.appendChild(link);
      link.click();
      link.remove();
    } catch (err) {
      toast.error('Failed to download file');
    }
  };

  const formatSize = (bytes) => {
    if (!bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return 'N/A';
    return new Date(dateStr).toLocaleString('en-US', {
      month: 'short', day: 'numeric', year: 'numeric',
      hour: '2-digit', minute: '2-digit'
    });
  };

  const getFileIcon = (format, size = 'md') => {
    const sizeClasses = {
      sm: 'w-6 h-6',
      md: 'w-10 h-10',
      lg: 'w-16 h-16'
    };
    const className = sizeClasses[size];

    switch (format?.toLowerCase()) {
      case 'csv':
      case 'parquet':
      case 'structured':
        return <FileSpreadsheet className={`${className} text-emerald-400`} />;
      case 'json':
        return <FileJson className={`${className} text-amber-400`} />;
      case 'pdf':
        return <FileType2 className={`${className} text-red-400`} />;
      case 'docx':
      case 'doc':
        return <FileText className={`${className} text-blue-400`} />;
      case 'ppt':
      case 'pptx':
        return <FileText className={`${className} text-orange-400`} />;
      case 'png':
      case 'jpg':
      case 'jpeg':
      case 'image':
        return <ImageIcon className={`${className} text-purple-400`} />;
      default:
        return <FileIcon className={`${className} text-slate-400`} />;
    }
  };

  // ==================== BUCKETS VIEW ====================
  if (view === 'buckets') {
    return (
      <div className="h-full flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h3 className="text-2xl font-bold text-white">Storage Buckets</h3>
            <p className="text-slate-400 text-sm mt-1">Select a bucket to browse files</p>
          </div>
          <button
            onClick={fetchBuckets}
            className="p-2 text-slate-400 hover:text-white hover:bg-white/10 rounded-lg transition-all"
            title="Refresh"
          >
            <RefreshCw className={`w-5 h-5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>

        {/* Buckets Grid */}
        {loading ? (
          <div className="flex-1 flex items-center justify-center">
            <div className="text-slate-400">Loading buckets...</div>
          </div>
        ) : buckets.length === 0 ? (
          <div className="flex-1 flex flex-col items-center justify-center text-slate-500">
            <Database className="w-16 h-16 mb-4 opacity-20" />
            <p className="text-lg">No buckets available</p>
            <p className="text-sm">Upload files to create buckets</p>
          </div>
        ) : (
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
            {buckets.map((bucket) => (
              <div
                key={bucket.bucket_name}
                onClick={() => enterBucket(bucket)}
                onDoubleClick={() => enterBucket(bucket)}
                className="group cursor-pointer bg-slate-800/50 border border-white/5 hover:border-blue-500/30
                         rounded-xl p-5 transition-all hover:bg-slate-800 hover:shadow-lg hover:shadow-blue-500/5"
              >
                <div className="flex items-start justify-between mb-4">
                  <div className="p-3 bg-blue-500/10 rounded-xl group-hover:bg-blue-500/20 transition-colors">
                    <Database className="w-8 h-8 text-blue-400" />
                  </div>
                  <span className="text-xs text-slate-500 bg-slate-900/50 px-2 py-1 rounded">
                    {bucket.file_count} files
                  </span>
                </div>

                <h4 className="text-white font-semibold truncate mb-1" title={bucket.bucket_name}>
                  {bucket.bucket_name}
                </h4>

                <div className="space-y-1 text-xs text-slate-400">
                  <div className="flex items-center">
                    <HardDrive className="w-3 h-3 mr-1.5" />
                    {formatSize(bucket.total_size)}
                  </div>
                  <div className="flex items-center">
                    <Calendar className="w-3 h-3 mr-1.5" />
                    {formatDate(bucket.latest_upload)}
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  // ==================== FILE BROWSER VIEW ====================
  return (
    <div className="h-full flex flex-col">
      {/* Header with Breadcrumbs and Search */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-4">
        {/* Breadcrumbs */}
        <div className="flex items-center gap-1 text-sm overflow-x-auto">
          <button
            onClick={goBack}
            className="p-1.5 text-slate-400 hover:text-white hover:bg-white/10 rounded-lg transition-all mr-1"
            title="Go back"
          >
            <ArrowLeft className="w-4 h-4" />
          </button>

          <button
            onClick={() => navigateToBreadcrumb(-1)}
            className="flex items-center text-slate-400 hover:text-white transition-colors px-2 py-1 rounded hover:bg-white/5"
          >
            <Home className="w-4 h-4" />
          </button>

          {breadcrumbs.map((crumb, index) => (
            <React.Fragment key={index}>
              <ChevronRight className="w-4 h-4 text-slate-600" />
              <button
                onClick={() => navigateToBreadcrumb(index)}
                className={`px-2 py-1 rounded transition-colors whitespace-nowrap ${
                  index === breadcrumbs.length - 1
                    ? 'text-white font-medium bg-white/10'
                    : 'text-slate-400 hover:text-white hover:bg-white/5'
                }`}
              >
                {crumb.is_bucket ? crumb.name : crumb.name.replace('/', '')}
              </button>
            </React.Fragment>
          ))}
        </div>

        {/* Search */}
        <form onSubmit={handleSearch} className="flex gap-2">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
            <input
              type="text"
              placeholder="Search in folder..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="bg-slate-900/50 border border-white/10 rounded-lg pl-9 pr-4 py-2 text-sm text-white
                       placeholder-slate-500 focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500/30 outline-none"
            />
          </div>
          <button
            type="submit"
            className="px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium rounded-lg transition-colors"
          >
            Search
          </button>
          <button
            onClick={fetchFolderContents}
            className="p-2 text-slate-400 hover:text-white hover:bg-white/10 rounded-lg transition-all"
            title="Refresh"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </form>
      </div>

      {/* Stats Bar */}
      <div className="flex items-center gap-4 mb-4 text-xs text-slate-400">
        <span className="flex items-center gap-1">
          <Folder className="w-3.5 h-3.5" />
          {folders.length} folder{folders.length !== 1 ? 's' : ''}
        </span>
        <span className="flex items-center gap-1">
          <FileIcon className="w-3.5 h-3.5" />
          {files.length} file{files.length !== 1 ? 's' : ''}
        </span>
        <span className="flex items-center gap-1">
          <HardDrive className="w-3.5 h-3.5" />
          {formatSize(
            [...folders, ...files].reduce((acc, item) => acc + (item.total_size || item.object_size || 0), 0)
          )} total
        </span>
      </div>

      {/* Main Content Area */}
      <div className="flex-1 flex gap-4 min-h-0">
        {/* File/Folder Grid */}
        <div className={`flex-1 overflow-y-auto ${selectedItem ? 'lg:pr-[320px]' : ''}`}>
          {loading ? (
            <div className="flex items-center justify-center h-64">
              <div className="text-slate-400">Loading...</div>
            </div>
          ) : folders.length === 0 && files.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-64 text-slate-500">
              <Folder className="w-16 h-16 mb-4 opacity-20" />
              <p className="text-lg">This folder is empty</p>
            </div>
          ) : (
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-3">
              {/* Folders */}
              {folders.map((folder) => (
                <div
                  key={folder.path}
                  onClick={() => selectFolder(folder)}
                  onDoubleClick={() => enterFolder(folder)}
                  className={`group cursor-pointer rounded-xl p-4 transition-all border ${
                    selectedItem?.type === 'folder' && selectedItem?.path === folder.path
                      ? 'bg-blue-500/20 border-blue-500/50'
                      : 'bg-slate-800/50 border-white/5 hover:border-blue-500/30 hover:bg-slate-800'
                  }`}
                >
                  <div className="flex flex-col items-center text-center">
                    <div className="p-3 mb-3 rounded-xl bg-amber-500/10 group-hover:bg-amber-500/20 transition-colors">
                      <Folder className="w-10 h-10 text-amber-400" />
                    </div>
                    <p className="text-sm font-medium text-white truncate w-full" title={folder.name}>
                      {folder.name.replace('/', '')}
                    </p>
                    <div className="mt-2 text-xs text-slate-400 space-y-0.5">
                      <div>{folder.file_count} items</div>
                      <div>{formatSize(folder.total_size)}</div>
                    </div>
                    <div className="mt-2 flex flex-wrap gap-1 justify-center">
                      {folder.formats?.slice(0, 3).map((fmt) => (
                        <span key={fmt} className="text-[10px] px-1.5 py-0.5 bg-slate-700/50 rounded text-slate-300">
                          {fmt}
                        </span>
                      ))}
                      {folder.formats?.length > 3 && (
                        <span className="text-[10px] px-1.5 py-0.5 bg-slate-700/50 rounded text-slate-300">
                          +{folder.formats.length - 3}
                        </span>
                      )}
                    </div>
                  </div>
                </div>
              ))}

              {/* Files */}
              {files.map((file) => (
                <div
                  key={file.catalog_id}
                  onClick={() => selectFile(file)}
                  onDoubleClick={() => previewFileOnDoubleClick(file)}
                  className={`group cursor-pointer rounded-xl p-4 transition-all border ${
                    selectedItem?.type === 'file' && selectedItem?.catalog_id === file.catalog_id
                      ? 'bg-blue-500/20 border-blue-500/50'
                      : 'bg-slate-800/50 border-white/5 hover:border-blue-500/30 hover:bg-slate-800'
                  }`}
                >
                  <div className="flex flex-col items-center text-center">
                    <div className="p-3 mb-3 rounded-xl bg-slate-700/30 group-hover:bg-slate-700/50 transition-colors">
                      {getFileIcon(file.file_format, 'md')}
                    </div>
                    <p className="text-sm font-medium text-white truncate w-full" title={file.display_name}>
                      {file.display_name}
                    </p>
                    <div className="mt-2 text-xs text-slate-400 space-y-0.5">
                      <div>{formatSize(file.object_size)}</div>
                      <div className="text-slate-500">{file.file_format?.toUpperCase() || 'UNKNOWN'}</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Right Side Panel */}
        {selectedItem && (
          <div className="absolute right-0 top-0 bottom-0 w-[320px] bg-slate-900 border-l border-white/10 overflow-y-auto">
            {/* Panel Header */}
            <div className="flex items-center justify-between p-4 border-b border-white/10">
              <h4 className="font-semibold text-white">Details</h4>
              <button
                onClick={() => setSelectedItem(null)}
                className="p-1.5 text-slate-400 hover:text-white hover:bg-white/10 rounded-lg transition-all"
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            {/* Panel Content */}
            <div className="p-4">
              {/* Icon and Name */}
              <div className="flex flex-col items-center mb-6">
                <div className="p-4 rounded-2xl bg-slate-800/50 mb-3">
                  {selectedItem.type === 'folder' ? (
                    <Folder className="w-16 h-16 text-amber-400" />
                  ) : (
                    getFileIcon(selectedItem.file_format, 'lg')
                  )}
                </div>
                <h5 className="text-lg font-semibold text-white text-center break-all" title={selectedItem.name || selectedItem.display_name}>
                  {selectedItem.type === 'folder'
                    ? selectedItem.name.replace('/', '')
                    : selectedItem.display_name}
                </h5>
                <span className="text-xs text-slate-400 mt-1">
                  {selectedItem.type === 'folder' ? 'Folder' : (selectedItem.file_format?.toUpperCase() || 'File')}
                </span>
              </div>

              {/* Folder Stats */}
              {selectedItem.type === 'folder' && (
                <div className="space-y-3 mb-6">
                  <div className="p-3 bg-slate-800/50 rounded-lg">
                    <div className="text-xs text-slate-400 mb-1">Contents</div>
                    <div className="text-white font-medium">{selectedItem.file_count} files</div>
                  </div>
                  <div className="p-3 bg-slate-800/50 rounded-lg">
                    <div className="text-xs text-slate-400 mb-1">Total Size</div>
                    <div className="text-white font-medium">{formatSize(selectedItem.total_size)}</div>
                  </div>
                  <div className="p-3 bg-slate-800/50 rounded-lg">
                    <div className="text-xs text-slate-400 mb-1">Last Modified</div>
                    <div className="text-white font-medium">{formatDate(selectedItem.latest_modified)}</div>
                  </div>
                  <div className="p-3 bg-slate-800/50 rounded-lg">
                    <div className="text-xs text-slate-400 mb-1">Full Path</div>
                    <div className="text-white font-mono text-xs break-all">{selectedItem.path}</div>
                  </div>

                  <button
                    onClick={() => enterFolder(selectedItem)}
                    className="w-full py-2.5 bg-blue-600 hover:bg-blue-500 text-white font-medium rounded-lg transition-colors"
                  >
                    Open Folder
                  </button>
                </div>
              )}

              {/* File Details */}
              {selectedItem.type === 'file' && selectedFileDetails && (
                <div className="space-y-3">
                  {/* Basic Info */}
                  <div className="p-3 bg-slate-800/50 rounded-lg">
                    <div className="text-xs text-slate-400 mb-1">Size</div>
                    <div className="text-white font-medium">
                      {formatSize(selectedFileDetails.catalog?.object_size || selectedFileDetails.minio_info?.size)}
                    </div>
                  </div>

                  <div className="p-3 bg-slate-800/50 rounded-lg">
                    <div className="text-xs text-slate-400 mb-1">Format</div>
                    <div className="text-white font-medium">
                      {selectedFileDetails.catalog?.file_format?.toUpperCase() || 'UNKNOWN'}
                    </div>
                  </div>

                  {selectedFileDetails.catalog?.row_count && (
                    <div className="p-3 bg-slate-800/50 rounded-lg">
                      <div className="text-xs text-slate-400 mb-1">Row Count</div>
                      <div className="text-white font-medium">
                        {selectedFileDetails.catalog.row_count.toLocaleString()} rows
                      </div>
                    </div>
                  )}

                  <div className="p-3 bg-slate-800/50 rounded-lg">
                    <div className="text-xs text-slate-400 mb-1">Bucket</div>
                    <div className="text-white font-medium">
                      {selectedFileDetails.catalog?.bucket_name}
                    </div>
                  </div>

                  <div className="p-3 bg-slate-800/50 rounded-lg">
                    <div className="text-xs text-slate-400 mb-1">Full Path</div>
                    <div className="text-white font-mono text-xs break-all">
                      {selectedFileDetails.catalog?.object_name}
                    </div>
                  </div>

                  <div className="p-3 bg-slate-800/50 rounded-lg">
                    <div className="text-xs text-slate-400 mb-1">Uploaded</div>
                    <div className="text-white font-medium">
                      {formatDate(selectedFileDetails.catalog?.created_at)}
                    </div>
                  </div>

                  <div className="p-3 bg-slate-800/50 rounded-lg">
                    <div className="text-xs text-slate-400 mb-1">Text Extracted</div>
                    <div className={`font-medium ${selectedFileDetails.catalog?.text_extracted ? 'text-emerald-400' : 'text-slate-400'}`}>
                      {selectedFileDetails.catalog?.text_extracted ? 'Yes' : 'No'}
                    </div>
                  </div>

                  {selectedFileDetails.catalog?.metadata && (
                    <div className="p-3 bg-slate-800/50 rounded-lg">
                      <div className="text-xs text-slate-400 mb-2">Metadata</div>
                      <div className="space-y-1">
                        {Object.entries(selectedFileDetails.catalog.metadata).slice(0, 5).map(([key, value]) => (
                          <div key={key} className="flex justify-between text-xs">
                            <span className="text-slate-400 capitalize">{key.replace(/_/g, ' ')}:</span>
                            <span className="text-white truncate max-w-[150px]" title={String(value)}>
                              {String(value).length > 20 ? String(value).slice(0, 20) + '...' : String(value)}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Actions */}
                  <div className="pt-4 border-t border-white/10 space-y-2">
                    <button
                      onClick={handleDownload}
                      className="w-full flex items-center justify-center gap-2 py-2.5 bg-blue-600 hover:bg-blue-500 text-white font-medium rounded-lg transition-colors"
                    >
                      <Download className="w-4 h-4" />
                      Download
                    </button>
                    <button
                      onClick={handleDelete}
                      className="w-full flex items-center justify-center gap-2 py-2.5 bg-red-600/20 hover:bg-red-600/30 text-red-400 hover:text-red-300 font-medium rounded-lg transition-colors border border-red-500/30"
                    >
                      <Trash2 className="w-4 h-4" />
                      Delete
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* File Preview Modal */}
      {previewFile && (
        <FilePreviewModal
          file={previewFile}
          onClose={closePreviewModal}
        />
      )}
    </div>
  );
};

export default FileBrowser;
