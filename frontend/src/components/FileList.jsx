// src/components/FileList.jsx
import React, { useState, useContext } from 'react';
import {
  FileCode, FileText, Image as ImageIcon,
  Table as TableIcon, File as FileIcon,
  ExternalLink, Calendar, HardDrive,
  Trash2, User, ChevronDown, ChevronUp, Info, Download
} from 'lucide-react';
import { toast } from 'react-toastify';
import { dashboardAPI } from '../services/api';
import AuthContext from '../context/AuthContext';

const FileList = ({ files, onRefresh }) => {
  const { auth } = useContext(AuthContext);
  const isAdmin = auth.user?.role === 'admin';
  const [expandedRow, setExpandedRow] = useState(null);

  const getFileIcon = (format) => {
    switch (format?.toLowerCase()) {
      case 'csv':
      case 'parquet':
        return <TableIcon className="w-5 h-5 text-emerald-400" />;
      case 'json':
        return <FileCode className="w-5 h-5 text-amber-400" />;
      case 'pdf':
        return <FileText className="w-5 h-5 text-red-500" />;
      case 'png':
      case 'jpg':
      case 'jpeg':
        return <ImageIcon className="w-5 h-5 text-purple-400" />;
      default:
        return <FileIcon className="w-5 h-5 text-blue-400" />;
    }
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return 'N/A';
    return new Date(dateStr).toLocaleDateString('en-US', {
      month: 'short', day: 'numeric', year: 'numeric',
      hour: '2-digit', minute: '2-digit'
    });
  };

  const formatSize = (bytes) => {
    if (!bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
  };

  const handleDownload = async (id, name) => {
    try {
      const response = await dashboardAPI.downloadFile(id);
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', name.split('/').pop());
      document.body.appendChild(link);
      link.click();
      link.remove();
    } catch (err) {
      toast.error('Failed to download asset');
    }
  };

  const handleDelete = async (id, name) => {
    if (!window.confirm(`Are you sure you want to delete ${name}?`)) return;
    try {
      await dashboardAPI.deleteFile(id);
      toast.success('Asset deleted successfully');
      onRefresh?.();
    } catch (err) {
      toast.error('Failed to delete asset');
    }
  };

  if (!files || files.length === 0) {
    return (
      <div className="p-20 text-center text-gray-400">
        <FileIcon className="w-12 h-12 mx-auto mb-4 opacity-20" />
        <p className="text-lg">No assets found</p>
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left border-collapse">
        <thead>
          <tr className="border-b border-white/5 bg-white/5">
            <th className="p-4 text-xs font-semibold text-gray-500 uppercase">Asset</th>
            <th className="p-4 text-xs font-semibold text-gray-500 uppercase">Details</th>
            {isAdmin && <th className="p-4 text-xs font-semibold text-gray-500 uppercase">Uploader</th>}
            <th className="p-4 text-xs font-semibold text-gray-500 uppercase text-right">Actions</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {files.map((file) => (
            <React.Fragment key={file.catalog_id}>
              <tr className="hover:bg-white/5 transition-colors group">
                <td className="p-4">
                  <div className="flex items-center">
                    <div className="mr-3 p-2 bg-gray-800 rounded-lg group-hover:bg-gray-700 transition-colors">
                      {getFileIcon(file.file_format)}
                    </div>
                    <div>
                      <p className="text-sm font-semibold text-white truncate max-w-[200px]" title={file.object_name}>
                        {file.object_name.split('/').pop()}
                      </p>
                      <p className="text-[10px] text-gray-500 font-mono">{file.file_format?.toUpperCase()}</p>
                    </div>
                  </div>
                </td>
                <td className="p-4">
                  <div className="space-y-1">
                    <div className="flex items-center text-xs text-gray-300">
                      <HardDrive className="w-3 h-3 mr-1 text-gray-500" />
                      {formatSize(file.object_size)}
                    </div>
                    <div className="flex items-center text-[10px] text-gray-500">
                      <Calendar className="w-3 h-3 mr-1 text-gray-500" />
                      {formatDate(file.created_at)}
                    </div>
                  </div>
                </td>
                {isAdmin && (
                  <td className="p-4">
                    <div className="flex items-center text-xs text-blue-400 font-medium bg-blue-500/10 px-2 py-1 rounded-md w-fit">
                      <User className="w-3 h-3 mr-1" />
                      {file.uploaded_by_user || 'System'}
                    </div>
                  </td>
                )}
                <td className="p-4 text-right">
                  <div className="flex items-center justify-end gap-2">
                    <button
                      onClick={() => setExpandedRow(expandedRow === file.catalog_id ? null : file.catalog_id)}
                      className="p-2 text-gray-400 hover:text-white hover:bg-white/10 rounded-lg transition-all"
                      title="Metadata"
                    >
                      <Info className="w-4 h-4" />
                    </button>
                    <button
                      onClick={() => handleDownload(file.catalog_id, file.object_name)}
                      className="p-2 text-gray-400 hover:text-blue-400 hover:bg-blue-400/10 rounded-lg transition-all"
                      title="Download Asset"
                    >
                      <Download className="w-4 h-4" />
                    </button>
                    <button
                      onClick={() => handleDelete(file.catalog_id, file.object_name)}
                      className="p-2 text-gray-400 hover:text-red-400 hover:bg-red-400/10 rounded-lg transition-all"
                      title="Delete Asset"
                    >
                      <Trash2 className="w-4 h-4" />
                    </button>
                  </div>
                </td>
              </tr>
              {expandedRow === file.catalog_id && (
                <tr className="bg-gray-900/50">
                  <td colSpan={isAdmin ? 4 : 3} className="p-4 border-l-2 border-blue-500">
                    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 text-xs">
                      <div>
                        <p className="text-gray-500 mb-1">Full Path</p>
                        <p className="text-gray-300 break-all font-mono">{file.object_name}</p>
                      </div>
                      <div>
                        <p className="text-gray-500 mb-1">MIME Type</p>
                        <p className="text-gray-300">{file.metadata?.mime_type || 'Unknown'}</p>
                      </div>
                      <div>
                        <p className="text-gray-500 mb-1">Tier</p>
                        <span className="px-2 py-0.5 bg-amber-500/10 text-amber-500 rounded text-[10px] uppercase font-bold">
                          {file.metadata?.enterprise_tier || 'Standard'}
                        </span>
                      </div>
                      <div>
                        <p className="text-gray-500 mb-1">Upload Source</p>
                        <p className="text-gray-300">API Gateway v2</p>
                      </div>
                    </div>
                  </td>
                </tr>
              )}
            </React.Fragment>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default FileList;