// src/components/FileUpload.jsx
import React, { useState, useRef } from 'react';
import { Upload, File, X, CheckCircle2, AlertCircle, Loader2 } from 'lucide-react';
import { toast } from 'react-toastify';
import { dashboardAPI } from '../services/api';

const FileUpload = ({ onUploadSuccess }) => {
  const [dragActive, setDragActive] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [status, setStatus] = useState({ type: null, message: '' });
  const [selectedFile, setSelectedFile] = useState(null);
  const inputRef = useRef(null);

  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      setSelectedFile(e.dataTransfer.files[0]);
    }
  };

  const handleChange = (e) => {
    e.preventDefault();
    if (e.target.files && e.target.files[0]) {
      setSelectedFile(e.target.files[0]);
    }
  };

  const onButtonClick = () => {
    inputRef.current.click();
  };

  const clearFile = () => {
    setSelectedFile(null);
    setStatus({ type: null, message: '' });
  };

  const uploadFile = async () => {
    if (!selectedFile) return;

    setUploading(true);

    const formData = new FormData();
    formData.append('file', selectedFile);

    try {
      await dashboardAPI.uploadFile(formData);
      toast.success('Asset ingested successfully! Pipeline triggered.');
      setSelectedFile(null);
      onUploadSuccess?.();
    } catch (err) {
      console.error(err);
      toast.error(err.response?.data?.detail || 'Ingestion failed. System check required.');
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className="bg-gray-800/50 backdrop-blur-xl border border-gray-700/50 p-8 rounded-2xl shadow-2xl mb-10 overflow-hidden relative">
      <div className="flex flex-col items-center">
        <div className="mb-6 text-center">
          <h3 className="text-2xl font-bold text-white mb-2">Ingest Data</h3>
          <p className="text-gray-400">Upload CSV, JSON, Parquet, or Documents to your Lakehouse</p>
        </div>

        {!selectedFile ? (
          <div
            className={`w-full max-w-xl border-2 border-dashed rounded-2xl p-12 transition-all duration-300 flex flex-col items-center justify-center cursor-pointer
              ${dragActive ? 'border-blue-500 bg-blue-500/10' : 'border-gray-600 hover:border-gray-500 bg-gray-900/40'}`}
            onDragEnter={handleDrag}
            onDragLeave={handleDrag}
            onDragOver={handleDrag}
            onDrop={handleDrop}
            onClick={onButtonClick}
          >
            <input
              ref={inputRef}
              type="file"
              className="hidden"
              onChange={handleChange}
              accept=".csv,.json,.parquet,.pdf,.docx,.doc,.png,.jpg,.jpeg,.tiff"
            />
            <div className="bg-blue-600/20 p-4 rounded-full mb-4">
              <Upload className="w-8 h-8 text-blue-400" />
            </div>
            <p className="text-lg text-white font-medium mb-1">Click to upload or drag and drop</p>
            <p className="text-sm text-gray-500">Maximum file size: 200MB</p>
          </div>
        ) : (
          <div className="w-full max-w-xl">
            <div className="bg-gray-900/60 border border-gray-700 rounded-xl p-5 flex items-center mb-6">
              <div className="bg-blue-600/20 p-3 rounded-lg mr-4">
                <File className="w-6 h-6 text-blue-400" />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-white font-medium truncate">{selectedFile.name}</p>
                <p className="text-xs text-gray-500">{(selectedFile.size / (1024 * 1024)).toFixed(2)} MB</p>
              </div>
              {!uploading && (
                <button
                  onClick={clearFile}
                  className="p-2 hover:bg-gray-800 rounded-full transition-colors"
                >
                  <X className="w-5 h-5 text-gray-400" />
                </button>
              )}
            </div>

            {/* Removed internal status display in favor of toasts */}

            <button
              onClick={uploadFile}
              disabled={uploading}
              className={`w-full py-4 rounded-xl font-bold flex items-center justify-center transition-all shadow-lg
                ${uploading ? 'bg-gray-700 cursor-not-allowed' : 'bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500 transform hover:-translate-y-1'}`}
            >
              {uploading ? (
                <>
                  <Loader2 className="w-5 h-5 mr-3 animate-spin" />
                  Processing Pipeline...
                </>
              ) : (
                'Start Ingestion'
              )}
            </button>
          </div>
        )}
      </div>

      {/* Background purely aesthetic elements */}
      <div className="absolute top-0 right-0 -mr-20 -mt-20 w-64 h-64 bg-blue-600/10 rounded-full blur-3xl pointer-events-none"></div>
      <div className="absolute bottom-0 left-0 -ml-20 -mb-20 w-64 h-64 bg-indigo-600/10 rounded-full blur-3xl pointer-events-none"></div>
    </div>
  );
};

export default FileUpload;