// src/components/FileUpload.jsx
import React, { useState, useRef } from 'react';
import { Upload, File, X, Loader2, Plus } from 'lucide-react';
import { toast } from 'react-toastify';
import { dashboardAPI } from '../services/api';

const FileUpload = ({ onUploadSuccess }) => {
  const [dragActive, setDragActive] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState(0);
  const [files, setFiles] = useState([]); // ✅ array of File objects

  const inputRef = useRef(null);

  /* -------------------- Drag handlers -------------------- */
  const handleDrag = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(e.type === 'dragenter' || e.type === 'dragover');
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files?.length) {
      addFiles(e.dataTransfer.files);
    }
  };

  const handleChange = (e) => {
    if (e.target.files?.length) {
      addFiles(e.target.files);
    }
  };

  /* -------------------- File helpers -------------------- */
  const addFiles = (fileList) => {
    const newFiles = Array.from(fileList);
    setFiles((prev) => [...prev, ...newFiles]);
  };

  const removeFile = (index) => {
    setFiles((prev) => prev.filter((_, i) => i !== index));
  };

  const clearAll = () => {
    setFiles([]);
    setProgress(0);
  };

  /* -------------------- Upload -------------------- */
  const uploadFiles = async () => {
    if (!files.length) return;

    setUploading(true);
    setProgress(0);

    const formData = new FormData();
    files.forEach((file) => {
      formData.append('files', file);
    });

    try {
      await dashboardAPI.uploadFile(formData, {
        onUploadProgress: (event) => {
          const percent = Math.round((event.loaded * 100) / event.total);
          setProgress(percent);
        },
      });

      toast.success('Assets ingested successfully!');
      clearAll();
      onUploadSuccess?.();
    } catch (err) {
      console.error(err);
      toast.error(
        err.response?.data?.detail?.message ||
        'Ingestion failed. System check required.'
      );
    } finally {
      setUploading(false);
    }
  };

  /* -------------------- UI -------------------- */
  return (
    <div className="bg-gray-800/50 backdrop-blur-xl border border-gray-700/50 p-8 rounded-2xl shadow-2xl mb-10 relative">

      {/* Header */}
      <div className="text-center mb-6">
        <h3 className="text-2xl font-bold text-white mb-1">Ingest Data</h3>
        <p className="text-gray-400">Upload files to your Lakehouse</p>
      </div>

      {/* Drop zone */}
      <div
        className={`w-full border-2 border-dashed rounded-2xl p-10 transition-all cursor-pointer
          ${dragActive ? 'border-blue-500 bg-blue-500/10' : 'border-gray-600 bg-gray-900/40 hover:border-gray-500'}`}
        onDragEnter={handleDrag}
        onDragLeave={handleDrag}
        onDragOver={handleDrag}
        onDrop={handleDrop}
        onClick={() => inputRef.current.click()}
      >
        <input
          ref={inputRef}
          type="file"
          multiple
          className="hidden"
          onChange={handleChange}
          accept=".csv,.json,.parquet,.pdf,.docx,.doc,.png,.jpg,.jpeg,.tiff"
        />

        <div className="flex flex-col items-center">
          <div className="bg-blue-600/20 p-4 rounded-full mb-4">
            <Upload className="w-8 h-8 text-blue-400" />
          </div>
          <p className="text-white font-medium">
            Click or drag files here
          </p>
          <p className="text-sm text-gray-500 mt-1">
            {files.length
              ? `${files.length} file(s) selected`
              : 'Max size: 200MB per file'}
          </p>
        </div>
      </div>

      {/* File list */}
      {files.length > 0 && (
        <div className="mt-6 space-y-3 max-h-64 overflow-y-auto">
          {files.map((file, idx) => (
            <div
              key={idx}
              className="flex items-center bg-gray-900/60 border border-gray-700 rounded-xl p-4"
            >
              <File className="w-5 h-5 text-blue-400 mr-3" />
              <div className="flex-1 min-w-0">
                <p className="text-white truncate">{file.name}</p>
                <p className="text-xs text-gray-500">
                  {(file.size / (1024 * 1024)).toFixed(2)} MB
                </p>
              </div>
              {!uploading && (
                <button
                  onClick={() => removeFile(idx)}
                  className="p-2 hover:bg-gray-800 rounded-full"
                >
                  <X className="w-4 h-4 text-gray-400" />
                </button>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Progress bar */}
      {uploading && (
        <div className="mt-6">
          <div className="w-full bg-gray-700 rounded-full h-2 overflow-hidden">
            <div
              className="bg-blue-500 h-2 transition-all"
              style={{ width: `${progress}%` }}
            />
          </div>
          <p className="text-sm text-gray-400 mt-2 text-center">
            Uploading… {progress}%
          </p>
        </div>
      )}

      {/* Actions */}
      {files.length > 0 && (
        <div className="mt-6 flex gap-3">
          <button
            onClick={uploadFiles}
            disabled={uploading}
            className={`flex-1 py-3 rounded-xl font-bold flex items-center justify-center
              ${uploading
                ? 'bg-gray-700 cursor-not-allowed'
                : 'bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-500 hover:to-indigo-500'
              }`}
          >
            {uploading ? (
              <>
                <Loader2 className="w-5 h-5 mr-2 animate-spin" />
                Processing…
              </>
            ) : (
              'Start Ingestion'
            )}
          </button>

          {!uploading && (
            <button
              onClick={() => inputRef.current.click()}
              className="px-4 rounded-xl bg-gray-700 hover:bg-gray-600 flex items-center"
            >
              <Plus className="w-5 h-5 text-white" />
            </button>
          )}
        </div>
      )}
    </div>
  );
};

export default FileUpload;