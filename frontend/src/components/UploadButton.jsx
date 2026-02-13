import React, { useRef, useState } from 'react';
import { Upload, X } from 'lucide-react';
import { dashboardAPI } from '../services/api';

const UploadButton = ({ onUploadSuccess }) => {
  const fileInputRef = useRef(null);

  const [open, setOpen] = useState(false);
  const [uploads, setUploads] = useState([]);

  const handleFileSelect = (e) => {
    const files = Array.from(e.target.files);

    const newUploads = files.map((file) => ({
      file,
      progress: 0,
      status: 'queued' // queued | uploading | done | error
    }));

    setUploads((prev) => [...prev, ...newUploads]);
    e.target.value = null;
  };

  const uploadFile = async (uploadItems) => {
    // If not array, wrap it (legacy support if needed, but we call it with array now)
    const items = Array.isArray(uploadItems) ? uploadItems : [uploadItems];
    if (items.length === 0) return;

    const formData = new FormData();
    items.forEach(item => {
      formData.append('files', item.file);
    });

    try {
      // Update status to uploading
      setUploads((u) =>
        u.map((item) =>
          items.some(i => i.file === item.file) ? { ...item, status: 'uploading' } : item
        )
      );

      await dashboardAPI.uploadFile(formData);

      // Update status to done
      setUploads((u) =>
        u.map((item) =>
          items.some(i => i.file === item.file) ? { ...item, progress: 100, status: 'done' } : item
        )
      );

      onUploadSuccess?.();
    } catch (err) {
      console.error(err);
      // Update status to error
      setUploads((u) =>
        u.map((item) =>
          items.some(i => i.file === item.file) ? { ...item, status: 'error' } : item
        )
      );
    }
  };

  const startUploads = () => {
    const queued = uploads.filter(u => u.status === 'queued');
    if (queued.length > 0) {
      uploadFile(queued);
    }
  };

  const removeFile = (index) => {
    setUploads((u) => u.filter((_, i) => i !== index));
  };

  return (
    <div className="relative">
      {/* Upload Button */}
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg transition"
      >
        <Upload className="w-4 h-4" />
        Upload
      </button>

      {/* Hidden Input */}
      <input
        ref={fileInputRef}
        type="file"
        multiple
        className="hidden"
        onChange={handleFileSelect}
      />

      {/* Popover */}
      {open && (
        <div className="absolute right-0 mt-3 w-96 bg-slate-800 border border-slate-700 rounded-xl shadow-xl z-50">
          {/* Caret */}
          <div className="absolute -top-2 right-6 w-4 h-4 bg-slate-800 rotate-45 border-l border-t border-slate-700" />

          {/* Header */}
          <div className="flex items-center justify-between px-4 py-3 border-b border-slate-700">
            <span className="text-white font-semibold">Upload Files</span>
            <button
              onClick={() => setOpen(false)}
              className="text-slate-400 hover:text-white"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          {/* Body */}
          <div className="p-4 space-y-3 max-h-64 overflow-y-auto">
            {uploads.length === 0 && (
              <p className="text-slate-400 text-sm">
                No files selected
              </p>
            )}

            {uploads.map((u, idx) => (
              <div
                key={idx}
                className="bg-slate-700/50 rounded-lg p-3"
              >
                <div className="flex items-center justify-between text-sm mb-1">
                  <span className="text-slate-200 truncate max-w-[240px]">
                    {u.file.name}
                  </span>

                  <div className="flex items-center gap-2">
                    <span className="text-slate-400 text-xs">
                      {u.status}
                    </span>

                    {u.status === 'queued' && (
                      <button
                        onClick={() => removeFile(idx)}
                        className="text-slate-400 hover:text-red-400 transition"
                        title="Remove file"
                      >
                        <X className="w-3.5 h-3.5" />
                      </button>
                    )}
                  </div>
                </div>
                <div className="w-full bg-slate-600 rounded-full h-2">
                  <div
                    className={`h-2 rounded-full transition-all ${u.status === 'done'
                      ? 'bg-green-500'
                      : u.status === 'error'
                        ? 'bg-red-500'
                        : 'bg-blue-500'
                      }`}
                    style={{ width: `${u.progress}%` }}
                  />
                </div>
              </div>
            ))}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between px-4 py-3 border-t border-slate-700">
            <button
              onClick={() => fileInputRef.current.click()}
              className="text-blue-400 hover:text-blue-300 text-sm"
            >
              Add more files
            </button>

            <button
              onClick={startUploads}
              className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg text-sm"
            >
              Start Upload
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export default UploadButton;
