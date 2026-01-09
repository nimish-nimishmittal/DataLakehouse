// src/components/FileUpload.jsx
import React, { useState } from 'react';
import { dashboardAPI } from '../services/api';

const FileUpload = ({ onUploadSuccess }) => {
  const [uploading, setUploading] = useState(false);
  const [message, setMessage] = useState('');

  const handleUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;

    setUploading(true);
    setMessage('');

    const formData = new FormData();
    formData.append('file', file);

    try {
      await dashboardAPI.uploadFile(formData);
      setMessage('Upload successful!');
      onUploadSuccess?.();
    } catch (err) {
      setMessage('Upload failed');
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className="bg-gray-800 p-6 rounded-lg mb-6">
      <h3 className="text-xl font-semibold text-white mb-4">Upload File</h3>
      <input
        type="file"
        onChange={handleUpload}
        disabled={uploading}
        className="block w-full text-sm text-gray-300 file:mr-4 file:py-2 file:px-4 file:rounded file:border-0 file:text-sm file:font-semibold file:bg-blue-600 file:text-white hover:file:bg-blue-700"
      />
      {uploading && <p className="text-yellow-400 mt-2">Uploading...</p>}
      {message && <p className={message.includes('success') ? 'text-green-400' : 'text-red-400'}>{message}</p>}
    </div>
  );
};

export default FileUpload;