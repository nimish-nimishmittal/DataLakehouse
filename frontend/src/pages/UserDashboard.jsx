// src/pages/UserDashboard.jsx
import React, { useState, useEffect } from 'react';
import Navbar from '../components/Navbar';
import FileUpload from '../components/FileUpload';
import FileList from '../components/FileList';
import { dashboardAPI } from '../services/api';

const UserDashboard = () => {
  const [metrics, setMetrics] = useState(null);
  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(true);

  const fetchData = async () => {
    try {
      const [metricsRes, filesRes] = await Promise.all([
        dashboardAPI.getMetrics(),
        dashboardAPI.getFiles()
      ]);
      setMetrics(metricsRes.data);
      setFiles(filesRes.data.files || filesRes.data);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  if (loading) return <div className="min-h-screen bg-gray-900 text-white flex items-center justify-center">Loading...</div>;

  return (
    <div className="min-h-screen bg-gray-900 text-white">
      <Navbar />
      <div className="max-w-7xl mx-auto p-8">
        <h2 className="text-3xl font-bold mb-8">My Dashboard</h2>

        {/* Metrics */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          <div className="bg-gray-800 p-6 rounded-lg">
            <h3 className="text-gray-400">My Files</h3>
            <p className="text-4xl font-bold">{metrics?.total_documents || 0}</p>
          </div>
          {/* Add more if needed */}
        </div>

        <FileUpload onUploadSuccess={fetchData} />

        <h3 className="text-2xl font-bold mb-4">My Uploaded Files</h3>
        {files.length === 0 ? (
          <p className="text-gray-400">No files uploaded yet.</p>
        ) : (
          <FileList files={files} />
        )}
      </div>
    </div>
  );
};

export default UserDashboard;