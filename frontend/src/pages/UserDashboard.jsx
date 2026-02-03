// src/pages/UserDashboard.jsx
import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { FileText, Activity, CloudUpload, RefreshCw, HardDrive, BarChart, Zap } from 'lucide-react';
import FileUpload from '../components/FileUpload';
import FileList from '../components/FileList';
import { dashboardAPI } from '../services/api';

const UserDashboard = () => {
  const [data, setData] = useState({ metrics: null, files: [] });
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const fetchData = async () => {
    setRefreshing(true);
    try {
      const [mRes, fRes] = await Promise.all([
        dashboardAPI.getMetrics(),
        dashboardAPI.getFiles({ limit: 10 })
      ]);
      setData({ metrics: mRes.data, files: fRes.data.files || [] });
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => { fetchData(); }, []);

  const StatCard = ({ title, value, icon: Icon, color, sub }) => (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className="stat-card"
    >
      <div className="flex items-center gap-5">
        <div className={`p-4 rounded-2xl bg-${color}-500/10 text-${color}-400 ring-1 ring-${color}-500/20`}>
          <Icon className="w-7 h-7" />
        </div>
        <div>
          <h3 className="text-slate-500 text-xs font-bold uppercase tracking-widest">{title}</h3>
          <p className="text-3xl font-black text-white mt-1 tabular-nums">{value}</p>
          {sub && <p className="text-[10px] text-slate-500 mt-1 font-medium bg-white/5 rounded-full px-2 py-0.5 w-fit">{sub}</p>}
        </div>
      </div>
    </motion.div>
  );

  if (loading) return (
    <div className="flex items-center justify-center h-[80vh]">
      <div className="relative">
        <div className="w-16 h-16 border-4 border-blue-500/20 border-t-blue-500 rounded-full animate-spin"></div>
        <Zap className="w-6 h-6 text-blue-500 absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 animate-pulse" />
      </div>
    </div>
  );

  return (
    <div className="space-y-10 pb-20">
      <header className="flex flex-col md:flex-row md:items-center justify-between gap-6">
        <div>
          <h2 className="text-4xl font-black text-white tracking-tight">Personal Workspace</h2>
          <p className="text-slate-400 mt-2 text-lg">Manage your secure data environment and assets</p>
        </div>
        <button
          onClick={fetchData}
          disabled={refreshing}
          className="flex items-center px-6 py-3 bg-slate-800/50 hover:bg-slate-700/50 border border-white/10 rounded-2xl transition-all text-sm font-bold backdrop-blur-md active:scale-95 group"
        >
          <RefreshCw className={`w-4 h-4 mr-2 text-blue-400 ${refreshing ? 'animate-spin' : 'group-hover:rotate-180 transition-transform duration-500'}`} />
          Refresh Registry
        </button>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
        <StatCard title="My Assets" value={data.metrics?.total_documents || 0} icon={FileText} color="blue" sub="Managed Files" />
        <StatCard title="Activity" value={data.metrics?.processed_today || 0} icon={Activity} color="emerald" sub="Processed Today" />
        <StatCard title="Storage" value={`${data.metrics?.total_storage_gb || 0} GB`} icon={HardDrive} color="indigo" sub="Quota: 10 GB" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-10 items-start">
        <div className="glass-card">
          <h3 className="text-xl font-bold text-white mb-6 flex items-center gap-2">
            <CloudUpload className="w-5 h-5 text-blue-400" />
            Ingest Data
          </h3>
          <FileUpload onUploadSuccess={fetchData} />
        </div>

        <div className="glass-card h-full">
          <h3 className="text-xl font-bold text-white mb-8 flex items-center gap-2">
            <BarChart className="w-5 h-5 text-indigo-400" />
            Resource Allocation
          </h3>
          <div className="space-y-10">
            <div>
              <div className="flex justify-between text-xs text-slate-400 mb-3 font-bold uppercase tracking-widest">
                <span>Storage Utilization</span>
                <span className="text-blue-400">{(data.metrics?.total_storage_gb || 0) * 10}%</span>
              </div>
              <div className="w-full bg-slate-900/50 h-4 rounded-full overflow-hidden border border-white/5 p-1">
                <motion.div
                  initial={{ width: 0 }}
                  animate={{ width: `${(data.metrics?.total_storage_gb || 0) * 10}%` }}
                  className="bg-gradient-to-r from-blue-600 to-indigo-600 h-full rounded-full shadow-[0_0_15px_rgba(59,130,246,0.5)]"
                />
              </div>
            </div>

            <div className="p-6 bg-blue-600/10 border border-blue-500/20 rounded-2xl flex gap-4">
              <Zap className="w-6 h-6 text-blue-400 shrink-0" />
              <p className="text-sm text-blue-100/80 leading-relaxed font-medium">
                Your account is currently in the <span className="text-white font-bold">Gold Tier</span>.
                Auto-scaling is enabled for mission-critical workloads.
              </p>
            </div>
          </div>
        </div>
      </div>

      <div className="space-y-6">
        <h3 className="text-2xl font-black text-white">Registry Logs</h3>
        <div className="glass-card p-0 overflow-hidden">
          <FileList files={data.files} onRefresh={fetchData} />
        </div>
      </div>
    </div>
  );
};

export default UserDashboard;