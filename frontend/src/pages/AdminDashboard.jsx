// src/pages/AdminDashboard.jsx
import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import {
  Users, Database, Activity, FileText,
  RefreshCw, Zap, Shield, TrendingUp,
  Terminal, Server
} from 'lucide-react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, AreaChart, Area
} from 'recharts';
import FileUpload from '../components/FileUpload';
import FileList from '../components/FileList';
import UserManager from '../components/UserManager';
import { dashboardAPI } from '../services/api';

const AdminDashboard = () => {
  const [data, setData] = useState({ metrics: null, files: [], health: null, auditLogs: [] });
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const fetchData = async () => {
    setRefreshing(true);
    try {
      const [mRes, fRes, hRes, aRes] = await Promise.all([
        dashboardAPI.getMetrics(),
        dashboardAPI.getFiles({ limit: 5 }),
        dashboardAPI.getHealth(),
        dashboardAPI.getAuditLogs()
      ]);
      setData({
        metrics: mRes.data,
        files: fRes.data.files || [],
        health: hRes.data,
        auditLogs: aRes.data.slice(0, 5)
      });
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => { fetchData(); }, []);

  const StatCard = ({ title, value, icon: Icon, color, sub, trend }) => (
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
          <div className="flex items-center gap-2 mt-1">
            {trend && <span className="text-[10px] text-emerald-400 font-bold flex items-center"><TrendingUp className="w-3 h-3 mr-0.5" /> {trend}</span>}
            <span className="text-[10px] text-slate-500 font-medium">{sub}</span>
          </div>
        </div>
      </div>
    </motion.div>
  );

  if (loading) return (
    <div className="flex items-center justify-center h-[80vh]">
      <div className="relative">
        <div className="w-16 h-16 border-4 border-blue-500/20 border-t-blue-500 rounded-full animate-spin"></div>
        <Shield className="w-6 h-6 text-blue-500 absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 animate-pulse" />
      </div>
    </div>
  );

  const chartData = [
    { name: 'Mon', ingest: 400 },
    { name: 'Tue', ingest: 300 },
    { name: 'Wed', ingest: 700 },
    { name: 'Thu', ingest: 500 },
    { name: 'Fri', ingest: 900 },
    { name: 'Sat', ingest: 600 },
    { name: 'Sun', ingest: 800 },
  ];

  return (
    <div className="space-y-10 pb-20">
      <header className="flex flex-col md:flex-row md:items-center justify-between gap-6">
        <div>
          <h2 className="text-4xl font-black text-white tracking-tight">Admin Command Center</h2>
          <p className="text-slate-400 mt-2 text-lg">Real-time system oversight and global orchestration</p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2 px-4 py-2 bg-emerald-500/10 border border-emerald-500/20 rounded-xl">
            <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
            <span className="text-[10px] font-bold text-emerald-400 uppercase tracking-widest">System Healthy</span>
          </div>
          <button
            onClick={fetchData}
            disabled={refreshing}
            className="flex items-center px-6 py-3 bg-slate-800/50 hover:bg-slate-700/50 border border-white/10 rounded-2xl transition-all text-sm font-bold backdrop-blur-md active:scale-95 group"
          >
            <RefreshCw className={`w-4 h-4 mr-2 text-blue-400 ${refreshing ? 'animate-spin' : 'group-hover:rotate-180 transition-transform duration-500'}`} />
            Sync Environment
          </button>
        </div>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard title="Total Users" value={data.metrics?.total_users || 0} icon={Users} color="blue" sub="Active Accounts" trend="+12%" />
        <StatCard title="Total Assets" value={data.metrics?.total_documents || 0} icon={FileText} color="indigo" sub="Catalog Items" trend="+8%" />
        <StatCard title="Storage" value="4.2 TB" icon={Database} color="purple" sub="Bucket Usage" trend="+5.4 GB" />
        <StatCard title="System Load" value="24%" icon={Activity} color="emerald" sub="API Gateway" trend="-2%" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        <div className="lg:col-span-2 glass-card">
          <div className="flex items-center justify-between mb-8">
            <h3 className="text-xl font-bold text-white flex items-center gap-2">
              <TrendingUp className="w-5 h-5 text-blue-400" />
              Ingestion Velocity
            </h3>
            <select className="bg-slate-900/50 border border-white/10 rounded-lg px-3 py-1 text-[10px] font-bold text-slate-400 uppercase outline-none">
              <option>Last 7 Days</option>
              <option>Last 30 Days</option>
            </select>
          </div>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData}>
                <defs>
                  <linearGradient id="colorIngest" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#ffffff05" vertical={false} />
                <XAxis dataKey="name" stroke="#64748b" fontSize={10} tickLine={false} axisLine={false} />
                <YAxis stroke="#64748b" fontSize={10} tickLine={false} axisLine={false} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #ffffff10', borderRadius: '12px' }}
                  itemStyle={{ color: '#fff', fontSize: '12px' }}
                />
                <Area type="monotone" dataKey="ingest" stroke="#3b82f6" strokeWidth={3} fillOpacity={1} fill="url(#colorIngest)" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="glass-card">
          <h3 className="text-xl font-bold text-white mb-6 flex items-center gap-2">
            <Server className="w-5 h-5 text-indigo-400" />
            Service Health
          </h3>
          <div className="space-y-4">
            {[
              { label: 'Core API', status: data.health?.api, color: 'blue' },
              { label: 'PostgreSQL', status: data.health?.postgres, color: 'emerald' },
              { label: 'MinIO S3', status: data.health?.minio, color: 'amber' },
              { label: 'Airflow', status: data.health?.airflow, color: 'indigo' },
            ].map((svc) => (
              <div key={svc.label} className="flex items-center justify-between p-4 bg-white/5 rounded-2xl border border-white/5 hover:border-white/10 transition-colors">
                <span className="text-sm font-medium text-slate-300">{svc.label}</span>
                <div className={`flex items-center gap-2 px-3 py-1 rounded-full text-[10px] font-bold uppercase 
                            ${svc.status === 'ok' ? 'bg-emerald-500/10 text-emerald-400' : 'bg-red-500/10 text-red-500'}`}>
                  {svc.status === 'ok' ? 'Online' : 'Degraded'}
                </div>
              </div>
            ))}
          </div>
          <div className="mt-8 p-4 bg-slate-900/50 rounded-2xl border border-white/5 border-dashed flex items-center gap-3">
            <Terminal className="w-5 h-5 text-slate-500" />
            <span className="text-[10px] font-mono text-slate-500">Uptime: 99.998% | LAT: 12ms</span>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-10">
        <div className="space-y-6">
          <h3 className="text-2xl font-black text-white">Identity Management</h3>
          <div className="glass-card p-0 overflow-hidden">
            <UserManager />
          </div>
        </div>
        <div className="space-y-6">
          <h3 className="text-2xl font-black text-white">Secure Audit Trail</h3>
          <div className="glass-card p-0 overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-left">
                <thead className="bg-white/5">
                  <tr className="text-xs font-bold text-slate-500 uppercase tracking-widest border-b border-white/5">
                    <th className="p-4 pl-6">Action</th>
                    <th className="p-4">Identity</th>
                    <th className="p-4 text-right pr-6">Time</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-white/5">
                  {data.auditLogs?.map((log) => (
                    <tr key={log.id} className="hover:bg-white/5 transition-colors group">
                      <td className="p-4 pl-6">
                        <span className="px-2 py-0.5 rounded-md text-[10px] font-bold bg-blue-500/10 text-blue-400 border border-blue-500/20 uppercase">
                          {log.action}
                        </span>
                      </td>
                      <td className="p-4 text-sm text-slate-300 font-medium">{log.username || 'System'}</td>
                      <td className="p-4 text-right pr-6 text-xs font-mono text-slate-500">
                        {new Date(log.created_at).toLocaleTimeString()}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>

      <div className="space-y-6">
        <h3 className="text-2xl font-black text-white">Recent Global Ingestions</h3>
        <div className="glass-card p-0 overflow-hidden">
          <FileList files={data.files} onRefresh={fetchData} />
        </div>
      </div>
    </div>
  );
};

export default AdminDashboard;