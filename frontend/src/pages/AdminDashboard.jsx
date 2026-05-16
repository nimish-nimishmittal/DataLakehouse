// src/pages/AdminDashboard.jsx
import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import {
  Users, FileText,
  RefreshCw, Shield,
  Terminal, Server, CheckCircle2, FolderOpen, BarChart2
} from 'lucide-react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Cell, LabelList
} from 'recharts';
import FileUpload from '../components/FileUpload';
import FileList from '../components/FileList';
import UserManager from '../components/UserManager';
import { dashboardAPI } from '../services/api';

const AUDIT_LIMIT = 8;

const AdminDashboard = () => {
  const [data, setData] = useState({ metrics: null, files: [], health: null, auditLogs: [] });
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [showAllAudit, setShowAllAudit] = useState(false);

  const fetchData = async () => {
    setRefreshing(true);
    try {
      const results = await Promise.allSettled([
        dashboardAPI.getMetrics(),
        dashboardAPI.getFiles({ limit: 5 }),
        dashboardAPI.getHealth(),
        dashboardAPI.getAuditLogs()
      ]);

      const [mRes, fRes, hRes, aRes] = results;

      if (mRes.status === 'rejected') console.error('Metrics failed', mRes.reason);
      if (fRes.status === 'rejected') console.error('Files failed', fRes.reason);
      if (hRes.status === 'rejected') console.error('Health failed', hRes.reason);
      if (aRes.status === 'rejected') console.error('Audit logs failed', aRes.reason);

      // /admin/audit-logs returns a plain array directly (not wrapped in a key)
      const rawAudit = aRes.status === 'fulfilled' ? aRes.value.data : [];
      const auditLogs = Array.isArray(rawAudit) ? rawAudit : [];

      setData({
        metrics: mRes.status === 'fulfilled' ? mRes.value.data : null,
        files: fRes.status === 'fulfilled' ? (fRes.value.data.files || []) : [],
        health: hRes.status === 'fulfilled' ? hRes.value.data : null,
        auditLogs,
      });
    } catch (err) {
      console.error('Critical dashboard error', err);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => { fetchData(); }, []);

  // ── StatCard ──────────────────────────────────────────────────────────────
  const StatCard = ({ title, value, icon: Icon, color, sub }) => (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      className="stat-card"
    >
      <div className="flex items-center gap-5">
        <div className={`p-4 rounded-2xl bg-${color}-500/10 text-${color}-400 ring-1 ring-${color}-500/20 shrink-0`}>
          <Icon className="w-7 h-7" />
        </div>
        <div className="min-w-0">
          <h3 className="text-slate-500 text-xs font-bold uppercase tracking-widest">{title}</h3>
          {/* FIX: was using || '-' which treated 0 as falsy, showing '-' instead of 0.
              Now using ?? (nullish coalescing) so genuine 0 values render correctly. */}
          <p className="text-3xl font-black text-white mt-1 tabular-nums">
            {value ?? '—'}
          </p>
          <span className="text-[10px] text-slate-500 font-medium">{sub}</span>
        </div>
      </div>
    </motion.div>
  );

  // ── Loading spinner ───────────────────────────────────────────────────────
  if (loading) return (
    <div className="flex items-center justify-center h-[80vh]">
      <div className="relative">
        <div className="w-16 h-16 border-4 border-blue-500/20 border-t-blue-500 rounded-full animate-spin" />
        <Shield className="w-6 h-6 text-blue-500 absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 animate-pulse" />
      </div>
    </div>
  );

  // ── File-type bar chart data ──────────────────────────────────────────────
  // Sourced from files_by_format: [{file_format, count}] — already in metrics
  const FORMAT_COLORS = {
    pdf:        '#f87171', // red-400
    docx:       '#60a5fa', // blue-400
    structured: '#34d399', // emerald-400
    image:      '#a78bfa', // violet-400
    ppt:        '#fb923c', // orange-400
    csv:        '#2dd4bf', // teal-400
    json:       '#facc15', // yellow-400
  };
  const DEFAULT_COLOR = '#94a3b8'; // slate-400 for unknown types

  const formatChartData = (data.metrics?.files_by_format ?? [])
    .map(item => ({
      format: item.file_format ?? 'unknown',
      count:  Number(item.count),
      color:  FORMAT_COLORS[item.file_format] ?? DEFAULT_COLOR,
    }))
    .sort((a, b) => b.count - a.count); // highest first → longest bar at top

  // Custom tooltip for the bar chart
  const FormatTooltip = ({ active, payload }) => {
    if (!active || !payload?.length) return null;
    const { format, count } = payload[0].payload;
    return (
      <div className="bg-[#0f172a] border border-white/10 rounded-xl px-4 py-2 text-xs">
        <span className="font-bold text-white uppercase">{format}</span>
        <span className="text-slate-400 ml-2">{count} file{count !== 1 ? 's' : ''}</span>
      </div>
    );
  };

  // ── Audit rows ────────────────────────────────────────────────────────────
  const visibleLogs = showAllAudit ? data.auditLogs : data.auditLogs.slice(0, AUDIT_LIMIT);
  const hasMore = data.auditLogs.length > AUDIT_LIMIT;

  return (
    <div className="space-y-10 pb-20">

      {/* ── Header ── */}
      <header className="flex flex-col md:flex-row md:items-center justify-between gap-6">
        <div>
          <h2 className="text-4xl font-black text-white tracking-tight">Admin Panel</h2>
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

      {/* ── Stat Cards ── */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard
          title="Total Users"
          value={data.metrics?.total_users}
          icon={Users}
          color="blue"
          sub="Active Accounts"
        />
        <StatCard
          title="Total Assets"
          value={data.metrics?.total_documents}
          icon={FileText}
          color="indigo"
          sub="Catalog Items"
        />
        {/* Replaced Storage + Extraction Rate with more actionable day-to-day metrics */}
        <StatCard
          title="Processed Today"
          value={data.metrics?.processed_today}
          icon={CheckCircle2}
          color="emerald"
          sub="Extracted Today"
        />
        <StatCard
          title="Raw Files"
          value={data.metrics?.files_in_raw}
          icon={FolderOpen}
          color="amber"
          sub="Awaiting Processing"
        />
      </div>

      {/* ── Files by Type chart + Service Health ── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">

        {/* Files by Type — horizontal bar chart */}
        <div className="lg:col-span-2 glass-card">
          <div className="flex items-center justify-between mb-6">
            <h3 className="text-xl font-bold text-white flex items-center gap-2">
              <BarChart2 className="w-5 h-5 text-blue-400" />
              Files by Type
            </h3>
            <span className="bg-slate-900/50 border border-white/10 rounded-lg px-3 py-1 text-[10px] font-bold text-slate-400 uppercase">
              All Time
            </span>
          </div>

          {formatChartData.length > 0 ? (
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={formatChartData}
                  layout="vertical"
                  margin={{ top: 0, right: 48, bottom: 0, left: 16 }}
                  barCategoryGap="28%"
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#ffffff06" horizontal={false} />
                  <XAxis
                    type="number"
                    stroke="#475569"
                    fontSize={10}
                    tickLine={false}
                    axisLine={false}
                    allowDecimals={false}
                  />
                  <YAxis
                    type="category"
                    dataKey="format"
                    stroke="#475569"
                    fontSize={11}
                    tickLine={false}
                    axisLine={false}
                    width={72}
                    tick={{ fill: '#94a3b8', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em' }}
                    tickFormatter={v => v.toUpperCase()}
                  />
                  <Tooltip content={<FormatTooltip />} cursor={{ fill: 'rgba(255,255,255,0.03)' }} />
                  <Bar dataKey="count" radius={[0, 6, 6, 0]} maxBarSize={28}>
                    {formatChartData.map((entry) => (
                      <Cell key={entry.format} fill={entry.color} fillOpacity={0.85} />
                    ))}
                    <LabelList
                      dataKey="count"
                      position="right"
                      style={{ fill: '#94a3b8', fontSize: 11, fontWeight: 700 }}
                    />
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="h-64 flex flex-col items-center justify-center gap-2">
              <BarChart2 className="w-8 h-8 text-slate-600" />
              <p className="text-slate-500 text-sm">No catalog data yet</p>
            </div>
          )}
        </div>

        {/* Service Health */}
        <div className="glass-card">
          <h3 className="text-xl font-bold text-white mb-6 flex items-center gap-2">
            <Server className="w-5 h-5 text-indigo-400" />
            Service Health
          </h3>
          <div className="space-y-3">
            {[
              { label: 'Core API',   status: data.health?.api },
              { label: 'PostgreSQL', status: data.health?.postgres },
              { label: 'MinIO S3',   status: data.health?.minio },
              { label: 'Airflow',    status: data.health?.airflow },
            ].map((svc) => (
              <div
                key={svc.label}
                className="flex items-center justify-between p-4 bg-white/5 rounded-2xl border border-white/5 hover:border-white/10 transition-colors"
              >
                <span className="text-sm font-medium text-slate-300">{svc.label}</span>
                <div className={`flex items-center gap-1.5 px-3 py-1 rounded-full text-[10px] font-bold uppercase
                  ${svc.status === 'ok'
                    ? 'bg-emerald-500/10 text-emerald-400'
                    : svc.status && svc.status !== 'unknown'
                      ? 'bg-red-500/10 text-red-400'
                      : 'bg-slate-500/10 text-slate-500'
                  }`}
                >
                  <span className={`w-1.5 h-1.5 rounded-full ${
                    svc.status === 'ok'
                      ? 'bg-emerald-400 animate-pulse'
                      : svc.status && svc.status !== 'unknown'
                        ? 'bg-red-400'
                        : 'bg-slate-500'
                  }`} />
                  {svc.status === 'ok'
                    ? 'Online'
                    : svc.status && svc.status !== 'unknown'
                      ? 'Offline'
                      : 'Unknown'}
                </div>
              </div>
            ))}
          </div>
          <div className="mt-6 p-4 bg-slate-900/50 rounded-2xl border border-white/5 border-dashed flex items-center gap-3">
            <Terminal className="w-4 h-4 text-slate-500 shrink-0" />
            <span className="text-[10px] font-mono text-slate-500">System Monitoring Active</span>
          </div>
        </div>
      </div>

      {/* ── Identity Management + Audit Trail ── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">

        <div className="space-y-4">
          <h3 className="text-2xl font-black text-white">Identity Management</h3>
          <div className="glass-card p-0 overflow-hidden">
            <UserManager />
          </div>
        </div>

        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-2xl font-black text-white">Secure Audit Trail</h3>
            {data.auditLogs.length > 0 && (
              <span className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">
                {visibleLogs.length} / {data.auditLogs.length} entries
              </span>
            )}
          </div>
          <div className="glass-card p-0 overflow-hidden">
            {/* FIX: previously rendered ALL rows with no cap — page blew out vertically.
                Now shows AUDIT_LIMIT rows by default with a toggle to expand. */}
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
                  {visibleLogs.length > 0 ? visibleLogs.map((log) => (
                    <tr key={log.id} className="hover:bg-white/5 transition-colors">
                      <td className="p-4 pl-6">
                        <div className="flex flex-col gap-1">
                          <span className="px-2 py-0.5 rounded-md text-[10px] font-bold bg-blue-500/10 text-blue-400 border border-blue-500/20 uppercase w-fit">
                            {log.action}
                          </span>
                          <span className="text-[10px] text-slate-500 truncate max-w-[160px]">{log.details}</span>
                        </div>
                      </td>
                      <td className="p-4 text-sm text-slate-300 font-medium whitespace-nowrap">
                        {log.username || 'System'}
                      </td>
                      <td className="p-4 text-right pr-6 text-xs font-mono text-slate-500 whitespace-nowrap">
                        {new Date(log.created_at).toLocaleTimeString()}
                      </td>
                    </tr>
                  )) : (
                    <tr>
                      <td colSpan="3" className="p-10 text-center text-slate-500 text-sm">No recent activity</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>

            {/* Show more / collapse toggle */}
            {hasMore && (
              <div className="border-t border-white/5 p-3 text-center">
                <button
                  onClick={() => setShowAllAudit(prev => !prev)}
                  className="text-[11px] font-bold text-slate-500 hover:text-blue-400 uppercase tracking-widest transition-colors px-4 py-2 rounded-xl hover:bg-blue-500/5"
                >
                  {showAllAudit
                    ? '▲ Collapse'
                    : `▼ Show ${data.auditLogs.length - AUDIT_LIMIT} more entries`}
                </button>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* ── Recent Global Ingestions ── */}
      <div className="space-y-4">
        <h3 className="text-2xl font-black text-white">Recent Global Ingestions</h3>
        <div className="glass-card p-0 overflow-hidden">
          <FileUpload onUploadSuccess={fetchData} />
          <div className="px-6 pb-6">
            <FileList files={data.files} onRefresh={fetchData} />
          </div>
        </div>
      </div>

    </div>
  );
};

export default AdminDashboard;