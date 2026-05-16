// src/pages/AuditLogs.jsx
import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import {
    Shield, Clock, User, Activity, Search,
    RefreshCw, LogIn, Upload, Trash2, Zap, Info
} from 'lucide-react';
import { dashboardAPI } from '../services/api';

// ── Action config ─────────────────────────────────────────────────────────
const ACTION_CONFIG = {
    LOGIN:        { color: 'emerald', Icon: LogIn   },
    UPLOAD:       { color: 'blue',    Icon: Upload  },
    DELETE_FILE:  { color: 'red',     Icon: Trash2  },
    DELETE_USER:  { color: 'red',     Icon: Trash2  },
    TRIGGER_JOB:  { color: 'violet',  Icon: Zap     },
};

const getActionStyle = (action = '') => {
    const upper = action.toUpperCase();
    const key = Object.keys(ACTION_CONFIG).find(k => upper === k)
              || (upper.includes('DELETE') ? 'DELETE_FILE' : null);
    const cfg = ACTION_CONFIG[key] || { color: 'amber', Icon: Info };

    const palettes = {
        emerald: { badge: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20', icon: 'text-emerald-400' },
        blue:    { badge: 'bg-blue-500/10    text-blue-400    border-blue-500/20',    icon: 'text-blue-400'    },
        red:     { badge: 'bg-red-500/10     text-red-400     border-red-500/20',     icon: 'text-red-400'     },
        violet:  { badge: 'bg-violet-500/10  text-violet-400  border-violet-500/20',  icon: 'text-violet-400'  },
        amber:   { badge: 'bg-amber-500/10   text-amber-400   border-amber-500/20',   icon: 'text-amber-400'   },
    };

    return { ...palettes[cfg.color], Icon: cfg.Icon };
};

// ── StatCard ──────────────────────────────────────────────────────────────
const StatCard = ({ icon: Icon, label, value, color }) => (
    <motion.div
        initial={{ opacity: 0, y: 16 }}
        animate={{ opacity: 1, y: 0 }}
        className="stat-card"
    >
        <div className="flex items-center gap-4">
            <div className={`p-3 rounded-2xl bg-${color}-500/10 text-${color}-400 ring-1 ring-${color}-500/20 shrink-0`}>
                <Icon className="w-5 h-5" />
            </div>
            <div>
                <p className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">{label}</p>
                <p className="text-2xl font-black text-white tabular-nums">{value ?? '—'}</p>
            </div>
        </div>
    </motion.div>
);

// ── Main page ─────────────────────────────────────────────────────────────
const AuditLogs = () => {
    const [logs, setLogs]             = useState([]);
    const [loading, setLoading]       = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [searchTerm, setSearchTerm] = useState('');
    const [actionFilter, setActionFilter] = useState('ALL');

    const fetchLogs = async () => {
        setRefreshing(true);
        try {
            const res = await dashboardAPI.getAuditLogs();
            setLogs(Array.isArray(res.data) ? res.data : []);
        } catch (err) {
            console.error('Failed to fetch audit logs', err);
        } finally {
            setLoading(false);
            setRefreshing(false);
        }
    };

    useEffect(() => { fetchLogs(); }, []);

    // Unique action types for filter tabs — derived from live data
    const actionTypes = ['ALL', ...Array.from(new Set(logs.map(l => l.action).filter(Boolean))).sort()];

    const filteredLogs = logs.filter(log => {
        const matchesSearch =
            log.action?.toLowerCase().includes(searchTerm.toLowerCase()) ||
            log.username?.toLowerCase().includes(searchTerm.toLowerCase()) ||
            log.details?.toLowerCase().includes(searchTerm.toLowerCase()) ||
            log.ip_address?.toLowerCase().includes(searchTerm.toLowerCase());
        const matchesFilter = actionFilter === 'ALL' || log.action === actionFilter;
        return matchesSearch && matchesFilter;
    });

    const stats = {
        total:   logs.length,
        logins:  logs.filter(l => l.action === 'LOGIN').length,
        uploads: logs.filter(l => l.action === 'UPLOAD').length,
        deletes: logs.filter(l => l.action?.includes('DELETE')).length,
    };

    return (
        <div className="space-y-10 pb-20">

            {/* ── Header ── */}
            <header className="flex flex-col md:flex-row md:items-center justify-between gap-6">
                <div>
                    <h2 className="text-4xl font-black text-white tracking-tight">Audit Trail</h2>
                    <p className="text-slate-400 mt-2 text-lg">
                        Immutable record of all privileged system operations
                    </p>
                </div>
                <button
                    onClick={fetchLogs}
                    disabled={refreshing}
                    className="flex items-center px-6 py-3 bg-slate-800/50 hover:bg-slate-700/50 border border-white/10 rounded-2xl transition-all text-sm font-bold backdrop-blur-md active:scale-95 group"
                >
                    <RefreshCw className={`w-4 h-4 mr-2 text-blue-400 ${refreshing ? 'animate-spin' : 'group-hover:rotate-180 transition-transform duration-500'}`} />
                    Sync Logs
                </button>
            </header>

            {/* ── Stat cards ── */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-6">
                <StatCard icon={Activity} label="Total Events"  value={stats.total}   color="blue"    />
                <StatCard icon={LogIn}    label="Login Events"  value={stats.logins}  color="emerald" />
                <StatCard icon={Upload}   label="Upload Events" value={stats.uploads} color="indigo"  />
                <StatCard icon={Trash2}   label="Delete Events" value={stats.deletes} color="red"     />
            </div>

            {/* ── Table card ── */}
            <div className="glass-card p-0 overflow-hidden">

                {/* Search + filter bar */}
                <div className="p-5 border-b border-white/5 flex flex-col sm:flex-row items-start sm:items-center gap-4">
                    <div className="relative flex-1 max-w-sm group">
                        <Search className="absolute left-4 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500 group-focus-within:text-blue-400 transition-colors pointer-events-none" />
                        <input
                            type="text"
                            placeholder="Search action, user, details, IP…"
                            className="w-full bg-slate-900/50 border border-white/10 rounded-xl py-2.5 pl-11 pr-4 text-sm text-white placeholder-slate-600 outline-none focus:border-blue-500/30 focus:ring-2 focus:ring-blue-500/10 transition-all"
                            value={searchTerm}
                            onChange={e => setSearchTerm(e.target.value)}
                        />
                    </div>

                    {/* Action filter tabs — built from live data, no hardcoding */}
                    <div className="flex items-center gap-1.5 flex-wrap">
                        {actionTypes.map(type => (
                            <button
                                key={type}
                                onClick={() => setActionFilter(type)}
                                className={`px-3 py-1.5 rounded-xl text-[11px] font-bold uppercase tracking-wider transition-all ${
                                    actionFilter === type
                                        ? 'bg-blue-600/20 text-blue-400 border border-blue-500/30'
                                        : 'text-slate-500 hover:text-slate-300 border border-transparent hover:border-white/10'
                                }`}
                            >
                                {type}
                            </button>
                        ))}
                    </div>

                    <span className="text-[11px] font-bold text-slate-600 uppercase tracking-widest sm:ml-auto whitespace-nowrap">
                        {filteredLogs.length} / {logs.length} entries
                    </span>
                </div>

                {/* Table */}
                <div className="overflow-x-auto">
                    <table className="w-full text-left">
                        <thead className="bg-white/5 border-b border-white/5">
                            <tr className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">
                                <th className="px-6 py-4">Timestamp</th>
                                <th className="px-6 py-4">Identity</th>
                                <th className="px-6 py-4">Action</th>
                                <th className="px-6 py-4">Details</th>
                                <th className="px-6 py-4">IP Address</th>
                                <th className="px-6 py-4">Trace ID</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-white/5">
                            {loading ? (
                                Array(6).fill(0).map((_, i) => (
                                    <tr key={i} className="animate-pulse">
                                        {Array(6).fill(0).map((_, j) => (
                                            <td key={j} className="px-6 py-4">
                                                <div className="h-3 bg-white/5 rounded w-full" />
                                            </td>
                                        ))}
                                    </tr>
                                ))
                            ) : filteredLogs.length > 0 ? (
                                filteredLogs.map((log, idx) => {
                                    const { badge, icon, Icon: ActionIcon } = getActionStyle(log.action);
                                    return (
                                        <motion.tr
                                            key={log.id ?? idx}
                                            initial={{ opacity: 0 }}
                                            animate={{ opacity: 1 }}
                                            transition={{ delay: Math.min(idx * 0.02, 0.4) }}
                                            className="hover:bg-white/5 transition-colors"
                                        >
                                            {/* Timestamp */}
                                            <td className="px-6 py-4 whitespace-nowrap">
                                                <div className="flex items-center gap-2">
                                                    <Clock className="w-3.5 h-3.5 text-slate-600 shrink-0" />
                                                    <span className="text-xs font-mono text-slate-400">
                                                        {log.created_at ? new Date(log.created_at).toLocaleString() : '—'}
                                                    </span>
                                                </div>
                                            </td>

                                            {/* Identity */}
                                            <td className="px-6 py-4 whitespace-nowrap">
                                                <div className="flex items-center gap-2">
                                                    <div className="w-6 h-6 rounded-full bg-blue-500/10 border border-blue-500/20 flex items-center justify-center shrink-0">
                                                        <User className="w-3 h-3 text-blue-400" />
                                                    </div>
                                                    <span className="text-sm font-medium text-slate-200">
                                                        {log.username || 'System'}
                                                    </span>
                                                </div>
                                            </td>

                                            {/* Action badge */}
                                            <td className="px-6 py-4 whitespace-nowrap">
                                                <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-[10px] font-bold border uppercase ${badge}`}>
                                                    <ActionIcon className={`w-3 h-3 ${icon}`} />
                                                    {log.action || '—'}
                                                </span>
                                            </td>

                                            {/* Details */}
                                            <td className="px-6 py-4 max-w-xs">
                                                <p className="text-xs text-slate-500 truncate" title={log.details}>
                                                    {log.details || <span className="text-slate-700 italic">—</span>}
                                                </p>
                                            </td>

                                            {/* IP */}
                                            <td className="px-6 py-4 whitespace-nowrap">
                                                <span className="text-xs font-mono text-slate-600">
                                                    {log.ip_address || '—'}
                                                </span>
                                            </td>

                                            {/* Trace ID */}
                                            <td className="px-6 py-4 whitespace-nowrap">
                                                <span className="text-xs font-mono text-slate-700">
                                                    REQ-{String(log.id ?? idx).padStart(6, '0')}
                                                </span>
                                            </td>
                                        </motion.tr>
                                    );
                                })
                            ) : (
                                <tr>
                                    <td colSpan="6" className="py-16 text-center">
                                        <Activity className="w-10 h-10 mx-auto mb-3 text-slate-700" />
                                        <p className="text-slate-500 font-medium">No records found</p>
                                        <p className="text-xs text-slate-700 mt-1">Try adjusting your search or filter</p>
                                    </td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
};

export default AuditLogs;