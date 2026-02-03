// src/pages/AuditLogs.jsx
import React, { useState, useEffect } from 'react';
import { Shield, Clock, User, Activity, Search, RefreshCw, FileText } from 'lucide-react';
import { dashboardAPI } from '../services/api';

const AuditLogs = () => {
    const [logs, setLogs] = useState([]);
    const [loading, setLoading] = useState(true);
    const [searchTerm, setSearchTerm] = useState('');

    const fetchLogs = async () => {
        setLoading(true);
        try {
            const res = await dashboardAPI.getAuditLogs();
            setLogs(res.data);
        } catch (err) {
            console.error('Failed to fetch audit logs', err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchLogs();
    }, []);

    const filteredLogs = logs.filter(log =>
        log.action.toLowerCase().includes(searchTerm.toLowerCase()) ||
        log.username?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        log.details?.toLowerCase().includes(searchTerm.toLowerCase())
    );

    const getActionColor = (action) => {
        if (action.includes('DELETE')) return 'text-red-400 bg-red-400/10 border-red-400/20';
        if (action.includes('LOGIN')) return 'text-emerald-400 bg-emerald-400/10 border-emerald-400/20';
        if (action.includes('UPLOAD')) return 'text-blue-400 bg-blue-400/10 border-blue-400/20';
        return 'text-amber-400 bg-amber-400/10 border-amber-400/20';
    };

    return (
        <div className="space-y-8 pb-20">
            <header className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                <div>
                    <h2 className="text-3xl font-bold text-white tracking-tight">System Audit Trail</h2>
                    <p className="text-gray-500 mt-1">Immutable record of all high-privilege operations</p>
                </div>
                <button
                    onClick={fetchLogs}
                    className="flex items-center px-4 py-2 bg-gray-800 hover:bg-gray-700 border border-white/5 rounded-xl transition-all text-sm"
                >
                    <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
                    Refresh
                </button>
            </header>

            <div className="bg-gray-800/40 backdrop-blur-xl border border-white/5 rounded-3xl overflow-hidden">
                <div className="p-6 border-b border-white/5 flex items-center gap-4 bg-gray-900/40">
                    <div className="relative flex-1 max-w-md">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-500" />
                        <input
                            type="text"
                            placeholder="Search logs by action, user, or details..."
                            className="w-full bg-gray-900/50 border border-white/10 rounded-xl py-2 pl-10 pr-4 text-white text-sm focus:ring-2 focus:ring-blue-500 outline-none transition-all"
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                        />
                    </div>
                </div>

                <div className="overflow-x-auto">
                    <table className="w-full text-left">
                        <thead>
                            <tr className="text-xs font-bold text-gray-500 uppercase tracking-wider border-b border-white/5">
                                <th className="p-6">Timestamp</th>
                                <th className="p-6">Identity</th>
                                <th className="p-6">Action</th>
                                <th className="p-6">Technical Details</th>
                                <th className="p-6">Trace ID</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-white/5">
                            {loading ? (
                                Array(5).fill(0).map((_, i) => (
                                    <tr key={i} className="animate-pulse">
                                        <td colSpan="5" className="p-6"><div className="h-4 bg-white/5 rounded w-full"></div></td>
                                    </tr>
                                ))
                            ) : filteredLogs.length > 0 ? (
                                filteredLogs.map((log) => (
                                    <tr key={log.id} className="hover:bg-white/5 transition-colors">
                                        <td className="p-6 whitespace-nowrap">
                                            <div className="flex items-center gap-3">
                                                <Clock className="w-4 h-4 text-gray-500" />
                                                <span className="text-sm text-gray-300 font-mono">
                                                    {new Date(log.created_at).toLocaleString()}
                                                </span>
                                            </div>
                                        </td>
                                        <td className="p-6">
                                            <div className="flex items-center gap-2">
                                                <div className="w-6 h-6 rounded-full bg-blue-500/20 flex items-center justify-center">
                                                    <User className="w-3 h-3 text-blue-400" />
                                                </div>
                                                <span className="text-sm font-medium text-white">{log.username || 'System'}</span>
                                            </div>
                                        </td>
                                        <td className="p-6">
                                            <span className={`px-2 py-1 rounded-md text-[10px] font-bold border uppercase ${getActionColor(log.action)}`}>
                                                {log.action}
                                            </span>
                                        </td>
                                        <td className="p-6">
                                            <p className="text-sm text-gray-400 italic max-w-xs truncate">{log.details || '-'}</p>
                                        </td>
                                        <td className="p-6">
                                            <span className="text-xs font-mono text-gray-600">REQ-{log.id.toString().padStart(6, '0')}</span>
                                        </td>
                                    </tr>
                                ))
                            ) : (
                                <tr>
                                    <td colSpan="5" className="p-12 text-center text-gray-500">
                                        <Activity className="w-12 h-12 mx-auto mb-4 opacity-20" />
                                        <p>No telemetry records found matching your criteria</p>
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
