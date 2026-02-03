// src/pages/SystemStatus.jsx
import React, { useState, useEffect } from 'react';
import { Server, Activity, ShieldCheck, Database, HardDrive, Cpu, Terminal, RefreshCw } from 'lucide-react';
import { dashboardAPI } from '../services/api';

const SystemStatus = () => {
    const [stats, setStats] = useState(null);
    const [health, setHealth] = useState(null);
    const [diagnostics, setDiagnostics] = useState(null);
    const [loading, setLoading] = useState(true);

    const fetchData = async () => {
        try {
            const [sRes, hRes, dRes] = await Promise.all([
                dashboardAPI.getSystemStats(),
                dashboardAPI.getHealth(),
                dashboardAPI.getDiagnostics()
            ]);
            setStats(sRes.data);
            setHealth(hRes.data);
            setDiagnostics(dRes.data);
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => { fetchData(); }, []);

    const StatusRow = ({ label, status, detail }) => (
        <div className="flex items-center justify-between p-4 bg-white/5 rounded-2xl border border-white/5">
            <div className="flex items-center gap-3">
                <div className={`w-2.5 h-2.5 rounded-full ${status === 'ok' ? 'bg-emerald-500 shadow-[0_0_10px_#10b981]' : 'bg-red-500'}`} />
                <span className="text-sm font-medium text-white">{label}</span>
            </div>
            <span className="text-xs text-gray-500 font-mono">{detail || (status === 'ok' ? 'Operational' : 'Error')}</span>
        </div>
    );

    if (loading) return <div className="flex items-center justify-center h-[80vh]"><RefreshCw className="w-8 h-8 text-blue-500 animate-spin" /></div>;

    return (
        <div className="space-y-8 pb-20">
            <header className="flex justify-between items-center">
                <div>
                    <h2 className="text-3xl font-bold text-white tracking-tight">System Reliability</h2>
                    <p className="text-gray-500 mt-1">Infrastructure vitals and performance diagnostics</p>
                </div>
                <button onClick={fetchData} className="p-3 bg-gray-800 hover:bg-gray-700 rounded-xl border border-white/5 transition-all">
                    <RefreshCw className="w-5 h-5 text-gray-400" />
                </button>
            </header>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                <div className="bg-gray-800/40 backdrop-blur-md border border-white/5 p-8 rounded-3xl">
                    <h3 className="text-xl font-bold text-white mb-6 flex items-center gap-2">
                        <Server className="w-5 h-5 text-blue-400" />
                        Service Health
                    </h3>
                    <div className="space-y-4">
                        <StatusRow label="Core API Gateway" status={health?.api} />
                        <StatusRow label="PostgreSQL Instance" status={health?.postgres} />
                        <StatusRow label="MinIO S3 Cluster" status={health?.minio} />
                        <StatusRow label="Airflow Scheduler" status={health?.airflow} />
                    </div>
                </div>

                <div className="bg-gray-800/40 backdrop-blur-md border border-white/5 p-8 rounded-3xl">
                    <h3 className="text-xl font-bold text-white mb-6 flex items-center gap-2">
                        <Activity className="w-5 h-5 text-emerald-400" />
                        System Metrics
                    </h3>
                    <div className="grid grid-cols-2 gap-4">
                        <div className="p-4 bg-white/5 rounded-2xl border border-white/5">
                            <p className="text-xs text-gray-500 mb-1">Total Users</p>
                            <p className="text-2xl font-bold text-white">{stats?.users_total}</p>
                        </div>
                        <div className="p-4 bg-white/5 rounded-2xl border border-white/5">
                            <p className="text-xs text-gray-500 mb-1">Total Assets</p>
                            <p className="text-2xl font-bold text-white">{stats?.files_total}</p>
                        </div>
                        <div className="p-4 bg-white/5 rounded-2xl border border-white/5">
                            <p className="text-xs text-gray-500 mb-1">API Version</p>
                            <p className="text-xl font-bold text-white">{stats?.api_version}</p>
                        </div>
                        <div className="p-4 bg-white/5 rounded-2xl border border-white/5">
                            <p className="text-xs text-gray-500 mb-1">CPU Load</p>
                            <p className="text-xl font-bold text-white">{diagnostics?.cpu_usage}%</p>
                        </div>
                    </div>
                </div>
            </div>

            <div className="bg-gray-900 border border-white/5 rounded-3xl p-6 font-mono text-sm shadow-2xl">
                <div className="flex items-center gap-2 mb-4 text-gray-500 border-b border-white/5 pb-4">
                    <Terminal className="w-4 h-4" />
                    <span>Live Diagnostic Console</span>
                </div>
                <div className="space-y-1">
                    <p className="text-emerald-400">$ system-info --os</p>
                    <p className="text-gray-300">{diagnostics?.os} {diagnostics?.python_version ? `(Python ${diagnostics.python_version})` : ''}</p>

                    <p className="text-emerald-400 mt-2">$ list --processes</p>
                    <p className="text-gray-300">Active Threads: {diagnostics?.active_threads}</p>
                    <p className="text-gray-300">Open File Descriptors: {diagnostics?.open_files}</p>

                    <p className="text-emerald-400 mt-2">$ network --stats</p>
                    <p className="text-gray-300 flex justify-between">
                        <span>SENT: {(diagnostics?.network?.bytes_sent / 1024 / 1024).toFixed(2)} MB</span>
                        <span>RECV: {(diagnostics?.network?.bytes_recv / 1024 / 1024).toFixed(2)} MB</span>
                    </p>

                    <p className="text-emerald-400 mt-2">$ df -h /</p>
                    <p className="text-gray-300">Used: {(diagnostics?.disk?.used / 1024 / 1024 / 1024).toFixed(2)} GB / Total: {(diagnostics?.disk?.total / 1024 / 1024 / 1024).toFixed(2)} GB</p>

                    <div className="flex items-center gap-1 mt-4">
                        <span className="text-emerald-400">$</span>
                        <div className="w-2 h-4 bg-emerald-400 animate-pulse" />
                    </div>
                </div>
            </div>
        </div>
    );
};

export default SystemStatus;
