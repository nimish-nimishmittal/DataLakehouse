// src/pages/Jobs.jsx
import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Activity, Play, CheckCircle2, XCircle, Clock, Search, RefreshCw, Layers } from 'lucide-react';
import { dashboardAPI } from '../services/api';
import { toast } from 'react-toastify';

const Jobs = () => {
    const [jobs, setJobs] = useState([]);
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);

    const fetchJobs = async () => {
        setRefreshing(true);
        try {
            const res = await dashboardAPI.getJobs();
            setJobs(res.data.jobs || []);
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
            setRefreshing(false);
        }
    };

    const handleTrigger = async (id) => {
        try {
            await dashboardAPI.triggerJob(id);
            toast.success(`Pipeline triggered: ${id}`);
            fetchJobs();
        } catch (err) {
            toast.error('Failed to trigger pipeline');
        }
    };

    useEffect(() => { fetchJobs(); }, []);

    const getStatusColor = (status) => {
        switch (status?.toLowerCase()) {
            case 'success': return 'text-emerald-400 bg-emerald-400/10 border-emerald-400/20';
            case 'running': return 'text-blue-400 bg-blue-400/10 border-blue-400/20';
            case 'failed': return 'text-red-400 bg-red-400/10 border-red-400/20';
            default: return 'text-gray-400 bg-gray-400/10 border-gray-400/20';
        }
    };

    const StatusIcon = ({ status }) => {
        switch (status?.toLowerCase()) {
            case 'success': return <CheckCircle2 className="w-4 h-4" />;
            case 'running': return <RefreshCw className="w-4 h-4 animate-spin" />;
            case 'failed': return <XCircle className="w-4 h-4" />;
            default: return <Clock className="w-4 h-4" />;
        }
    };

    if (loading) return (
        <div className="flex items-center justify-center h-[80vh]">
            <RefreshCw className="w-8 h-8 text-blue-500 animate-spin" />
        </div>
    );

    return (
        <div className="space-y-8 pb-20">
            <header className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                <div>
                    <h2 className="text-3xl font-bold text-white tracking-tight">Data Pipelines</h2>
                    <p className="text-gray-500 mt-1">Monitor ETL orchestration and automation tasks</p>
                </div>
                <button
                    onClick={fetchJobs}
                    disabled={refreshing}
                    className="flex items-center px-4 py-2 bg-gray-800 hover:bg-gray-700 border border-white/5 rounded-xl transition-all text-sm font-medium"
                >
                    <RefreshCw className={`w-4 h-4 mr-2 ${refreshing ? 'animate-spin' : ''}`} />
                    Refresh Status
                </button>
            </header>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {jobs.length === 0 ? (
                    <div className="col-span-full p-20 bg-gray-800/40 rounded-3xl border border-dashed border-white/5 text-center text-gray-500">
                        <Activity className="w-12 h-12 mx-auto mb-4 opacity-20" />
                        <p>No active pipelines detected in Airflow.</p>
                    </div>
                ) : jobs.map((job) => (
                    <motion.div
                        key={job.id}
                        initial={{ opacity: 0, y: 20 }}
                        animate={{ opacity: 1, y: 0 }}
                        className="bg-gray-800/40 backdrop-blur-md border border-white/5 p-6 rounded-3xl space-y-4"
                    >
                        <div className="flex justify-between items-start">
                            <div className="p-3 bg-blue-500/10 text-blue-400 rounded-2xl">
                                <Layers className="w-6 h-6" />
                            </div>
                            <span className={`flex items-center gap-1.5 px-3 py-1 rounded-full text-[10px] font-bold uppercase border ${getStatusColor(job.status)}`}>
                                <StatusIcon status={job.status} />
                                {job.status}
                            </span>
                        </div>

                        <div>
                            <h3 className="text-lg font-bold text-white truncate">{job.label}</h3>
                            <p className="text-xs text-gray-500 font-mono mt-1">{job.id}</p>
                        </div>

                        <div className="pt-4 flex items-center justify-between border-t border-white/5">
                            <div className="flex items-center gap-2 text-[10px] text-gray-400 uppercase font-bold tracking-wider">
                                <Clock className="w-3.5 h-3.5" />
                                Last Run: {job.last_run ? new Date(job.last_run).toLocaleDateString() : 'N/A'}
                            </div>
                            <button
                                onClick={() => handleTrigger(job.id)}
                                className="bg-blue-600 hover:bg-blue-500 p-2 rounded-xl text-white transition-all shadow-lg shadow-blue-500/20"
                                title="Trigger Pipeline"
                            >
                                <Play className="w-4 h-4 fill-current" />
                            </button>
                        </div>
                    </motion.div>
                ))}
            </div>

            <div className="bg-blue-600/10 border border-blue-500/20 p-8 rounded-3xl text-center">
                <p className="text-blue-400 text-sm font-medium">
                    Want to add a new pipeline? Drop a Python script in <code>/airflow/dags</code> to automate your data.
                </p>
            </div>
        </div>
    );
};

export default Jobs;
