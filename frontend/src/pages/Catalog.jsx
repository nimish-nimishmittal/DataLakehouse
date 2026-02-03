// src/pages/Catalog.jsx
import React, { useState, useEffect } from 'react';
import { Search, Filter, Database, RefreshCw, Layers, FileText, Download } from 'lucide-react';
import FileList from '../components/FileList';
import { dashboardAPI } from '../services/api';

const Catalog = () => {
    const [files, setFiles] = useState([]);
    const [loading, setLoading] = useState(true);
    const [search, setSearch] = useState('');
    const [format, setFormat] = useState('');

    const fetchFiles = async () => {
        try {
            const res = await dashboardAPI.getFiles({ search, format });
            setFiles(res.data.files || []);
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => { fetchFiles(); }, [search, format]);

    return (
        <div className="space-y-10 pb-20">
            <header className="flex flex-col md:flex-row md:items-center justify-between gap-6">
                <div>
                    <h2 className="text-4xl font-black text-white tracking-tight">Enterprise Catalog</h2>
                    <p className="text-slate-400 mt-2 text-lg">Cross-platform data discovery and management</p>
                </div>
                <div className="flex items-center gap-3 p-1.5 bg-slate-900/50 rounded-2xl border border-white/5 backdrop-blur-md">
                    <button
                        onClick={() => setFormat('')}
                        className={`px-4 py-2 rounded-xl text-xs font-bold uppercase transition-all ${!format ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/20' : 'text-slate-500 hover:text-white'}`}
                    >
                        All
                    </button>
                    <button
                        onClick={() => setFormat('csv')}
                        className={`px-4 py-2 rounded-xl text-xs font-bold uppercase transition-all ${format === 'csv' ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/20' : 'text-slate-500 hover:text-white'}`}
                    >
                        CSV
                    </button>
                    <button
                        onClick={() => setFormat('json')}
                        className={`px-4 py-2 rounded-xl text-xs font-bold uppercase transition-all ${format === 'json' ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/20' : 'text-slate-500 hover:text-white'}`}
                    >
                        JSON
                    </button>
                </div>
            </header>

            <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
                <div className="lg:col-span-3 relative group">
                    <Search className="absolute left-5 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-500 group-focus-within:text-blue-500 transition-colors" />
                    <input
                        type="text"
                        placeholder="Search assets by schema, name, or metadata tags..."
                        className="w-full bg-slate-900/40 border border-white/10 rounded-2xl py-5 pl-14 pr-6 text-white placeholder-slate-600 focus:ring-4 focus:ring-blue-500/10 focus:border-blue-500/30 outline-none transition-all text-lg shadow-2xl"
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                    />
                </div>

                <button
                    onClick={fetchFiles}
                    className="flex items-center justify-center gap-3 bg-blue-600 hover:bg-blue-500 text-white font-black rounded-2xl transition-all shadow-xl shadow-blue-600/30 active:scale-95 text-lg"
                >
                    <RefreshCw className="w-6 h-6" />
                    Sync
                </button>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                {[
                    { label: 'Structured', count: files.filter(f => f.file_format !== 'pdf').length, icon: Database, color: 'blue' },
                    { label: 'Unstructured', count: files.filter(f => f.file_format === 'pdf').length, icon: FileText, color: 'indigo' },
                    { label: 'Total Volume', count: `${(files.reduce((acc, f) => acc + (f.object_size || 0), 0) / 1024 / 1024).toFixed(1)} MB`, icon: Layers, color: 'emerald' },
                    { label: 'Ready Assets', count: files.length, icon: RefreshCw, color: 'purple' },
                ].map((stat) => (
                    <div key={stat.label} className="glass-card p-4 flex items-center gap-4">
                        <div className={`p-3 rounded-xl bg-${stat.color}-500/10 text-${stat.color}-400`}>
                            <stat.icon className="w-5 h-5" />
                        </div>
                        <div>
                            <p className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">{stat.label}</p>
                            <p className="text-lg font-black text-white">{stat.count}</p>
                        </div>
                    </div>
                ))}
            </div>

            <div className="glass-card p-0 overflow-hidden shadow-2xl border-white/5">
                <FileList files={files} onRefresh={fetchFiles} />
            </div>
        </div>
    );
};

export default Catalog;
