// src/pages/Search.jsx
import React, { useState } from 'react';
import { Search as SearchIcon, FileText, Calendar, ExternalLink, Zap, Database, RefreshCw } from 'lucide-react';
import { dashboardAPI } from '../services/api';

const SearchPage = () => {
    const [query, setQuery] = useState('');
    const [results, setResults] = useState([]);
    const [loading, setLoading] = useState(false);

    const handleSearch = async (e) => {
        e.preventDefault();
        if (!query.trim()) return;
        setLoading(true);
        try {
            const res = await dashboardAPI.searchDocuments(query);
            setResults(res.data || []);
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="space-y-10 pb-20">
            <header>
                <h2 className="text-4xl font-black text-white tracking-tight">Semantic Discovery</h2>
                <p className="text-slate-400 mt-2 text-lg">Search through extracted content of all unstructured assets</p>
            </header>

            <form onSubmit={handleSearch} className="relative group">
                <SearchIcon className="absolute left-6 top-1/2 -translate-y-1/2 w-6 h-6 text-slate-500 group-focus-within:text-blue-500 transition-colors" />
                <input
                    type="text"
                    placeholder="Enter keywords, phrases, or document metadata..."
                    className="w-full bg-slate-900/40 border border-white/10 rounded-3xl py-6 pl-16 pr-32 text-white placeholder-slate-600 focus:ring-4 focus:ring-blue-500/10 focus:border-blue-500/30 outline-none transition-all text-xl shadow-2xl"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                />
                <button
                    type="submit"
                    className="absolute right-4 top-1/2 -translate-y-1/2 bg-blue-600 hover:bg-blue-500 text-white font-black px-6 py-3 rounded-2xl transition-all shadow-xl shadow-blue-600/30 active:scale-95 flex items-center gap-2"
                >
                    {loading ? <RefreshCw className="w-5 h-5 animate-spin" /> : <Zap className="w-5 h-5" />}
                    Analyze
                </button>
            </form>

            <div className="space-y-6">
                {results.length > 0 ? (
                    results.map((doc) => (
                        <div key={doc.id} className="glass-card hover:bg-slate-900/60 transition-all border border-white/5 group">
                            <div className="flex gap-6">
                                <div className="p-4 bg-blue-500/10 rounded-2xl h-fit">
                                    <FileText className="w-8 h-8 text-blue-400" />
                                </div>
                                <div className="flex-1 space-y-3">
                                    <div className="flex justify-between items-start">
                                        <h3 className="text-xl font-bold text-white group-hover:text-blue-400 transition-colors">{doc.object_name}</h3>
                                        <div className="flex items-center gap-4 text-xs font-mono text-slate-500">
                                            <div className="flex items-center gap-1">
                                                <Calendar className="w-3 h-3" />
                                                {new Date(doc.created_at).toLocaleDateString()}
                                            </div>
                                            <span className="bg-white/5 px-2 py-0.5 rounded uppercase font-bold text-[10px] tracking-widest">{doc.file_type}</span>
                                        </div>
                                    </div>
                                    <p className="text-slate-400 leading-relaxed italic text-sm">
                                        "...{doc.preview}..."
                                    </p>
                                    <div className="pt-4 border-t border-white/5 flex gap-4">
                                        <button className="text-xs font-bold text-blue-400 flex items-center gap-1 hover:underline">
                                            <ExternalLink className="w-3 h-3" /> View Asset
                                        </button>
                                        <button className="text-xs font-bold text-slate-500 flex items-center gap-1 hover:text-white transition-colors">
                                            <Database className="w-3 h-3" /> Lineage Graph
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    ))
                ) : !loading && query && (
                    <div className="glass-card text-center p-20 border-dashed border-white/10">
                        <Database className="w-16 h-16 text-slate-700 mx-auto mb-4" />
                        <h4 className="text-xl font-bold text-white mb-2">No Semantic Matches</h4>
                        <p className="text-slate-500">Try adjusting your query or extending the search scope.</p>
                    </div>
                )}
            </div>
        </div>
    );
};

export default SearchPage;
