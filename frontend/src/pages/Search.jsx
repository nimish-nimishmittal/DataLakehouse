// src/pages/Search.jsx
import React, { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Search, Sparkles, Database, ChevronDown, ChevronUp,
  RefreshCw, Layers, Cpu, Zap, TableProperties, X,
  AlertCircle, BoxSelect, SlidersHorizontal
} from 'lucide-react';
import { searchAPI } from '../services/api';

// ── Similarity helpers ────────────────────────────────────────────────────
const simColor = (s) => {
  if (s >= 0.85) return { bar: 'bg-emerald-500', text: 'text-emerald-400', badge: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' };
  if (s >= 0.70) return { bar: 'bg-blue-500',    text: 'text-blue-400',    badge: 'bg-blue-500/10 text-blue-400 border-blue-500/20' };
  if (s >= 0.55) return { bar: 'bg-amber-500',   text: 'text-amber-400',   badge: 'bg-amber-500/10 text-amber-400 border-amber-500/20' };
  return           { bar: 'bg-slate-600',         text: 'text-slate-400',   badge: 'bg-slate-500/10 text-slate-400 border-slate-500/20' };
};

const simLabel = (s) => {
  if (s >= 0.85) return 'High';
  if (s >= 0.70) return 'Good';
  if (s >= 0.55) return 'Weak';
  return 'Low';
};

const sourceStyle = (src) => {
  if (src === 'ollama_768') return 'bg-violet-500/10 text-violet-400 border border-violet-500/20';
  if (src === 'minilm_384') return 'bg-amber-500/10  text-amber-400  border border-amber-500/20';
  if (src === 'both')       return 'bg-blue-500/10   text-blue-400   border border-blue-500/20';
  return 'bg-slate-500/10 text-slate-400 border border-slate-500/20';
};

const sourceLabel = (src) => {
  if (src === 'ollama_768') return '768d · Ollama';
  if (src === 'minilm_384') return '384d · MiniLM';
  if (src === 'both')       return 'Dual';
  return src || '—';
};

const shortTable = (name) =>
  name.replace(/^data_/, '').replace(/_/g, ' ');

// ── Sub-components ────────────────────────────────────────────────────────

const StatCard = ({ icon: Icon, label, value, color, sub }) => (
  <motion.div
    initial={{ opacity: 0, y: 16 }}
    animate={{ opacity: 1, y: 0 }}
    className="stat-card"
  >
    <div className="flex items-center gap-4">
      <div className={`p-3 rounded-2xl bg-${color}-500/10 text-${color}-400 ring-1 ring-${color}-500/20 shrink-0`}>
        <Icon className="w-5 h-5" />
      </div>
      <div className="min-w-0">
        <p className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">{label}</p>
        <p className="text-2xl font-black text-white tabular-nums">{value ?? '—'}</p>
        {sub && <p className="text-[10px] text-slate-600 mt-0.5">{sub}</p>}
      </div>
    </div>
  </motion.div>
);

const ResultCard = ({ result, index }) => {
  const [expanded, setExpanded] = useState(false);
  const sim = result.similarity ?? 0;
  const colors = simColor(sim);
  const rowEntries = result.row_data
    ? Object.entries(result.row_data).filter(([, v]) => v !== null && v !== '')
    : [];

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.04 }}
      className={`glass-card p-0 overflow-hidden transition-all duration-200 ${
        expanded ? 'border-blue-500/20' : 'hover:border-white/10'
      }`}
    >
      {/* Main row */}
      <button
        className="w-full text-left p-5 flex items-start gap-4"
        onClick={() => setExpanded(e => !e)}
      >
        {/* Rank */}
        <span className="text-xs font-black text-slate-700 w-5 shrink-0 pt-0.5 tabular-nums">
          {index + 1}
        </span>

        {/* Info */}
        <div className="flex-1 min-w-0">
          <div className="flex flex-wrap items-center gap-2 mb-2">
            {/* Table name */}
            <span className="px-2.5 py-0.5 rounded-lg bg-blue-500/10 text-blue-400 border border-blue-500/20 text-[10px] font-bold uppercase tracking-wider">
              {shortTable(result.table_name)}
            </span>
            {/* Row id */}
            <span className="px-2 py-0.5 rounded-lg bg-white/5 text-slate-500 text-[10px] font-mono border border-white/5">
              row {result.row_id}
            </span>
            {/* Source badge */}
            <span className={`px-2.5 py-0.5 rounded-lg text-[10px] font-bold ${sourceStyle(result.source)}`}>
              {sourceLabel(result.source)}
            </span>
          </div>
          {/* Serialised text preview */}
          <p className="text-xs text-slate-500 truncate leading-relaxed">
            {result.row_text}
          </p>
        </div>

        {/* Similarity score */}
        <div className="shrink-0 text-right ml-4 flex flex-col items-end gap-1.5">
          <span className={`text-xl font-black tabular-nums ${colors.text}`}>
            {(sim * 100).toFixed(1)}
            <span className="text-xs font-medium">%</span>
          </span>
          <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full border ${colors.badge}`}>
            {simLabel(sim)}
          </span>
          {/* Similarity bar */}
          <div className="w-20 h-1 bg-white/5 rounded-full overflow-hidden">
            <div
              className={`h-full rounded-full ${colors.bar} transition-all duration-700`}
              style={{ width: `${Math.round(sim * 100)}%` }}
            />
          </div>
        </div>

        {/* Expand chevron */}
        <span className="text-slate-600 ml-2 mt-0.5 shrink-0">
          {expanded ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
        </span>
      </button>

      {/* Expanded row data */}
      <AnimatePresence>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <div className="px-5 pb-5 border-t border-white/5 pt-4">
              <p className="text-[10px] font-bold text-slate-600 uppercase tracking-widest mb-3">
                Row Data
              </p>
              {rowEntries.length > 0 ? (
                <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-2">
                  {rowEntries.map(([col, val]) => (
                    <div key={col} className="bg-white/5 border border-white/5 rounded-xl p-3">
                      <p className="text-[10px] text-slate-600 font-bold uppercase tracking-wider mb-1 truncate">
                        {col}
                      </p>
                      <p className="text-xs text-slate-300 break-words line-clamp-3" title={String(val)}>
                        {String(val)}
                      </p>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-xs text-slate-600">No row data available.</p>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
};

// ── Main page ─────────────────────────────────────────────────────────────

const SUGGESTED = [
  'employee names and departments',
  'quarterly revenue figures',
  'software license renewals',
  'student performance scores',
  'IT asset inventory',
  'customer analysis data',
  'project status updates',
  'privacy policy clauses',
];

const SearchPage = () => {
  const [query, setQuery]             = useState('');
  const [topK, setTopK]               = useState(10);
  const [tableFilter, setTableFilter] = useState([]); // list of selected table names
  const [results, setResults]         = useState(null);
  const [searching, setSearching]     = useState(false);
  const [searchErr, setSearchErr]     = useState('');

  const [status, setStatus]           = useState(null);
  const [tables, setTables]           = useState([]);
  const [statusLoading, setStatusLoading] = useState(true);

  const [showFilters, setShowFilters] = useState(false);
  const inputRef = useRef(null);

  // Load status + table list on mount
  useEffect(() => {
    const load = async () => {
      setStatusLoading(true);
      try {
        const [sRes, tRes] = await Promise.allSettled([
          searchAPI.getEmbedStatus(),
          searchAPI.getEmbedTables(),
        ]);
        if (sRes.status === 'fulfilled') setStatus(sRes.value.data);
        if (tRes.status === 'fulfilled') setTables(tRes.value.data.tables || []);
      } catch (e) {
        console.error('Failed loading search metadata', e);
      } finally {
        setStatusLoading(false);
      }
    };
    load();
  }, []);

  const doSearch = async (q = query) => {
    const trimmed = q.trim();
    if (!trimmed) return;
    setSearching(true);
    setSearchErr('');
    setResults(null);
    try {
      const res = await searchAPI.semanticSearch(trimmed, topK, tableFilter.length > 0 ? tableFilter : null);
      setResults(res.data);
    } catch (e) {
      setSearchErr(e.response?.data?.error || e.message || 'Search failed');
    } finally {
      setSearching(false);
    }
  };

  const handleSuggest = (q) => {
    setQuery(q);
    doSearch(q);
  };

  const clearResults = () => {
    setResults(null);
    setQuery('');
    setTableFilter([]);
    inputRef.current?.focus();
  };

  const emb   = status?.embeddings || {};
  const queue = status?.queue       || {};

  return (
    <div className="space-y-10 pb-20">

      {/* ── Header ── */}
      <header className="flex flex-col md:flex-row md:items-center justify-between gap-6">
        <div>
          <h2 className="text-4xl font-black text-white tracking-tight flex items-center gap-3">
            Semantic Search
            <span className="px-2 py-0.5 rounded-lg bg-violet-500/10 text-violet-400 border border-violet-500/20 text-sm font-bold">
              pgvector
            </span>
          </h2>
          <p className="text-slate-400 mt-2 text-lg">
            Natural language search across all embedded tables
          </p>
        </div>
        <div className="flex items-center gap-3 flex-wrap">
          {!statusLoading && emb.total_rows > 0 && (
            <div className="flex items-center gap-2 px-4 py-2 bg-emerald-500/10 border border-emerald-500/20 rounded-xl">
              <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
              <span className="text-[10px] font-bold text-emerald-400 uppercase tracking-widest">
                {emb.total_rows?.toLocaleString()} rows indexed
              </span>
            </div>
          )}
          {queue.pending > 0 && (
            <div className="flex items-center gap-2 px-4 py-2 bg-amber-500/10 border border-amber-500/20 rounded-xl">
              <div className="w-2 h-2 rounded-full bg-amber-500 animate-pulse" />
              <span className="text-[10px] font-bold text-amber-400 uppercase tracking-widest">
                {queue.pending} pending
              </span>
            </div>
          )}
        </div>
      </header>

      {/* ── Stat cards ── */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard
          icon={Database}
          label="Total Embeddings"
          value={emb.total_rows?.toLocaleString()}
          color="blue"
          sub="rows indexed"
        />
        <StatCard
          icon={TableProperties}
          label="Tables Indexed"
          value={emb.tables_indexed}
          color="indigo"
          sub="across all sources"
        />
        <StatCard
          icon={Cpu}
          label="768-dim rows"
          value={emb.rows_with_768?.toLocaleString()}
          color="violet"
          sub="Ollama · nomic-embed-text"
        />
        <StatCard
          icon={Zap}
          label="384-dim rows"
          value={emb.rows_with_384?.toLocaleString()}
          color="amber"
          sub="MiniLM fallback"
        />
      </div>

      {/* ── Search bar ── */}
      <div className="glass-card space-y-4">
        {/* Input row */}
        <div className="flex gap-3">
          <div className="flex-1 relative group">
            <Search className="absolute left-5 top-1/2 -translate-y-1/2 w-5 h-5 text-slate-500 group-focus-within:text-blue-400 transition-colors pointer-events-none" />
            <input
              ref={inputRef}
              type="text"
              placeholder="Ask anything about your data… e.g. employees in engineering"
              className="w-full bg-slate-900/40 border border-white/10 rounded-2xl py-4 pl-14 pr-5 text-white placeholder-slate-600 focus:ring-4 focus:ring-blue-500/10 focus:border-blue-500/30 outline-none transition-all text-base shadow-xl"
              value={query}
              onChange={e => setQuery(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && doSearch()}
            />
          </div>

          {/* Filters toggle */}
          <button
            onClick={() => setShowFilters(f => !f)}
            className={`flex items-center gap-2 px-4 rounded-2xl border transition-all text-sm font-bold ${
              showFilters || tableFilter
                ? 'bg-blue-600/20 border-blue-500/30 text-blue-400'
                : 'bg-slate-900/40 border-white/10 text-slate-400 hover:text-white hover:border-white/20'
            }`}
          >
            <SlidersHorizontal className="w-4 h-4" />
            <span className="hidden sm:inline">Filters</span>
            {tableFilter.length > 0 && (
              <span className="w-2 h-2 rounded-full bg-blue-400" />
            )}
          </button>

          {/* Search button */}
          <button
            onClick={() => doSearch()}
            disabled={!query.trim() || searching}
            className="flex items-center gap-2 px-6 bg-blue-600 hover:bg-blue-500 disabled:opacity-40 disabled:cursor-not-allowed text-white font-black rounded-2xl transition-all shadow-lg shadow-blue-600/20 active:scale-95 text-sm whitespace-nowrap"
          >
            {searching
              ? <RefreshCw className="w-4 h-4 animate-spin" />
              : <Sparkles className="w-4 h-4" />
            }
            {searching ? 'Searching…' : 'Search'}
          </button>
        </div>

        {/* Filters panel */}
        <AnimatePresence>
          {showFilters && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.18 }}
              className="overflow-hidden"
            >
              <div className="pt-2 flex flex-col sm:flex-row gap-4 border-t border-white/5">
                <div className="flex-1">
                  <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-2 block">
                    Filter by table
                  </label>
                  <div className="bg-slate-900/50 border border-white/10 rounded-xl p-2 max-h-44 overflow-y-auto space-y-0.5">
                    {tables.length === 0 ? (
                      <p className="text-xs text-slate-600 p-2">No tables indexed yet</p>
                    ) : (
                      <>
                        <button
                          onClick={() => setTableFilter([])}
                          className={`w-full text-left px-3 py-1.5 rounded-lg text-xs transition-colors ${
                            tableFilter.length === 0
                              ? 'bg-blue-500/20 text-blue-300 font-bold'
                              : 'text-slate-500 hover:text-slate-300 hover:bg-white/5'
                          }`}
                        >
                          All tables ({tables.length})
                        </button>
                        {tables.map(t => {
                          const selected = tableFilter.includes(t.table_name);
                          return (
                            <button
                              key={t.table_name}
                              onClick={() => setTableFilter(prev =>
                                selected
                                  ? prev.filter(x => x !== t.table_name)
                                  : [...prev, t.table_name]
                              )}
                              className={`w-full text-left px-3 py-1.5 rounded-lg text-xs transition-colors flex items-center justify-between gap-2 ${
                                selected
                                  ? 'bg-blue-500/20 text-blue-300 font-bold border border-blue-500/20'
                                  : 'text-slate-500 hover:text-slate-300 hover:bg-white/5'
                              }`}
                            >
                              <span className="truncate">{shortTable(t.table_name)}</span>
                              <span className="text-slate-600 shrink-0 font-mono">{t.embedded_rows}</span>
                            </button>
                          );
                        })}
                      </>
                    )}
                  </div>
                </div>
                <div className="sm:w-36">
                  <label className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-2 block">
                    Top K results
                  </label>
                  <input
                    type="number" min={1} max={50}
                    className="w-full bg-slate-900/50 border border-white/10 rounded-xl py-3 px-4 text-sm text-slate-300 outline-none focus:border-blue-500/30"
                    value={topK}
                    onChange={e => setTopK(Math.min(50, Math.max(1, Number(e.target.value))))}
                  />
                </div>
                {tableFilter.length > 0 && (
                  <div className="flex items-end">
                    <button
                      onClick={() => setTableFilter([])}
                      className="flex items-center gap-1.5 px-4 py-3 text-xs text-slate-500 hover:text-red-400 hover:bg-red-500/5 border border-white/5 rounded-xl transition-all"
                    >
                      <X className="w-3 h-3" />
                      Clear filter
                    </button>
                  </div>
                )}
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Error */}
        {searchErr && (
          <div className="flex items-center gap-3 p-4 bg-red-500/10 border border-red-500/20 rounded-xl text-red-400 text-sm">
            <AlertCircle className="w-4 h-4 shrink-0" />
            {searchErr}
          </div>
        )}
      </div>

      {/* ── Suggested queries (when idle) ── */}
      {!results && !searching && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          className="glass-card"
        >
          <h3 className="text-sm font-bold text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
            <Sparkles className="w-4 h-4 text-violet-400" />
            Suggested Queries
          </h3>
          <div className="flex flex-wrap gap-2">
            {SUGGESTED.map(q => (
              <button
                key={q}
                onClick={() => handleSuggest(q)}
                className="text-sm px-4 py-2 bg-white/5 hover:bg-blue-500/10 border border-white/5 hover:border-blue-500/20 text-slate-400 hover:text-blue-400 rounded-xl transition-all duration-200 font-medium"
              >
                {q}
              </button>
            ))}
          </div>

          {/* Table index overview */}
          {tables.length > 0 && (
            <div className="mt-6 pt-6 border-t border-white/5">
              <h3 className="text-sm font-bold text-slate-500 uppercase tracking-widest mb-4 flex items-center gap-2">
                <Layers className="w-4 h-4 text-blue-400" />
                Indexed Tables
              </h3>
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-2 max-h-60 overflow-y-auto pr-1">
                {tables.map(t => (
                  <button
                    key={t.table_name}
                    onClick={() => { setTableFilter(prev => prev.includes(t.table_name) ? prev.filter(x => x !== t.table_name) : [...prev, t.table_name]); setShowFilters(true); }}
                    className="text-left px-3 py-2.5 bg-white/5 hover:bg-blue-500/10 border border-white/5 hover:border-blue-500/20 rounded-xl transition-all group"
                  >
                    <p className="text-xs font-bold text-slate-300 group-hover:text-blue-300 truncate transition-colors">
                      {shortTable(t.table_name)}
                    </p>
                    <p className="text-[10px] text-slate-600 mt-0.5 font-mono">
                      {t.embedded_rows} rows
                      {t.rows_768 > 0 && <span className="text-violet-600"> · 768d</span>}
                      {t.rows_384 > 0 && <span className="text-amber-700"> · 384d</span>}
                    </p>
                  </button>
                ))}
              </div>
            </div>
          )}
        </motion.div>
      )}

      {/* ── Loading skeleton ── */}
      {searching && (
        <div className="space-y-3">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="glass-card animate-pulse flex items-center gap-4 p-5">
              <div className="w-5 h-3 bg-white/5 rounded" />
              <div className="flex-1 space-y-2">
                <div className="flex gap-2">
                  <div className="h-4 w-32 bg-white/5 rounded-lg" />
                  <div className="h-4 w-16 bg-white/5 rounded-lg" />
                </div>
                <div className="h-3 w-3/4 bg-white/5 rounded" />
              </div>
              <div className="w-14 h-8 bg-white/5 rounded-xl" />
            </div>
          ))}
        </div>
      )}

      {/* ── Results ── */}
      {results && !searching && (
        <div className="space-y-4">
          {/* Results header */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <h3 className="text-xl font-black text-white">Results</h3>
              <span className="px-3 py-1 rounded-xl bg-blue-500/10 text-blue-400 border border-blue-500/20 text-xs font-bold">
                {results.total} match{results.total !== 1 ? 'es' : ''}
              </span>
              <span className="text-xs text-slate-600 font-mono">
                for "{results.query}"
                {tableFilter.length === 1 && ` in ${shortTable(tableFilter[0])}`}{tableFilter.length > 1 && ` across ${tableFilter.length} tables`}
              </span>
            </div>
            <button
              onClick={clearResults}
              className="flex items-center gap-1.5 text-xs text-slate-500 hover:text-slate-300 transition-colors px-3 py-1.5 hover:bg-white/5 rounded-xl"
            >
              <X className="w-3.5 h-3.5" />
              Clear
            </button>
          </div>

          {/* No results */}
          {results.results.length === 0 ? (
            <div className="glass-card flex flex-col items-center justify-center py-16 gap-3">
              <Search className="w-10 h-10 text-slate-700" />
              <p className="text-slate-500 font-medium">No results found</p>
              <p className="text-xs text-slate-600">
                Try a broader query or check that the embedding DAG has run
              </p>
            </div>
          ) : (
            <div className="space-y-3">
              {results.results.map((r, i) => (
                <ResultCard key={`${r.table_name}-${r.row_id}`} result={r} index={i} />
              ))}
            </div>
          )}
        </div>
      )}

    </div>
  );
};

export default SearchPage;