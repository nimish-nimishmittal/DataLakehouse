// src/pages/RAG.jsx
import React, { useState, useEffect, useRef, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  BrainCircuit, Sparkles, Send, RefreshCw, ChevronDown, ChevronUp,
  MessageSquare, BarChart2, TrendingUp, X, Database, Layers,
  ToggleLeft, ToggleRight, AlertCircle, CheckCircle, Cpu,
  SlidersHorizontal, RotateCcw, User, Bot, TableProperties, Zap
} from 'lucide-react';
import { searchAPI } from '../services/api';

// ── Constants ─────────────────────────────────────────────────────────────
const RAG_BASE = 'http://localhost:5000';

const MODES = [
  {
    id:    'qa',
    label: 'Q & A',
    icon:  MessageSquare,
    color: 'blue',
    desc:  'Ask questions and get answers grounded in your data',
    placeholder: 'e.g. What was the total revenue in Q3?',
  },
  {
    id:    'summarize',
    label: 'Summarize',
    icon:  BarChart2,
    color: 'violet',
    desc:  'Summarize a table or topic — patterns, outliers, key takeaways',
    placeholder: 'e.g. Summarize the sales data for North region',
  },
  {
    id:    'predict',
    label: 'Predict',
    icon:  TrendingUp,
    color: 'emerald',
    desc:  'Trend analysis and predictions based on your data',
    placeholder: 'e.g. What are the revenue trends and what can we expect next quarter?',
  },
];

const MODE_MAP = Object.fromEntries(MODES.map(m => [m.id, m]));

const shortTable = (name) => (name || '').replace(/^data_/, '').replace(/_/g, ' ');

const simColor = (s) => {
  if (s >= 0.85) return { text: 'text-emerald-400', badge: 'bg-emerald-500/10 border-emerald-500/20 text-emerald-400' };
  if (s >= 0.70) return { text: 'text-blue-400',    badge: 'bg-blue-500/10 border-blue-500/20 text-blue-400' };
  if (s >= 0.55) return { text: 'text-amber-400',   badge: 'bg-amber-500/10 border-amber-500/20 text-amber-400' };
  return             { text: 'text-slate-400',       badge: 'bg-slate-500/10 border-slate-500/20 text-slate-400' };
};

// ── Source citation card ──────────────────────────────────────────────────
const SourceCard = ({ source, index }) => {
  const [open, setOpen] = useState(false);
  const sim    = source.similarity ?? 0;
  const colors = simColor(sim);
  const entries = source.row_data
    ? Object.entries(source.row_data).filter(([, v]) => v !== null && String(v).trim())
    : [];

  return (
    <div className="bg-slate-900/60 border border-white/5 rounded-xl overflow-hidden">
      <button
        className="w-full text-left px-3 py-2.5 flex items-center gap-3"
        onClick={() => setOpen(o => !o)}
      >
        <span className="text-[10px] font-black text-slate-700 w-4 shrink-0">{index + 1}</span>
        <span className="px-2 py-0.5 rounded-lg bg-blue-500/10 text-blue-400 border border-blue-500/20 text-[9px] font-bold uppercase tracking-wider truncate flex-1">
          {shortTable(source.table_name)}
        </span>
        <span className={`text-[10px] font-bold tabular-nums ${colors.text}`}>
          {(sim * 100).toFixed(0)}%
        </span>
        {open
          ? <ChevronUp className="w-3 h-3 text-slate-600 shrink-0" />
          : <ChevronDown className="w-3 h-3 text-slate-600 shrink-0" />}
      </button>
      <AnimatePresence>
        {open && entries.length > 0 && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.15 }}
            className="overflow-hidden"
          >
            <div className="px-3 pb-3 pt-1 border-t border-white/5 grid grid-cols-2 gap-1.5">
              {entries.slice(0, 8).map(([col, val]) => (
                <div key={col} className="bg-white/5 rounded-lg px-2 py-1.5">
                  <p className="text-[9px] text-slate-600 font-bold uppercase tracking-wider truncate">{col}</p>
                  <p className="text-[10px] text-slate-300 truncate" title={String(val)}>{String(val)}</p>
                </div>
              ))}
              {entries.length > 8 && (
                <p className="text-[9px] text-slate-600 col-span-2 text-center pt-1">
                  +{entries.length - 8} more fields
                </p>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
};

// ── Chat message bubble ───────────────────────────────────────────────────
const MessageBubble = ({ msg, isLast }) => {
  const [showSources, setShowSources] = useState(false);
  const isUser      = msg.role === 'user';
  const modeInfo    = msg.mode ? MODE_MAP[msg.mode] : null;
  const ModeIcon    = modeInfo?.icon;
  const modeColor   = modeInfo?.color || 'blue';

  // Render markdown-like formatting: **bold**, newlines → <br/>
  const renderText = (text) => {
    if (!text) return null;
    return text.split('\\n').map((line, i) => {
      const parts = line.split(/(\*\*[^*]+\*\*)/g);
      return (
        <React.Fragment key={i}>
          {parts.map((part, j) =>
            part.startsWith('**') && part.endsWith('**')
              ? <strong key={j} className="font-bold text-white">{part.slice(2, -2)}</strong>
              : part
          )}
          {i < text.split('\\n').length - 1 && <br />}
        </React.Fragment>
      );
    });
  };

  if (isUser) {
    return (
      <motion.div
        initial={{ opacity: 0, x: 20 }}
        animate={{ opacity: 1, x: 0 }}
        className="flex justify-end"
      >
        <div className="flex items-end gap-2 max-w-[80%]">
          <div className="bg-blue-600/20 border border-blue-500/20 rounded-2xl rounded-br-md px-4 py-3">
            {modeInfo && (
              <div className={`flex items-center gap-1.5 mb-1.5 text-${modeColor}-400`}>
                {ModeIcon && <ModeIcon className="w-3 h-3" />}
                <span className="text-[9px] font-bold uppercase tracking-widest">{modeInfo.label}</span>
              </div>
            )}
            <p className="text-sm text-slate-200 leading-relaxed">{msg.content}</p>
          </div>
          <div className="w-7 h-7 rounded-full bg-blue-600/30 border border-blue-500/20 flex items-center justify-center shrink-0 mb-0.5">
            <User className="w-3.5 h-3.5 text-blue-400" />
          </div>
        </div>
      </motion.div>
    );
  }

  // Assistant message
  return (
    <motion.div
      initial={{ opacity: 0, x: -20 }}
      animate={{ opacity: 1, x: 0 }}
      className="flex justify-start"
    >
      <div className="flex items-end gap-2 max-w-[88%]">
        <div className="w-7 h-7 rounded-full bg-violet-600/30 border border-violet-500/20 flex items-center justify-center shrink-0 mb-0.5">
          <Bot className="w-3.5 h-3.5 text-violet-400" />
        </div>
        <div className="flex-1 space-y-3">
          {/* Answer bubble */}
          <div className="glass-card p-4 rounded-2xl rounded-bl-md">
            {msg.streaming && !msg.content && (
              <div className="flex items-center gap-2 text-slate-500">
                <div className="flex gap-1">
                  {[0,1,2].map(i => (
                    <div
                      key={i}
                      className="w-1.5 h-1.5 rounded-full bg-violet-500 animate-bounce"
                      style={{ animationDelay: `${i * 0.15}s` }}
                    />
                  ))}
                </div>
                <span className="text-xs text-slate-600">Thinking…</span>
              </div>
            )}
            {msg.content && (
              <p className="text-sm text-slate-300 leading-relaxed whitespace-pre-wrap">
                {renderText(msg.content)}
                {msg.streaming && isLast && (
                  <span className="inline-block w-0.5 h-4 bg-violet-400 animate-pulse ml-0.5 align-middle" />
                )}
              </p>
            )}
            {msg.error && (
              <div className="flex items-center gap-2 text-red-400 text-sm">
                <AlertCircle className="w-4 h-4 shrink-0" />
                {msg.error}
              </div>
            )}
          </div>

          {/* Source citations */}
          {msg.sources && msg.sources.length > 0 && !msg.streaming && (
            <div>
              <button
                onClick={() => setShowSources(s => !s)}
                className="flex items-center gap-1.5 text-[10px] font-bold text-slate-500 hover:text-slate-300 transition-colors px-1"
              >
                <Database className="w-3 h-3" />
                {msg.sources.length} source{msg.sources.length !== 1 ? 's' : ''} used
                {showSources ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
              </button>
              <AnimatePresence>
                {showSources && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: 'auto', opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.18 }}
                    className="overflow-hidden mt-2"
                  >
                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                      {msg.sources.map((s, i) => (
                        <SourceCard key={`${s.table_name}-${s.row_id}-${i}`} source={s} index={i} />
                      ))}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          )}
        </div>
      </div>
    </motion.div>
  );
};

// ── Main RAG page ─────────────────────────────────────────────────────────
const RAGPage = () => {
  // Mode & settings
  const [mode,        setMode]        = useState('qa');
  const [multiTurn,   setMultiTurn]   = useState(true);
  const [topK,        setTopK]        = useState(10);
  const [tableFilter, setTableFilter] = useState([]);
  const [showFilters, setShowFilters] = useState(false);

  // Input
  const [query,    setQuery]    = useState('');
  const [loading,  setLoading]  = useState(false);

  // Chat history — list of { role, content, mode?, sources?, streaming?, error? }
  const [messages, setMessages] = useState([]);

  // Sidebar data
  const [tables,      setTables]      = useState([]);
  const [ragStatus,   setRagStatus]   = useState(null);
  const [statusLoading, setStatusLoading] = useState(true);

  const inputRef    = useRef(null);
  const bottomRef   = useRef(null);
  const abortRef    = useRef(null);   // AbortController for in-flight stream

  const currentMode = MODE_MAP[mode];

  // ── Load tables + RAG status ────────────────────────────────────────────
  useEffect(() => {
    const load = async () => {
      setStatusLoading(true);
      try {
        const [tRes] = await Promise.allSettled([searchAPI.getEmbedTables()]);
        if (tRes.status === 'fulfilled') setTables(tRes.value.data.tables || []);

        // RAG status — direct fetch (same JWT)
        const token = localStorage.getItem('token');
        const rRes  = await fetch(`${RAG_BASE}/rag/status`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (rRes.ok) setRagStatus(await rRes.json());
      } catch (e) {
        console.error('Failed loading RAG metadata', e);
      } finally {
        setStatusLoading(false);
      }
    };
    load();
  }, []);

  // ── Auto-scroll to bottom ───────────────────────────────────────────────
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // ── Build conversation history for multi-turn ───────────────────────────
  const buildHistory = useCallback(() => {
    if (!multiTurn) return [];
    return messages
      .filter(m => !m.streaming && !m.error && m.content)
      .map(m => ({ role: m.role, content: m.content }));
  }, [messages, multiTurn]);

  // ── Submit ──────────────────────────────────────────────────────────────
  const handleSubmit = async () => {
    const trimmed = query.trim();
    if (!trimmed || loading) return;

    // Abort any in-flight stream
    if (abortRef.current) abortRef.current.abort();
    abortRef.current = new AbortController();

    const userMsg = { role: 'user', content: trimmed, mode };
    const assistantMsg = { role: 'assistant', content: '', sources: null, streaming: true, mode };

    setMessages(prev => [...prev, userMsg, assistantMsg]);
    setQuery('');
    setLoading(true);

    const token   = localStorage.getItem('token');
    const history = buildHistory();

    try {
      const resp = await fetch(`${RAG_BASE}/rag/query`, {
        method:  'POST',
        headers: {
          'Content-Type':  'application/json',
          'Authorization': `Bearer ${token}`,
        },
        body: JSON.stringify({
          query:                trimmed,
          mode,
          top_k:                topK,
          table_filter:         tableFilter.length > 0 ? tableFilter : null,
          conversation_history: history,
        }),
        signal: abortRef.current.signal,
      });

      if (!resp.ok) {
        const err = await resp.text();
        setMessages(prev => {
          const copy = [...prev];
          copy[copy.length - 1] = { ...copy[copy.length - 1], streaming: false, error: `HTTP ${resp.status}: ${err.slice(0, 120)}` };
          return copy;
        });
        return;
      }

      const reader  = resp.body.getReader();
      const decoder = new TextDecoder();
      let   buffer  = '';
      let   answer  = '';
      let   sources = null;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const lines = buffer.split('\n');
        buffer = lines.pop(); // Keep incomplete line

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          const payload = line.slice(6);

          if (payload === '[DONE]') {
            setMessages(prev => {
              const copy = [...prev];
              copy[copy.length - 1] = {
                ...copy[copy.length - 1],
                content:   answer,
                sources,
                streaming: false,
              };
              return copy;
            });
            setLoading(false);
            return;
          }

          if (payload.startsWith('[ERROR]')) {
            const errMsg = payload.slice(7);
            setMessages(prev => {
              const copy = [...prev];
              copy[copy.length - 1] = {
                ...copy[copy.length - 1],
                streaming: false,
                error:     errMsg,
              };
              return copy;
            });
            setLoading(false);
            return;
          }

          if (payload.startsWith('[SOURCES]')) {
            try { sources = JSON.parse(payload.slice(9)); } catch {}
            continue;
          }

          // Regular token — replace escaped newlines back
          const token_text = payload.replace(/\\n/g, '\n');
          answer += token_text;

          setMessages(prev => {
            const copy = [...prev];
            copy[copy.length - 1] = {
              ...copy[copy.length - 1],
              content: answer,
            };
            return copy;
          });
        }
      }

      // Stream ended without [DONE] — finalise anyway
      setMessages(prev => {
        const copy = [...prev];
        copy[copy.length - 1] = { ...copy[copy.length - 1], content: answer, sources, streaming: false };
        return copy;
      });

    } catch (err) {
      if (err.name === 'AbortError') return;
      setMessages(prev => {
        const copy = [...prev];
        copy[copy.length - 1] = { ...copy[copy.length - 1], streaming: false, error: err.message };
        return copy;
      });
    } finally {
      setLoading(false);
      inputRef.current?.focus();
    }
  };

  const clearChat = () => {
    if (abortRef.current) abortRef.current.abort();
    setMessages([]);
    setLoading(false);
    inputRef.current?.focus();
  };

  // ── Status pill ─────────────────────────────────────────────────────────
  const modelOk = ragStatus?.model_loaded;

  return (
    <div className="flex gap-6 h-[calc(100vh-6rem)] pb-4">

      {/* ── Left: Chat area ─────────────────────────────────────────── */}
      <div className="flex-1 flex flex-col min-w-0">

        {/* Header */}
        <div className="flex items-center justify-between mb-4 shrink-0">
          <div>
            <h2 className="text-3xl font-black text-white tracking-tight flex items-center gap-3">
              RAG Chat
              <span className="px-2 py-0.5 rounded-lg bg-violet-500/10 text-violet-400 border border-violet-500/20 text-sm font-bold">
                gemma3:270m
              </span>
            </h2>
            <p className="text-slate-400 mt-1 text-sm">
              Ask questions, get summaries, and predict trends from your lakehouse data
            </p>
          </div>
          <div className="flex items-center gap-3">
            {/* Model status */}
            {!statusLoading && (
              <div className={`flex items-center gap-2 px-3 py-1.5 rounded-xl border text-[10px] font-bold uppercase tracking-widest ${
                modelOk
                  ? 'bg-emerald-500/10 border-emerald-500/20 text-emerald-400'
                  : 'bg-amber-500/10 border-amber-500/20 text-amber-400'
              }`}>
                <div className={`w-1.5 h-1.5 rounded-full ${modelOk ? 'bg-emerald-500 animate-pulse' : 'bg-amber-500'}`} />
                {modelOk ? 'Model Ready' : 'Model Loading'}
              </div>
            )}
            {/* Multi-turn toggle */}
            <button
              onClick={() => setMultiTurn(v => !v)}
              className={`flex items-center gap-2 px-3 py-1.5 rounded-xl border text-[10px] font-bold uppercase tracking-widest transition-all ${
                multiTurn
                  ? 'bg-blue-500/10 border-blue-500/20 text-blue-400'
                  : 'bg-white/5 border-white/10 text-slate-500'
              }`}
              title={multiTurn ? 'Multi-turn ON — conversation has memory' : 'Single-shot — no memory'}
            >
              {multiTurn ? <ToggleRight className="w-4 h-4" /> : <ToggleLeft className="w-4 h-4" />}
              {multiTurn ? 'Multi-turn' : 'Single-shot'}
            </button>
            {/* Clear */}
            {messages.length > 0 && (
              <button
                onClick={clearChat}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl border border-white/10 bg-white/5 text-slate-400 hover:text-red-400 hover:border-red-500/20 hover:bg-red-500/5 transition-all text-[10px] font-bold uppercase tracking-widest"
              >
                <RotateCcw className="w-3 h-3" />
                Clear
              </button>
            )}
          </div>
        </div>

        {/* Mode selector */}
        <div className="flex gap-2 mb-4 shrink-0">
          {MODES.map(m => {
            const Icon    = m.icon;
            const active  = mode === m.id;
            const cActive = `bg-${m.color}-500/10 border-${m.color}-500/20 text-${m.color}-400`;
            return (
              <button
                key={m.id}
                onClick={() => setMode(m.id)}
                className={`flex items-center gap-2 px-4 py-2.5 rounded-xl border transition-all text-sm font-bold ${
                  active
                    ? cActive
                    : 'bg-white/5 border-white/10 text-slate-400 hover:text-slate-200 hover:bg-white/10'
                }`}
              >
                <Icon className="w-4 h-4" />
                {m.label}
              </button>
            );
          })}
        </div>

        {/* Messages area */}
        <div className="flex-1 overflow-y-auto space-y-4 pr-1 mb-4 min-h-0">
          {messages.length === 0 ? (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="flex flex-col items-center justify-center h-full gap-4 text-center"
            >
              <div className="w-16 h-16 rounded-2xl bg-violet-500/10 border border-violet-500/20 flex items-center justify-center">
                <BrainCircuit className="w-8 h-8 text-violet-400" />
              </div>
              <div>
                <p className="text-white font-bold text-lg">Ready to answer</p>
                <p className="text-slate-500 text-sm mt-1 max-w-sm">{currentMode.desc}</p>
              </div>
              {/* Quick examples */}
              <div className="flex flex-wrap gap-2 justify-center max-w-lg mt-2">
                {[
                  'What are the top performing regions?',
                  'Summarize the employee data',
                  'What revenue trends do you see?',
                  'Which products have the highest churn risk?',
                ].map(q => (
                  <button
                    key={q}
                    onClick={() => { setQuery(q); inputRef.current?.focus(); }}
                    className="text-xs px-3 py-1.5 bg-white/5 hover:bg-blue-500/10 border border-white/5 hover:border-blue-500/20 text-slate-400 hover:text-blue-400 rounded-xl transition-all"
                  >
                    {q}
                  </button>
                ))}
              </div>
            </motion.div>
          ) : (
            messages.map((msg, i) => (
              <MessageBubble key={i} msg={msg} isLast={i === messages.length - 1} />
            ))
          )}
          <div ref={bottomRef} />
        </div>

        {/* Input row */}
        <div className="shrink-0 space-y-2">
          {/* Filters bar */}
          <div className="flex items-center gap-2">
            <button
              onClick={() => setShowFilters(f => !f)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-xl border text-[10px] font-bold uppercase tracking-widest transition-all ${
                showFilters || tableFilter.length > 0
                  ? 'bg-blue-500/10 border-blue-500/20 text-blue-400'
                  : 'bg-white/5 border-white/10 text-slate-500 hover:text-slate-300'
              }`}
            >
              <SlidersHorizontal className="w-3 h-3" />
              Filters
              {tableFilter.length > 0 && (
                <span className="ml-1 px-1.5 py-0.5 rounded-full bg-blue-500/20 text-blue-300 text-[9px]">
                  {tableFilter.length}
                </span>
              )}
            </button>
            {tableFilter.length > 0 && (
              <div className="flex gap-1.5 flex-wrap">
                {tableFilter.map(t => (
                  <span key={t} className="flex items-center gap-1 px-2 py-0.5 rounded-lg bg-blue-500/10 border border-blue-500/20 text-blue-400 text-[10px] font-bold">
                    {shortTable(t)}
                    <button onClick={() => setTableFilter(prev => prev.filter(x => x !== t))}>
                      <X className="w-2.5 h-2.5 hover:text-red-400" />
                    </button>
                  </span>
                ))}
              </div>
            )}
          </div>

          <AnimatePresence>
            {showFilters && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: 'auto', opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                transition={{ duration: 0.15 }}
                className="overflow-hidden"
              >
                <div className="glass-card p-3 flex gap-4">
                  <div className="flex-1">
                    <p className="text-[9px] font-bold text-slate-600 uppercase tracking-widest mb-2">
                      Filter by table
                    </p>
                    <div className="bg-slate-900/50 border border-white/10 rounded-xl p-2 max-h-36 overflow-y-auto space-y-0.5">
                      <button
                        onClick={() => setTableFilter([])}
                        className={`w-full text-left px-2.5 py-1.5 rounded-lg text-[11px] transition-colors ${
                          tableFilter.length === 0
                            ? 'bg-blue-500/20 text-blue-300 font-bold'
                            : 'text-slate-500 hover:text-slate-300 hover:bg-white/5'
                        }`}
                      >
                        All tables ({tables.length})
                      </button>
                      {tables.map(t => {
                        const sel = tableFilter.includes(t.table_name);
                        return (
                          <button
                            key={t.table_name}
                            onClick={() => setTableFilter(prev =>
                              sel ? prev.filter(x => x !== t.table_name) : [...prev, t.table_name]
                            )}
                            className={`w-full text-left px-2.5 py-1.5 rounded-lg text-[11px] flex items-center justify-between transition-colors ${
                              sel
                                ? 'bg-blue-500/20 text-blue-300 font-bold border border-blue-500/20'
                                : 'text-slate-500 hover:text-slate-300 hover:bg-white/5'
                            }`}
                          >
                            <span className="truncate">{shortTable(t.table_name)}</span>
                            <span className="text-slate-600 font-mono ml-2 shrink-0">{t.embedded_rows}</span>
                          </button>
                        );
                      })}
                    </div>
                  </div>
                  <div className="w-28">
                    <p className="text-[9px] font-bold text-slate-600 uppercase tracking-widest mb-2">
                      Top K rows
                    </p>
                    <input
                      type="number" min={1} max={50}
                      value={topK}
                      onChange={e => setTopK(Math.min(50, Math.max(1, Number(e.target.value))))}
                      className="w-full bg-slate-900/50 border border-white/10 rounded-xl py-2 px-3 text-sm text-slate-300 outline-none focus:border-blue-500/30"
                    />
                    <p className="text-[9px] text-slate-600 mt-1">rows retrieved for context</p>
                  </div>
                </div>
              </motion.div>
            )}
          </AnimatePresence>

          {/* Text input */}
          <div className="flex gap-3">
            <div className="flex-1 relative">
              <textarea
                ref={inputRef}
                rows={2}
                placeholder={currentMode.placeholder}
                value={query}
                onChange={e => setQuery(e.target.value)}
                onKeyDown={e => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    handleSubmit();
                  }
                }}
                className="w-full bg-slate-900/40 border border-white/10 rounded-2xl py-3 px-4 text-white placeholder-slate-600 focus:ring-4 focus:ring-blue-500/10 focus:border-blue-500/30 outline-none transition-all text-sm shadow-xl resize-none leading-relaxed"
              />
              <p className="absolute bottom-2.5 right-3 text-[9px] text-slate-700">⏎ send · shift+⏎ newline</p>
            </div>
            <button
              onClick={handleSubmit}
              disabled={!query.trim() || loading}
              className="flex flex-col items-center justify-center gap-1 w-14 bg-blue-600 hover:bg-blue-500 disabled:opacity-40 disabled:cursor-not-allowed text-white font-black rounded-2xl transition-all shadow-lg shadow-blue-600/20 active:scale-95"
            >
              {loading
                ? <RefreshCw className="w-5 h-5 animate-spin" />
                : <Send className="w-5 h-5" />}
            </button>
          </div>
        </div>
      </div>

      {/* ── Right: Info sidebar ──────────────────────────────────────── */}
      <div className="w-72 shrink-0 flex flex-col gap-4 overflow-y-auto">

        {/* RAG status card */}
        <div className="glass-card p-4">
          <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-3 flex items-center gap-2">
            <Cpu className="w-3.5 h-3.5 text-violet-400" />
            RAG Engine
          </h3>
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <span className="text-xs text-slate-500">Status</span>
              <span className={`text-xs font-bold ${ragStatus?.status === 'ok' ? 'text-emerald-400' : 'text-amber-400'}`}>
                {ragStatus?.status ?? '…'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-xs text-slate-500">Model</span>
              <span className="text-xs font-mono text-violet-400">{ragStatus?.model ?? 'gemma3:270m'}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-xs text-slate-500">Loaded</span>
              {ragStatus?.model_loaded
                ? <CheckCircle className="w-4 h-4 text-emerald-400" />
                : <AlertCircle className="w-4 h-4 text-amber-400" />}
            </div>
          </div>
          {!ragStatus?.model_loaded && !statusLoading && (
            <div className="mt-3 p-2.5 bg-amber-500/5 border border-amber-500/20 rounded-xl">
              <p className="text-[10px] text-amber-400 leading-relaxed">
                Run: <code className="font-mono bg-white/5 px-1 rounded">docker exec -it ollama_rag ollama pull gemma3:270m</code>
              </p>
            </div>
          )}
        </div>

        {/* Mode info */}
        <div className="glass-card p-4">
          <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-3 flex items-center gap-2">
            <Sparkles className="w-3.5 h-3.5 text-blue-400" />
            Current Mode
          </h3>
          {MODES.map(m => {
            const Icon   = m.icon;
            const active = mode === m.id;
            return (
              <button
                key={m.id}
                onClick={() => setMode(m.id)}
                className={`w-full flex items-start gap-3 p-2.5 rounded-xl mb-1.5 transition-all text-left ${
                  active ? `bg-${m.color}-500/10 border border-${m.color}-500/20` : 'hover:bg-white/5'
                }`}
              >
                <Icon className={`w-4 h-4 mt-0.5 shrink-0 ${active ? `text-${m.color}-400` : 'text-slate-600'}`} />
                <div>
                  <p className={`text-xs font-bold ${active ? `text-${m.color}-400` : 'text-slate-500'}`}>{m.label}</p>
                  <p className="text-[10px] text-slate-600 leading-relaxed mt-0.5">{m.desc}</p>
                </div>
              </button>
            );
          })}
        </div>

        {/* Indexed tables */}
        <div className="glass-card p-4 flex-1">
          <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-3 flex items-center gap-2">
            <Layers className="w-3.5 h-3.5 text-blue-400" />
            Indexed Tables
            <span className="ml-auto text-slate-600">{tables.length}</span>
          </h3>
          <div className="space-y-1 max-h-60 overflow-y-auto pr-0.5">
            {tables.map(t => (
              <button
                key={t.table_name}
                onClick={() => setTableFilter(prev =>
                  prev.includes(t.table_name)
                    ? prev.filter(x => x !== t.table_name)
                    : [...prev, t.table_name]
                )}
                className={`w-full text-left px-2.5 py-2 rounded-xl text-[11px] flex items-center justify-between transition-all group ${
                  tableFilter.includes(t.table_name)
                    ? 'bg-blue-500/10 border border-blue-500/20 text-blue-300'
                    : 'hover:bg-white/5 text-slate-500 hover:text-slate-300'
                }`}
              >
                <span className="truncate font-medium">{shortTable(t.table_name)}</span>
                <span className="shrink-0 ml-2 font-mono text-slate-600">{t.embedded_rows}</span>
              </button>
            ))}
          </div>
        </div>

        {/* Conversation stats */}
        {messages.length > 0 && (
          <div className="glass-card p-4">
            <h3 className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-3 flex items-center gap-2">
              <MessageSquare className="w-3.5 h-3.5 text-blue-400" />
              Session
            </h3>
            <div className="space-y-1.5">
              <div className="flex items-center justify-between">
                <span className="text-xs text-slate-500">Messages</span>
                <span className="text-xs font-bold text-white">{messages.length}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-xs text-slate-500">Mode</span>
                <span className="text-xs font-bold text-white capitalize">{multiTurn ? 'Multi-turn' : 'Single-shot'}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-xs text-slate-500">Tables scoped</span>
                <span className="text-xs font-bold text-white">{tableFilter.length || 'All'}</span>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default RAGPage;