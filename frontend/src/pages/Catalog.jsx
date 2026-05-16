// src/pages/Catalog.jsx
import React from 'react';
import FileBrowser from '../components/FileBrowser';

const Catalog = () => {
  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <header className="mb-6">
        <h2 className="text-3xl font-black text-white tracking-tight">Enterprise Catalog</h2>
        <p className="text-slate-400 mt-1">Browse buckets, folders, and files with detailed metadata</p>
      </header>

      {/* File Browser */}
      <div className="flex-1 min-h-0 bg-slate-900/30 border border-white/5 rounded-xl overflow-hidden">
        <div className="h-full p-4">
          <FileBrowser />
        </div>
      </div>
    </div>
  );
};

export default Catalog;