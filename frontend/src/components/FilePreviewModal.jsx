import React, { useState, useEffect, useRef } from 'react';
import { X, FileText, Table as TableIcon, FileJson, Loader2, AlertCircle, File, Maximize2, Minimize2, ImageIcon, ZoomIn, ZoomOut, RotateCcw } from 'lucide-react';
import { dashboardAPI } from '../services/api';
import { Viewer, Worker } from '@react-pdf-viewer/core';
import { defaultLayoutPlugin } from '@react-pdf-viewer/default-layout';
import '@react-pdf-viewer/core/lib/styles/index.css';
import '@react-pdf-viewer/default-layout/lib/styles/index.css';
import { renderAsync } from "docx-preview";

const IMAGE_FORMATS = ['image', 'png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'bmp', 'ico', 'tiff', 'avif'];

  // Helper function to check if a file format is an image
  const isImageFormat = (format) => {
    if (!format) return false;
    const normalizedFormat = format.toLowerCase();
    return IMAGE_FORMATS.includes(normalizedFormat) ||
           IMAGE_FORMATS.some(imgFormat => normalizedFormat.endsWith('.' + imgFormat));
  };

const FilePreviewModal = ({ file, onClose }) => {
  const [content, setContent] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState('visual'); // 'visual' or 'text'
  const [rawFileUrl, setRawFileUrl] = useState(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [zoom, setZoom] = useState(1);
  const docxContainerRef = useRef(null);

  // Initialize PDF viewer plugin
  const defaultLayoutPluginInstance = defaultLayoutPlugin();

  // Render DOCX file when visual tab is active and format is docx
  useEffect(() => {
    if (activeTab === 'visual' && content?.format === 'docx' && rawFileUrl && docxContainerRef.current) {
      const renderDocx = async () => {
        try {
          const response = await fetch(rawFileUrl);
          const blob = await response.blob();
          docxContainerRef.current.innerHTML = '';
          await renderAsync(blob, docxContainerRef.current, null, {
            className: "docx-renderer",
            inWrapper: true,
          });
        } catch (err) {
          console.error('DOCX render failed:', err);
        }
      };
      renderDocx();
    }
  }, [activeTab, content?.format, rawFileUrl]);

  useEffect(() => {
    const fetchPreview = async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await dashboardAPI.previewFile(file.catalog_id);
        setContent(res.data);
        // Build URL for raw file (for PDF/DOCX visual preview)
        const token = localStorage.getItem('token');
        setRawFileUrl(`${dashboardAPI.apiBaseUrl}/files/raw/${file.catalog_id}?token=${token}`);
      } catch (err) {
        console.error('Preview fetch failed:', err);
        setError(err.response?.data?.detail || 'Failed to load file preview');
      } finally {
        setLoading(false);
      }
    };

    if (file?.catalog_id) {
      fetchPreview();
    }
  }, [file?.catalog_id]);

  // Handle escape key
  useEffect(() => {
    const handleEscape = (e) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleEscape);
    return () => window.removeEventListener('keydown', handleEscape);
  }, [onClose]);

  const formatValue = (value) => {
    if (value === null || value === undefined) return <span className="text-slate-500 italic">null</span>;
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  };

  const renderTable = (columns, rows) => {
    if (!columns || columns.length === 0) {
      return <p className="text-slate-400">No data available</p>;
    }

    return (
      <div className="overflow-auto max-h-[60vh]">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-slate-800 z-10">
            <tr>
              {columns.map((col, idx) => (
                <th
                  key={idx}
                  className="px-4 py-3 text-left font-semibold text-white border-b border-slate-700 whitespace-nowrap"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rowIdx) => (
              <tr key={rowIdx} className="border-b border-slate-800/50 hover:bg-slate-800/30">
                {columns.map((col, colIdx) => (
                  <td key={colIdx} className="px-4 py-2 text-slate-300 whitespace-nowrap">
                    {rowIdx !== undefined && row[col] !== undefined ? formatValue(row[col]) : formatValue(row[colIdx])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  const renderJsonTree = (data, level = 0) => {
    if (data === null || typeof data !== 'object') {
      return (
        <span className={typeof data === 'string' ? 'text-emerald-400' : 'text-amber-400'}>
          {typeof data === 'string' ? `"${data}"` : String(data)}
        </span>
      );
    }

    if (Array.isArray(data)) {
      if (data.length === 0) return <span className="text-slate-400">[]</span>;
      return (
        <div style={{ paddingLeft: level * 16 }}>
          <span className="text-slate-400">[</span>
          {data.map((item, idx) => (
            <div key={idx} style={{ paddingLeft: 16 }}>
              <span className="text-slate-500">{idx}: </span>
              {renderJsonTree(item, level + 1)}
              {idx < data.length - 1 && <span className="text-slate-400">,</span>}
            </div>
          ))}
          <span className="text-slate-400">]</span>
        </div>
      );
    }

    // Object
    const entries = Object.entries(data);
    if (entries.length === 0) return <span className="text-slate-400">{'{}'}</span>;

    return (
      <div style={{ paddingLeft: level * 16 }}>
        <span className="text-slate-400">{'{'}</span>
        {entries.map(([key, value], idx) => (
          <div key={key} style={{ paddingLeft: 16 }}>
            <span className="text-blue-400">"{key}"</span>
            <span className="text-slate-400">: </span>
            {renderJsonTree(value, level + 1)}
            {idx < entries.length - 1 && <span className="text-slate-400">,</span>}
          </div>
        ))}
        <span className="text-slate-400">{'}'}</span>
      </div>
    );
  };

  const renderContent = () => {
    if (loading) {
      return (
        <div className="flex flex-col items-center justify-center h-64">
          <Loader2 className="w-12 h-12 text-blue-500 animate-spin mb-4" />
          <p className="text-slate-400">Loading preview...</p>
        </div>
      );
    }

    if (error) {
      return (
        <div className="flex flex-col items-center justify-center h-64">
          <AlertCircle className="w-12 h-12 text-red-500 mb-4" />
          <p className="text-red-400">{error}</p>
        </div>
      );
    }

    if (!content) return null;

    const format = content.format;

    // Table formats (CSV, Parquet)
    if (format === 'csv' || format === 'parquet') {
      return (
        <div>
          <div className="flex items-center gap-2 mb-4 pb-3 border-b border-slate-700">
            <TableIcon className="w-5 h-5 text-emerald-400" />
            <span className="text-white font-medium">
              {format.toUpperCase()} Preview ({content.total_rows} rows)
            </span>
          </div>
          {renderTable(content.columns, content.rows)}
        </div>
      );
    }

    // JSON format
    if (format === 'json') {
      return (
        <div>
          <div className="flex items-center gap-2 mb-4 pb-3 border-b border-slate-700">
            <FileJson className="w-5 h-5 text-amber-400" />
            <span className="text-white font-medium">JSON Preview</span>
          </div>
          <div className="bg-slate-900 rounded-lg p-4 overflow-auto max-h-[60vh] font-mono text-sm">
            {renderJsonTree(content.content)}
          </div>
        </div>
      );
    }

    // Text formats (TXT, PDF, DOCX, PPTX)
    if (format === 'txt' || format === 'pdf' || format === 'docx' || format === 'pptx') {
      // PDF and DOCX get visual preview with tabs
      if (format === 'pdf' || format === 'docx') {
        return (
          <div className="flex flex-col h-full">
            {/* Tab header */}
            <div className="flex items-center gap-2 mb-4 pb-3 border-b border-slate-700">
              <FileText className="w-5 h-5 text-blue-400" />
              <span className="text-white font-medium flex-1">
                {format === 'pdf' && `PDF Preview (${content.pages} pages)`}
                {format === 'docx' && 'Document Preview'}
              </span>
              <div className="flex gap-2">
                <button
                  onClick={() => setActiveTab('visual')}
                  className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                    activeTab === 'visual'
                      ? 'bg-blue-600 text-white'
                      : 'bg-slate-700 text-slate-300 hover:bg-slate-600'
                  }`}
                >
                  Visual
                </button>
                <button
                  onClick={() => setActiveTab('text')}
                  className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                    activeTab === 'text'
                      ? 'bg-blue-600 text-white'
                      : 'bg-slate-700 text-slate-300 hover:bg-slate-600'
                  }`}
                >
                  Extracted Text
                </button>
              </div>
            </div>

            {/* Tab content */}
            {activeTab === 'visual' ? (
              <div className="flex-1 min-h-0 flex flex-col">
                {format === 'pdf' ? (
                  <div className="flex-1 bg-white rounded-lg overflow-hidden">
                    <Worker workerUrl="https://unpkg.com/pdfjs-dist@3.11.174/build/pdf.worker.min.js">
                      <Viewer
                        fileUrl={rawFileUrl}
                        plugins={[defaultLayoutPluginInstance]}
                      />
                    </Worker>
                  </div>
                ) : (
                  // ==================== DOCX VISUAL PREVIEW ====================
                  <div className="flex-1 min-h-0 bg-white rounded-lg overflow-auto">
                    <div
                      ref={docxContainerRef}
                      className="docx-container mx-auto p-6"
                    />
                  </div>
                )}
              </div>
            ) : (
              <div className="flex-1 min-h-0">
                <div className="bg-slate-800/50 rounded-lg p-3 mb-4">
                  <p className="text-xs text-slate-400">
                    This is the text extracted from the {format === 'pdf' ? 'PDF' : 'document'}.
                    Switch to "Visual" tab to see the original document.
                  </p>
                </div>
                <pre className="bg-slate-900 rounded-lg p-4 overflow-auto max-h-[50vh] font-mono text-sm text-slate-300 whitespace-pre-wrap">
                  {content.text}
                </pre>
              </div>
            )}
          </div>
        );
      }

      // PPTX and TXT remain as before
      if (format === 'pptx') {
        return (
          <div>
            <div className="flex items-center gap-2 mb-4 pb-3 border-b border-slate-700">
              <FileText className="w-5 h-5 text-blue-400" />
              <span className="text-white font-medium">
                Presentation Preview ({content.total_slides} slides)
              </span>
            </div>
            <div className="space-y-4 max-h-[60vh] overflow-auto">
              {content.slides.map((slide) => (
                <div key={slide.slide_num} className="bg-slate-800/50 rounded-lg p-4">
                  <span className="text-xs text-slate-400 uppercase tracking-wide">Slide {slide.slide_num}</span>
                  <p className="text-slate-300 mt-2 whitespace-pre-wrap">{slide.text || '(No text)'}</p>
                </div>
              ))}
            </div>
          </div>
        );
      }

      // TXT format
      return (
        <div>
          <div className="flex items-center gap-2 mb-4 pb-3 border-b border-slate-700">
            <FileText className="w-5 h-5 text-blue-400" />
            <span className="text-white font-medium">Text Preview</span>
          </div>
          <pre className="bg-slate-900 rounded-lg p-4 overflow-auto max-h-[60vh] font-mono text-sm text-slate-300 whitespace-pre-wrap">
            {content.text}
          </pre>
        </div>
      );
    }

    // Image formats
    if (isImageFormat(format)) {
      return (
        <div className="flex flex-col h-full">
          {/* Toolbar */}
          <div className="flex items-center gap-2 mb-4 pb-3 border-b border-slate-700">
            <ImageIcon className="w-5 h-5 text-violet-400" />
            <span className="text-white font-medium flex-1">
              Image Preview
              {content.width && content.height && (
                <span className="ml-2 text-xs text-slate-400 font-normal">
                  {content.width} × {content.height}px
                </span>
              )}
            </span>
            {/* Zoom controls */}
            <div className="flex items-center gap-1 bg-slate-800 rounded-lg p-1">
              <button
                onClick={() => setZoom((z) => Math.max(0.25, +(z - 0.25).toFixed(2)))}
                className="p-1.5 text-slate-300 hover:text-white hover:bg-slate-700 rounded-md transition-all disabled:opacity-40"
                disabled={zoom <= 0.25}
                title="Zoom out"
              >
                <ZoomOut className="w-4 h-4" />
              </button>
              <span className="text-xs text-slate-300 w-12 text-center font-mono select-none">
                {Math.round(zoom * 100)}%
              </span>
              <button
                onClick={() => setZoom((z) => Math.min(4, +(z + 0.25).toFixed(2)))}
                className="p-1.5 text-slate-300 hover:text-white hover:bg-slate-700 rounded-md transition-all disabled:opacity-40"
                disabled={zoom >= 4}
                title="Zoom in"
              >
                <ZoomIn className="w-4 h-4" />
              </button>
              <div className="w-px h-4 bg-slate-700 mx-1" />
              <button
                onClick={() => setZoom(1)}
                className="p-1.5 text-slate-300 hover:text-white hover:bg-slate-700 rounded-md transition-all"
                title="Reset zoom"
              >
                <RotateCcw className="w-4 h-4" />
              </button>
            </div>
          </div>

          {/* Image viewport */}
          <div className="flex-1 min-h-0 overflow-auto bg-slate-950 rounded-lg flex items-start justify-center">
            <div
              className="transition-transform duration-150 ease-out p-4"
              style={{ transform: `scale(${zoom})`, transformOrigin: 'top center' }}
            >
              <img
                src={rawFileUrl}
                alt={file?.display_name}
                className="max-w-full rounded shadow-lg select-none"
                style={{ display: 'block' }}
                onError={(e) => {
                  e.target.style.display = 'none';
                  setError('Failed to load image');
                }}
              />
            </div>
          </div>
        </div>
      );
    }

    return <p className="text-slate-400">Preview not available for this file type</p>;
  };

  return (
    <div
      className="fixed inset-0 bg-black/70 backdrop-blur-sm z-50 flex items-center justify-center p-4"
      onClick={onClose}
    >
      <div
        className={`bg-slate-900 border border-slate-700 rounded-2xl w-full flex flex-col shadow-2xl transition-all duration-300 ${
          isFullscreen
            ? 'max-w-full max-h-[98vh] h-[98vh]'
            : 'max-w-6xl max-h-[80vh]'
        }`}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-700">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-slate-800 rounded-lg">
              {content?.format === 'csv' || content?.format === 'parquet' ? (
                <TableIcon className="w-5 h-5 text-emerald-400" />
              ) : content?.format === 'json' ? (
                <FileJson className="w-5 h-5 text-amber-400" />
              ) : isImageFormat(content?.format) ? (
                <ImageIcon className="w-5 h-5 text-violet-400" />
              ) : (
                <FileText className="w-5 h-5 text-blue-400" />
              )}
            </div>
            <div>
              <h3 className="text-lg font-semibold text-white">{file?.display_name}</h3>
              <p className="text-xs text-slate-400 uppercase">{content?.format || 'Unknown'}</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setIsFullscreen((f) => !f)}
              className="p-2 text-slate-400 hover:text-white hover:bg-slate-800 rounded-lg transition-all"
              title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}
            >
              {isFullscreen ? <Minimize2 className="w-5 h-5" /> : <Maximize2 className="w-5 h-5" />}
            </button>
            <button
              onClick={onClose}
              className="p-2 text-slate-400 hover:text-white hover:bg-slate-800 rounded-lg transition-all"
              title="Close"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 min-h-0 overflow-hidden p-6">{renderContent()}</div>
      </div>
    </div>
  );
};

export default FilePreviewModal;