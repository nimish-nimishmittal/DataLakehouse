// src/components/FileList.jsx
import React from 'react';

const FileList = ({ files }) => {
  return (
    <div className="bg-gray-800 rounded-lg overflow-hidden">
      <table className="w-full text-left">
        <thead className="bg-gray-700">
          <tr>
            <th className="p-4 text-gray-300">Filename</th>
            <th className="p-4 text-gray-300">Size</th>
            <th className="p-4 text-gray-300">Format</th>
            <th className="p-4 text-gray-300">Uploaded By</th>
            <th className="p-4 text-gray-300">Metadata</th>
            <th className="p-4 test-gray-300">Source</th>
          </tr>
        </thead>
        <tbody>
          {files.map((file) => (
            <tr key={file.object_name} className="border-t border-gray-700 hover:bg-gray-700">
              <td className="p-4 text-white">{file.object_name.split('/').pop()}</td>
              <td className="p-4 text-gray-300">{(file.object_size / 1024).toFixed(1)} KB</td>
              <td className="p-4 text-gray-300">{file.file_format || 'unknown'}</td>
              <td className="p-4 text-gray-300">{file.uploaded_by || 'system'}</td>
              <td className="p-4 text-gray-300">
                <pre className="text-xs overflow-x-auto">
                  {JSON.stringify(file.metadata || {}, null, 2)}
                </pre>
              </td>
              <td className="p-4 text-gray-300">{file.uploaded_by}</td>

            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default FileList;