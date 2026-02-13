// src/components/Navbar.jsx
import React, { useContext } from 'react';
import { Database, LogOut, User } from 'lucide-react';
import AuthContext from '../context/AuthContext';
import UploadButton from './UploadButton';

const Navbar = () => {
  const { auth, logout } = useContext(AuthContext);

  return (
    <header className="bg-slate-800/50 backdrop-blur-sm border-b border-slate-700 sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-6 py-4">
        <div className="flex items-center justify-between">

          {/* Brand */}
          <div className="flex items-center gap-3">
            <Database className="w-8 h-8 text-blue-400" />
            <div>
              <h1 className="text-2xl font-bold text-white">
                Lakehouse Dashboard
              </h1>
              <p className="text-sm text-slate-400">
                {auth.user?.role || 'User'} Panel
              </p>
            </div>
          </div>

          {/* User + Actions */}
          <div className="flex items-center gap-4">
            <UploadButton />
            <div className="flex items-center gap-2 px-4 py-2 bg-slate-700/50 rounded-lg border border-slate-600">
              <User className="w-4 h-4 text-slate-300" />
              <span className="text-slate-200 text-sm font-medium">
                {auth.user?.sub || 'User'}
              </span>
            </div>

            <button
              onClick={logout}
              className="flex items-center gap-2 px-4 py-2 bg-red-600/80 hover:bg-red-600 text-white rounded-lg transition"
            >
              <LogOut className="w-4 h-4" />
              <span>Logout</span>
            </button>
          </div>

        </div>
      </div>
    </header>
  );
};

export default Navbar;
