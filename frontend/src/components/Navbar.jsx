// src/components/Navbar.jsx
import React, { useContext } from 'react';
import AuthContext from '../context/AuthContext';

const Navbar = () => {
  const { auth, logout } = useContext(AuthContext);

  return (
    <nav className="bg-gray-800 p-4 shadow-lg">
      <div className="max-w-7xl mx-auto flex justify-between items-center">
        <h1 className="text-2xl font-bold text-white">Lakehouse Dashboard</h1>
        <div className="flex items-center gap-4">
          <span className="text-gray-300">Welcome, {auth.user?.username || 'User'} ({auth.user?.role})</span>
          <button
            onClick={logout}
            className="bg-red-600 hover:bg-red-700 px-4 py-2 rounded text-white transition"
          >
            Logout
          </button>
        </div>
      </div>
    </nav>
  );
};

export default Navbar;