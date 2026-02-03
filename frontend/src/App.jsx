// src/App.jsx
import React, { useContext } from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

import AuthContext from './context/AuthContext';
import Layout from './components/Layout';
import Login from './pages/Login';
import Register from './pages/Register';
import AdminDashboard from './pages/AdminDashboard';
import UserDashboard from './pages/UserDashboard';
import Catalog from './pages/Catalog';
import Jobs from './pages/Jobs';
import SystemStatus from './pages/SystemStatus';
import Users from './pages/Users';
import AuditLogs from './pages/AuditLogs';
import SearchPage from './pages/Search';

const ProtectedRoute = ({ children }) => {
  const { auth } = useContext(AuthContext);
  if (auth.loading) return <div className="flex items-center justify-center h-screen bg-[#0f172a] text-white">Loading...</div>;
  if (!auth.token) return <Navigate to="/login" />;
  return <Layout>{children}</Layout>;
};

const App = () => {
  const { auth } = useContext(AuthContext);

  return (
    <>
      <ToastContainer theme="dark" position="bottom-right" />
      <Routes>
        <Route path="/login" element={auth.token ? <Navigate to="/dashboard" /> : <Login />} />
        <Route path="/register" element={auth.token ? <Navigate to="/dashboard" /> : <Register />} />

        <Route path="/dashboard" element={
          <ProtectedRoute>
            {auth.user?.role === 'admin' ? <AdminDashboard /> : <UserDashboard />}
          </ProtectedRoute>
        } />

        <Route path="/catalog" element={
          <ProtectedRoute>
            <Catalog />
          </ProtectedRoute>
        } />

        <Route path="/jobs" element={
          <ProtectedRoute>
            <Jobs />
          </ProtectedRoute>
        } />

        <Route path="/system" element={
          <ProtectedRoute>
            <SystemStatus />
          </ProtectedRoute>
        } />

        <Route path="/users" element={
          <ProtectedRoute>
            <Users />
          </ProtectedRoute>
        } />

        <Route path="/audit" element={
          <ProtectedRoute>
            <AuditLogs />
          </ProtectedRoute>
        } />

        <Route path="/search" element={
          <ProtectedRoute>
            <SearchPage />
          </ProtectedRoute>
        } />

        <Route path="/" element={<Navigate to="/dashboard" />} />
        <Route path="*" element={<Navigate to="/dashboard" />} />
      </Routes>
    </>
  );
};

export default App;