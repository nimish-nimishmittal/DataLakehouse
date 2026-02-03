// src/context/AuthContext.jsx
import React, { createContext, useState, useEffect } from 'react';
import { jwtDecode } from 'jwt-decode';
import { dashboardAPI } from '../services/api';
import api from '../services/api';

const AuthContext = createContext();

export const AuthProvider = ({ children }) => {
  const [auth, setAuth] = useState({ token: null, user: null, loading: true });

  useEffect(() => {
    const token = localStorage.getItem('token');
    if (token) {
      try {
        const decoded = jwtDecode(token);
        setAuth({ token, user: decoded, loading: false });
      } catch (e) {
        localStorage.removeItem('token');
        setAuth({ token: null, user: null, loading: false });
      }
    } else {
      setAuth({ token: null, user: null, loading: false });
    }
  }, []);

  const login = async (username, password) => {
    try {
        const formData = new URLSearchParams();
        formData.append('username', username);
        formData.append('password', password);

        const res = await api.post('/auth/login', formData, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
        });

        const token = res.data.access_token;
        localStorage.setItem('token', token);
        const decoded = jwtDecode(token);
        setAuth({ token, user: decoded, loading: false });
        return true;
    } catch (err) {
        console.error('Login failed:', err.response?.data || err);
        return false;
    }
    };

    const register = async (username, password) => {
    try {
        await api.post('/auth/register', { username, password });
        return true;
    } catch (err) {
        console.error('Register failed:', err.response?.data || err);
        return false;
    }
    };

  const logout = () => {
    localStorage.removeItem('token');
    setAuth({ token: null, user: null, loading: false });
  };

  return (
    <AuthContext.Provider value={{ auth, login, register, logout }}>
      {children}
    </AuthContext.Provider>
  );
};

export default AuthContext;