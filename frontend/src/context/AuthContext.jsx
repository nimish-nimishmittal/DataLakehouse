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
        setAuth({
          token,
          user: {
            username: decoded.sub,
            role: decoded.role,
          },
          loading: false,
        });
      } catch (e) {
        localStorage.removeItem('token');
        setAuth({ token: null, user: null, loading: false });
      }
    } else {
      setAuth({ token: null, user: null, loading: false });
    }
  }, []);

  const login = async (username, password) => {
    const params = new URLSearchParams();
    params.append("username", username);
    params.append("password", password);

    const response = await api.post(
      "/auth/login",
      params,
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
      }
    );

    const token = response.data.access_token;
    const decoded = jwtDecode(token);

    // Store token
    localStorage.setItem("token", token);

    // Update auth state (THIS replaces setAuthToken)
    setAuth({
      token,
      user: {
        username: decoded.sub,
        role: decoded.role,
      },
      loading: false,
    });
  };


    // const register = async (username, password) => {
    // try {
    //     await api.post('/auth/register', { username, password });
    //     return true;
    // } catch (err) {
    //     console.error('Register failed:', err.response?.data || err);
    //     return false;
    // }
    // };

  const logout = () => {
    localStorage.removeItem('token');
    setAuth({ token: null, user: null, loading: false });
  };

  return (
    <AuthContext.Provider value={{ auth, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
};

export default AuthContext;