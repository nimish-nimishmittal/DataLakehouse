// src/pages/Login.jsx
import React, { useState, useContext } from 'react';
import { useNavigate } from 'react-router-dom';
import { motion } from 'framer-motion';
import AuthContext from '../context/AuthContext';

const Login = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');

  const { login } = useContext(AuthContext);
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    const success = await login(username, password);
    if (success) {
      navigate('/dashboard');
    } else {
      setError('Invalid username or password');
    }
  };

  return (
    <div className="min-h-screen flex">

      {/* LEFT IMAGE SECTION */}
      <div className="hidden md:flex w-1/2 relative">
        <img
          src="https://images.unsplash.com/photo-1526374965328-7f61d4dc18c5"
          alt="bg"
          className="w-full h-full object-cover"
        />

        {/* overlay */}
        <div className="absolute inset-0 bg-black/50 backdrop-blur-sm flex items-center justify-center">
          <h1 className="text-4xl font-bold text-white tracking-wide animate-float">
            Namaste 🙏
          </h1>
        </div>
      </div>

      {/* RIGHT FORM SECTION */}
      <div className="w-full md:w-1/2 flex items-center justify-center bg-[#0b0f1a]">

        <motion.div
          initial={{ opacity: 0, x: 80 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.6 }}
          className="glass p-10 rounded-3xl w-[90%] max-w-md"
        >
          <h2 className="text-3xl font-bold text-white mb-6 text-center">
            Login
          </h2>

          {error && (
            <motion.p
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              className="text-red-400 mb-4 text-center"
            >
              {error}
            </motion.p>
          )}

          <form onSubmit={handleSubmit}>

            <input
              type="text"
              placeholder="Username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="input-field w-full mb-4"
              required
            />

            <input
              type="password"
              placeholder="Password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="input-field w-full mb-6"
              required
            />

            <motion.button
              whileTap={{ scale: 0.95 }}
              whileHover={{ scale: 1.02 }}
              type="submit"
              className="btn-primary w-full"
            >
              Login
            </motion.button>

          </form>
        </motion.div>
      </div>
    </div>
  );
};

export default Login;