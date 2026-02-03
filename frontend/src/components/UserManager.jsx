// src/components/UserManager.jsx
import React, { useState, useEffect } from 'react';
import { User, Shield, Trash2, UserPlus, RefreshCw, X, Check } from 'lucide-react';
import { dashboardAPI } from '../services/api';
import { toast } from 'react-toastify';
import { clsx } from 'clsx';
import { motion } from 'framer-motion';

const UserManager = () => {
    const [users, setUsers] = useState([]);
    const [loading, setLoading] = useState(true);
    const [showAddForm, setShowAddForm] = useState(false);
    const [newUser, setNewUser] = useState({ username: '', password: '', role: 'user' });
    const [error, setError] = useState('');
    const [success, setSuccess] = useState('');
    const [actionLoading, setActionLoading] = useState(false);

    const fetchUsers = async () => {
        try {
            const res = await dashboardAPI.getUsers();
            setUsers(res.data);
        } catch (err) {
            console.error('Failed to fetch users', err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchUsers();
    }, []);

    const handleAddUser = async (e) => {
        e.preventDefault();
        setError('');
        setSuccess('');
        setActionLoading(true);

        try {
            await dashboardAPI.addUser(newUser);
            setSuccess('User registered successfully');
            setNewUser({ username: '', password: '', role: 'user' });
            setShowAddForm(false);
            fetchUsers();
            toast.success('User account provisioned');
        } catch (err) {
            setError(err.response?.data?.detail || 'Failed to add user');
            toast.error('Provisioning failed');
        } finally {
            setActionLoading(false);
        }
    };

    const handleDeleteUser = async (userId, username) => {
        if (!window.confirm(`Permanently revoke access for ${username}?`)) return;

        try {
            await dashboardAPI.deleteUser(userId);
            fetchUsers();
            toast.success('Access revoked');
        } catch (err) {
            toast.error('Failed to revoke access');
        }
    };

    if (loading) return <div className="p-10 flex justify-center"><RefreshCw className="w-8 h-8 text-blue-500 animate-spin" /></div>;

    return (
        <div className="flex flex-col">
            <div className="p-6 border-b border-white/5 flex items-center justify-between bg-white/5">
                <div className="flex items-center gap-2">
                    <Shield className="w-5 h-5 text-blue-400" />
                    <span className="text-sm font-bold text-white uppercase tracking-widest">Active Identities</span>
                </div>
                <button
                    onClick={() => setShowAddForm(!showAddForm)}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white rounded-xl text-xs font-black transition-all shadow-lg shadow-blue-600/20 active:scale-95"
                >
                    {showAddForm ? <X className="w-4 h-4" /> : <UserPlus className="w-4 h-4" />}
                    {showAddForm ? 'Cancel' : 'Provision User'}
                </button>
            </div>

            {showAddForm && (
                <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: 'auto', opacity: 1 }}
                    className="p-8 border-b border-white/5 bg-slate-900/60 backdrop-blur-md"
                >
                    <form onSubmit={handleAddUser} className="grid grid-cols-1 md:grid-cols-4 gap-4 items-end">
                        <div className="space-y-2">
                            <label className="text-[10px] font-bold text-slate-500 uppercase ml-1">Username</label>
                            <input
                                type="text"
                                className="input-field w-full"
                                placeholder="e.g. jdoe"
                                value={newUser.username}
                                onChange={(e) => setNewUser({ ...newUser, username: e.target.value })}
                                required
                            />
                        </div>
                        <div className="space-y-2">
                            <label className="text-[10px] font-bold text-slate-500 uppercase ml-1">Password</label>
                            <input
                                type="password"
                                className="input-field w-full"
                                placeholder="••••••••"
                                value={newUser.password}
                                onChange={(e) => setNewUser({ ...newUser, password: e.target.value })}
                                required
                            />
                        </div>
                        <div className="space-y-2">
                            <label className="text-[10px] font-bold text-slate-500 uppercase ml-1">System Role</label>
                            <select
                                className="input-field w-full appearance-none"
                                value={newUser.role}
                                onChange={(e) => setNewUser({ ...newUser, role: e.target.value })}
                            >
                                <option value="user">Standard User</option>
                                <option value="admin">System Administrator</option>
                            </select>
                        </div>
                        <button
                            type="submit"
                            disabled={actionLoading}
                            className="bg-emerald-600 hover:bg-emerald-500 text-white font-black py-2.5 px-6 rounded-xl transition-all shadow-lg shadow-emerald-600/20 active:scale-95 flex items-center justify-center gap-2 h-[46px]"
                        >
                            {actionLoading ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Check className="w-4 h-4" />}
                            Create Account
                        </button>
                    </form>
                </motion.div>
            )}

            <div className="overflow-x-auto">
                <table className="w-full text-left">
                    <thead className="bg-[#0f172a]/50">
                        <tr className="text-[10px] font-black text-slate-500 uppercase tracking-widest border-b border-white/5">
                            <th className="p-6 pl-8">Identity</th>
                            <th className="p-6">Access Level</th>
                            <th className="p-6">Registry ID</th>
                            <th className="p-6 text-right pr-8">Operations</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-white/5">
                        {users.map((user) => (
                            <tr key={user.id} className="hover:bg-white/5 transition-all group">
                                <td className="p-6 pl-8">
                                    <div className="flex items-center gap-4">
                                        <div className="w-10 h-10 rounded-full bg-gradient-to-br from-slate-800 to-slate-900 border border-white/5 flex items-center justify-center text-slate-400 group-hover:border-blue-500/30 transition-colors shadow-inner">
                                            <User className="w-5 h-5" />
                                        </div>
                                        <span className="text-white font-bold tracking-tight">{user.username}</span>
                                    </div>
                                </td>
                                <td className="p-6">
                                    <span className={clsx(
                                        "inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-[10px] font-black uppercase tracking-widest border",
                                        user.role === 'admin'
                                            ? "bg-purple-500/10 text-purple-400 border-purple-500/20 shadow-[0_0_15px_rgba(168,85,247,0.1)]"
                                            : "bg-slate-700/50 text-slate-400 border-white/5"
                                    )}>
                                        {user.role === 'admin' && <Shield className="w-3 h-3" />}
                                        {user.role}
                                    </span>
                                </td>
                                <td className="p-6">
                                    <span className="text-xs font-mono text-slate-600 group-hover:text-slate-400 transition-colors">PID-{user.id.toString().padStart(6, '0')}</span>
                                </td>
                                <td className="p-6 text-right pr-8">
                                    {user.username !== 'admin' && (
                                        <button
                                            onClick={() => handleDeleteUser(user.id, user.username)}
                                            className="p-3 text-slate-500 hover:text-red-400 hover:bg-red-500/10 rounded-xl transition-all duration-300"
                                            title="Revoke Access"
                                        >
                                            <Trash2 className="w-4 h-4" />
                                        </button>
                                    )}
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

export default UserManager;
