// src/components/UserManager.jsx
import React, { useState, useEffect } from 'react';
import { User, Shield, Trash2, RefreshCw } from 'lucide-react';
import { dashboardAPI } from '../services/api';
import { toast } from 'react-toastify';

const UserManager = () => {
    const [users, setUsers] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');
    const [actionLoading, setActionLoading] = useState(false);

    const fetchUsers = async () => {
        try {
            const res = await dashboardAPI.getUsers();
            // Backend returns { users: [...], total: N } — unwrap the array
            setUsers(res.data?.users || []);
        } catch (err) {
            console.error('Failed to fetch users', err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchUsers();
    }, []);


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
                <div className="flex items-center gap-2 px-3 py-1.5 bg-slate-700/50 border border-white/5 rounded-xl">
                    <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Managed via LDAP</span>
                </div>
            </div>

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
                                    <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-[10px] font-black uppercase tracking-widest border bg-slate-700/50 text-slate-400 border-white/5">
                                        LDAP User
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