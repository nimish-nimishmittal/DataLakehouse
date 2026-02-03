// src/pages/Users.jsx
import React from 'react';
import UserManager from '../components/UserManager';
import { Users as UsersIcon, ShieldCheck, UserPlus } from 'lucide-react';

const Users = () => {
    return (
        <div className="space-y-10 pb-20">
            <header className="flex flex-col md:flex-row md:items-center justify-between gap-6">
                <div className="flex items-center gap-5">
                    <div className="p-4 bg-blue-600/10 rounded-2xl ring-1 ring-blue-500/20">
                        <UsersIcon className="w-8 h-8 text-blue-400" />
                    </div>
                    <div>
                        <h2 className="text-4xl font-black text-white tracking-tight">Identity & Access</h2>
                        <p className="text-slate-400 mt-1 text-lg">Manage enterprise-grade user roles and permissions</p>
                    </div>
                </div>

                <div className="flex items-center gap-4 px-6 py-3 bg-slate-800/40 rounded-2xl border border-white/10 backdrop-blur-md">
                    <ShieldCheck className="w-5 h-5 text-emerald-400" />
                    <span className="text-xs font-bold text-slate-300 uppercase tracking-widest">RBAC v2 Enabled</span>
                </div>
            </header>

            <div className="grid grid-cols-1 lg:grid-cols-4 gap-8">
                <div className="lg:col-span-3">
                    <div className="glass-card p-0 overflow-hidden shadow-2xl">
                        <UserManager />
                    </div>
                </div>

                <div className="space-y-6">
                    <div className="glass-card bg-gradient-to-br from-blue-600/10 to-indigo-600/10 border-blue-500/10">
                        <h4 className="text-sm font-bold text-white mb-4 uppercase tracking-widest flex items-center gap-2">
                            <UserPlus className="w-4 h-4 text-blue-400" />
                            Access Summary
                        </h4>
                        <div className="space-y-4">
                            <div className="flex justify-between items-center text-xs">
                                <span className="text-slate-500 font-bold uppercase">Admin Slots</span>
                                <span className="text-white font-black">2 / 5</span>
                            </div>
                            <div className="w-full bg-slate-900 h-1.5 rounded-full overflow-hidden">
                                <div className="bg-blue-600 h-full w-[40%]" />
                            </div>

                            <div className="flex justify-between items-center text-xs">
                                <span className="text-slate-500 font-bold uppercase">Standard Users</span>
                                <span className="text-white font-black">128</span>
                            </div>

                            <div className="mt-6 p-4 bg-white/5 rounded-xl border border-white/5">
                                <p className="text-[10px] text-slate-500 leading-relaxed font-medium italic">
                                    "Administrator roles have global write access to all buckets and can trigger system-wide ETL runs."
                                </p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Users;
