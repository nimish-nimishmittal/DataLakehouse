// src/components/Sidebar.jsx
import React, { useContext } from 'react';
import { Link, useLocation } from 'react-router-dom';
import {
    LayoutDashboard,
    FileStack,
    Settings,
    Users,
    Activity,
    Database,
    ChevronRight,
    LogOut,
    Shield,
    Search
} from 'lucide-react';
import AuthContext from '../context/AuthContext';
import { clsx } from 'clsx';

const Sidebar = () => {
    const { auth, logout } = useContext(AuthContext);
    const location = useLocation();
    const isAdmin = auth.user?.role === 'admin';

    const menuItems = [
        { name: 'Dashboard', icon: LayoutDashboard, path: '/dashboard' },
        { name: 'Data Catalog', icon: FileStack, path: '/catalog' },
        { name: 'Semantic Search', icon: Search, path: '/search' },
        { name: 'Data Pipelines', icon: Activity, path: '/jobs' },
    ];

    if (isAdmin) {
        menuItems.push({ name: 'Identity & Access', icon: Users, path: '/users' });
        menuItems.push({ name: 'Audit Trail', icon: Shield, path: '/audit' });
        menuItems.push({ name: 'Cluster Health', icon: Database, path: '/system' });
    }

    return (
        <aside className="w-72 h-screen fixed left-0 top-0 glass border-r border-white/5 flex flex-col z-50">
            <div className="p-8">
                <div className="flex items-center gap-3 mb-10">
                    <div className="w-10 h-10 bg-blue-600 rounded-xl flex items-center justify-center shadow-lg shadow-blue-600/30 animate-pulse">
                        <Database className="text-white w-6 h-6" />
                    </div>
                    <div>
                        <h1 className="text-xl font-bold text-white tracking-tight">Data Lakehouse</h1>
                        <p className="text-[10px] text-blue-400 font-bold uppercase tracking-widest">Enterprise Platform</p>
                    </div>
                </div>

                <nav className="space-y-2">
                    {menuItems.map((item) => {
                        const isActive = location.pathname === item.path;
                        return (
                            <Link
                                key={item.name}
                                to={item.path}
                                className={clsx(
                                    "flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-300 group",
                                    isActive
                                        ? "bg-blue-600/10 text-blue-400 border border-blue-500/20 shadow-[0_0_20px_rgba(59,130,246,0.1)]"
                                        : "text-slate-400 hover:text-white hover:bg-white/5 border border-transparent"
                                )}
                            >
                                <item.icon className={clsx(
                                    "w-5 h-5 transition-transform duration-300 group-hover:scale-110",
                                    isActive ? "text-blue-400" : "text-slate-500"
                                )} />
                                <span className="font-medium">{item.name}</span>
                                {isActive && (
                                    <ChevronRight className="w-4 h-4 ml-auto opacity-50" />
                                )}
                            </Link>
                        );
                    })}
                </nav>
            </div>

            <div className="mt-auto p-6 border-t border-white/5 bg-white/5">
                <div className="flex items-center gap-3 p-3 rounded-2xl bg-slate-900/50 border border-white/5 mb-4">
                    <div className="w-10 h-10 rounded-full bg-gradient-to-tr from-blue-500 to-indigo-600 flex items-center justify-center text-white font-bold shadow-inner">
                        {auth.user?.username?.[0].toUpperCase() || 'U'}
                    </div>
                    <div className="overflow-hidden">
                        <p className="text-sm font-bold text-white truncate">{auth.user?.username}</p>
                        <p className="text-[10px] text-slate-500 uppercase font-bold tracking-tight">{auth.user?.role}</p>
                    </div>
                </div>
                <button
                    onClick={logout}
                    className="w-full flex items-center justify-center gap-2 py-3 px-4 text-slate-400 hover:text-red-400 hover:bg-red-500/10 rounded-xl transition-all duration-300 text-sm font-bold group"
                >
                    <LogOut className="w-4 h-4 group-hover:-translate-x-1 transition-transform" />
                    Logout Session
                </button>
            </div>
        </aside>
    );
};

export default Sidebar;
