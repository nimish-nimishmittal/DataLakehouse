// src/pages/Users.jsx
import React from 'react';
import UserManager from '../components/UserManager';
import { Users as UsersIcon, ShieldCheck, UserPlus } from 'lucide-react';

const Users = () => {
    return (
        <div className="space-y-10 pb-20">
            <div className="grid grid-cols-1 lg:grid-cols-4 gap-8">
                <div className="lg:col-span-3">
                    <div className="glass-card p-0 overflow-hidden shadow-2xl">
                        <UserManager />
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Users;