// src/components/Layout.jsx
import React from 'react';
import Sidebar from './Sidebar';

const Layout = ({ children }) => {
    return (
        <div className="min-h-screen bg-[#0b0f1a] flex overflow-hidden">
            <Sidebar />
            <main className="flex-1 ml-72 h-screen overflow-y-auto p-10 relative">
                <div className="max-w-7xl mx-auto space-y-10">
                    {children}
                </div>
            </main>
        </div>
    );
};

export default Layout;
