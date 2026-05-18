import React from 'react';
import { Outlet, Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { LogOut, Code2 } from 'lucide-react';

export const Layout: React.FC = () => {
  const { isAuthenticated, logout } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  return (
    <div className="min-h-screen bg-slate-900 text-slate-50 flex flex-col">
      <header className="bg-slate-800 border-b border-slate-700">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center">
              <Link to="/" className="flex items-center space-x-2">
                <Code2 className="h-8 w-8 text-blue-500" />
                <span className="font-bold text-xl tracking-tight">Online Judge</span>
              </Link>
            </div>
            <div className="flex items-center space-x-4">
              {isAuthenticated ? (
                <>
                  <Link to="/problems" className="text-slate-300 hover:text-white px-3 py-2 rounded-md text-sm font-medium transition-colors">Problems</Link>
                  <button onClick={handleLogout} className="text-slate-300 hover:text-white flex items-center space-x-1 px-3 py-2 rounded-md text-sm font-medium transition-colors">
                    <LogOut className="h-4 w-4" />
                    <span>Logout</span>
                  </button>
                </>
              ) : (
                <>
                  <Link to="/login" className="text-slate-300 hover:text-white px-3 py-2 rounded-md text-sm font-medium transition-colors">Login</Link>
                  <Link to="/signup" className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-md text-sm font-medium shadow-sm transition-colors">Sign Up</Link>
                </>
              )}
            </div>
          </div>
        </div>
      </header>

      <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <Outlet />
      </main>
      
      <footer className="bg-slate-800 border-t border-slate-700 py-6 text-center text-slate-400 text-sm">
        <p>&copy; {new Date().getFullYear()} Online Judge at Scale. All rights reserved.</p>
      </footer>
    </div>
  );
};
