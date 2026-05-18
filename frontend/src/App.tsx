import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { Layout } from './components/Layout';
import { ProtectedRoute } from './components/ProtectedRoute';
import { Login } from './pages/Login';
import { Signup } from './pages/Signup';
import { Problems } from './pages/Problems';
import { ProblemDetail } from './pages/ProblemDetail';
import { SubmissionDetail } from './pages/SubmissionDetail';
import { useAuth } from './context/AuthContext';

export const App: React.FC = () => {
  const { isAuthenticated } = useAuth();

  return (
    <Routes>
      <Route element={<Layout />}>
        {/* Public Routes */}
        <Route path="/login" element={!isAuthenticated ? <Login /> : <Navigate to="/problems" replace />} />
        <Route path="/signup" element={!isAuthenticated ? <Signup /> : <Navigate to="/problems" replace />} />
        
        {/* Protected Routes */}
        <Route element={<ProtectedRoute />}>
          <Route path="/problems" element={<Problems />} />
          <Route path="/problems/:id" element={<ProblemDetail />} />
          <Route path="/submissions/:id" element={<SubmissionDetail />} />
        </Route>

        {/* Fallback Route */}
        <Route path="*" element={<Navigate to={isAuthenticated ? "/problems" : "/login"} replace />} />
      </Route>
    </Routes>
  );
};

export default App;
