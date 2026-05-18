import React, { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import Editor from '@monaco-editor/react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { api } from '../lib/api';
import type { Problem } from '../types';
import { Play } from 'lucide-react';

export const ProblemDetail: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [code, setCode] = useState('// Write your solution here\n');
  const [language, setLanguage] = useState('cpp');
  const [submitting, setSubmitting] = useState(false);

  const { data: problem, isLoading } = useQuery({
    queryKey: ['problem', id],
    queryFn: async () => {
      const { data } = await api.get<Problem>(`/problems/${id}`);
      return data;
    },
  });

  const handleSubmit = async () => {
    if (!code.trim() || submitting) return;
    setSubmitting(true);
    try {
      const { data } = await api.post('/submissions', {
        problem_id: id,
        language,
        code,
      });
      navigate(`/submissions/${data.submission_id}`);
    } catch (err) {
      alert('Failed to submit code. Please try again.');
    } finally {
      setSubmitting(false);
    }
  };

  if (isLoading) return <div className="text-center py-12 text-slate-400">Loading problem...</div>;
  if (!problem) return <div className="text-center py-12 text-red-400">Problem not found.</div>;

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 h-[calc(100vh-8rem)]">
      {/* Problem Statement Panel */}
      <div className="bg-slate-800 rounded-xl border border-slate-700 overflow-hidden flex flex-col shadow-xl">
        <div className="p-6 border-b border-slate-700 bg-slate-800/50">
          <h2 className="text-2xl font-bold text-white mb-2">{problem.title}</h2>
          <div className="flex gap-4 text-sm text-slate-400">
            <span className="bg-slate-700/50 px-2.5 py-1 rounded-md border border-slate-600">Time Limit: {problem.time_limit_ms}ms</span>
            <span className="bg-slate-700/50 px-2.5 py-1 rounded-md border border-slate-600">Memory Limit: {problem.memory_limit_mb}MB</span>
          </div>
        </div>
        <div className="p-6 overflow-y-auto flex-1 prose prose-invert max-w-none">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>
            {problem.statement_md || 'No problem statement available.'}
          </ReactMarkdown>
        </div>
      </div>

      {/* Editor Panel */}
      <div className="bg-slate-800 rounded-xl border border-slate-700 flex flex-col shadow-xl overflow-hidden">
        <div className="p-4 border-b border-slate-700 bg-slate-800/50 flex justify-between items-center">
          <select 
            value={language}
            onChange={(e) => setLanguage(e.target.value)}
            className="bg-slate-900 border border-slate-600 text-white text-sm rounded-md px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="cpp">C++ (g++)</option>
            <option value="python">Python 3</option>
            <option value="java">Java (class Solution)</option>
          </select>
          <button
            onClick={handleSubmit}
            disabled={submitting}
            className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-1.5 rounded-md text-sm font-medium flex items-center gap-2 disabled:opacity-50 transition-colors shadow-sm"
          >
            {submitting ? (
              <div className="h-4 w-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
            ) : (
              <Play className="h-4 w-4" />
            )}
            <span>Submit</span>
          </button>
        </div>
        <div className="flex-1">
          <Editor
            height="100%"
            language={language === 'cpp' ? 'cpp' : language}
            theme="vs-dark"
            value={code}
            onChange={(value) => setCode(value || '')}
            options={{
              minimap: { enabled: false },
              fontSize: 14,
              padding: { top: 16 },
              scrollBeyondLastLine: false,
            }}
          />
        </div>
      </div>
    </div>
  );
};
