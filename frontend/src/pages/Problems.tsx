import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { api } from '../lib/api';
import type { Problem } from '../types';

const fetchProblems = async (): Promise<Problem[]> => {
  const { data } = await api.get('/problems');
  return data;
};

export const Problems: React.FC = () => {
  const { data: problems, isLoading, error } = useQuery({
    queryKey: ['problems'],
    queryFn: fetchProblems,
  });

  if (isLoading) {
    return <div className="text-center py-12 text-slate-400">Loading problems...</div>;
  }

  if (error) {
    return <div className="text-center py-12 text-red-400">Failed to load problems.</div>;
  }

  return (
    <div>
      <h1 className="text-3xl font-bold mb-8">Problem Catalogue</h1>
      <div className="bg-slate-800 rounded-xl overflow-hidden border border-slate-700 shadow-xl">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-slate-900/50 text-slate-400 text-sm uppercase tracking-wider border-b border-slate-700">
              <th className="p-4 font-medium">Problem</th>
              <th className="p-4 font-medium w-32">Difficulty</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {problems?.map((problem) => (
              <tr key={problem.id} className="hover:bg-slate-700/30 transition-colors">
                <td className="p-4">
                  <Link to={`/problems/${problem.id}`} className="text-blue-400 hover:text-blue-300 font-medium">
                    {problem.title}
                  </Link>
                </td>
                <td className="p-4">
                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                    problem.difficulty === 'EASY' ? 'bg-green-500/10 text-green-400 border border-green-500/20' :
                    problem.difficulty === 'MEDIUM' ? 'bg-yellow-500/10 text-yellow-400 border border-yellow-500/20' :
                    'bg-red-500/10 text-red-400 border border-red-500/20'
                  }`}>
                    {problem.difficulty}
                  </span>
                </td>
              </tr>
            ))}
            {(!problems || problems.length === 0) && (
              <tr>
                <td colSpan={2} className="p-8 text-center text-slate-400">
                  No problems available at the moment.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};
