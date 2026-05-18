import React, { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { Client } from '@stomp/stompjs';
import SockJS from 'sockjs-client';
import { api } from '../lib/api';
import type { Submission, PerTestVerdict } from '../types';
import { CheckCircle2, XCircle, Clock, AlertTriangle, Loader2 } from 'lucide-react';

export const SubmissionDetail: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const [liveSubmission, setLiveSubmission] = useState<Submission | null>(null);

  const { data: initialSubmission, isLoading } = useQuery({
    queryKey: ['submission', id],
    queryFn: async () => {
      const { data } = await api.get<Submission>(`/submissions/${id}`);
      return data;
    },
    // Polling fallback if WS isn't connected
    refetchInterval: (query) => {
      const status = liveSubmission?.status || query.state.data?.status;
      return (status !== 'ACCEPTED' && status !== 'WRONG_ANSWER' && status !== 'TIME_LIMIT_EXCEEDED' && status !== 'MEMORY_LIMIT_EXCEEDED' && status !== 'RUNTIME_ERROR' && status !== 'COMPILE_ERROR') ? 3000 : false;
    },
  });

  useEffect(() => {
    if (initialSubmission && !liveSubmission) {
      setLiveSubmission(initialSubmission);
    }
  }, [initialSubmission]);

  useEffect(() => {
    if (!id) return;

    const token = localStorage.getItem('accessToken');
    const client = new Client({
      webSocketFactory: () => new SockJS('/ws'),
      connectHeaders: {
        Authorization: `Bearer ${token}`
      },
      debug: function (str) {
        console.log(str);
      },
      reconnectDelay: 5000,
      heartbeatIncoming: 4000,
      heartbeatOutgoing: 4000,
    });

    client.onConnect = function (frame) {
      console.log('Connected: ' + frame);
      client.subscribe(`/topic/submissions/${id}`, (message) => {
        if (message.body) {
          const event = JSON.parse(message.body);
          setLiveSubmission(prev => {
            const newPerTest = [...(prev?.per_test || [])];
            if (event.per_test) {
              event.per_test.forEach((pt: PerTestVerdict) => {
                const idx = newPerTest.findIndex(existing => existing.ordinal === pt.ordinal);
                if (idx >= 0) newPerTest[idx] = pt;
                else newPerTest.push(pt);
              });
            }
            return {
              ...prev,
              ...event,
              per_test: newPerTest.sort((a, b) => a.ordinal - b.ordinal)
            } as Submission;
          });
        }
      });
    };

    client.onStompError = function (frame) {
      console.error('Broker reported error: ' + frame.headers['message']);
      console.error('Additional details: ' + frame.body);
    };

    client.activate();

    return () => {
      client.deactivate();
    };
  }, [id]);

  if (isLoading && !liveSubmission) {
    return <div className="text-center py-12 text-slate-400">Loading submission details...</div>;
  }

  if (!liveSubmission) {
    return <div className="text-center py-12 text-red-400">Submission not found.</div>;
  }

  const isTerminal = ['ACCEPTED', 'WRONG_ANSWER', 'TIME_LIMIT_EXCEEDED', 'MEMORY_LIMIT_EXCEEDED', 'RUNTIME_ERROR', 'COMPILE_ERROR'].includes(liveSubmission.status || liveSubmission.verdict || '');
  
  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'ACCEPTED': return <CheckCircle2 className="h-6 w-6 text-green-500" />;
      case 'WRONG_ANSWER': return <XCircle className="h-6 w-6 text-red-500" />;
      case 'TIME_LIMIT_EXCEEDED': return <Clock className="h-6 w-6 text-yellow-500" />;
      case 'MEMORY_LIMIT_EXCEEDED': return <AlertTriangle className="h-6 w-6 text-yellow-500" />;
      case 'RUNTIME_ERROR': return <AlertTriangle className="h-6 w-6 text-orange-500" />;
      case 'COMPILE_ERROR': return <XCircle className="h-6 w-6 text-orange-500" />;
      default: return <Loader2 className="h-6 w-6 text-blue-500 animate-spin" />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'ACCEPTED': return 'text-green-400';
      case 'WRONG_ANSWER': return 'text-red-400';
      case 'TIME_LIMIT_EXCEEDED': return 'text-yellow-400';
      case 'MEMORY_LIMIT_EXCEEDED': return 'text-yellow-400';
      case 'RUNTIME_ERROR': return 'text-orange-400';
      case 'COMPILE_ERROR': return 'text-orange-400';
      default: return 'text-blue-400';
    }
  };

  return (
    <div className="max-w-4xl mx-auto">
      <h1 className="text-3xl font-bold mb-8">Submission Details</h1>
      
      <div className="bg-slate-800 rounded-xl overflow-hidden border border-slate-700 shadow-xl mb-8">
        <div className="p-6 border-b border-slate-700 bg-slate-800/50 flex items-center justify-between">
          <div className="flex items-center space-x-4">
            {getStatusIcon(liveSubmission.verdict || liveSubmission.status)}
            <span className={`text-xl font-bold ${getStatusColor(liveSubmission.verdict || liveSubmission.status)}`}>
              {liveSubmission.verdict || liveSubmission.status || 'PENDING'}
            </span>
          </div>
          <div className="text-sm text-slate-400 font-medium">
            Language: <span className="text-white bg-slate-700 px-2 py-1 rounded border border-slate-600">{liveSubmission.language}</span>
          </div>
        </div>
        
        <div className="p-6 grid grid-cols-2 md:grid-cols-4 gap-4 bg-slate-900/30">
          <div>
            <div className="text-sm text-slate-400 mb-1">Time</div>
            <div className="font-medium text-slate-200">{liveSubmission.execution_time_ms ? `${liveSubmission.execution_time_ms} ms` : '-'}</div>
          </div>
          <div>
            <div className="text-sm text-slate-400 mb-1">Memory</div>
            <div className="font-medium text-slate-200">{liveSubmission.memory_used_mb ? `${liveSubmission.memory_used_mb} MB` : '-'}</div>
          </div>
          <div>
            <div className="text-sm text-slate-400 mb-1">Problem</div>
            <div className="font-medium text-blue-400 truncate">{liveSubmission.problem_id}</div>
          </div>
          <div>
            <div className="text-sm text-slate-400 mb-1">Submission ID</div>
            <div className="font-medium text-slate-300 text-xs truncate" title={liveSubmission.id}>{liveSubmission.id.substring(0, 8)}...</div>
          </div>
        </div>
      </div>

      {liveSubmission.per_test && liveSubmission.per_test.length > 0 && (
        <div className="bg-slate-800 rounded-xl overflow-hidden border border-slate-700 shadow-xl">
          <div className="p-4 border-b border-slate-700 bg-slate-800/50">
            <h3 className="font-medium text-white">Test Case Breakdown</h3>
          </div>
          <div className="p-4 grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
            {liveSubmission.per_test.map((pt) => (
              <div key={pt.ordinal} className="bg-slate-900 border border-slate-700 rounded-lg p-3 text-center flex flex-col items-center justify-center relative overflow-hidden">
                <div className="text-xs text-slate-400 mb-2">Test {pt.ordinal}</div>
                {getStatusIcon(pt.verdict)}
                <div className="text-xs text-slate-500 mt-2">{pt.time_ms}ms</div>
                <div className={`absolute bottom-0 left-0 w-full h-1 ${pt.verdict === 'ACCEPTED' ? 'bg-green-500/50' : 'bg-red-500/50'}`}></div>
              </div>
            ))}
            {!isTerminal && (
               <div className="bg-slate-900/50 border border-slate-700/50 border-dashed rounded-lg p-3 text-center flex flex-col items-center justify-center">
                 <Loader2 className="h-6 w-6 text-slate-500 animate-spin" />
               </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};
