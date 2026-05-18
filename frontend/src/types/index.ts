export interface User {
  id: string;
  username: string;
  email?: string;
}

export interface AuthResponse {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
  userId: string;
}

export interface Problem {
  id: string;
  title: string;
  difficulty: string;
  statement_md?: string;
  language_choices?: string[];
  time_limit_ms?: number;
  memory_limit_mb?: number;
}

export interface Submission {
  id: string;
  user_id: string;
  problem_id: string;
  contest_id: string;
  language: string;
  status: string;
  verdict?: string;
  execution_time_ms?: number;
  memory_used_mb?: number;
  per_test?: PerTestVerdict[];
}

export interface PerTestVerdict {
  ordinal: number;
  verdict: string;
  time_ms: number;
  memory_kb: number;
}
