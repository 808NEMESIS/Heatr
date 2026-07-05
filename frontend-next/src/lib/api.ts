/**
 * API wrapper voor Heatr FastAPI backend.
 *
 * Dev-mode: stuurt altijd "Bearer dev-token" — backend accepteert elke valid-looking token.
 * Later te vervangen door echte Supabase JWT via useAuth().
 */

const API_BASE = import.meta.env.VITE_API_BASE || '/api';

export class ApiError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

export async function apiFetch<T = unknown>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const token =
    typeof window !== 'undefined'
      ? sessionStorage.getItem('heatr_token') || 'dev-token'
      : 'dev-token';

  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
      ...(options.headers || {}),
    },
  });

  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    try {
      const body = await res.json();
      msg = typeof body.detail === 'string' ? body.detail : body.message || msg;
    } catch {
      /* no-op */
    }
    // Auth-fouten globaal signaleren — Shell toont een banner zodat de
    // operator "Niet geautoriseerd" ziet in plaats van stille lege lijsten.
    if (res.status === 401 || res.status === 403) {
      window.dispatchEvent(
        new CustomEvent('heatr:auth-error', { detail: { status: res.status, path, message: msg } })
      );
    }
    throw new ApiError(msg, res.status);
  }

  if (res.status === 204) return null as T;
  const text = await res.text();
  if (!text) return null as T;
  return JSON.parse(text) as T;
}

export const api = {
  get: <T = unknown>(path: string) => apiFetch<T>(path),
  post: <T = unknown>(path: string, body?: unknown) =>
    apiFetch<T>(path, { method: 'POST', body: body ? JSON.stringify(body) : undefined }),
  patch: <T = unknown>(path: string, body?: unknown) =>
    apiFetch<T>(path, { method: 'PATCH', body: body ? JSON.stringify(body) : undefined }),
  del: <T = unknown>(path: string) =>
    apiFetch<T>(path, { method: 'DELETE' }),
};
