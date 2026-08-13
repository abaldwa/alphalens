// Typed fetch client against the AlphaLens datastore API
// (datastore/api/main.py). Mirrors the old dashboard/static/js/api.js
// fetch-wrapper conventions (same error format) but typed and configured
// via a Vite env var instead of same-origin `window.location.origin`,
// since the new frontend is served by the Vite dev server / a separate
// static host, not by the FastAPI app itself.

const API_BASE_URL: string =
  (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? 'http://localhost:8000'

export class ApiError extends Error {
  status: number
  url: string

  constructor(status: number, statusText: string, url: string, detail?: string) {
    // Prefer the backend's own explanation (e.g. main.py's DuckDB lock-conflict
    // handler: "Database is temporarily locked by another process...") over the
    // generic HTTP status text, which tells an operator nothing actionable.
    super(detail || `${status} ${statusText} — ${url}`)
    this.name = 'ApiError'
    this.status = status
    this.url = url
  }
}

async function errorDetail(resp: Response): Promise<string | undefined> {
  try {
    const body = await resp.clone().json()
    return typeof body?.detail === 'string' ? body.detail : undefined
  } catch {
    return undefined
  }
}

function buildUrl(path: string, params?: Record<string, string | number | boolean | undefined>): string {
  const url = new URL(path, API_BASE_URL)
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined) url.searchParams.set(k, String(v))
    }
  }
  return url.toString()
}

async function doFetch(url: string, init?: RequestInit): Promise<Response> {
  let resp: Response
  try {
    resp = await fetch(url, init)
  } catch {
    // fetch() itself throws (not an HTTP error response) when the API is
    // unreachable entirely — refused connection, server down/restarting.
    throw new ApiError(0, 'Network Error', url, `Could not reach the API at ${url} — is the backend running?`)
  }
  if (!resp.ok) throw new ApiError(resp.status, resp.statusText, url, await errorDetail(resp))
  return resp
}

export async function apiGet<T>(
  path: string,
  params?: Record<string, string | number | boolean | undefined>,
): Promise<T> {
  const url = buildUrl(path, params)
  const resp = await doFetch(url)
  return (await resp.json()) as T
}

export async function apiPost<T>(path: string, body?: unknown): Promise<T> {
  const url = buildUrl(path)
  const resp = await doFetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  return (await resp.json()) as T
}

export async function apiPut<T>(path: string, body?: unknown): Promise<T> {
  const url = buildUrl(path)
  const resp = await doFetch(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  return (await resp.json()) as T
}

/** PATCH for partial updates. The portfolios router serves PATCH (not PUT)
 * for name/description/is_active, and pages/portfolios.tsx has been importing
 * this since it was written — the export was simply never added, so the
 * frontend build has been failing on it. */
export async function apiPatch<T>(path: string, body?: unknown): Promise<T> {
  const url = buildUrl(path)
  const resp = await doFetch(url, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  return (await resp.json()) as T
}

export async function apiDelete<T>(path: string): Promise<T> {
  const url = buildUrl(path)
  const resp = await doFetch(url, { method: 'DELETE' })
  return (await resp.json()) as T
}

export { API_BASE_URL }
