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

  constructor(status: number, statusText: string, url: string) {
    super(`${status} ${statusText} — ${url}`)
    this.name = 'ApiError'
    this.status = status
    this.url = url
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

export async function apiGet<T>(
  path: string,
  params?: Record<string, string | number | boolean | undefined>,
): Promise<T> {
  const url = buildUrl(path, params)
  const resp = await fetch(url)
  if (!resp.ok) throw new ApiError(resp.status, resp.statusText, url)
  return (await resp.json()) as T
}

export async function apiPost<T>(path: string, body?: unknown): Promise<T> {
  const url = buildUrl(path)
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  if (!resp.ok) throw new ApiError(resp.status, resp.statusText, url)
  return (await resp.json()) as T
}

export async function apiPut<T>(path: string, body?: unknown): Promise<T> {
  const url = buildUrl(path)
  const resp = await fetch(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  if (!resp.ok) throw new ApiError(resp.status, resp.statusText, url)
  return (await resp.json()) as T
}

export async function apiDelete<T>(path: string): Promise<T> {
  const url = buildUrl(path)
  const resp = await fetch(url, { method: 'DELETE' })
  if (!resp.ok) throw new ApiError(resp.status, resp.statusText, url)
  return (await resp.json()) as T
}

export { API_BASE_URL }
