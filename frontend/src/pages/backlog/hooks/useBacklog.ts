import { useQuery } from '@tanstack/react-query'
import { apiGet } from '@/shared/api/client'

export interface BacklogItem {
  item_id: string
  title: string
  description?: string
  category: string
  status: 'blocked' | 'pending' | 'in-progress' | 'resolved'
  priority: number
  criticality: 'critical' | 'high' | 'medium' | 'low'
  reason_critical?: string
  assigned_to?: string
  created_at: string
  updated_at: string
}

export interface BacklogStats {
  total_items: number
  blocked_count: number
  pending_count: number
  in_progress_count: number
  resolved_count: number
  critical_count: number
  high_count: number
}

export function useBacklogItems(status?: string, criticality?: string) {
  const params = new URLSearchParams()
  if (status) params.append('status', status)
  if (criticality) params.append('criticality', criticality)

  return useQuery({
    queryKey: ['backlog-items', status, criticality],
    queryFn: () => apiGet<BacklogItem[]>(`/api/v1/backlog?${params.toString()}`),
    staleTime: 30000,
  })
}

export function useBacklogItem(itemId: string) {
  return useQuery({
    queryKey: ['backlog-item', itemId],
    queryFn: () =>
      apiGet<BacklogItem & { blocks_on: any[]; blocks: any[] }>(`/api/v1/backlog/${itemId}`),
    enabled: !!itemId,
  })
}

export function useBacklogStats() {
  return useQuery({
    queryKey: ['backlog-stats'],
    queryFn: () => apiGet<BacklogStats>('/api/v1/backlog/stats/summary'),
    staleTime: 60000,
  })
}
