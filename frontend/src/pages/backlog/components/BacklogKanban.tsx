import { useMemo } from 'react'
import { cn } from '@/lib/utils'
import type { BacklogItem } from '../hooks'

interface BacklogKanbanProps {
  items: BacklogItem[]
  isLoading: boolean
  onItemClick?: (item: BacklogItem) => void
}

const statusColumns = [
  { id: 'blocked', label: '🔴 Blocked', color: 'bg-red-50' },
  { id: 'pending', label: '⏳ Pending', color: 'bg-yellow-50' },
  { id: 'in-progress', label: '🔵 In Progress', color: 'bg-blue-50' },
  { id: 'resolved', label: '✅ Resolved', color: 'bg-green-50' },
]

const criticalities: Record<string, string> = {
  critical: '🔴',
  high: '🟠',
  medium: '🟡',
  low: '⚪',
}

function BacklogCard({ item, onClick }: { item: BacklogItem; onClick?: () => void }) {
  return (
    <button
      onClick={onClick}
      className="w-full bg-white border border-gray-200 rounded-lg p-3 text-left hover:shadow-md hover:border-primary transition-all group"
    >
      <div className="flex items-start justify-between gap-2">
        <div className="flex-1 min-w-0">
          <div className="font-mono text-xs text-muted-foreground">{item.item_id}</div>
          <div className="font-medium text-sm truncate group-hover:text-primary">{item.title}</div>
        </div>
        <div className="text-lg flex-shrink-0">{criticalities[item.criticality]}</div>
      </div>

      <div className="mt-2 flex items-center gap-2 flex-wrap">
        <span className="inline-block px-2 py-0.5 bg-gray-100 text-gray-700 text-xs rounded">
          {item.category}
        </span>
        <span className="inline-block px-2 py-0.5 bg-blue-100 text-blue-700 text-xs rounded">
          P{item.priority}
        </span>
        {item.assigned_to && (
          <span className="inline-block px-2 py-0.5 bg-green-100 text-green-700 text-xs rounded">
            {item.assigned_to}
          </span>
        )}
      </div>

      {(item.blocks_on_count || 0) > 0 && (
        <div className="mt-2 text-xs text-red-600">
          Blocked by {item.blocks_on_count} item{item.blocks_on_count !== 1 ? 's' : ''}
        </div>
      )}

      {(item.blocks_count || 0) > 0 && (
        <div className="mt-1 text-xs text-amber-600">
          Blocks {item.blocks_count} item{item.blocks_count !== 1 ? 's' : ''}
        </div>
      )}
    </button>
  )
}

export function BacklogKanban({ items, isLoading, onItemClick }: BacklogKanbanProps) {
  const itemsByStatus = useMemo(() => {
    const grouped: Record<string, BacklogItem[]> = {
      blocked: [],
      pending: [],
      'in-progress': [],
      resolved: [],
    }

    items.forEach((item) => {
      if (grouped[item.status]) {
        grouped[item.status].push(item)
      }
    })

    return grouped
  }, [items])

  if (isLoading) {
    return <div className="text-sm text-muted-foreground">Loading backlog items...</div>
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 overflow-x-auto pb-4">
      {statusColumns.map((column) => (
        <div key={column.id} className={cn('rounded-lg p-4', column.color, 'min-h-[600px]')}>
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-semibold text-sm">{column.label}</h3>
            <span className="inline-block px-2 py-0.5 bg-gray-200 text-gray-800 text-xs rounded-full font-mono">
              {itemsByStatus[column.id]?.length || 0}
            </span>
          </div>

          <div className="flex flex-col gap-3">
            {itemsByStatus[column.id]?.map((item) => (
              <BacklogCard key={item.item_id} item={item} onClick={() => onItemClick?.(item)} />
            ))}

            {(!itemsByStatus[column.id] || itemsByStatus[column.id].length === 0) && (
              <div className="text-center text-xs text-gray-500 py-8">No items</div>
            )}
          </div>
        </div>
      ))}
    </div>
  )
}
