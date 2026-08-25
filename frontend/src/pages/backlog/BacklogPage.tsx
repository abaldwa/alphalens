import { useState } from 'react'
import { ChevronDown, LayoutGrid, Table } from 'lucide-react'
import { AppShell } from '@/lib/ui'
import { useBacklogItems, useBacklogStats, type BacklogItem } from './hooks'
import { BacklogTable } from './components/BacklogTable'
import { BacklogKanban } from './components/BacklogKanban'

type ViewType = 'table' | 'kanban'
type FilterStatus = 'all' | 'blocked' | 'pending' | 'in-progress' | 'resolved'
type FilterCriticality = 'all' | 'critical' | 'high' | 'medium' | 'low'

export function BacklogPage() {
  const [viewType, setViewType] = useState<ViewType>('table')
  const [filterStatus, setFilterStatus] = useState<FilterStatus>('all')
  const [filterCriticality, setFilterCriticality] = useState<FilterCriticality>('all')

  const { data: items = [], isPending: isLoadingItems } = useBacklogItems(
    filterStatus === 'all' ? undefined : filterStatus,
    filterCriticality === 'all' ? undefined : filterCriticality
  )
  const { data: stats } = useBacklogStats()

  const handleItemClick = (item: BacklogItem) => {
    // Navigate to item detail page
    console.log('Clicked item:', item.item_id)
  }

  return (
    <AppShell>
      <div className="container mx-auto py-6 space-y-6">
        {/* Header */}
        <div className="space-y-2">
          <h1 className="text-3xl font-bold">Backlog</h1>
          <p className="text-muted-foreground">
            Project work items, blockers, technical debt, and defects
          </p>
        </div>

        {/* Stats */}
        {stats && (
          <div className="grid grid-cols-2 md:grid-cols-7 gap-3">
            <StatCard label="Total" value={stats.total_items} />
            <StatCard label="Blocked" value={stats.blocked_count} variant="red" />
            <StatCard label="Pending" value={stats.pending_count} variant="yellow" />
            <StatCard label="In Progress" value={stats.in_progress_count} variant="blue" />
            <StatCard label="Resolved" value={stats.resolved_count} variant="green" />
            <StatCard label="🔴 Critical" value={stats.critical_count} variant="red" />
            <StatCard label="🟠 High" value={stats.high_count} variant="orange" />
          </div>
        )}

        {/* Controls */}
        <div className="flex flex-wrap items-center gap-4 bg-secondary/50 p-4 rounded-lg">
          {/* View Toggle */}
          <div className="flex gap-2">
            <button
              onClick={() => setViewType('table')}
              className={`p-2 rounded transition-colors ${
                viewType === 'table' ? 'bg-primary text-white' : 'bg-white text-gray-700'
              }`}
              title="Table view"
            >
              <Table size={20} />
            </button>
            <button
              onClick={() => setViewType('kanban')}
              className={`p-2 rounded transition-colors ${
                viewType === 'kanban' ? 'bg-primary text-white' : 'bg-white text-gray-700'
              }`}
              title="Kanban view"
            >
              <LayoutGrid size={20} />
            </button>
          </div>

          {/* Filters */}
          <div className="flex flex-wrap gap-3">
            <FilterSelect
              label="Status"
              value={filterStatus}
              onChange={(v) => setFilterStatus(v as FilterStatus)}
              options={[
                { value: 'all', label: 'All Statuses' },
                { value: 'blocked', label: 'Blocked' },
                { value: 'pending', label: 'Pending' },
                { value: 'in-progress', label: 'In Progress' },
                { value: 'resolved', label: 'Resolved' },
              ]}
            />

            <FilterSelect
              label="Criticality"
              value={filterCriticality}
              onChange={(v) => setFilterCriticality(v as FilterCriticality)}
              options={[
                { value: 'all', label: 'All Levels' },
                { value: 'critical', label: '🔴 Critical' },
                { value: 'high', label: '🟠 High' },
                { value: 'medium', label: '🟡 Medium' },
                { value: 'low', label: '⚪ Low' },
              ]}
            />
          </div>
        </div>

        {/* Content */}
        <div className="bg-white rounded-lg p-4">
          {viewType === 'table' ? (
            <BacklogTable items={items} isLoading={isLoadingItems} onItemClick={handleItemClick} />
          ) : (
            <BacklogKanban items={items} isLoading={isLoadingItems} onItemClick={handleItemClick} />
          )}
        </div>

        {/* Legend */}
        <div className="bg-secondary/30 p-4 rounded-lg text-sm">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <h4 className="font-semibold mb-2">Criticality</h4>
              <div className="space-y-1 text-xs">
                <div>
                  <span className="inline-block w-4 h-4 bg-red-200 rounded mr-2" />
                  Critical - Blocks implementation / 100% failure risk
                </div>
                <div>
                  <span className="inline-block w-4 h-4 bg-orange-200 rounded mr-2" />
                  High - Gates a major phase / cascading failures
                </div>
                <div>
                  <span className="inline-block w-4 h-4 bg-yellow-200 rounded mr-2" />
                  Medium - Improves workflow / low risk if delayed
                </div>
                <div>
                  <span className="inline-block w-4 h-4 bg-blue-200 rounded mr-2" />
                  Low - Optimization / can be done anytime
                </div>
              </div>
            </div>
            <div>
              <h4 className="font-semibold mb-2">Status</h4>
              <div className="space-y-1 text-xs">
                <div>
                  <span className="inline-block w-4 h-4 bg-red-500 rounded mr-2" />
                  Blocked - Cannot start due to dependencies
                </div>
                <div>
                  <span className="inline-block w-4 h-4 bg-yellow-500 rounded mr-2" />
                  Pending - Ready to start
                </div>
                <div>
                  <span className="inline-block w-4 h-4 bg-blue-500 rounded mr-2" />
                  In Progress - Currently being worked on
                </div>
                <div>
                  <span className="inline-block w-4 h-4 bg-green-500 rounded mr-2" />
                  Resolved - Completed
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </AppShell>
  )
}

function StatCard({
  label,
  value,
  variant = 'default',
}: {
  label: string
  value: number
  variant?: 'default' | 'red' | 'yellow' | 'blue' | 'green' | 'orange'
}) {
  const colorMap: Record<string, string> = {
    default: 'bg-primary/10 text-primary',
    red: 'bg-red-100 text-red-700',
    yellow: 'bg-yellow-100 text-yellow-700',
    blue: 'bg-blue-100 text-blue-700',
    green: 'bg-green-100 text-green-700',
    orange: 'bg-orange-100 text-orange-700',
  }

  return (
    <div className={`rounded-lg p-3 text-center ${colorMap[variant]}`}>
      <div className="text-xs font-medium opacity-75">{label}</div>
      <div className="text-2xl font-bold">{value}</div>
    </div>
  )
}

interface FilterSelectProps {
  label: string
  value: string
  onChange: (value: string) => void
  options: Array<{ value: string; label: string }>
}

function FilterSelect({ label, value, onChange, options }: FilterSelectProps) {
  return (
    <div className="relative inline-block">
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="appearance-none bg-white border border-gray-300 rounded px-3 py-2 pr-8 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
      >
        {options.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
      <ChevronDown size={16} className="absolute right-2 top-1/2 transform -translate-y-1/2 pointer-events-none text-gray-500" />
    </div>
  )
}
