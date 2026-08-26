import { useMemo } from 'react'
import { AgGridReact } from 'ag-grid-react'
import { ModuleRegistry, ClientSideRowModelModule, ValidationModule } from 'ag-grid-community'
import { cn } from '@/lib/utils'
import type { BacklogItem } from '../hooks'
import 'ag-grid-community/styles/ag-theme-quartz.css'

ModuleRegistry.registerModules([ClientSideRowModelModule, ValidationModule])

interface BacklogTableProps {
  items: BacklogItem[]
  isLoading: boolean
  onItemClick?: (item: BacklogItem) => void
}

const CriticalityCell = (props: any) => {
  const { value } = props
  const colorMap: Record<string, string> = {
    critical: 'bg-red-100 text-red-900',
    high: 'bg-orange-100 text-orange-900',
    medium: 'bg-yellow-100 text-yellow-900',
    low: 'bg-blue-100 text-blue-900',
  }
  return (
    <span className={cn('inline-block px-2 py-1 rounded text-xs font-medium', colorMap[value] || 'bg-gray-100')}>
      {value?.toUpperCase()}
    </span>
  )
}

const StatusCell = (props: any) => {
  const { value } = props
  const colorMap: Record<string, string> = {
    blocked: 'bg-red-50 text-red-700',
    pending: 'bg-yellow-50 text-yellow-700',
    'in-progress': 'bg-blue-50 text-blue-700',
    resolved: 'bg-green-50 text-green-700',
  }
  return (
    <span className={cn('inline-block px-2 py-1 rounded text-xs font-medium', colorMap[value])}>
      {value?.toUpperCase().replace('-', ' ')}
    </span>
  )
}

const PriorityCell = (props: any) => {
  const { value } = props
  const priorityLabels: Record<number, string> = {
    1: '🔴 P1',
    2: '🟠 P2',
    3: '🟡 P3',
    4: '🟢 P4',
    5: '⚪ P5',
  }
  return <span className="text-xs font-mono">{priorityLabels[value] || value}</span>
}

const DomainCell = (props: any) => {
  const { data } = props
  const domainMap: Record<string, { label: string; color: string }> = {
    'FEAT': { label: 'Features', color: 'bg-blue-100 text-blue-900' },
    'BACK': { label: 'Backtest', color: 'bg-purple-100 text-purple-900' },
    'FRON': { label: 'Frontend', color: 'bg-pink-100 text-pink-900' },
    'STRA': { label: 'Strategy', color: 'bg-green-100 text-green-900' },
    'MOME': { label: 'Momentum', color: 'bg-emerald-100 text-emerald-900' },
    'DATA': { label: 'Data', color: 'bg-orange-100 text-orange-900' },
    'TEST': { label: 'Testing', color: 'bg-yellow-100 text-yellow-900' },
    'AGEN': { label: 'Agents', color: 'bg-indigo-100 text-indigo-900' },
    'BUIL': { label: 'Build', color: 'bg-slate-100 text-slate-900' },
  }

  const parts = data.item_id.split('-')
  const domainCode = parts.length >= 2 ? parts[1] : 'OTHER'
  const domain = domainMap[domainCode]

  return domain ? (
    <span className={cn('inline-block px-2 py-1 rounded text-xs font-medium', domain.color)}>
      {domain.label}
    </span>
  ) : (
    <span className="text-xs text-gray-500">{domainCode}</span>
  )
}

const DocumentReferenceCell = (props: any) => {
  const { value } = props
  if (!value) return <span className="text-xs text-gray-400">—</span>

  const isUrl = value.startsWith('http://') || value.startsWith('https://')
  return isUrl ? (
    <a
      href={value}
      target="_blank"
      rel="noopener noreferrer"
      className="text-xs text-blue-600 hover:text-blue-800 underline truncate max-w-xs block"
      title={value}
    >
      {value}
    </a>
  ) : (
    <span className="text-xs text-gray-700 truncate max-w-xs block" title={value}>
      {value}
    </span>
  )
}

export function BacklogTable({ items, isLoading, onItemClick }: BacklogTableProps) {
  const columnDefs = useMemo(
    () => [
      { field: 'item_id', headerName: 'ID', width: 110, pinned: 'left' },
      { field: 'title', headerName: 'Title', flex: 1, minWidth: 250 },
      { headerName: 'Domain', width: 120, cellRenderer: DomainCell },
      { field: 'status', headerName: 'Status', width: 120, cellRenderer: StatusCell },
      { field: 'priority', headerName: 'Priority', width: 80, cellRenderer: PriorityCell },
      { field: 'criticality', headerName: 'Criticality', width: 110, cellRenderer: CriticalityCell },
      { field: 'document_reference', headerName: 'Reference', width: 200, cellRenderer: DocumentReferenceCell },
      { field: 'assigned_to', headerName: 'Assigned To', width: 120 },
      { field: 'blocks_on_count', headerName: 'Blocked By', width: 80, type: 'numericColumn' },
      { field: 'blocks_count', headerName: 'Blocks', width: 80, type: 'numericColumn' },
    ],
    []
  )

  const defaultColDef = useMemo(
    () => ({
      sortable: true,
      filter: true,
      resizable: true,
    }),
    []
  )

  if (isLoading) {
    return <div className="text-sm text-muted-foreground">Loading backlog items...</div>
  }

  return (
    <div className="ag-theme-quartz" style={{ height: '600px', width: '100%' }}>
      <AgGridReact
        rowData={items}
        columnDefs={columnDefs}
        defaultColDef={defaultColDef}
        pagination={true}
        paginationPageSize={50}
        onRowClicked={(event) => onItemClick?.(event.data)}
        enableCellTextSelection={true}
      />
    </div>
  )
}
