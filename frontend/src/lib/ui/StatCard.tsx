import { cn } from '@/lib/utils'
import { Card, CardContent, CardHeader, CardTitle } from '@/lib/ui/primitives/card'

export interface StatCardProps {
  label: React.ReactNode
  value: React.ReactNode
  hint?: React.ReactNode
  tone?: 'default' | 'teal' | 'blue' | 'purple' | 'green' | 'amber' | 'red'
  className?: string
}

const TONE_CLASS: Record<NonNullable<StatCardProps['tone']>, string> = {
  default: 'text-foreground',
  teal: 'text-teal',
  blue: 'text-blue',
  purple: 'text-purple',
  green: 'text-green',
  amber: 'text-amber',
  red: 'text-red',
}

/** Small metric tile — the "single number + label" building block used
 * across screener/overview pages (composed from the Card primitive). */
export function StatCard({ label, value, hint, tone = 'default', className }: StatCardProps) {
  return (
    <Card className={cn(className)}>
      <CardHeader className="pb-1">
        <CardTitle className="font-mono-data text-xs uppercase tracking-wide text-muted-foreground">
          {label}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className={cn('font-mono-data text-2xl font-semibold', TONE_CLASS[tone])}>{value}</div>
        {hint ? <div className="mt-1 text-xs text-muted-foreground">{hint}</div> : null}
      </CardContent>
    </Card>
  )
}
