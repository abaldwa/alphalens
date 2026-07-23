import { ResponsiveContainer } from 'recharts'

import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/lib/ui/primitives/card'

export interface ResponsiveChartCardProps {
  title: string
  description?: string
  height?: number
  children: React.ReactElement
}

/**
 * Card wrapper around Recharts' ResponsiveContainer — the shared building
 * block for every line/bar/area chart in the app so charts are always
 * actually responsive (no fixed-pixel <LineChart width=...> usage).
 */
export function ResponsiveChartCard({ title, description, height = 280, children }: ResponsiveChartCardProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        {description ? <CardDescription>{description}</CardDescription> : null}
      </CardHeader>
      <CardContent>
        <div style={{ width: '100%', height }}>
          <ResponsiveContainer width="100%" height="100%">
            {children}
          </ResponsiveContainer>
        </div>
      </CardContent>
    </Card>
  )
}
