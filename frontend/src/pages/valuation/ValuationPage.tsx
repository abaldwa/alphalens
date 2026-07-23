import { SectionListPage } from '@/lib/ui'

export function ValuationPage() {
  return (
    <SectionListPage
      title="Valuation"
      description="Batch DCF/relative valuation ranking across the universe."
      endpoint="/api/v1/valuation/batch/ranked"
    />
  )
}
