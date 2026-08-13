import { Link } from 'react-router-dom'

// Back-to-hub link shown at the top of every split-out section page.
export function BackToReportLink() {
  return (
    <Link to="/momentum-dynamic-report" className="mb-4 inline-block text-sm text-primary underline">
      ← Back to Strategy Report
    </Link>
  )
}
