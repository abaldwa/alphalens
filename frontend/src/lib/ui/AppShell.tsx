import * as React from 'react'
import { Link, useLocation } from 'react-router-dom'
import {
  ChevronDown,
  Menu,
  PanelLeftClose,
  PanelLeftOpen,
  LayoutDashboard,
  LineChart,
  TrendingUp,
  Scale,
  Search,
  Brain,
  Rocket,
  Users,
  Settings,
  Landmark,
  type LucideIcon,
} from 'lucide-react'

import { cn } from '@/lib/utils'
import { Button } from '@/lib/ui/primitives/button'
import { Sheet, SheetContent, SheetTrigger } from '@/lib/ui/primitives/sheet'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/lib/ui/primitives/tooltip'
import { NAV_SECTIONS, type NavSection } from '@/lib/ui/nav'
import { CopilotPanel } from '@/lib/ui/CopilotPanel'

const SECTION_ICONS: Record<string, LucideIcon> = {
  home: LayoutDashboard,
  technical: LineChart,
  fundamental: TrendingUp,
  valuation: Scale,
  forensic: Search,
  ml: Brain,
  momentum: Rocket,
  big_investors: Users,
  ops: Settings,
  macro: Landmark,
}

const SIDEBAR_COLLAPSED_KEY = 'alphalens.sidebarCollapsed'

function isActive(href: string, pathname: string): boolean {
  return pathname === href
}

/** A section is "active" (and its sub-menu auto-expanded) if the current
 * path matches the section link or any of its sub-item links. */
function sectionActive(section: NavSection, pathname: string): boolean {
  if (isActive(section.href, pathname)) return true
  return (section.subItems ?? []).some((si) => isActive(si.href, pathname))
}

/** Top-bar breadcrumb brand: "AlphaLens" on Home, "AlphaLens.<Section>"
 * everywhere else (e.g. "AlphaLens.Technical"), matching the app's
 * dotted section-naming convention. */
function brandLabel(pathname: string): string {
  const section = NAV_SECTIONS.find((s) => sectionActive(s, pathname))
  if (!section || section.id === 'home') return 'AlphaLens'
  return `AlphaLens.${section.label.replace(/\s+/g, '')}`
}

function NavList({
  pathname,
  onNavigate,
  collapsed = false,
}: {
  pathname: string
  onNavigate?: () => void
  collapsed?: boolean
}) {
  const [expanded, setExpanded] = React.useState<Record<string, boolean>>(() =>
    Object.fromEntries(NAV_SECTIONS.map((s) => [s.id, sectionActive(s, pathname)])),
  )

  return (
    <nav className="flex flex-col gap-0.5 px-2">
      {NAV_SECTIONS.map((s) => {
        const active = sectionActive(s, pathname)
        const hasSub = (s.subItems?.length ?? 0) > 0
        const isOpen = expanded[s.id] ?? active
        const Icon = SECTION_ICONS[s.id] ?? LayoutDashboard

        if (collapsed) {
          return (
            <Tooltip key={s.id}>
              <TooltipTrigger asChild>
                <Link
                  to={s.href}
                  onClick={onNavigate}
                  aria-label={s.label}
                  className={cn(
                    'flex items-center justify-center rounded-[var(--radius-token)] p-2.5 transition-colors',
                    active
                      ? 'bg-white/10 text-white'
                      : 'text-sidebar-foreground/70 hover:bg-white/5 hover:text-sidebar-foreground',
                  )}
                >
                  <Icon className="size-4.5 shrink-0" />
                </Link>
              </TooltipTrigger>
              <TooltipContent side="right">{s.label}</TooltipContent>
            </Tooltip>
          )
        }

        return (
          <div key={s.id} className="flex flex-col">
            <div className="flex items-center">
              <Link
                to={s.href}
                onClick={onNavigate}
                className={cn(
                  'flex flex-1 items-center gap-2.5 rounded-[var(--radius-token)] px-3 py-2 text-sm font-medium transition-colors',
                  active
                    ? 'bg-white/10 text-white'
                    : 'text-sidebar-foreground/70 hover:bg-white/5 hover:text-sidebar-foreground',
                )}
              >
                <Icon className="size-4 shrink-0" />
                <span className="truncate">{s.label}</span>
              </Link>
              {hasSub ? (
                <button
                  type="button"
                  aria-label={`Toggle ${s.label} sub-menu`}
                  onClick={() => setExpanded((e) => ({ ...e, [s.id]: !isOpen }))}
                  className="rounded-[var(--radius-token)] p-2 text-sidebar-foreground/60 hover:bg-white/5 hover:text-sidebar-foreground"
                >
                  <ChevronDown className={cn('size-3.5 transition-transform', isOpen && 'rotate-180')} />
                </button>
              ) : null}
            </div>
            {hasSub && isOpen ? (
              <div className="ml-3 flex flex-col gap-0.5 border-l border-white/10 pl-2">
                {s.subItems!.map((si) => {
                  const subActive = isActive(si.href, pathname)
                  const linkClassName = cn(
                    'rounded-[var(--radius-token)] px-3 py-1.5 text-xs font-medium transition-colors',
                    subActive
                      ? 'bg-white/10 text-white'
                      : 'text-sidebar-foreground/60 hover:bg-white/5 hover:text-sidebar-foreground',
                  )
                  if (si.external) {
                    return (
                      <a
                        key={si.id}
                        href={si.href}
                        target="_blank"
                        rel="noopener noreferrer"
                        className={linkClassName}
                      >
                        {si.label}
                      </a>
                    )
                  }
                  return (
                    <Link key={si.id} to={si.href} onClick={onNavigate} className={linkClassName}>
                      {si.label}
                    </Link>
                  )
                })}
              </div>
            ) : null}
          </div>
        )
      })}
    </nav>
  )
}

export interface AppShellProps {
  /** Breadcrumb-style page title shown in the top bar. */
  title: string
  /** Optional short description under the title. */
  description?: string
  /** Optional right-aligned top-bar content (filters, actions). */
  actions?: React.ReactNode
  children: React.ReactNode
}

/**
 * Shared shell for every section entry: a dark, collapsible sidebar rail
 * (all 9 AlphaLens sections) plus a top bar with a page title. Below
 * 768px the sidebar collapses into a Sheet drawer triggered by a menu
 * button. Import from `@/lib/ui` — this is a library component, not
 * page-specific code, so every Vite entry's page composes it the same way.
 */
export function AppShell({ title, description, actions, children }: AppShellProps) {
  const { pathname } = useLocation()
  const [open, setOpen] = React.useState(false)
  const [collapsed, setCollapsed] = React.useState(
    () => typeof window !== 'undefined' && window.localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === '1',
  )

  const toggleCollapsed = () => {
    setCollapsed((c) => {
      const next = !c
      window.localStorage.setItem(SIDEBAR_COLLAPSED_KEY, next ? '1' : '0')
      return next
    })
  }

  return (
    <TooltipProvider delayDuration={200}>
    <div className="flex min-h-screen w-full bg-background text-foreground">
      {/* Desktop sidebar rail — collapses to an icon-only strip to free up
          screen width for data-dense pages (screeners, wide tables). State
          persists across route navigation via
          localStorage. */}
      <aside
        className={cn(
          'hidden shrink-0 flex-col border-r border-sidebar-border bg-sidebar py-4 transition-[width] duration-150 md:flex',
          collapsed ? 'w-16' : 'w-60',
        )}
      >
        <div className={cn('flex items-center pb-4', collapsed ? 'justify-center px-2' : 'justify-between px-4')}>
          {collapsed ? null : <span className="text-sm font-semibold tracking-tight text-white">AlphaLens</span>}
          <Button
            variant="ghost"
            size="icon"
            aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            onClick={toggleCollapsed}
            className="size-7 text-sidebar-foreground/70 hover:bg-white/5 hover:text-white"
          >
            {collapsed ? <PanelLeftOpen className="size-4" /> : <PanelLeftClose className="size-4" />}
          </Button>
        </div>
        <NavList pathname={pathname} collapsed={collapsed} />
      </aside>

      {/* Mobile drawer */}
      <Sheet open={open} onOpenChange={setOpen}>
        <SheetContent side="left" className="py-4 md:hidden">
          <div className="px-4 pb-4 text-sm font-semibold tracking-tight text-white">AlphaLens</div>
          <NavList pathname={pathname} onNavigate={() => setOpen(false)} />
        </SheetContent>
      </Sheet>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex flex-wrap items-center gap-3 border-b border-border bg-card px-4 py-3 md:px-6">
          <Sheet open={open} onOpenChange={setOpen}>
            <SheetTrigger asChild>
              <Button variant="ghost" size="icon" className="md:hidden" aria-label="Open navigation">
                <Menu />
              </Button>
            </SheetTrigger>
          </Sheet>
          <div className="min-w-0 flex-1">
            <div className="text-xs text-muted-foreground">{brandLabel(pathname)}</div>
            <h1 className="truncate text-lg font-semibold">{title}</h1>
            {description ? <p className="break-words text-sm text-muted-foreground">{description}</p> : null}
          </div>
          {actions ? <div className="flex w-full flex-wrap items-center gap-2 sm:w-auto">{actions}</div> : null}
        </header>
        <main className="flex-1 overflow-x-hidden p-4 md:p-6">{children}</main>
      </div>

      <CopilotPanel />
    </div>
    </TooltipProvider>
  )
}
