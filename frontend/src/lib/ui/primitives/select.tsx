import * as React from 'react'

import { cn } from '@/lib/utils'

/**
 * Minimal shadcn-Select-compatible API backed by a plain native <select> --
 * this repo doesn't have @radix-ui/react-select installed and every other
 * page (experimentation.tsx, rebalance.tsx, etc.) already just uses native
 * <select>, so this shim lets call sites keep the familiar
 * Select/SelectTrigger/SelectContent/SelectItem shape without pulling in a
 * new dependency. SelectTrigger/SelectValue are accepted for API
 * compatibility but not rendered as separate DOM -- Select renders the
 * actual <select> itself, using SelectTrigger's `id`/`className` and
 * SelectContent's SelectItem children as <option>s.
 */

interface SelectContextValue {
  value?: string
  onValueChange?: (value: string) => void
}

const SelectContext = React.createContext<SelectContextValue>({})

interface SelectProps {
  value?: string
  onValueChange?: (value: string) => void
  children?: React.ReactNode
}

function findTriggerProps(children: React.ReactNode): { id?: string; className?: string } {
  let result: { id?: string; className?: string } = {}
  React.Children.forEach(children, (child) => {
    if (React.isValidElement(child) && (child.type as { displayName?: string }).displayName === 'SelectTrigger') {
      const props = child.props as { id?: string; className?: string }
      result = { id: props.id, className: props.className }
    }
  })
  return result
}

function findContent(children: React.ReactNode): React.ReactNode {
  let content: React.ReactNode = null
  React.Children.forEach(children, (child) => {
    if (React.isValidElement(child) && (child.type as { displayName?: string }).displayName === 'SelectContent') {
      content = (child.props as { children?: React.ReactNode }).children
    }
  })
  return content
}

function Select({ value, onValueChange, children }: SelectProps) {
  const { id, className } = findTriggerProps(children)
  const content = findContent(children)
  return (
    <SelectContext.Provider value={{ value, onValueChange }}>
      <select
        id={id}
        data-slot="select"
        className={cn(
          'flex h-9 w-full rounded-[var(--radius-token)] border border-border bg-background px-3 py-1 text-sm shadow-[var(--shadow-token)] outline-none transition-colors focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30 disabled:cursor-not-allowed disabled:opacity-50',
          className,
        )}
        value={value ?? ''}
        onChange={(e) => onValueChange?.(e.target.value)}
      >
        {content}
      </select>
    </SelectContext.Provider>
  )
}

/** Accepted for API compatibility; rendered inline by Select itself. */
function SelectTrigger({ children }: { id?: string; className?: string; children?: React.ReactNode }) {
  return <>{children}</>
}
SelectTrigger.displayName = 'SelectTrigger'

/** Accepted for API compatibility; native <select> shows the chosen <option> text itself. */
function SelectValue(_props: { placeholder?: string }) {
  return null
}
SelectValue.displayName = 'SelectValue'

function SelectContent({ children }: { children?: React.ReactNode }) {
  return <>{children}</>
}
SelectContent.displayName = 'SelectContent'

function SelectItem({ value, children, ...props }: { value: string; children?: React.ReactNode } & React.ComponentProps<'option'>) {
  return (
    <option value={value} {...props}>
      {children}
    </option>
  )
}
SelectItem.displayName = 'SelectItem'

export { Select, SelectTrigger, SelectValue, SelectContent, SelectItem }
