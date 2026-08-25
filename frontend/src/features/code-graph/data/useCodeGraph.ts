import { useQuery } from '@tanstack/react-query';

export interface GraphNode {
  id: string;
  type: 'module' | 'function' | 'table' | 'json_file';
  language?: 'python' | 'typescript';
  subsystem?: string;
  name?: string;
  path?: string;
  purpose?: string;
}

export interface GraphEdge {
  from: string;
  to: string;
  kind: 'import' | 'call' | 'reads' | 'writes' | 'http-call';
  line?: number;
  snippet?: string;
  route?: string;
}

export interface CodeGraph {
  nodes: GraphNode[];
  edges: GraphEdge[];
  metadata?: Record<string, unknown>;
}

/**
 * Fetch the full module graph (Stage 1/2).
 */
export function useModuleGraph(options?: { refetchInterval?: number }) {
  return useQuery({
    queryKey: ['code-graph', 'module-graph'],
    queryFn: async () => {
      const res = await fetch('/code-graph/module_graph.json');
      if (!res.ok) throw new Error(`Failed to fetch module graph: ${res.status}`);
      return (await res.json()) as CodeGraph;
    },
    staleTime: 1000 * 60 * 5,
    refetchInterval: options?.refetchInterval,
  });
}

/**
 * Fetch a call graph for a specific entry point (Stage 3, on-demand).
 */
export function useCallGraph(scope: string) {
  return useQuery({
    queryKey: ['code-graph', 'call-graph', scope],
    queryFn: async () => {
      const res = await fetch(`/code-graph/call_graph_${scope}.json`);
      if (!res.ok) throw new Error(`Failed to fetch call graph: ${res.status}`);
      return (await res.json()) as CodeGraph;
    },
    enabled: !!scope,
    staleTime: 1000 * 60 * 5,
  });
}

/**
 * Fetch the summary (SUMMARY.md converted to JSON, if available).
 */
export function useCodeGraphSummary() {
  return useQuery({
    queryKey: ['code-graph', 'summary'],
    queryFn: async () => {
      // For now, return the markdown as text; in future could parse to structured data
      const res = await fetch('/code-graph/SUMMARY.md');
      if (!res.ok) throw new Error(`Failed to fetch summary: ${res.status}`);
      return await res.text();
    },
    staleTime: 1000 * 60 * 5,
  });
}
