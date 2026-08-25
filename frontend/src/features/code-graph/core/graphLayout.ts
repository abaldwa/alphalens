import dagre from 'dagre';
import type { CodeGraph, GraphNode } from '../data/useCodeGraph';

export interface LayoutNode {
  id: string;
  label: string;
  width: number;
  height: number;
  type: GraphNode['type'];
  subsystem?: string;
  x?: number;
  y?: number;
}

export interface LayoutEdge {
  id: string;
  source: string;
  target: string;
  kind: string;
}

/**
 * Convert a CodeGraph to a hierarchical layout using Dagre.
 *
 * Groups nodes by subsystem for visual organization.
 */
export function layoutCodeGraph(graph: CodeGraph): {
  nodes: LayoutNode[];
  edges: LayoutEdge[];
} {
  const dagreGraph = new dagre.graphlib.Graph({ compound: true });
  dagreGraph.setGraph({ rankdir: 'TB', nodesep: 80, ranksep: 80 });
  dagreGraph.setDefaultEdgeLabel(() => ({}));

  // Group nodes by subsystem
  const nodesBySubsystem = new Map<string, LayoutNode[]>();
  const nodeMap = new Map<string, GraphNode>();

  for (const node of graph.nodes) {
    nodeMap.set(node.id, node);
    const subsystem = node.subsystem || 'uncategorized';

    // Calculate label and size based on type
    let label = node.name || node.id.split(':').pop() || node.id;
    const typeLabel = `[${node.type}]`;
    label = `${label}\n${typeLabel}`;

    const layoutNode: LayoutNode = {
      id: node.id,
      label,
      width: Math.max(150, label.length * 7),
      height: 90,
      type: node.type,
      subsystem,
    };

    if (!nodesBySubsystem.has(subsystem)) {
      nodesBySubsystem.set(subsystem, []);
    }
    nodesBySubsystem.get(subsystem)!.push(layoutNode);

    // Add to dagre graph
    dagreGraph.setNode(node.id, {
      width: layoutNode.width,
      height: layoutNode.height,
    });

    // Add subsystem as compound node if not already present
    if (!dagreGraph.hasNode(subsystem)) {
      dagreGraph.setNode(subsystem, {
        label: subsystem,
        clusterLabelPos: 'top',
        style: 'rounded',
      });
    }

    // Add node to its subsystem cluster
    dagreGraph.setParent(node.id, subsystem);
  }

  // Add edges
  const edgeMap = new Map<string, LayoutEdge>();
  for (const edge of graph.edges) {
    const edgeId = `${edge.from}→${edge.to}`;
    edgeMap.set(edgeId, {
      id: edgeId,
      source: edge.from,
      target: edge.to,
      kind: edge.kind,
    });
    dagreGraph.setEdge(edge.from, edge.to);
  }

  // Layout
  dagre.layout(dagreGraph);

  // Extract positioned nodes
  const layoutNodes: LayoutNode[] = [];
  dagreGraph.nodes().forEach((nodeId: string) => {
    const node = nodeMap.get(nodeId);
    if (node) {
      const dagreNode = dagreGraph.node(nodeId);
      const subsystem = node.subsystem || 'uncategorized';
      const subsystemNodes = nodesBySubsystem.get(subsystem) || [];
      const layoutNode = subsystemNodes.find((n) => n.id === nodeId);

      if (layoutNode) {
        layoutNode.x = dagreNode.x;
        layoutNode.y = dagreNode.y;
        layoutNodes.push(layoutNode);
      }
    }
  });

  return {
    nodes: layoutNodes,
    edges: Array.from(edgeMap.values()),
  };
}

/**
 * Subsystem color palette (for visualization).
 */
export const SUBSYSTEM_COLORS: Record<string, string> = {
  backtest: '#3b82f6',
  datastore: '#8b5cf6',
  features: '#ec4899',
  ingestion: '#f59e0b',
  config: '#6366f1',
  frontend: '#10b981',
  dashboard: '#06b6d4',
  paper_trading: '#f97316',
  strategies: '#84cc16',
  systems: '#64748b',
  other: '#9ca3af',
};

/**
 * Node type color palette (for secondary distinction).
 */
export const NODE_TYPE_COLORS: Record<string, string> = {
  module: '#e0e7ff',
  function: '#fce7f3',
  table: '#dbeafe',
  json_file: '#fef3c7',
};
