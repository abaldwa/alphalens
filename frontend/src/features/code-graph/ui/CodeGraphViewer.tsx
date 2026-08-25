import { useMemo, useState } from 'react';
import type { CodeGraph } from '../data/useCodeGraph';
import { layoutCodeGraph, SUBSYSTEM_COLORS, NODE_TYPE_COLORS } from '../core/graphLayout';
import { cn } from '@/lib/utils';

interface CodeGraphViewerProps {
  graph: CodeGraph;
  onNodeClick?: (nodeId: string) => void;
}

/**
 * Interactive hierarchical visualization of the code graph.
 *
 * - Nodes grouped by subsystem
 * - Color-coded by type (module, function, table, json)
 * - Click to inspect node details
 */
export function CodeGraphViewer({ graph, onNodeClick }: CodeGraphViewerProps) {
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const [selectedSubsystem, setSelectedSubsystem] = useState<string | null>(null);

  const layout = useMemo(() => layoutCodeGraph(graph), [graph]);

  // Filter nodes if subsystem is selected
  const visibleNodes = useMemo(() => {
    if (!selectedSubsystem) return layout.nodes;
    return layout.nodes.filter((n) => n.subsystem === selectedSubsystem);
  }, [layout.nodes, selectedSubsystem]);

  const visibleEdges = useMemo(() => {
    const visibleNodeIds = new Set(visibleNodes.map((n) => n.id));
    return layout.edges.filter((e) => visibleNodeIds.has(e.source) && visibleNodeIds.has(e.target));
  }, [layout.edges, visibleNodes]);

  // Canvas dimensions
  const padding = 40;
  const width = 1600;
  const height = 1000;

  // Find bounds
  let minX = Infinity,
    maxX = -Infinity,
    minY = Infinity,
    maxY = -Infinity;
  visibleNodes.forEach((n) => {
    if (n.x !== undefined && n.y !== undefined) {
      minX = Math.min(minX, n.x - n.width / 2);
      maxX = Math.max(maxX, n.x + n.width / 2);
      minY = Math.min(minY, n.y - n.height / 2);
      maxY = Math.max(maxY, n.y + n.height / 2);
    }
  });

  const graphWidth = maxX - minX + padding * 2;
  const graphHeight = maxY - minY + padding * 2;
  const scale = Math.min(width / graphWidth, height / graphHeight, 1);
  const offsetX = (width - graphWidth * scale) / 2 - minX * scale;
  const offsetY = (height - graphHeight * scale) / 2 - minY * scale;

  // Subsystems for filter buttons
  const subsystems: string[] = Array.from(
    new Set(layout.nodes.map((n) => n.subsystem).filter((s): s is string => Boolean(s))),
  );

  return (
    <div className="flex flex-col gap-4">
      {/* Filters */}
      <div className="flex flex-wrap gap-2">
        <button
          onClick={() => setSelectedSubsystem(null)}
          className={cn(
            'px-3 py-1 rounded text-sm font-medium',
            selectedSubsystem === null ? 'bg-blue-600 text-white' : 'bg-gray-200 text-gray-900',
          )}
        >
          All
        </button>
        {subsystems.map((subsys) => (
          <button
            key={subsys}
            onClick={() => setSelectedSubsystem(subsys)}
            className={cn(
              'px-3 py-1 rounded text-sm font-medium transition',
              selectedSubsystem === subsys ? 'text-white' : 'bg-gray-200 text-gray-900',
            )}
            style={selectedSubsystem === subsys ? { backgroundColor: SUBSYSTEM_COLORS[subsys] || '#666' } : {}}
          >
            {subsys}
          </button>
        ))}
      </div>

      {/* Graph Canvas */}
      <div className="border rounded-lg bg-gray-50 overflow-auto h-screen">
        <svg width={width} height={height} className="bg-white block">
          {/* Edges */}
          {visibleEdges.map((edge) => {
            const source = visibleNodes.find((n) => n.id === edge.source);
            const target = visibleNodes.find((n) => n.id === edge.target);
            if (!source?.x || !source?.y || !target?.x || !target?.y) return null;

            const x1 = offsetX + source.x * scale;
            const y1 = offsetY + source.y * scale;
            const x2 = offsetX + target.x * scale;
            const y2 = offsetY + target.y * scale;

            const isSelected = selectedNode === edge.source || selectedNode === edge.target;
            const strokeWidth = isSelected ? 4 : 2.5;
            const strokeColor =
              edge.kind === 'call'
                ? '#ef4444'
                : edge.kind === 'writes'
                  ? '#f97316'
                  : edge.kind === 'reads'
                    ? '#3b82f6'
                    : edge.kind === 'http-call'
                      ? '#8b5cf6'
                      : '#9ca3af';

            return (
              <g key={edge.id}>
                <line x1={x1} y1={y1} x2={x2} y2={y2} stroke={strokeColor} strokeWidth={strokeWidth} opacity="0.6" />
                {/* Arrowhead */}
                <polygon
                  points={`${x2},${y2} ${x2 - 12},${y2 - 8} ${x2 - 12},${y2 + 8}`}
                  fill={strokeColor}
                  opacity="0.6"
                />
              </g>
            );
          })}

          {/* Nodes */}
          {visibleNodes.map((node) => {
            if (node.x === undefined || node.y === undefined) return null;

            const x = offsetX + node.x * scale;
            const y = offsetY + node.y * scale;
            const w = (node.width * scale) / 2;
            const h = (node.height * scale) / 2;

            const isSelected = selectedNode === node.id;
            const bgColor = NODE_TYPE_COLORS[node.type] || '#e5e7eb';
            const borderColor = SUBSYSTEM_COLORS[node.subsystem || 'other'] || '#666';

            return (
              <g
                key={node.id}
                onClick={() => {
                  setSelectedNode(node.id);
                  onNodeClick?.(node.id);
                }}
                className="cursor-pointer"
              >
                {/* Node rectangle */}
                <rect
                  x={x - w}
                  y={y - h}
                  width={w * 2}
                  height={h * 2}
                  fill={bgColor}
                  stroke={borderColor}
                  strokeWidth={isSelected ? 3 : 2}
                  rx="4"
                />

                {/* Node label */}
                <text
                  x={x}
                  y={y}
                  textAnchor="middle"
                  dy="0.3em"
                  fontSize={Math.max(14, 18 * scale)}
                  fill="#1f2937"
                  fontWeight={isSelected ? 'bold' : 'normal'}
                  className="pointer-events-none"
                >
                  {node.label.split('\n')[0]}
                </text>
              </g>
            );
          })}
        </svg>
      </div>

      {/* Node details */}
      {selectedNode && (
        <div className="border rounded-lg p-4 bg-blue-50">
          <div className="font-mono text-sm text-gray-600">{selectedNode}</div>
          <p className="text-sm text-gray-700 mt-1">
            Incoming: {visibleEdges.filter((e) => e.target === selectedNode).length} | Outgoing:{' '}
            {visibleEdges.filter((e) => e.source === selectedNode).length}
          </p>
        </div>
      )}
    </div>
  );
}
