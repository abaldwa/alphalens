import { useCallback, useMemo, useState } from 'react';
import type { Node, Edge } from '@xyflow/react';
import {
  ReactFlow,
  ReactFlowProvider,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  useReactFlow,
  MiniMap,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import type { CodeGraph } from '../data/useCodeGraph';
import { SUBSYSTEM_COLORS, NODE_TYPE_COLORS } from '../core/graphLayout';
import { cn } from '@/lib/utils';

interface CodeGraphViewerProps {
  graph: CodeGraph;
  onNodeClick?: (nodeId: string) => void;
}

/**
 * Interactive graph visualization using React Flow.
 * - Draggable nodes, zoom/pan, minimap
 * - Grouped by subsystem with color coding
 * - Click to drill down into call graphs
 */
function CodeGraphViewerInner({ graph, onNodeClick }: CodeGraphViewerProps) {
  const [selectedSubsystem, setSelectedSubsystem] = useState<string | null>(null);
  const { fitView } = useReactFlow();

  // Convert CodeGraph to React Flow format
  const { nodes: rfNodes, edges: rfEdges } = useMemo(() => {
    const nodes: Node[] = graph.nodes.map((node) => ({
      id: node.id,
      data: {
        label: node.name || node.id.split(':').pop() || node.id,
        type: node.type,
        subsystem: node.subsystem,
      },
      position: { x: Math.random() * 800, y: Math.random() * 800 },
      style: {
        background: NODE_TYPE_COLORS[node.type] || '#e5e7eb',
        border: `3px solid ${SUBSYSTEM_COLORS[node.subsystem || 'other'] || '#666'}`,
        borderRadius: '8px',
        padding: '12px 8px',
        fontSize: '12px',
        fontWeight: 600,
        textAlign: 'center',
        minWidth: '100px',
        maxWidth: '140px',
        minHeight: '50px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        cursor: 'pointer',
        whiteSpace: 'normal',
        wordBreak: 'break-word',
        lineHeight: '1.3',
        boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
        transition: 'all 0.2s ease',
      },
    }));

    const edges: Edge[] = graph.edges.map((edge, idx) => ({
      id: `${edge.from}-${edge.to}-${idx}`,
      source: edge.from,
      target: edge.to,
      animated: edge.kind === 'call',
      style: {
        stroke:
          edge.kind === 'call'
            ? '#ef4444'
            : edge.kind === 'writes'
              ? '#f97316'
              : edge.kind === 'reads'
                ? '#3b82f6'
                : edge.kind === 'http-call'
                  ? '#8b5cf6'
                  : '#9ca3af',
        strokeWidth: 2,
      },
      markerEnd: { type: 'arrowclosed' },
    }));

    return { nodes, edges };
  }, [graph]);

  // Filter nodes by subsystem
  const filteredNodes = useMemo(() => {
    if (!selectedSubsystem) return rfNodes;
    return rfNodes.filter((n) => n.data.subsystem === selectedSubsystem);
  }, [rfNodes, selectedSubsystem]);

  const filteredEdges = useMemo(() => {
    const visibleNodeIds = new Set(filteredNodes.map((n) => n.id));
    return rfEdges.filter((e) => visibleNodeIds.has(e.source) && visibleNodeIds.has(e.target));
  }, [rfEdges, filteredNodes]);

  const [nodes, setNodes] = useNodesState(filteredNodes);
  const [edges, setEdges] = useEdgesState(filteredEdges);

  const subsystems: string[] = Array.from(
    new Set(graph.nodes.map((n) => n.subsystem).filter((s): s is string => Boolean(s))),
  ).sort();

  const handleNodeClick = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      onNodeClick?.(node.id);
    },
    [onNodeClick],
  );

  const handleFitView = useCallback(() => {
    setTimeout(() => fitView({ padding: 0.2, duration: 800 }), 100);
  }, [fitView]);

  const handleSelectSubsystem = useCallback(
    (subsys: string | null) => {
      setSelectedSubsystem(subsys);
      if (subsys === null) {
        setNodes(rfNodes);
        setEdges(rfEdges);
      } else {
        setNodes(filteredNodes);
        setEdges(filteredEdges);
      }
    },
    [setNodes, setEdges, rfNodes, rfEdges, filteredNodes, filteredEdges],
  );

  return (
    <div className="w-full h-[700px] border rounded-lg overflow-hidden bg-gray-50 flex flex-col">
      {/* Filter buttons */}
      <div className="flex flex-wrap gap-2 p-4 border-b bg-white z-10">
        <button
          onClick={() => handleSelectSubsystem(null)}
          className={cn(
            'px-3 py-1 rounded text-sm font-medium transition',
            selectedSubsystem === null ? 'bg-blue-600 text-white shadow-md' : 'bg-gray-200 text-gray-900 hover:bg-gray-300',
          )}
        >
          All ({rfNodes.length})
        </button>
        {subsystems.map((subsys) => {
          const count = rfNodes.filter((n) => n.data.subsystem === subsys).length;
          return (
            <button
              key={subsys}
              onClick={() => handleSelectSubsystem(subsys)}
              className={cn(
                'px-3 py-1 rounded text-sm font-medium transition text-white shadow-sm',
                selectedSubsystem === subsys ? 'opacity-100 shadow-md ring-2 ring-offset-1 ring-black' : 'opacity-70 hover:opacity-90',
              )}
              style={{ backgroundColor: SUBSYSTEM_COLORS[subsys] || '#666' }}
              title={`${count} nodes`}
            >
              {subsys} ({count})
            </button>
          );
        })}
        <button
          onClick={handleFitView}
          className="ml-auto px-3 py-1 rounded text-sm font-medium bg-green-600 text-white hover:bg-green-700 transition shadow-sm"
        >
          Fit View
        </button>
      </div>

      {/* React Flow Canvas */}
      <div className="flex-1 relative">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={() => {}}
          onEdgesChange={() => {}}
          onNodeClick={handleNodeClick}
          fitView
        >
          <Background color="#f0f0f0" gap={16} />
          <Controls position="bottom-left" />
          <MiniMap position="top-right" />
        </ReactFlow>
      </div>
    </div>
  );
}

export function CodeGraphViewer(props: CodeGraphViewerProps) {
  return (
    <ReactFlowProvider>
      <CodeGraphViewerInner {...props} />
    </ReactFlowProvider>
  );
}
