import { useEffect, useState } from 'react';
import { useModuleGraph, useCallGraph } from '@/features/code-graph/data/useCodeGraph';
import { CodeGraphViewer } from '@/features/code-graph/ui/CodeGraphViewer';

/**
 * Code Graph viewer page.
 *
 * Displays the full module graph (Stage 1/2), with click-through drill-down to call graphs (Stage 3).
 * Auto-updating sections show graph stats, subsystems, dead code candidates, and key tables.
 */
export default function CodeGraphPage() {
  const moduleGraphQuery = useModuleGraph({
    refetchInterval: 5 * 60 * 1000, // Auto-refresh every 5 minutes
  });
  const [selectedScope, setSelectedScope] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const callGraphQuery = useCallGraph(selectedScope || '');

  // Track when data last updated
  useEffect(() => {
    if (moduleGraphQuery.data) {
      setLastUpdated(new Date());
    }
  }, [moduleGraphQuery.data]);

  if (moduleGraphQuery.isPending) {
    return <div className="p-8 text-center">Loading code graph...</div>;
  }

  if (moduleGraphQuery.error) {
    return (
      <div className="p-8 text-center text-red-600">
        <p>Error loading code graph: {moduleGraphQuery.error.message}</p>
        <p className="text-sm text-gray-500 mt-2">
          Generated files should be at <code>frontend/public/code-graph/*.json</code>
        </p>
      </div>
    );
  }

  const graph = moduleGraphQuery.data;

  // Extract subsystems
  const subsystems = Array.from(
    new Set(graph?.nodes.map((n) => n.subsystem).filter((s): s is string => Boolean(s)) || []),
  ).sort();

  // Find nodes with no incoming edges (dead code candidates)
  const incomingEdges = new Map<string, number>();
  graph?.edges.forEach((e) => {
    incomingEdges.set(e.target, (incomingEdges.get(e.target) || 0) + 1);
  });

  const deadCodeCandidates = graph?.nodes
    .filter((n) => !incomingEdges.has(n.id) && n.type === 'module')
    .slice(0, 20) // Show top 20
    .map((n) => n.id) || [];

  // Extract key tables
  const keyTables = graph?.nodes
    .filter((n) => n.type === 'table')
    .map((n) => {
      const outgoing = graph.edges.filter((e) => e.source === n.id).length;
      const incoming = graph.edges.filter((e) => e.target === n.id).length;
      return { id: n.id, name: n.name || n.id, outgoing, incoming };
    })
    .sort((a, b) => b.incoming + b.outgoing - (a.incoming + a.outgoing))
    .slice(0, 10) || [];

  return (
    <div className="p-8">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h1 className="text-3xl font-bold">Code Graph</h1>
            <p className="text-gray-600 mt-1">
              Module structure, data dependencies, and call-graph drill-down
              <a href="/docs/codegraph/SUMMARY.md" className="ml-2 text-blue-600 hover:underline text-sm">
                (see SUMMARY.md)
              </a>
            </p>
          </div>
          <div className="text-right">
            <div className="text-xs text-gray-500">
              {lastUpdated ? `Updated ${lastUpdated.toLocaleTimeString()}` : 'Loading...'}
            </div>
            <button
              onClick={() => moduleGraphQuery.refetch()}
              className="mt-2 px-3 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700"
            >
              Refresh Now
            </button>
            <label className="ml-2 text-xs text-gray-600">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
                className="mr-1"
              />
              Auto-refresh
            </label>
          </div>
        </div>
      </div>

      {/* Key Stats */}
      <div className="grid grid-cols-4 gap-4 mb-8">
        <div className="bg-blue-50 p-4 rounded-lg border border-blue-200">
          <div className="text-sm text-gray-600 font-medium">Modules</div>
          <div className="text-2xl font-bold mt-2">
            {graph?.nodes.filter((n) => n.type === 'module').length || 0}
          </div>
        </div>
        <div className="bg-purple-50 p-4 rounded-lg border border-purple-200">
          <div className="text-sm text-gray-600 font-medium">Tables</div>
          <div className="text-2xl font-bold mt-2">{graph?.nodes.filter((n) => n.type === 'table').length || 0}</div>
        </div>
        <div className="bg-amber-50 p-4 rounded-lg border border-amber-200">
          <div className="text-sm text-gray-600 font-medium">JSON Files</div>
          <div className="text-2xl font-bold mt-2">
            {graph?.nodes.filter((n) => n.type === 'json_file').length || 0}
          </div>
        </div>
        <div className="bg-green-50 p-4 rounded-lg border border-green-200">
          <div className="text-sm text-gray-600 font-medium">Dependencies</div>
          <div className="text-2xl font-bold mt-2">{graph?.edges.length || 0}</div>
        </div>
      </div>

      {/* Subsystems Section */}
      <div className="mb-8 border rounded-lg p-6 bg-white">
        <h2 className="text-lg font-bold mb-4">Subsystems ({subsystems.length})</h2>
        <div className="flex flex-wrap gap-2">
          {subsystems.map((subsys) => {
            const count = graph?.nodes.filter((n) => n.subsystem === subsys).length || 0;
            return (
              <div key={subsys} className="px-3 py-2 bg-gray-100 rounded text-sm">
                <span className="font-medium">{subsys}</span>
                <span className="text-gray-600 ml-2">({count})</span>
              </div>
            );
          })}
        </div>
      </div>

      {/* Key Tables Section */}
      {keyTables.length > 0 && (
        <div className="mb-8 border rounded-lg p-6 bg-white">
          <h2 className="text-lg font-bold mb-4">Most Connected Tables</h2>
          <div className="space-y-2">
            {keyTables.map((table) => (
              <div key={table.id} className="flex items-center justify-between px-4 py-3 bg-gray-50 rounded">
                <span className="font-mono text-sm">{table.name}</span>
                <div className="text-xs text-gray-600">
                  <span className="mr-3">📤 {table.outgoing} writes</span>
                  <span>📥 {table.incoming} reads</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Dead Code Candidates Section */}
      {deadCodeCandidates.length > 0 && (
        <div className="mb-8 border rounded-lg p-6 bg-red-50 border-red-200">
          <h2 className="text-lg font-bold mb-4 text-red-900">Potential Dead Code ({deadCodeCandidates.length}+)</h2>
          <p className="text-sm text-red-800 mb-4">
            Modules with no incoming dependencies. Verify before removal.
          </p>
          <div className="space-y-2 max-h-64 overflow-y-auto">
            {deadCodeCandidates.map((moduleId) => (
              <div key={moduleId} className="px-4 py-2 bg-white rounded text-sm font-mono border border-red-100">
                {moduleId}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Main viewer */}
      <div className="mb-8 border rounded-lg bg-white">
        <div className="px-6 py-4 border-b bg-gray-50">
          <h2 className="text-lg font-bold">Interactive Module Map</h2>
          <p className="text-sm text-gray-600">Click on a module to drill down into its call graph</p>
        </div>
        {graph && (
          <div className="p-6">
            <CodeGraphViewer
              graph={graph}
              onNodeClick={(nodeId) => {
                // Trigger drill-down if it's a module
                if (nodeId.includes(':')) {
                  setSelectedScope(nodeId.replace(/^.*?:/, '').replace(/\./g, '-'));
                }
              }}
            />
          </div>
        )}
      </div>

      {/* Call graph drill-down */}
      {selectedScope && (
        <div className="border rounded-lg p-6 bg-gray-50">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-bold">Call Graph: {selectedScope}</h2>
            <button
              onClick={() => setSelectedScope(null)}
              className="text-gray-600 hover:text-gray-900 text-sm underline"
            >
              Close
            </button>
          </div>

          {callGraphQuery.isPending && <div className="text-center text-gray-600">Loading call graph...</div>}
          {callGraphQuery.error && (
            <div className="text-red-600 text-sm">Error: {callGraphQuery.error.message}</div>
          )}
          {callGraphQuery.data && <CodeGraphViewer graph={callGraphQuery.data} />}
        </div>
      )}
    </div>
  );
}
