declare module 'dagre' {
  export namespace graphlib {
    class Graph {
      constructor(options?: any);
      setGraph(options: any): void;
      setDefaultEdgeLabel(fn: () => any): void;
      setNode(id: string, label?: any): void;
      setParent(id: string, parent: string): void;
      setEdge(from: string, to: string, label?: any): void;
      hasNode(id: string): boolean;
      nodes(): string[];
      node(id: string): any;
    }
  }

  export function layout(graph: any): void;
}
