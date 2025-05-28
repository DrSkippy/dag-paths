import networkx as nx
import logging
from typing import Dict, List, Tuple, Any

logger = logging.getLogger(__name__)

def create_dag(data: Dict) -> nx.DiGraph:
    """Create a NetworkX DAG from the data.
    
    Args:
        data (dict): DAG data structure with nodes and their relationships
        
    Returns:
        nx.DiGraph: NetworkX directed graph representing the DAG
    """
    logger.info("Creating DAG from data")
    G = nx.DiGraph()
    
    # Add nodes and edges
    for node, info in data.items():
        G.add_node(node, **info)
        if 'predecessors' in info:
            for pred in info['predecessors']:
                G.add_edge(pred, node)
        if 'successors' in info:
            for succ in info['successors']:
                G.add_edge(node, succ)
    
    logger.info(f"Created DAG with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
    return G

def find_longest_paths(G: nx.DiGraph) -> Tuple[List[str], List[str]]:
    """Find the longest paths in the DAG by edges and nodes.
    
    Args:
        G (nx.DiGraph): NetworkX directed graph
        
    Returns:
        tuple: (longest_path_by_edges, longest_path_by_nodes)
            Both paths are lists of nodes representing the path
            Returns (None, None) if no paths are found
    """
    logger.info("Finding longest paths in DAG")
    # Find all paths between all pairs of nodes
    all_paths = []
    for source in G.nodes():
        for target in G.nodes():
            if source != target:
                try:
                    paths = list(nx.all_simple_paths(G, source, target))
                    all_paths.extend(paths)
                except nx.NetworkXNoPath:
                    continue
    
    if not all_paths:
        logger.info("No paths found in the DAG")
        return None, None
    
    # Find longest path by number of edges
    longest_path_by_edges = max(all_paths, key=len)
    
    # Find longest path by number of nodes
    # In a DAG, the number of edges is always one less than the number of nodes
    longest_path_by_nodes = longest_path_by_edges
    
    logger.info(f"Found longest path with {len(longest_path_by_edges)} nodes")
    return longest_path_by_edges, longest_path_by_nodes

def analyze_network(G: nx.DiGraph) -> Dict[str, Any]:
    """Perform basic network analysis on the DAG.
    
    Args:
        G (nx.DiGraph): NetworkX directed graph
        
    Returns:
        dict: Dictionary containing various network metrics
    """
    logger.info("Performing network analysis")
    metrics = {
        'total_nodes': G.number_of_nodes(),
        'total_edges': G.number_of_edges(),
        'is_dag': nx.is_directed_acyclic_graph(G),
        'node_types': {},
        'node_states': {},
        'in_degree_centrality': nx.in_degree_centrality(G),
        'out_degree_centrality': nx.out_degree_centrality(G),
        'topological_sort': list(nx.topological_sort(G)) if nx.is_directed_acyclic_graph(G) else None
    }
    
    # Count node types and states
    for node, attrs in G.nodes(data=True):
        node_type = attrs.get('type', 'Unknown')
        node_state = attrs.get('state', 'Unknown')
        
        metrics['node_types'][node_type] = metrics['node_types'].get(node_type, 0) + 1
        metrics['node_states'][node_state] = metrics['node_states'].get(node_state, 0) + 1
    
    logger.info(f"Network analysis complete. Found {len(metrics['node_types'])} node types and {len(metrics['node_states'])} states")
    logger.debug(f"Node types: {metrics['node_types']}")
    logger.debug(f"Node states: {metrics['node_states']}")
    
    return metrics 