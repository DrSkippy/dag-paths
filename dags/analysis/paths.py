import networkx as nx
import logging
from typing import Dict, List, Tuple, Any
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
import pydot

logger = logging.getLogger(__name__)

@dataclass
class PathInfo:
    """Class to store information about a path."""
    nodes: List[str]
    target_date: datetime
    start_date: datetime
    closed_date: datetime

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

def plot_dag(G: nx.DiGraph, output_path: Path, highlight_paths: List[List[str]] = None) -> None:
    """Create a visual representation of the DAG using pydot.
    
    Args:
        G (nx.DiGraph): NetworkX directed graph
        output_path (Path): Path to save the output PNG file
        highlight_paths (List[List[str]], optional): List of paths to highlight in the graph
    """
    logger.info(f"Creating DAG visualization at {output_path}")
    
    # Create pydot graph with high DPI settings
    dot = pydot.Dot(graph_type='digraph', rankdir='TB', dpi='1200')
    dot.set_node_defaults(shape='box', style='filled', fillcolor='lightblue')
    dot.set_edge_defaults(arrowhead='vee')
    
    # Add nodes
    for node, attrs in G.nodes(data=True):
        # Create node label with type and state
        node_type = attrs.get('type', 'Unknown')
        node_state = attrs.get('state', 'Unknown')
        label = f"{node}\n{node_type}\n{node_state}"
        
        # Create node with attributes
        pydot_node = pydot.Node(
            node,
            label=label,
            fillcolor='lightgreen' if highlight_paths and any(node in path for path in highlight_paths) else 'lightblue'
        )
        dot.add_node(pydot_node)
    
    # Add edges
    for source, target in G.edges():
        # Create edge with attributes
        edge = pydot.Edge(
            source,
            target,
            color='red' if highlight_paths and any(
                source in path and target in path and 
                path.index(source) + 1 == path.index(target) 
                for path in highlight_paths
            ) else 'black'
        )
        dot.add_edge(edge)
    
    # Save the graph with high resolution settings
    try:
        # Set graph attributes for high resolution
        dot.set('dpi', '1200')
        dot.set('size', '60, 3')  # Not intended to print!
        dot.set('ratio', 'fill')
        dot.set('concentrate', 'true')  # Merge edges between same nodes
        
        # Write high resolution PNG
        dot.write_png(str(output_path), prog='dot')
        logger.info(f"Successfully created high resolution DAG visualization at {output_path}")
    except Exception as e:
        logger.error(f"Failed to create DAG visualization: {e}")
        raise

def find_paths_with_dates(G: nx.DiGraph, temporal_data: Dict[str, Any], max_paths: int = 20) -> List[PathInfo]:
    """Find paths in the DAG and sort them by target date.
    
    Args:
        G (nx.DiGraph): NetworkX directed graph
        temporal_data (Dict[str, Any]): Dictionary mapping node IDs to temporal information
        max_paths (int): Maximum number of paths to return
        
    Returns:
        List[PathInfo]: List of paths sorted by target date (latest first)
    """
    logger.info(f"Finding paths with dates (max {max_paths} paths)")
    all_paths = []
    
    # Find all paths between all pairs of nodes
    for source in G.nodes():
        for target in G.nodes():
            if source != target:
                try:
                    paths = list(nx.all_simple_paths(G, source, target))
                    if len(paths) > 1:
                        logger.info(f"Found {len(paths)} paths between {source} and {target}")
                    all_paths.extend(paths)
                except nx.NetworkXNoPath:
                    continue
    
    if not all_paths:
        logger.info("No paths found in the DAG")
        return []
    
    # Convert paths to PathInfo objects with temporal data
    path_infos = []
    for path in all_paths:
        # Get the latest target date in the path
        target_dates = []
        start_dates = []
        closed_dates = []
        
        for node in path:
            if node in temporal_data:
                if temporal_data[node].target_date:
                    target_dates.append(temporal_data[node].target_date)
                if temporal_data[node].start_date:
                    start_dates.append(temporal_data[node].start_date)
                if temporal_data[node].closed_date:
                    closed_dates.append(temporal_data[node].closed_date)
                # Calculate in and out degrees for nodes in path
                temporal_data[node].in_degree = G.in_degree(node)
                temporal_data[node].out_degree = G.out_degree(node)
        
        # Use the latest dates if available
        latest_target = max(target_dates) if target_dates else None
        earliest_start = min(start_dates) if start_dates else None
        latest_closed = max(closed_dates) if closed_dates else None
        
        path_infos.append(PathInfo(
            nodes=path,
            target_date=latest_target,
            start_date=earliest_start,
            closed_date=latest_closed
        ))
    
    # Sort paths by target date (latest first) and take top max_paths
    sorted_paths = sorted(
        path_infos,
        key=lambda x: (x.target_date or datetime.min),
        reverse=True
    )
    
    logger.info(f"Found {len(sorted_paths)} paths with target dates")
    logger.info(f"Returning {max_paths} paths")

    return sorted_paths[:max_paths]

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