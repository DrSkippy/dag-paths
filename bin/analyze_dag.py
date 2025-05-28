#!/usr/bin/env -S poetry run python

import logging
from dags import (
    setup_logging,
    read_dag_data,
    get_data_file_path,
    create_dag,
    find_longest_paths,
    analyze_network,
)

def print_network_metrics(metrics):
    """Print network metrics in a readable format."""
    print("\nNetwork Analysis Results:")
    print("=" * 50)
    print(f"Total Nodes: {metrics['total_nodes']}")
    print(f"Total Edges: {metrics['total_edges']}")
    print(f"Is DAG: {metrics['is_dag']}")
    
    print("\nNode Types:")
    print("-" * 20)
    for node_type, count in sorted(metrics['node_types'].items()):
        print(f"{node_type}: {count}")
    
    print("\nNode States:")
    print("-" * 20)
    for state, count in sorted(metrics['node_states'].items()):
        print(f"{state}: {count}")
    
    print("\nTop 5 Most Central Nodes (In-Degree):")
    print("-" * 30)
    sorted_in_centrality = sorted(
        metrics['in_degree_centrality'].items(),
        key=lambda x: x[1],
        reverse=True
    )[:5]
    for node, centrality in sorted_in_centrality:
        print(f"Node {node}: {centrality:.3f}")
    
    print("\nTop 5 Most Central Nodes (Out-Degree):")
    print("-" * 30)
    sorted_out_centrality = sorted(
        metrics['out_degree_centrality'].items(),
        key=lambda x: x[1],
        reverse=True
    )[:5]
    for node, centrality in sorted_out_centrality:
        print(f"Node {node}: {centrality:.3f}")

def main():
    # Set up logging
    logger = setup_logging()
    logger.info("Starting DAG analysis")
    
    # Get the data file path
    data_file = get_data_file_path()
    
    if not data_file.exists():
        logger.error(f"Data file not found: {data_file}")
        return
    
    try:
        # Read and process the data
        data = read_dag_data(data_file)
        G = create_dag(data)
        
        # Perform network analysis
        metrics = analyze_network(G)
        print_network_metrics(metrics)
        
        # Find longest paths
        longest_path_by_edges, longest_path_by_nodes = find_longest_paths(G)
        
        if longest_path_by_edges:
            print("\nLongest Path Analysis:")
            print("=" * 50)
            print(f"Longest path: {' -> '.join(longest_path_by_edges)}")
            print(f"Number of edges in longest path: {len(longest_path_by_edges) - 1}")
            print(f"Number of nodes in longest path: {len(longest_path_by_nodes)}")
            
            # Log the same information
            logger.info("Longest Path Analysis:")
            logger.info(f"Longest path: {' -> '.join(longest_path_by_edges)}")
            logger.info(f"Number of edges in longest path: {len(longest_path_by_edges) - 1}")
            logger.info(f"Number of nodes in longest path: {len(longest_path_by_nodes)}")
        else:
            print("\nNo paths found in the DAG")
            logger.info("No paths found in the DAG")
            
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise

if __name__ == "__main__":
    main() 