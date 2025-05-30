#!/usr/bin/env -S poetry run python
import sys
import os
import logging
from pathlib import Path
from datetime import datetime

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from dags import (
    setup_logging,
    read_dag_data,
    get_data_file_path,
    create_dag,
    find_paths_with_dates,
    find_sorted_paths,
    analyze_path_timing,
    analyze_network,
    plot_dag
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

def print_path_info(path_info, temporal_data):
    """Print detailed information about a path."""
    print("\nPath Details:")
    print(f"Number of nodes: {len(path_info.nodes)}")
    print(f"Latest target date: {path_info.target_date}")
    print(f"Earliest start date: {path_info.start_date}")
    print(f"Latest closed date: {path_info.closed_date}")
    
    print("\nNode Details:")
    for node in path_info.nodes:
        print(f"\nNode: {node}")
        if node in temporal_data:
            print(f"  # Predecessors: {temporal_data[node].in_degree} # Successors: {temporal_data[node].out_degree}")
            print(f"  Start date: {temporal_data[node].start_date}", 
                  f" Target date: {temporal_data[node].target_date}")
            print(f"  Closed date: {temporal_data[node].closed_date}")

def print_timing_inconsistencies(sorted_paths, temporal_data):

    # Analyze timing inconsistencies
    timing_issues = analyze_path_timing(sorted_paths, temporal_data)
    
    # Print timing issues in a structured format
    print("\nTiming Analysis Results:")
    print("=" * 50)
    for node in timing_issues:
        print(f"Issues for Node: {node}")
        print("-" * 40)
        found_issues = False
        for issue_type, issues in timing_issues[node].items():
            if issues:
                found_issues = True
                print(f"{issue_type.replace('_', ' ').title()}:")
                logging.info(f"{issue_type} = {issues}")
                if issue_type in ['missing_start_dates', 'missing_target_dates', 'target_passed_without_close']:
                    print(f"  Target Date: {issues['target_date']}")
                    print(f"  Start Date: {issues['start_date']}")
                    print(f"  Closed Date: {issues['closed_date']}")
                elif issue_type in ['end_before_predecessor_end', 'start_before_predecessor_end']:
                    for issue in issues:
                        print(f"  Path: {' -> '.join(issue['path'])}")
                        print(f"    Predecessor: {issue['predecessor']}")
                        print(f"    Node Date: {issue.get('node_date') or issue.get('start_date')}")
                        print(f"    Predecessor Date: {issue['predecessor_date']}")
        if not found_issues:
            print("(No Issues)")
        print()
    
    if not any(timing_issues.values()):
        print("No timing issues found!")
    

def main():
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Get data file path
    data_file = get_data_file_path()
    if not data_file.exists():
        logger.error(f"Data file not found: {data_file}")
        sys.exit(1)
    
    # Read DAG data
    logger.info(f"Reading DAG data from {data_file}")
    data, temporal_data = read_dag_data(data_file)
    
    # Create DAG
    G = create_dag(data)
    
    # Perform network analysis
    metrics = analyze_network(G)
    
    # Print network metrics
    print("\nNetwork Analysis:")
    print(f"Total nodes: {metrics['total_nodes']}")
    print(f"Total edges: {metrics['total_edges']}")
    print(f"Is DAG: {metrics['is_dag']}")
    print("\nNode Types:")
    for node_type, count in metrics['node_types'].items():
        print(f"  {node_type}: {count}")
    print("\nNode States:")
    for state, count in metrics['node_states'].items():
        print(f"  {state}: {count}")
    
    # Find paths with latest target dates
    paths = find_paths_with_dates(G, temporal_data)
    
    max_paths = 20
    selected_sorted_paths = find_sorted_paths(paths, max_paths=max_paths)
    
    # Print path information
    print(f"\nTop {max_paths} Paths by Target Date:")
    for i, path_info in enumerate(selected_sorted_paths, 1):
        print(f"\nPath {i}:")
        print(f"Path: {' -> '.join(path_info.nodes)}")
        print_path_info(path_info, temporal_data)
    
    # Call analyze_path_timing with the sorted paths and temporal data
    print_timing_inconsistencies(paths, temporal_data)
    
    # Create output directory for visualizations
    output_dir = project_root / 'output'
    output_dir.mkdir(exist_ok=True)
    
    # Create visualization of the full DAG
    full_dag_path = output_dir / 'full_dag.png'
    plot_dag(G, full_dag_path)
    print(f"\nFull DAG visualization saved to: {full_dag_path}")
    
    # Create visualization highlighting the top paths
    if paths:
        highlighted_dag_path = output_dir / 'highlighted_dag.png'
        plot_dag(G, highlighted_dag_path, highlight_paths=[p.nodes for p in paths])
        print(f"Highlighted DAG visualization saved to: {highlighted_dag_path}")

   

if __name__ == '__main__':
    main() 