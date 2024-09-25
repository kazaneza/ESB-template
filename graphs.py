# graphs.py

def create_all_graphs():
    from graph.graph1 import create_graph_failure
    from graph.graph2 import create_graph_success
    from graph.graph3 import create_graph_pending

    return {
        'graph_failure': create_graph_failure(),
        'graph_success': create_graph_success(),
        'graph_pending': create_graph_pending()
    }
