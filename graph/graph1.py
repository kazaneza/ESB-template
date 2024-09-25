import matplotlib
matplotlib.use('Agg') 

import matplotlib.pyplot as plt
import io
import base64

def create_graph_failure():
    fig, ax = plt.subplots()

    ax.plot([1, 2, 3, 4], [1, 4, 9, 16], color='white', linewidth=2.5)

    ax.set_xlabel('X Axis', color='white')
    ax.set_ylabel('Y Axis', color='white')
   
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
 
    ax.spines['top'].set_color('none')
    ax.spines['right'].set_color('none')
 
    # Corrected line without the extra quote
    ax.grid(color='white', linestyle='--', linewidth=0.5)

    img = io.BytesIO()
    fig.savefig(img, format='png', bbox_inches='tight', transparent=True)  
    img.seek(0) 
    graph_url_failure = base64.b64encode(img.getvalue()).decode('utf-8')
  
    plt.close(fig)

    return f"data:image/png;base64,{graph_url_failure}"
