import matplotlib
matplotlib.use('Agg')  # Use non-GUI backend for generating images

import matplotlib.pyplot as plt
import io
import base64

def create_graph():
    # Example: simple plot
    plt.figure()  # Start a new figure
    plt.plot([1, 2, 3, 4], [1, 4, 9, 16])
    plt.xlabel('X Axis')
    plt.ylabel('Y Axis')

    # Save the plot to a BytesIO object and encode it in base64
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)  # Move the cursor to the start of the stream
    graph_url = base64.b64encode(img.getvalue()).decode()

    # Close the plot to avoid memory leaks
    plt.close()

    # Return the image as a base64 string
    return f"data:image/png;base64,{graph_url}"
