import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import numpy as np

# Data for the chart
labels = ['January', 'February', 'March', 'April', 'May', 'June', 'July']
data = [12, 19, 3, 5, 2, 3, 7]

# Create a figure and a single subplot
fig, ax = plt.subplots()

# Function to draw rounded bars
def draw_rounded_bar(ax, left, height, width=0.8, corner_radius=0.1, **kwargs):
    """
    Custom function to draw a bar with rounded corners using FancyBboxPatch.
    """
    # Define the rectangle with rounded corners (boxstyle="round")
    bbox = FancyBboxPatch(
        (left, 0), width, height,
        boxstyle=f"round,pad=0.02,rounding_size={corner_radius}",
        edgecolor='black', facecolor=kwargs.get('color', 'blue'),
        linewidth=kwargs.get('linewidth', 1)
    )
    ax.add_patch(bbox)

# Draw each bar with rounded corners
for i, (label, value) in enumerate(zip(labels, data)):
    draw_rounded_bar(ax, i, value, width=0.6, corner_radius=0.15, color='green')

# Set labels and titles
ax.set_xticks(np.arange(len(labels)))
ax.set_xticklabels(labels)
ax.set_ylim(0, max(data) + 5)
ax.set_ylabel('Transactions')
ax.set_title('Total Successful Transactions')

# Display the chart
plt.show()
