import matplotlib.pyplot as plt
import base64
from io import BytesIO
import plotly.graph_objects as go


def get_graph():
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    image_png = buffer.getvalue()
    graph = base64.b64encode(image_png)
    graph = graph.decode('utf-8')
    buffer.close()
    return graph


def get_plot(x, y, title, xlabel, ylabel, scatter=None, x_sca=None, y_sca=None):
    plt.switch_backend('AGG')
    plt.figure(figsize=(10, 5))
    plt.title(title)
    plt.plot(x, y)
    plt.xticks(rotation=45)
    if scatter is not None:
        plt.scatter(x_sca, y_sca, color="red")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    graph = get_graph()
    return graph

