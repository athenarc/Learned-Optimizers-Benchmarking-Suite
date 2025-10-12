import nbformat
from nbconvert import PythonExporter

# Load the notebook
with open("/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/experiments/experiment5/experiment5.ipynb") as f:
    notebook = nbformat.read(f, as_version=4)

# Convert to Python script
exporter = PythonExporter()
(script, _) = exporter.from_notebook_node(notebook)

# Save the script
with open("/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/experiments/experiment5/experiment5.py", "w") as f:
    f.write(script)
