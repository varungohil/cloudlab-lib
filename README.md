# cloudlab-lib

A Python library for managing and automating experiments on CloudLab nodes.

## Installation

```bash
# Clone repository
git clone https://github.com/varungohil/cloudlab_lib.git

# Install dependencies
pip install -r requirements.txt
```

## Usage

```python
import cloudlab_lib

# Initialize agent with server configuration
agent = cloudlab_lib.CloudLabAgent('server-config.json')

# Run command on a single node
# Returns tuple: (stdout_lines, stderr_lines, exit_status)
result = agent.run("node-0", "ls -la")
stdout, stderr, status = result

# Run command on all nodes concurrently
# Returns dict: {node_name: (stdout_lines, stderr_lines, exit_status)}
results = agent.run("all", "ls -la")
for node, (stdout, stderr, status) in results.items():
    print(f"Node {node} status: {status}")

# Run command on specific nodes concurrently
# Returns dict: {node_name: (stdout_lines, stderr_lines, exit_status)}
node_list = ["node-0", "node-1"]
results = agent.run(node_list, "ls -la")
for node, (stdout, stderr, status) in results.items():
    print(f"Node {node} status: {status}")
```
