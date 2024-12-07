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

# Run ls -la command on node-0
agent.run("node-0", "ls -la")

```
