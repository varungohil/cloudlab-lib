import json
import threading
from threading import Thread
from paramiko import SSHClient, Ed25519Key, RSAKey, AutoAddPolicy

class ThreadWithRetval(threading.Thread):
    def __init__(self, target, args=()):
        super().__init__(target=target, args=args)
        self.result = None

    def run(self):
        self.result = self.target(*self.args)

    def join(self, *args):
        super().join(*args)
        return self.result


class CloudLabAgent:
    """A class to manage and run experiments on CloudLab nodes."""
    
    def __init__(self, server_configs_json, with_ml_libs=False):
        """
        Initialize CloudLabAgent with server configurations.
        
        Args:
            server_configs_json (str): Path to JSON file containing server configurations

        """
        # Parse server configuration.
        with open(server_configs_json, 'r') as f:
            json_data = json.load(f)
        self.account_username_ = json_data['account']['username']
        self.account_ssh_key_filename_ = json_data['account']['ssh_key_filename']
        self.ssh_port_ = json_data['account']['port']
        self.nodes_ = json_data['nodes']
        self.num_nodes = len(self.nodes_)
        self.ssh_suffix_ = json_data["ssh_suffix"]
        self.password_ = json_data["account"]["password"]
        self.master_node_ = json_data["master_node"]
        self.worker_join_token_ = ""

        self.ssh_clients_ = {}
        self.unconnected_nodes_ = []

        if "ed25519" in self.account_ssh_key_filename_:
            key = Ed25519Key.from_private_key_file(self.account_ssh_key_filename_, password=self.password_)
        elif "id_rsa" in self.account_ssh_key_filename_:
            key = RSAKey.from_private_key_file(self.account_ssh_key_filename_, password=self.password_)
        else:
            print("Error: Unknown key type.")
            assert False

        for node in self.nodes_:
            print(f"Connecting to Node {node}")
            ssh_client = SSHClient()
            ssh_client.set_missing_host_key_policy(AutoAddPolicy())
            try:
                ssh_client.connect(f"{node}" + self.ssh_suffix_, self.ssh_port_, self.account_username_, pkey=key)
            except:
                print(f"Could not connect to Node {node}")
                self.unconnected_nodes_.append(node)
            self.ssh_clients_[node] = ssh_client

    def run_on_node(self, node, cmd, exit_on_err = False):
        """
        Execute a command on a specified node via SSH.
        
        Args:
            node (str): Node identifier to run command on
            cmd (str): Command to execute
            exit_on_err (bool): Whether to exit program if command fails
            
        Returns:
            tuple: (stdout_lines, stderr_lines, exit_status)
        """
        _, stdout, stderr = self.ssh_clients_[node].exec_command(cmd)
        stdout_lines = stdout.readlines()
        stderr_lines = stderr.readlines()
        exit_status = stdout.channel.recv_exit_status()
        if exit_status:
            print(f"STDOUT : {node} {cmd} = ")
            print(' '.join(stdout_lines))
            print(f"STDERR : {node} {cmd} = ")
            print(' '.join(stderr_lines))
            if exit_on_err:
                exit(1)
        return stdout_lines, stderr_lines, exit_status
    
    def concurrent_run(self, nodes, cmd, exit_on_err = False):
        """
        Execute a command concurrently across multiple nodes using threads.
        
        Args:
            nodes (list): List of node identifiers to run command on
            cmd (str): Command to execute on each node
            exit_on_err (bool): Whether to exit program if command fails
            
        Returns:
            dict: Dictionary mapping node identifiers to their command execution results
                  Each result is a tuple of (stdout_lines, stderr_lines, exit_status)
        """
        threads = {}
        results = {}
        for node in nodes:
            thread = ThreadWithRetval(target=self.run_on_node, args=(node, cmd, exit_on_err))
            threads[node] = thread
            thread.start()

        for node in nodes:
            results[node] = threads[node].join()

        return results
    
    def run(self, nodes, cmd, exit_on_err = False):
        """
        Execute a command on one or multiple nodes.
        
        Args:
            nodes (str|list): Target node(s) - can be "all", a list of nodes, or a single node
            cmd (str): Command to execute
            exit_on_err (bool): Whether to exit program if command fails
            
        Returns:
            dict|tuple: Results from command execution
        """
        if nodes == "all":
            return self.concurrent_run(self.nodes_, cmd, exit_on_err)
        elif isinstance(nodes, list) and len(nodes) > 0 :
            return self.concurrent_run(nodes, cmd, exit_on_err)
        else:
            return self.run_on_node(nodes, cmd, exit_on_err)

    def scp(self, node, local_path, remote_path, exit_on_err = False):
        """
        Copy a file from local machine to remote node.
        
        Args:
            node (str): Target node identifier
            local_path (str): Path to source file on local machine
            remote_path (str): Destination path on remote node
            exit_on_err (bool): Whether to exit program if transfer fails
        """
        ftp_client = self.ssh_clients_[node].open_sftp()
        ftp_client.put(local_path, remote_path)
        ftp_client.close()

    def scpget(self, node, local_path, remote_path, exit_on_err = False):
        """
        Copy a file from remote node to local machine.
        
        Args:
            node (str): Source node identifier
            local_path (str): Destination path on local machine
            remote_path (str): Path to source file on remote node
            exit_on_err (bool): Whether to exit program if transfer fails
        """
        ftp_client = self.ssh_clients_[node].open_sftp()
        ftp_client.get(remote_path, local_path)
        ftp_client.close()

    def reboot(self, nodes):
        """
        Reboot the specified node.
        
        Args:
            node (str): Node identifier to reboot
            
        Returns:
            tuple: Result of run() command (stdout_lines, stderr_lines, exit_status)
        """
        cmd =  '''
        sudo reboot
        '''
        return self.run(nodes, cmd)
            
    def install_deps(self, nodes):
        """
        Install system dependencies and Python packages on specified node(s).
        
        Args:
            node (str): Node identifier or 'all' to install on all nodes
            
        Returns:
            tuple: Result of run() command if single node, None if 'all'
        """
        cmd = '''
        sudo apt-get update
        sudo apt-get install -y htop powercap-utils python3 python3-pip linux-tools-$(uname -r) linux-cloud-tools-$(uname -r) git libssl-dev libz-dev luarocks tcpdump
        pip3 install aiohttp asyncio pandas numpy scikit-learn matplotlib psutil
        sudo luarocks install luasocket
        yes | sudo apt install python3-locust
        pip install locust-plugins
        pip install locust-swarm

        '''
        return self.run(nodes, cmd)


    def install_docker(self, nodes):
        """
        Install Docker and related packages on specified node(s).
        
        Args:
            node (str): Node identifier or 'all' to install on all nodes
            
        Returns:
            tuple: Result of run() command if single node, None if 'all'
        """
        cmd = '''
        # Add Docker's official GPG key:
        sudo apt-get update
        sudo apt-get install ca-certificates curl -y
        sudo install -m 0755 -d /etc/apt/keyrings
        sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
        sudo chmod a+r /etc/apt/keyrings/docker.asc

        # Add the repository to Apt sources:
        echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
        $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
        sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
        sudo apt-get update


        sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y

        sudo chmod 666 /var/run/docker.sock
        '''
        return self.run(nodes, cmd, exit_on_err = True)


    def initialize_docker_swarm(self):
        """
        Initialize a Docker swarm on the master node.
        
        Returns:
            tuple: (stdout_lines, stderr_lines, exit_status) from swarm initialization
        """
        cmd = "sudo docker swarm init --advertise-addr `hostname -i`"
        stdout, stderr , exit_status = self.run_on_node(self.master_node_, cmd, exit_on_err=True) 
        worker_join_token = stdout[4].strip()
        print(f"Join token is '{worker_join_token}' ")
        self.worker_join_token_ = worker_join_token
        return stdout, stderr, exit_status


    def join_workers_to_swarm(self, nodes):
        """
        Join specified nodes to the Docker swarm as workers.
        
        Args:
            nodes (list): List of node identifiers to join as workers
            
        Returns:
            tuple: Dictionaries of (stdouts, stderrs, exit_statuses) keyed by node
        """
        stdouts = {}
        stderrs = {}
        exit_statuses = {}
        for node in nodes:
            print(f"Trying to join node {node} as worker to swarm")
            stdout, stderr, exit_status = self.run_on_node(node, self.worker_join_token_)
            stdouts[node] = stdout
            stderrs[node] = stderr
            exit_statuses[node] = exit_status
        return stdouts, stderrs, exit_statuses

    def leave_swarm(self, node):
        """
        Remove specified node from Docker swarm.
        
        Args:
            node (str): Node identifier to remove from swarm
            
        Returns:
            tuple: Result of run() command
        """
        cmd = '''
        sudo docker swarm leave -f
        '''
        return self.run_on_node(node, cmd)
    
    def create_docker_swarm(self):
        """
        Create a Docker swarm with current master node and join all workers.
        
        Returns:
            tuple: Dictionaries containing results of initialization and join operations
        """
        stdouts = {}
        stderrs = {}
        exit_statuses = {}
        stdout_init, stderr_init, exit_status_init = self.initialize_docker_swarm()
        stdout_join, stderr_join, exit_status_join = self.join_workers_to_swarm(self.nodes_)
        stdouts["init"] = stdout_init
        stderrs["init"] = stderr_init
        exit_statuses["init"] = exit_status_init
        stdouts["join"] = stdout_join
        stderrs["join"] = stderr_join
        exit_statuses["join"] = exit_status_join
        return stdouts, stderrs, exit_statuses
    
    def destroy_docker_swarm(self):
        """
        Remove all nodes from the Docker swarm.
        
        Returns:
            tuple: Dictionaries of (stdouts, stderrs, exit_statuses) keyed by node
        """
        stdouts = {}
        stderrs = {}
        exit_statuses = {}
        for node in self.nodes_:
            if node != self.master_node_:   
                stdout, stderr, exit_status = self.leave_swarm(node)
                stdouts[node] = stdout
                stderrs[node] = stderr
                exit_statuses[node] = exit_status
        stdout, stderr, exit_status = self.leave_swarm(self.master_node_)
        stdouts[self.master_node_] = stdout
        stderrs[self.master_node_] = stderr
        exit_statuses[self.master_node_] = exit_status
        return stdouts, stderrs, exit_statuses

    def set_power_governor(self, nodes, governor):
        """
        Set CPU power governor on specified node.
        
        Args:
            node (str): Node identifier
            governor (str): Power governor setting to apply
            
        Returns:
            tuple: Result of run() command
        """
        cmd = f"sudo cpupower frequency-set -g {governor}"
        return self.run(nodes, cmd)

    def set_frequency(self, nodes, cpus, frequency):
        """
        Set CPU frequency for specified cores on a node.
        
        Args:
            node (str): Node identifier
            cpus (str): CPU cores to configure (e.g., "0-3" or "0,1,2,3")
            frequency (str): Frequency to set (e.g., "2.4GHz")
            
        Returns:
            tuple: Result of run() command
        """
        cmd = f"sudo cpupower -c {cpus} frequency-set -f {frequency}"
        return self.run(nodes ,cmd)

    def setup_deathstarbench(self, nodes, user, location="~", branch="main", commit=""):
        """
        Sets up DeathStarBench benchmark suite on specified node.
        
        Args:
            node (str): Node identifier to install on
            user (str): GitHub username/org containing DeathStarBench fork
            location (str): Directory to clone into (default: "~")
            branch (str): Git branch to checkout (default: "main") 
            commit (str): Optional specific commit to checkout
            
        Returns:
            tuple: Result of run() command (stdout, stderr, exit_status)
        """
        if commit == "":
            cmd = f'''
                cd {location}
                git clone https://github.com/{user}/DeathStarBench.git --recurse-submodules
                cd DeathStarBench
                git checkout {branch}
                echo export DSB_ROOT=`pwd` >> ~/.bashrc
                source ~/.bashrc
                pip3 install asyncio aiohttp
                sudo apt install -y libssl-dev libz-dev luarocks
                sudo luarocks install luasocket
                cd wrk2
                make
            '''
        else:
            cmd = f'''
                cd {location}
                git clone https://github.com/{user}/DeathStarBench.git --recurse-submodules
                cd DeathStarBench
                git checkout {branch}
                git checkout {commit}
                echo export DSB_ROOT=`pwd` >> ~/.bashrc
                source ~/.bashrc
                pip3 install asyncio aiohttp
                sudo apt install -y libssl-dev libz-dev luarocks
                sudo luarocks install luasocket
                cd wrk2
                make
            ''' 
        return self.run(nodes, cmd)
    
    def run_wrk(self, node, wrk_params, wrk_path="default"):
        """
        Run wrk2 HTTP benchmarking tool on specified node.
        
        Args:
            node (str): Node identifier to run wrk on
            wrk_params (dict): Parameters for wrk2 benchmark run
            wrk_path (str): Path to wrk2 executable (default: DeathStarBench/wrk2)
            
        Example wrk_params:
            {
                "dist": "exp",           # Request inter-arrival timedistribution (exp/const/normal)
                "threads": "4",          # Number of threads
                "connections": "100",    # Number of connections
                "duration": "30s",       # Duration of test
                "rate": "1000",         # Target request rate
                "timeout": "5s",        # Request timeout
                "script": "script.lua",  # Lua script path
                "url": "http://localhost:8080", # Target URL
                "extra_params": ""       # Additional parameters
            }
            
        Returns:
            tuple: Result of run() command (stdout, stderr, exit_status)
        """
        if wrk_path == "default":
            wrk_path = f"/home/{self.account_username_}/DeathStarBench/wrk2"
        cmd = f"{wrk_path}/wrk -D {wrk_params['dist']} -t {wrk_params['threads']} -c {wrk_params['connections']} -d{wrk_params['duration']} -R{wrk_params['rate']} -T{wrk_params['timeout']} -s {wrk_path}/{wrk_params['script']} {wrk_params['url']} {wrk_params['extra_params']}" 
        print(cmd)
        return self.run_on_node(node, cmd)
    
    def run_locust(self, node, locust_params):
        """
        Run Locust load testing tool on specified node.
        
        Args:
            node (str): Node identifier to run Locust on
            locust_params (dict): Parameters for Locust test run
            
        Example locust_params:
            {
                "script": "locustfile.py",     # Locust test script path
                "url": "http://localhost:8080", # Target URL/host
                "tags": "tag1,tag2",           # Test tags to run
                "processes": "4",              # Number of worker processes
                "wait_distrib": "constant(1)",  # Wait time distribution
                "throughput_per_user": "10",   # Target RPS per user
                "max_users": "100",           # Max number of users
                "user_spawn_rate": "10",      # Users to spawn per second
                "duration": "5m",              # Test duration
                "output_csv" : "random",       # Name of output csv file (.csv is automatically added)
                "extra_params": ""       # Additional parameters
            }
            
        Returns:
            tuple: Result of run() command (stdout, stderr, exit_status)
        """
        cmd = f"locust --headless -f {locust_params['script']} -H {locust_params['url']} --tag {locust_params['tags']} --processes {locust_params['processes']} -w {locust_params['wait_distrib']} -tu {locust_params['throughput_per_user']} -u {locust_params['max_users']} -r {locust_params['user_spawn_rate']} -t{locust_params['duration']} --csv {locust_params['output_csv']} {locust_params['extra_params']}" 
        print(cmd)
        return self.run_on_node(node, cmd)
    