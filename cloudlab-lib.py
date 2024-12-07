import json
import threading
from paramiko import SSHClient, Ed25519Key, RSAKey, AutoAddPolicy
#
class CloudLabAgent:
    def __init__(self, server_configs_json, with_ml_libs=False):
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

    def run(self, node, cmd, exit_on_err = False):
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

    def scp(self, node, local_path, remote_path, exit_on_err = False):
        ftp_client = self.ssh_clients_[node].open_sftp()
        ftp_client.put(local_path, remote_path)
        ftp_client.close()

    def scpget(self, node, local_path, remote_path, exit_on_err = False):
        ftp_client = self.ssh_clients_[node].open_sftp()
        ftp_client.get(remote_path, local_path)
        ftp_client.close()

    def reboot(self, node):
        cmd =  '''
        sudo reboot
        '''
        return self.run(node, cmd)
            
    def install_deps(self, node):
        cmd = '''
        sudo apt-get update
        sudo apt-get install -y htop powercap-utils python3 python3-pip linux-tools-$(uname -r) linux-cloud-tools-$(uname -r) git libssl-dev libz-dev luarocks tcpdump
        pip3 install aiohttp asyncio pandas numpy scikit-learn matplotlib psutil
        sudo luarocks install luasocket
        '''
        if node == "all":
            threads = {}
            for node in self.nodes_:
                thread = threading.Thread(target=self.run, args=(node, cmd, True))
                threads[node] = thread
                thread.start()
                
            for node in self.nodes_:
                threads[node].join()
        else:
            return self.run(node, cmd)


    def install_docker(self, node):
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
            if node == "all":
                threads = {}
                for node in self.nodes_:
                    thread = threading.Thread(target=self.run, args=(node, cmd, True))
                    threads[node] = thread
                    thread.start()
                    
                for node in self.nodes_:
                    threads[node].join()
            else:
                return self.run(node, cmd, exit_on_err = True)


    def initialize_docker_swarm(self):
        cmd = "sudo docker swarm init --advertise-addr `hostname -i`"
        stdout, stderr , exit_status = self.run(self.master_node_, cmd, exit_on_err=True) 
        worker_join_token = stdout[4].strip()
        print(f"Join token is '{worker_join_token}' ")
        self.worker_join_token_ = worker_join_token
        return stdout, stderr, exit_status


    def join_workers_to_swarm(self, nodes):
        stdouts = {}
        stderrs = {}
        exit_statuses = {}
        for node in nodes:
            print(f"Trying to join node {node} as worker to swarm")
            stdout, stderr, exit_status = self.run(node, self.worker_join_token_)
            stdouts[node] = stdout
            stderrs[node] = stderr
            exit_statuses[node] = exit_status
        return stdouts, stderrs, exit_statuses

    def leave_swarm(self, node):
        cmd = '''
        sudo docker swarm leave -f
        '''
        return self.run(node, cmd)
    
    def create_docker_swarm(self):
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
        stdouts = {}
        stderrs = {}
        exit_statuses = {}
        for node in self.nodes_:
            stdout, stderr, exit_status = self.leave_swarm(node)
            stdouts[node] = stdout
            stderrs[node] = stderr
            exit_statuses[node] = exit_status
        return stdouts, stderrs, exit_statuses

    def set_power_governor(self, node, governor):
        cmd = f"sudo cpupower frequency-set -g {governor}"
        return self.run(node, cmd)

    def set_frequency(self, node, cpus, frequency):
        cmd = f"sudo cpupower -c {cpus} frequency-set -f {frequency}"
        return self.run(node ,cmd)