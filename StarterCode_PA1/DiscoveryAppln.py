import sys
import time
import argparse
import logging
import configparser
import threading
import subprocess
import socket
from kazoo.recipe.lock import Lock
from kazoo.client import KazooClient  # ZooKeeper client
from kazoo.exceptions import NodeExistsError
from CS6381_MW.DiscoveryMW import DiscoveryMW
from CS6381_MW import discovery_pb2

# Helper function for getting new port for quorum spawning
def get_free_port():
    """Bind a temporary socket to port 0 and return the OS-assigned free port."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))  # Let OS choose a free port
    port = s.getsockname()[1]
    s.close()
    return port

##################################
#       Discovery Application Class
##################################
class DiscoveryAppln:
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None          # Middleware handle
        self.zk = None              # ZooKeeper client
        self.registry = {"publishers": {}, "subscribers": {}, "brokers": {}}
        self.is_primary = False     # True if this instance becomes leader
        self.leader_path = "/discovery/leader"
        # Replica-related path for quorum
        self.replicas_path = "/discovery/replicas"
        # Lease settings (in seconds)
        self.lease_duration = 30          # How long each lease is valid
        self.lease_renew_interval = 10    # How frequently to renew the lease
        self.max_leader_duration = 120    # Maximum time this instance can remain primary
        self.leader_start_time = None     # Time when this instance became leader
        self.lease_thread = None          # Thread for lease renewal
        self.args = None                # Will store command-line args
        self.bootstrap_complete = False # Flag to indicate bootstrapping is complete

    ########################################
    # Quorum Check Helper
    ########################################
    def quorum_met(self):
        """Return True if at least 3 Discovery replicas are registered."""
        try:
            replicas = self.zk.get_children(self.replicas_path)
            self.logger.info(f"Quorum check: {len(replicas)} replicas present.")
            return len(replicas) >= 3
        except Exception as e:
            self.logger.error(f"Error checking quorum: {str(e)}")
            return False

    ########################################
    # Spawn a New Replica
    ########################################
    def spawn_replica(self):
        """Attempt to spawn a new Discovery replica using a global lock to ensure only one spawns."""
        self.logger.info("Attempting to spawn a new Discovery replica to restore quorum.")
        
        # Use a global lock in ZooKeeper to coordinate spawns.
        spawn_lock = Lock(self.zk, "/discovery/spawn_lock")
        try:
            # Acquire the lock with a timeout (say, 5 seconds)
            if spawn_lock.acquire(timeout=5):
                # Once the lock is acquired, re-check the replica count.
                replicas = self.zk.get_children(self.replicas_path)
                if len(replicas) >= 3:
                    self.logger.info("Quorum restored while waiting for lock; no need to spawn.")
                    spawn_lock.release()
                    return
                
                free_port = get_free_port()
                self.logger.info("Spawn lock acquired; proceeding to spawn new replica.")
                # Construct the command using stored args
                cmd = [
                    "gnome-terminal",
                    "--", "bash", "-c",
                    f"python3 {sys.argv[0]} -p {free_port} -a {self.args.addr} -z {self.args.zookeeper} -c {self.args.config} -l {self.args.loglevel}; exec bash"
                ]
                try:
                    subprocess.Popen(cmd)
                    self.logger.info(f"Spawned new Discovery replica with command: {' '.join(cmd)}")
                except Exception as e:
                    self.logger.error(f"Failed to spawn a new replica: {str(e)}")
                finally:
                    spawn_lock.release()
            else:
                self.logger.info("Could not acquire spawn lock; another replica may be spawning.")
        except Exception as e:
            self.logger.error(f"Error acquiring spawn lock: {str(e)}")


    ########################################
    # Wait for Bootstrap Completion
    ########################################
    def wait_for_bootstrap(self):
        """Wait until at least 3 replica nodes are registered, then mark bootstrap as complete."""
        self.logger.info("Waiting for bootstrap: expecting at least 3 replicas before enabling auto-spawn...")
        while True:
            try:
                replicas = self.zk.get_children(self.replicas_path)
                self.logger.info(f"Bootstrap check: {len(replicas)} replicas present.")
                if len(replicas) >= 3:
                    self.logger.info("Bootstrap complete: quorum achieved.")
                    break
            except Exception as e:
                self.logger.error(f"Error during bootstrap wait: {str(e)}")
            time.sleep(2)
        self.bootstrap_complete = True

    ########################################
    # Configure Discovery Application
    ########################################
    def configure(self, args):
        """ Configure the Discovery Application with warm-passive replication, lease, and quorum support """
        try:
            self.args = args  # Save args for future use (e.g., spawning replicas)
            self.logger.info("DiscoveryAppln::configure")
            config_obj = configparser.ConfigParser()
            config_obj.read(args.config)

            self.dissemination_strategy = config_obj.get("Dissemination", "Strategy", fallback="Direct")
            self.logger.info(f"Dissemination strategy set to: {self.dissemination_strategy}")

            # Connect to ZooKeeper
            self.logger.info(f"Connecting to ZooKeeper at {args.zookeeper}")
            self.zk = KazooClient(hosts=args.zookeeper)
            self.zk.start(timeout=10)

            # Ensure base paths exist
            self.logger.info("Ensuring /discovery path exists")
            self.zk.ensure_path("/discovery")
            self.logger.info(f"Ensuring {self.replicas_path} path exists")
            self.zk.ensure_path(self.replicas_path)

            # Register this instance as a replica
            discovery_address = f"{args.addr}:{args.port}"
            replica_node = f"{self.replicas_path}/{discovery_address}"
            try:
                self.zk.create(replica_node, discovery_address.encode(), ephemeral=True)
                self.logger.info(f"Registered replica node: {replica_node}")
            except NodeExistsError:
                self.zk.delete(replica_node)
                self.zk.create(replica_node, discovery_address.encode(), ephemeral=True)
                self.logger.info(f"Updated replica node: {replica_node}")

            # Do not spawn new replicas until bootstrapping is complete.
            @self.zk.ChildrenWatch(self.replicas_path)
            def watch_replicas(children):
                num = len(children)
                self.logger.info(f"Replica watch: {num} replicas present.")
                if self.bootstrap_complete and num < 3:
                    self.logger.warning("Quorum not met: fewer than 3 replicas active.")
                    self.spawn_replica()

            # Wait for bootstrap: ensure that at least 3 replicas are active before proceeding.
            self.wait_for_bootstrap()

            # Attempt to become the leader by creating the ephemeral leader node.
            lease_expiry = time.time() + self.lease_duration
            leader_data = f"{discovery_address}|{lease_expiry}"
            try:
                self.zk.create(self.leader_path, leader_data.encode(), ephemeral=True)
                self.is_primary = True
                self.leader_start_time = time.time()  # Record when we became leader
                self.logger.info(f"Instance {discovery_address} became primary with lease expiring at {lease_expiry}.")
                self.start_lease_renewal(discovery_address)
            except NodeExistsError:
                self.is_primary = False
                self.logger.info("A leader already exists. Running as backup.")
                @self.zk.DataWatch(self.leader_path)
                def watch_leader(data, stat, event):
                    if event is not None and event.type == "DELETED":
                        self.logger.info("Leader znode deleted. Attempting to become primary...")
                        time.sleep(1)
                        try:
                            self.zk.ensure_path("/discovery")
                            if not self.zk.exists(self.leader_path):
                                new_expiry = time.time() + self.lease_duration
                                new_data = f"{discovery_address}|{new_expiry}"
                                self.zk.create(self.leader_path, new_data.encode(), ephemeral=True)
                                self.is_primary = True
                                self.leader_start_time = time.time()  # Reset the leader start time
                                self.logger.info(f"This instance has now become the primary with lease expiring at {new_expiry}!")
                                self.start_lease_renewal(discovery_address)
                            else:
                                self.logger.info("Leader node already recreated by another instance.")
                                self.is_primary = False
                        except NodeExistsError:
                            self.logger.info("Another instance became primary while we were trying.")
                            self.is_primary = False
                        except Exception as e:
                            self.logger.error(f"Error during leadership transition: {str(e)}")
                            self.is_primary = False

            # Log current leader information for debugging
            if self.zk.exists(self.leader_path):
                data, _ = self.zk.get(self.leader_path)
                self.logger.info(f"Current leader in ZooKeeper: {data.decode()}")
            else:
                self.logger.error("Leader node does not exist after attempted creation.")

            # Initialize the middleware
            self.logger.debug("DiscoveryAppln::configure - initializing middleware")
            self.mw_obj = DiscoveryMW(self.logger, self.zk)
            self.mw_obj.configure(args)
            self.mw_obj.set_upcall_handle(self)

            self.logger.info("DiscoveryAppln::configure - configuration complete")
            self.dump_zk_nodes()

        except Exception as e:
            self.logger.error(f"DiscoveryAppln::configure - Exception: {str(e)}")
            raise e

    ########################################
    # Lease Renewal Thread
    ########################################
    def start_lease_renewal(self, discovery_address):
        """Start a background thread to renew the lease periodically, but relinquish leadership after max duration."""
        if self.lease_thread and self.lease_thread.is_alive():
            self.logger.info("Lease renewal thread already running.")
            return

        def renew_lease():
            while self.is_primary:
                if time.time() - self.leader_start_time >= self.max_leader_duration:
                    self.logger.info("Max leader duration reached. Relinquishing primary role.")
                    try:
                        if self.zk.exists(self.leader_path):
                            self.zk.delete(self.leader_path)
                            self.logger.info("Deleted leader znode to relinquish leadership.")
                            time.sleep(1)
                    except Exception as e:
                        self.logger.error(f"Error deleting leader znode: {str(e)}")
                    self.is_primary = False
                    break

                time.sleep(self.lease_renew_interval)
                new_expiry = time.time() + self.lease_duration
                new_data = f"{discovery_address}|{new_expiry}"
                try:
                    self.zk.set(self.leader_path, new_data.encode())
                    self.logger.info(f"Lease renewed; new expiry time: {new_expiry}")
                except Exception as e:
                    self.logger.error(f"Failed to renew lease: {str(e)}")
                    break

        import threading
        self.lease_thread = threading.Thread(target=renew_lease, daemon=True)
        self.lease_thread.start()

    ########################################
    # Handle Registration Request
    ########################################
    def register(self, register_req):
        """ Handle publisher/subscriber registration """
        if not self.quorum_met():
            self.logger.warning("Quorum not met. Rejecting registration.")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_REGISTER
            response.register_resp.status = discovery_pb2.STATUS_CHECK_AGAIN
            response.register_resp.reason = "Quorum not met; please wait until all replicas are active."
            return response

        role_map = {
            discovery_pb2.ROLE_PUBLISHER: "Publisher",
            discovery_pb2.ROLE_SUBSCRIBER: "Subscriber",
            discovery_pb2.ROLE_BOTH: "Broker"
        }
        role = role_map.get(register_req.role, "Unknown")
        entity_id = register_req.info.id
        self.logger.info(f"Registering {role}: {entity_id}")

        if not self.is_primary:
            self.logger.warning("Not primary; cannot process registration. Inform client to contact the leader.")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_REGISTER
            response.register_resp.status = discovery_pb2.STATUS_NOT_PRIMARY
            return response

        category = role.lower() + "s"
        entity_path = f"/{category}/{entity_id}"
        entity_data = f"{register_req.info.addr}:{register_req.info.port}"

        try:
            self.zk.ensure_path(f"/{category}")
            if self.zk.exists(entity_path):
                self.logger.info(f"Node {entity_path} already exists, updating it")
                self.zk.delete(entity_path)
            self.zk.create(entity_path, entity_data.encode(), ephemeral=True)
            self.logger.info(f"Created/updated ZooKeeper node {entity_path}")
        except Exception as e:
            self.logger.error(f"Failed to register {role} in ZooKeeper: {str(e)}")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_REGISTER
            response.register_resp.status = discovery_pb2.STATUS_FAILURE
            return response

        self.registry[category][entity_id] = {
            "addr": register_req.info.addr,
            "port": register_req.info.port,
            "topics": list(register_req.topiclist)
        }

        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_REGISTER
        response.register_resp.status = discovery_pb2.STATUS_SUCCESS
        return response

    ########################################
    # Handle Lookup Request
    ########################################
    def lookup(self, lookup_req):
        """ Handle subscriber lookup request """
        self.logger.info(f"Lookup request for topics: {lookup_req.topiclist}")

        if not self.quorum_met():
            self.logger.warning("Quorum not met. Rejecting lookup request.")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            response.lookup_resp.reason = "Quorum not met; please wait until all replicas are active."
            return response

        if not self.is_primary:
            self.logger.warning("Not primary; lookup request cannot be processed here.")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            return response

        matched_nodes = []

        if self.dissemination_strategy == "Direct":
            path = "/publishers"
            self.logger.info("Direct mode: Looking up publishers")
        else:
            path = "/brokers"
            self.logger.info("ViaBroker mode: Looking up brokers")

        try:
            if self.zk.exists(path):
                nodes = self.zk.get_children(path)
                self.logger.info(f"Found {len(nodes)} nodes in ZooKeeper under {path}")
                for node_id in nodes:
                    if node_id == "leader":
                        self.logger.warning(f"Skipping 'leader' node found in {path}")
                        continue
                    try:
                        data, _ = self.zk.get(f"{path}/{node_id}")
                        node_data = data.decode()
                        if ":" in node_data:
                            addr, port = node_data.split(":")
                            node_info = discovery_pb2.RegistrantInfo(id=node_id, addr=addr, port=int(port))
                            matched_nodes.append(node_info)
                            self.logger.info(f"Added {node_id} at {addr}:{port} to response")
                        else:
                            self.logger.error(f"Node {node_id} has invalid data format: {node_data}")
                    except Exception as e:
                        self.logger.error(f"Error retrieving node {node_id}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error during lookup: {str(e)}")

        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
        response.lookup_resp.publishers.extend(matched_nodes)
        self.logger.info(f"Returning {len(matched_nodes)} matches for topics {lookup_req.topiclist}")
        return response

    ########################################
    # Run the Discovery Service
    ########################################
    def run(self):
        """ Run Discovery Service Event Loop """
        self.logger.info("DiscoveryAppln::run - entering event loop")
        self.mw_obj.event_loop()

    def dump_zk_nodes(self):
        """ Debug method to dump ZooKeeper node structure """
        self.logger.info("Current ZooKeeper nodes:")

        def print_tree(path, level=0):
            try:
                children = self.zk.get_children(path)
            except Exception:
                children = []
            for child in children:
                child_path = f"{path}/{child}" if path != "/" else f"/{child}"
                data = None
                try:
                    data_bytes, _ = self.zk.get(child_path)
                    data = data_bytes.decode() if data_bytes else None
                except Exception:
                    pass
                self.logger.info(f"{'  ' * level}|- {child} {f'({data})' if data else ''}")
                print_tree(child_path, level + 1)
        print_tree("/")
        

###################################
# Parse command line arguments
###################################
def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Discovery Service")
    parser.add_argument("-p", "--port", type=int, default=5555, help="Port number to run Discovery Service")
    parser.add_argument("-a", "--addr", default="localhost", help="IP address of Discovery Service")
    parser.add_argument("-z", "--zookeeper", default="127.0.0.1:2181", help="ZooKeeper address (default: 127.0.0.1:2181)")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file (default: config.ini)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[10,20,30,40,50], help="Logging level")
    return parser.parse_args()


###################################
# Main function
###################################
def main():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("DiscoveryAppln")
    args = parseCmdLineArgs()
    logger.setLevel(args.loglevel)
    app = DiscoveryAppln(logger)
    app.configure(args)
    app.run()

if __name__ == "__main__":
    main()
