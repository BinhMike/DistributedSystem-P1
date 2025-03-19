###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Broker application with warm-passive replication
#
# Created: Spring 2023
#
###############################################

import sys
import time
import argparse
import logging
import signal
import atexit
import threading
import socket
import subprocess
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from CS6381_MW.BrokerMW import BrokerMW

# Helper function for getting new port for quorum spawning
def get_free_port():
    """Bind a temporary socket to port 0 and return the OS-assigned free port."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))  # Let OS choose a free port
    port = s.getsockname()[1]
    s.close()
    return port

class BrokerAppln():
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None
        self.zk = None
        self.name = None
        self.is_primary = False
        self.leader_path = "/broker/leader"
        self.replicas_path = "/broker/replicas"
        
        # Lease settings
        self.lease_duration = 30
        self.lease_renew_interval = 10
        self.max_leader_duration = 120
        self.leader_start_time = None
        self.lease_thread = None
        
        # Quorum management
        self.bootstrap_complete = False
        self.args = None
        
        # Set up signal handling
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def configure(self, args):
        try:
            self.logger.info("BrokerAppln::configure")
            self.name = args.name
            self.args = args
            
            # Connect to ZooKeeper
            self.logger.info(f"BrokerAppln::configure - Connecting to ZooKeeper at {args.zookeeper}")
            self.zk = KazooClient(hosts=args.zookeeper)
            self.zk.start(timeout=10)
            self.logger.info("BrokerAppln::configure - Connected to ZooKeeper")
            
            # Ensure base paths exist
            self.zk.ensure_path("/broker")
            self.zk.ensure_path(self.replicas_path)
            
            # Register this instance as a replica
            broker_address = f"{args.addr}:{args.port}"
            replica_node = f"{self.replicas_path}/{broker_address}"
            try:
                self.zk.create(replica_node, broker_address.encode(), ephemeral=True)
                self.logger.info(f"BrokerAppln::configure - Registered replica node: {replica_node}")
            except NodeExistsError:
                self.zk.delete(replica_node)
                self.zk.create(replica_node, broker_address.encode(), ephemeral=True)
                self.logger.info(f"BrokerAppln::configure - Updated replica node: {replica_node}")
                
            # Wait for bootstrap: ensure that at least 3 replicas are active
            self.wait_for_bootstrap()
                
            # Set up replica watcher for quorum maintenance
            @self.zk.ChildrenWatch(self.replicas_path)
            def watch_replicas(children):
                num = len(children)
                self.logger.info(f"BrokerAppln::watch_replicas - {num} replicas present")
                if self.bootstrap_complete and num < 3:
                    self.logger.warning("BrokerAppln::watch_replicas - Quorum not met: fewer than 3 replicas active.")
                    self.spawn_replica()
            
            # Attempt to become the leader by creating the ephemeral leader node
            lease_expiry = time.time() + self.lease_duration
            leader_data = f"{broker_address}|{lease_expiry}"
            try:
                self.zk.create(self.leader_path, leader_data.encode(), ephemeral=True)
                self.is_primary = True
                self.leader_start_time = time.time()
                self.logger.info(f"BrokerAppln::configure - Instance {broker_address} became primary with lease expiring at {lease_expiry}.")
                self.start_lease_renewal(broker_address)
            except NodeExistsError:
                self.is_primary = False
                self.logger.info("BrokerAppln::configure - A leader already exists. Running as backup.")
                @self.zk.DataWatch(self.leader_path)
                def watch_leader(data, stat, event):
                    if event is not None and event.type == "DELETED":
                        self.logger.info("BrokerAppln::watch_leader - Leader znode deleted. Attempting to become primary...")
                        time.sleep(1)  # Small delay to avoid race conditions
                        try:
                            self.zk.ensure_path("/broker")
                            if not self.zk.exists(self.leader_path):
                                new_expiry = time.time() + self.lease_duration
                                new_data = f"{broker_address}|{new_expiry}"
                                self.zk.create(self.leader_path, new_data.encode(), ephemeral=True)
                                self.is_primary = True
                                self.leader_start_time = time.time()
                                self.logger.info(f"BrokerAppln::watch_leader - This instance has now become the primary with lease expiring at {new_expiry}!")
                                self.start_lease_renewal(broker_address)
                            else:
                                self.logger.info("BrokerAppln::watch_leader - Leader node already recreated by another instance.")
                                self.is_primary = False
                        except NodeExistsError:
                            self.logger.info("BrokerAppln::watch_leader - Another instance became primary while we were trying.")
                            self.is_primary = False
                        except Exception as e:
                            self.logger.error(f"BrokerAppln::watch_leader - Error during leadership transition: {str(e)}")
                            self.is_primary = False
            
            # Initialize middleware
            self.logger.debug("BrokerAppln::configure - Initializing middleware")
            self.mw_obj = BrokerMW(self.logger)
            self.mw_obj.configure(args, self.is_primary, self.zk)
            self.mw_obj.set_upcall_handle(self)
            
            self.logger.info("BrokerAppln::configure - Configuration complete")
            self.dump_zk_nodes()
            
        except Exception as e:
            self.logger.error(f"BrokerAppln::configure - Exception: {str(e)}")
            raise e
    
    def wait_for_bootstrap(self):
        """Wait until at least 3 replica nodes are registered, then mark bootstrap as complete."""
        self.logger.info("BrokerAppln::wait_for_bootstrap - Waiting for at least 3 replicas before enabling auto-spawn...")
        while True:
            try:
                replicas = self.zk.get_children(self.replicas_path)
                self.logger.info(f"BrokerAppln::wait_for_bootstrap - Bootstrap check: {len(replicas)} replicas present.")
                if len(replicas) >= 3:
                    self.logger.info("BrokerAppln::wait_for_bootstrap - Bootstrap complete: quorum achieved.")
                    break
            except Exception as e:
                self.logger.error(f"BrokerAppln::wait_for_bootstrap - Error during bootstrap wait: {str(e)}")
            time.sleep(2)
        self.bootstrap_complete = True
    
    def spawn_replica(self):
        """Attempt to spawn a new Broker replica using a global lock to ensure only one spawns."""
        self.logger.info("BrokerAppln::spawn_replica - Attempting to spawn a new Broker replica to restore quorum.")
        
        # Use a global lock in ZooKeeper to coordinate spawns.
        spawn_lock = self.zk.Lock("/broker/spawn_lock")
        try:
            # Acquire the lock with a timeout (say, 5 seconds)
            if spawn_lock.acquire(timeout=5):
                # Once the lock is acquired, re-check the replica count.
                replicas = self.zk.get_children(self.replicas_path)
                if len(replicas) >= 3:
                    self.logger.info("BrokerAppln::spawn_replica - Quorum restored while waiting for lock; no need to spawn.")
                    spawn_lock.release()
                    return
                
                free_port = get_free_port()
                self.logger.info("BrokerAppln::spawn_replica - Spawn lock acquired; proceeding to spawn new replica.")
                # Construct the command using stored args
                cmd = [
                    "gnome-terminal",
                    "--", "bash", "-c",
                    f"python3 {sys.argv[0]} -n broker_{free_port} -p {free_port} -a {self.args.addr} -z {self.args.zookeeper} -l {self.args.loglevel}; exec bash"
                ]
                try:
                    subprocess.Popen(cmd)
                    self.logger.info(f"BrokerAppln::spawn_replica - Spawned new Broker replica with command: {' '.join(cmd)}")
                except Exception as e:
                    self.logger.error(f"BrokerAppln::spawn_replica - Failed to spawn a new replica: {str(e)}")
                finally:
                    spawn_lock.release()
            else:
                self.logger.info("BrokerAppln::spawn_replica - Could not acquire spawn lock; another replica may be spawning.")
        except Exception as e:
            self.logger.error(f"BrokerAppln::spawn_replica - Error acquiring spawn lock: {str(e)}")
    
    def start_lease_renewal(self, broker_address):
        """Start a background thread to renew the lease periodically, but relinquish leadership after max duration."""
        if self.lease_thread and self.lease_thread.is_alive():
            self.logger.info("BrokerAppln::start_lease_renewal - Lease renewal thread already running.")
            return

        def renew_lease():
            while self.is_primary:
                if time.time() - self.leader_start_time >= self.max_leader_duration:
                    self.logger.info("BrokerAppln::renew_lease - Max leader duration reached. Relinquishing primary role.")
                    try:
                        if self.zk.exists(self.leader_path):
                            self.zk.delete(self.leader_path)
                            self.logger.info("BrokerAppln::renew_lease - Deleted leader znode to relinquish leadership.")
                            time.sleep(1)
                    except Exception as e:
                        self.logger.error(f"BrokerAppln::renew_lease - Error deleting leader znode: {str(e)}")
                    self.is_primary = False
                    break

                time.sleep(self.lease_renew_interval)
                new_expiry = time.time() + self.lease_duration
                new_data = f"{broker_address}|{new_expiry}"
                try:
                    self.zk.set(self.leader_path, new_data.encode())
                    self.logger.info(f"BrokerAppln::renew_lease - Lease renewed; new expiry time: {new_expiry}")
                except Exception as e:
                    self.logger.error(f"BrokerAppln::renew_lease - Failed to renew lease: {str(e)}")
                    break

        self.lease_thread = threading.Thread(target=renew_lease, daemon=True)
        self.lease_thread.start()
        self.logger.info("BrokerAppln::start_lease_renewal - Lease renewal thread started.")
    
    def invoke_operation(self):
        """Invoked by the middleware when a message is processed"""
        # This method can be empty or contain minimal logging
        # since we're just using it as a callback notification
        pass
    
    def signal_handler(self, signum, frame):
        self.logger.info(f"BrokerAppln::signal_handler - Received signal {signum}")
        self.cleanup()
        sys.exit(0)
    
    def cleanup(self):
        """Clean up resources and deregister from ZooKeeper"""
        try:
            self.logger.info("BrokerAppln::cleanup - Performing cleanup operations")
            
            # Stop the lease renewal thread if it's running
            self.is_primary = False
            if self.lease_thread and self.lease_thread.is_alive():
                self.lease_thread.join(timeout=2)
                
            # Explicitly delete our leader node if we're the primary
            if self.zk and self.zk.connected and self.leader_path:
                if self.zk.exists(self.leader_path):
                    try:
                        data, _ = self.zk.get(self.leader_path)
                        our_addr = f"{self.args.addr}:{self.args.port}"
                        
                        # Only delete if it's our node
                        if data and our_addr in data.decode():
                            self.logger.info("BrokerAppln::cleanup - Deleting our leader node from ZooKeeper")
                            self.zk.delete(self.leader_path)
                    except Exception as e:
                        self.logger.warning(f"BrokerAppln::cleanup - Error deleting leader node: {e}")
            
            # Close the ZooKeeper connection
            if self.zk:
                self.logger.info("BrokerAppln::cleanup - Closing ZooKeeper connection")
                self.zk.stop()
                self.zk.close()
                
            # Close middleware resources
            if self.mw_obj and hasattr(self.mw_obj, 'cleanup'):
                self.mw_obj.cleanup()
                
            self.logger.info("BrokerAppln::cleanup - Cleanup complete")
            
        except Exception as e:
            self.logger.error(f"BrokerAppln::cleanup - Error during cleanup: {e}")
    
    def dump_zk_nodes(self):
        """Debug method to dump ZooKeeper node structure"""
        self.logger.info("BrokerAppln::dump_zk_nodes - Current ZooKeeper nodes:")

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

    def quorum_met(self):
        """Return True if at least 3 Broker replicas are registered."""
        try:
            replicas = self.zk.get_children(self.replicas_path)
            self.logger.info(f"Quorum check: {len(replicas)} replicas present.")
            return len(replicas) >= 3
        except Exception as e:
            self.logger.error(f"Error checking quorum: {str(e)}")
            return False

    def run(self):
        """ Run Broker Service Event Loop """
        self.logger.info("BrokerAppln::run - entering event loop")
        try:
            # Register cleanup handlers
            atexit.register(self.cleanup)
            # Run the event loop
            while True:
                self.mw_obj.event_loop(timeout=100)  # 100ms timeout
                time.sleep(0.01)  # Small sleep to prevent CPU spinning
        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt received, shutting down")
        except Exception as e:
            self.logger.error(f"Exception in event loop: {e}")
        finally:
            # Ensure cleanup happens
            self.cleanup()

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Broker Application")
    parser.add_argument("-n", "--name", default="broker", help="Broker name")
    parser.add_argument("-p", "--port", type=int, default=5555, help="Broker port")
    parser.add_argument("-a", "--addr", default="localhost", help="Broker's advertised address")
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", help="ZooKeeper Address")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[10,20,30,40,50], help="Logging level")
    return parser.parse_args()

def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("BrokerAppln")
    args = parseCmdLineArgs()
    logger.setLevel(args.loglevel)
    
    app = BrokerAppln(logger)
    
    try:
        app.configure(args)
        app.run()
    except Exception as e:
        logger.error(f"Exception in main: {e}")
        if hasattr(app, 'cleanup'):
            app.cleanup()
        sys.exit(1)

if __name__ == "__main__":
    main()

