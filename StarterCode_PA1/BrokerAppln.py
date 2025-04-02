###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################

import sys
import time
import argparse
import logging
import signal
import subprocess
import socket
import threading
import zmq

from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock
from kazoo.exceptions import NodeExistsError
from CS6381_MW.BrokerMW import BrokerMW
from CS6381_MW import discovery_pb2


# Helper function for getting new port for quorum spawning
def get_free_port():
    """Bind a temporary socket to port 0 and return the OS-assigned free port."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))  # Let OS choose a free port
    port = s.getsockname()[1]
    s.close()
    return port


class BrokerAppln():
    """Broker application class that manages the broker functionality."""
    
    def __init__(self, logger):
        """Initialize the broker application."""
        self.logger = logger
        self.mw_obj = None
        self.name = None
        self.zk = None
        
        # ZooKeeper paths
        self.zk_path = "/brokers"
        self.leader_path = "/brokers/leader"
        self.replicas_path = "/brokers/replicas"
        
        # Capture SIGINT for clean shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Leadership and lease settings
        self.is_primary = False
        self.lease_duration = 30          # How long each lease is valid
        self.lease_renew_interval = 10    # How frequently to renew the lease
        self.max_leader_duration = 120    # Maximum time this instance can remain primary
        self.leader_start_time = None     # Time when this instance became leader
        self.lease_thread = None          # Thread for lease renewal
        
        # Command-line args and bootstrap status
        self.args = None                  # Will store command-line args
        self.bootstrap_complete = False   # Flag to indicate bootstrapping is complete
        self.broker_address = None        # This broker's advertised address:port

    def configure(self, args):
        """Configure the broker application with command line arguments."""
        self.logger.info("BrokerAppln::configure")

        try:
            self.name = args.name
            self.args = args
            self.broker_address = f"{args.addr}:{args.port}"
            
            # Initialize ZooKeeper
            self._init_zookeeper(args.zookeeper)
            
            # Initialize middleware
            self._init_middleware(args)
            
            # Setup replica tracking
            self._setup_replica_tracking()
            
            # Wait for bootstrap completion (quorum of 3 replicas)
            self.wait_for_bootstrap()
            
            # Attempt leadership election
            self._attempt_leadership_election()
            
            # Register with discovery service
            self.mw_obj.register(self.name)
            
            self.logger.info("BrokerAppln::configure - completed")
        except Exception as e:
            self.logger.error(f"BrokerAppln::configure - Exception: {str(e)}")
            raise e

    def _init_zookeeper(self, zk_address):
        """Initialize ZooKeeper connection and create required paths."""
        self.logger.info(f"BrokerAppln::_init_zookeeper - Connecting to ZooKeeper at {zk_address}")
        self.zk = KazooClient(hosts=zk_address)
        self.zk.start()
        self.logger.info("BrokerAppln::_init_zookeeper - Connected to ZooKeeper")
        
        # Create necessary paths
        self.zk.ensure_path(self.zk_path)
        self.zk.ensure_path(self.replicas_path)

    def _init_middleware(self, args):
        """Initialize the middleware component."""
        self.logger.debug("BrokerAppln::_init_middleware - Initializing middleware")
        self.mw_obj = BrokerMW(self.logger, self.zk, False)
        self.mw_obj.configure(args)

    def _setup_replica_tracking(self):
        """Register this broker as a replica and setup watch for other replicas."""
        # Register this instance as a replica
        replica_node = f"{self.replicas_path}/{self.broker_address}"
        try:
            self.zk.create(replica_node, self.broker_address.encode(), ephemeral=True)
            self.logger.info(f"Registered replica node: {replica_node}")
        except NodeExistsError:
            self.zk.delete(replica_node)
            self.zk.create(replica_node, self.broker_address.encode(), ephemeral=True)
            self.logger.info(f"Updated replica node: {replica_node}")

        # Setup watch on replicas for quorum maintenance
        @self.zk.ChildrenWatch(self.replicas_path)
        def watch_replicas(children):
            num = len(children)
            self.logger.info(f"Replica watch: {num} replicas present.")
            if self.bootstrap_complete and num < 3:
                self.logger.info("Quorum not met: fewer than 3 replicas active.")
                self.spawn_replica()

    def _attempt_leadership_election(self):
        """Attempt to become the primary broker by creating leader znode."""
        lease_expiry = time.time() + self.lease_duration
        leader_data = f"{self.broker_address}|{lease_expiry}"
        
        try:
            self.zk.create(self.leader_path, leader_data.encode(), ephemeral=True)
            self._become_primary(lease_expiry)
            
        except NodeExistsError:
            self._become_backup()
    
    def _become_primary(self, lease_expiry):
        """Handle transition to primary role."""
        self.is_primary = True
        self.leader_start_time = time.time()
        self.logger.info(f"Instance {self.broker_address} became primary with lease expiring at {lease_expiry}.")
        self.start_lease_renewal(self.broker_address)
        self.mw_obj.update_primary_status(True)
        
    def _become_backup(self):
        """Handle backup role and watch for leader changes."""
        self.is_primary = False
        self.logger.info("A leader already exists. Running as backup.")
        
        @self.zk.DataWatch(self.leader_path)
        def watch_leader(data, stat, event):
            if event is not None and event.type == "DELETED":
                self._handle_leader_deletion()
    
    def _handle_leader_deletion(self):
        """Handle the situation when the leader znode is deleted."""
        self.logger.info("Leader znode deleted. Attempting to become primary...")
        time.sleep(1)  # Brief pause to reduce race conditions
        
        try:
            self.zk.ensure_path(self.zk_path)
            
            if not self.zk.exists(self.leader_path):
                new_expiry = time.time() + self.lease_duration
                new_data = f"{self.broker_address}|{new_expiry}"
                self.zk.create(self.leader_path, new_data.encode(), ephemeral=True)
                self._become_primary(new_expiry)
                
            else:
                self.logger.info("Leader node already recreated by another instance.")
                self.is_primary = False
                self.mw_obj.update_primary_status(False)
                
        except NodeExistsError:
            self.logger.info("Another instance became primary while we were trying.")
            self.is_primary = False
            self.mw_obj.update_primary_status(False)
        except Exception as e:
            self.logger.error(f"Error during leadership transition: {str(e)}")
            self.is_primary = False
            self.mw_obj.update_primary_status(False)

    def quorum_met(self):
        """Return True if at least 3 Broker replicas are registered."""
        try:
            replicas = self.zk.get_children(self.replicas_path)
            self.logger.info(f"Quorum check: {len(replicas)} replicas present.")
            return len(replicas) >= 3
        except Exception as e:
            self.logger.error(f"Error checking quorum: {str(e)}")
            return False

    def spawn_replica(self):
        """Attempt to spawn a new broker replica using a global lock to ensure only one spawns."""
        self.logger.info("Attempting to spawn a new Broker replica to restore quorum.")
        
        spawn_lock_path = "/brokers/spawn_lock"
        
        # Check for stale locks and clean up if necessary
        self._cleanup_stale_lock(spawn_lock_path)
        
        # Try to acquire the lock with retries
        self._acquire_spawn_lock_and_spawn(spawn_lock_path)

    def _cleanup_stale_lock(self, spawn_lock_path):
        """Check for and clean up stale spawn locks."""
        if self.zk.exists(f"{spawn_lock_path}/lock"):
            try:
                data, stat = self.zk.get(f"{spawn_lock_path}/lock")
                lock_age = time.time() - (stat.ctime / 1000)  # ZK time is in milliseconds
                
                # If lock is older than 60 seconds, consider it stale
                if lock_age > 60:
                    self.logger.warning(f"Detected stale lock (age: {lock_age:.1f}s). Attempting to clean up.")
                    try:
                        self.zk.delete(f"{spawn_lock_path}/lock")
                        self.logger.info("Successfully cleaned up stale lock node.")
                        time.sleep(1)  # Give ZK a moment to fully process the deletion
                    except Exception as e:
                        self.logger.error(f"Failed to clean up stale lock: {str(e)}")
            except Exception as e:
                self.logger.error(f"Error checking lock age: {str(e)}")

    def _acquire_spawn_lock_and_spawn(self, spawn_lock_path):
        """Try to acquire spawn lock and spawn a replica if successful."""
        max_retries = 2
        for attempt in range(max_retries):
            try:
                spawn_lock = Lock(self.zk, spawn_lock_path)
                timeout = 5 if attempt == 0 else 10
                
                self.logger.info(f"Attempting to acquire spawn lock (attempt {attempt+1}/{max_retries}, timeout: {timeout}s)")
                if spawn_lock.acquire(timeout=timeout):
                    try:
                        # Re-check quorum status with lock held
                        if self.quorum_met():
                            self.logger.info("Quorum restored while waiting for lock; no need to spawn.")
                            return
                        
                        self._spawn_new_process()
                        return  # Successfully spawned
                    finally:
                        # Always release the lock
                        try:
                            spawn_lock.release()
                            self.logger.info("Spawn lock released successfully")
                        except Exception as e:
                            self.logger.error(f"Error releasing spawn lock: {str(e)}")
                else:
                    self.logger.warning(f"Could not acquire spawn lock on attempt {attempt+1}")
            except Exception as e:
                self.logger.error(f"Exception during lock handling on attempt {attempt+1}: {str(e)}")
            
            # Wait before retrying
            if attempt < max_retries - 1:
                backoff = 3 * (attempt + 1)
                self.logger.info(f"Waiting {backoff}s before retry...")
                time.sleep(backoff)
        
        self.logger.error("Failed to spawn a new replica after multiple attempts")

    def _spawn_new_process(self):
        """Spawn a new broker process."""
        free_port = get_free_port()
        self.logger.info(f"Spawning new replica on port {free_port}")
        
        # Build command, carefully handling optional arguments
        cmd = ["gnome-terminal", "--", "bash", "-c"]
        spawn_cmd = (f"python3 {sys.argv[0]} -p {free_port} -a {self.args.addr} "
                     f"-z {self.args.zookeeper} -n broker_replica_{free_port} "
                     f"-l {self.args.loglevel}")
        
        # Add config flag only if it was provided
        if hasattr(self.args, 'config') and self.args.config:
            spawn_cmd += f" -c {self.args.config}"
        
        spawn_cmd += "; exec bash"
        cmd.append(spawn_cmd)
        
        # Launch the new broker process
        subprocess.Popen(cmd)
        self.logger.info(f"Spawned new Broker replica with command: {spawn_cmd}")

    def wait_for_bootstrap(self):
        """Wait until at least 3 replica nodes are registered, then mark bootstrap as complete."""
        self.logger.info("Waiting for bootstrap: expecting at least 3 replicas before enabling auto-spawn...")
        while True:
            if self.quorum_met():
                self.logger.info("Bootstrap complete: quorum achieved.")
                break
            time.sleep(2)
        self.bootstrap_complete = True

    def start_lease_renewal(self, discovery_address):
        """Start a background thread to renew the lease periodically, but relinquish leadership after max duration."""
        if self.lease_thread and self.lease_thread.is_alive():
            self.logger.info("Lease renewal thread already running.")
            return

        def renew_lease():
            while self.is_primary:
                if time.time() - self.leader_start_time >= self.max_leader_duration:
                    self._relinquish_leadership()
                    break

                time.sleep(self.lease_renew_interval)
                self._renew_lease(discovery_address)

        self.lease_thread = threading.Thread(target=renew_lease, daemon=True)
        self.lease_thread.start()
    
    def _relinquish_leadership(self):
        """Voluntarily give up leadership role after maximum duration."""
        self.logger.info("Max leader duration reached. Relinquishing primary role.")
        try:
            if self.zk.exists(self.leader_path):
                self.zk.delete(self.leader_path)
                self.logger.info("Deleted leader znode to relinquish leadership.")
                time.sleep(5)
        except Exception as e:
            self.logger.error(f"Error deleting leader znode: {str(e)}")
        self.is_primary = False
        self.mw_obj.update_primary_status(False)
    
    def _renew_lease(self, discovery_address):
        """Renew the leadership lease."""
        new_expiry = time.time() + self.lease_duration
        new_data = f"{discovery_address}|{new_expiry}"
        try:
            self.zk.set(self.leader_path, new_data.encode())
            self.logger.info(f"Lease renewed; new expiry time: {new_expiry}")
        except Exception as e:
            self.logger.error(f"Failed to renew lease: {str(e)}")
            self.is_primary = False
            self.mw_obj.update_primary_status(False)

    def driver(self):
        """Start the broker event loop."""
        try:
            self.logger.info("BrokerAppln::driver - starting event loop")
            self.mw_obj.set_upcall_handle(self)
            
            # Main event loop
            while True:
                # Process any network events (with timeout)
                self.mw_obj.event_loop(timeout=100)  # 100ms timeout
                
                # Small sleep to prevent CPU spinning
                time.sleep(0.01)
                
        except Exception as e:
            self.logger.error(f"BrokerAppln::driver - error: {str(e)}")
            self.cleanup()
            return 

    def invoke_operation(self):
        """Invoke operation for message forwarding."""
        try:
            # Event_loop will handle forwarding when messages are received
            return None
        except Exception as e:
            self.logger.error(f"BrokerAppln::invoke_operation - error: {str(e)}")
            return None
    
    def signal_handler(self, signum, frame):
        """Handle shutdown when interrupt signal is received."""
        self.logger.info(f"BrokerAppln::signal_handler - received signal {signum}")
        self.cleanup()
        sys.exit(0)

    def cleanup(self):
        """Cleanup the middleware."""
        self.logger.info("BrokerAppln::cleanup")
        if self.mw_obj:
            self.mw_obj.cleanup()


def parseCmdLineArgs():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Broker Application")
    parser.add_argument("-n", "--name", default="broker", help="Broker name")
    parser.add_argument("-p", "--port", type=int, default=6000, help="Broker port")
    parser.add_argument("-a", "--addr", default="localhost", help="Broker's advertised address") 
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", help="ZooKeeper Address")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level (default=INFO)")
    return parser.parse_args()


def main():
    """Main program entry point."""
    # Configure logger
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.getLogger("kazoo").setLevel(logging.WARNING)
    logger = logging.getLogger("BrokerAppln")
    
    # Parse command line arguments
    args = parseCmdLineArgs()
    logger.setLevel(args.loglevel)
    
    # Create broker application instance
    broker_app = BrokerAppln(logger)
    
    try:
        # Configure the application
        broker_app.configure(args)
        
        # Start the event loop
        broker_app.driver()
        
    except Exception as e:
        logger.error(f"Exception in main: {str(e)}")
        broker_app.cleanup()
        sys.exit(1)


if __name__ == "__main__":
    main()

