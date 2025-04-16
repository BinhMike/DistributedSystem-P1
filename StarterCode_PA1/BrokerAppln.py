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
import random
import traceback

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
        
        # ZooKeeper paths for unified structure
        self.zk_paths = {
            "discovery_leader": "/discovery/leader",
            "discovery_replicas": "/discovery/replicas",
            "publishers": "/publishers",
            "brokers": "/brokers",
            "subscribers": "/subscribers",
            "load_balancers": "/load_balancers"
        }
        
        # Group-specific paths - will be constructed based on group name
        self.group = None  # Will store the broker group name
        self.leader_path = None  # Will be set dynamically based on group
        self.replicas_path = None  # Will be set dynamically based on group
        
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
            
            # Set the broker group
            self.group = args.group
            
            # Ensure base paths exist in the unified structure 
            self._init_zookeeper(args.zookeeper)
            
            # Construct all the broker paths
            self._setup_broker_paths()
            
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
            
            # Set up watch for load balancers (even if we don't find one now)
            self._setup_lb_watch()
            
            # Try to find and connect to LB (this remains the same)
            if self._find_and_connect_to_lb():
                self.logger.info("Connected to load balancer")
            else:
                self.logger.info("No load balancer found, running in standalone mode")
            
            self.logger.info("BrokerAppln::configure - completed")
        except Exception as e:
            self.logger.error(f"BrokerAppln::configure - Exception: {str(e)}")
            raise e
            
    def _setup_broker_paths(self):
        """Setup all broker-related paths in the unified ZooKeeper structure."""
        # Group-specific path under brokers
        brokers_path = self.zk_paths["brokers"]
        group_path = f"{brokers_path}/{self.group}"
        self.zk.ensure_path(group_path)
        
        # Leader and replica paths for this group
        self.leader_path = f"{group_path}/leader"
        self.replicas_path = f"{group_path}/replicas"
        self.zk.ensure_path(self.replicas_path)
        
        self.logger.info(f"Broker paths set up: leader={self.leader_path}, replicas={self.replicas_path}")

    def _init_zookeeper(self, zk_address):
        """Initialize ZooKeeper connection and create required paths."""
        self.logger.info(f"BrokerAppln::_init_zookeeper - Connecting to ZooKeeper at {zk_address}")
        self.zk = KazooClient(hosts=zk_address)
        self.zk.start()
        self.logger.info("BrokerAppln::_init_zookeeper - Connected to ZooKeeper")
        
        # Create all necessary base paths in the unified structure
        for path in self.zk_paths.values():
            if not self.zk.exists(path):
                self.zk.ensure_path(path)
                self.logger.info(f"Created base ZooKeeper path: {path}")

    def _init_middleware(self, args):
        """Initialize the middleware component."""
        self.logger.debug("BrokerAppln::_init_middleware - Initializing middleware")
        
        # Initialize middleware with the right primary status
        self.mw_obj = BrokerMW(self.logger, self.zk, False)
        self.mw_obj.configure(args)
        
        # By default, look for a load balancer in ZooKeeper
        if self._find_and_connect_to_lb():
            self.logger.info("Connected to load balancer automatically")
        else:
            self.logger.info("No load balancer found, operating in standalone mode")

    def _find_and_connect_to_lb(self):
        """Find load balancer in ZooKeeper and connect to it"""
        try:
            lb_path = "/load_balancers"
            if not self.zk.exists(lb_path):
                self.logger.info("Load balancer path doesn't exist yet")
                return False
            
            # Get list of load balancers
            lb_nodes = self.zk.get_children(lb_path)
            if not lb_nodes:
                self.logger.info("No load balancer nodes found")
                return False
            
            # Pick the first one
            lb_node = lb_nodes[0]
            lb_data_path = f"{lb_path}/{lb_node}"
            
            # Get load balancer connection info
            if not self.zk.exists(lb_data_path):
                self.logger.warning(f"Load balancer node {lb_node} disappeared")
                return False
                
            lb_data, _ = self.zk.get(lb_data_path)
            if not lb_data:
                self.logger.warning("Load balancer data is empty")
                return False
                
            # Parse connection info
            lb_info = lb_data.decode().split(":")
            if len(lb_info) < 3:
                self.logger.error(f"Invalid load balancer data format: {lb_data.decode()}")
                return False
                
            lb_addr, pub_port, sub_port = lb_info[0], lb_info[1], lb_info[2]
            self.logger.info(f"Found load balancer at {lb_addr}:{pub_port}")
            
            # Connect to load balancer
            result = self.mw_obj.connect_to_lb(lb_addr, int(pub_port), self.group)
            if result:
                self.logger.info(f"Successfully connected to load balancer at {lb_addr}:{pub_port}")
                return True
            else:
                self.logger.error("Failed to connect to load balancer")
                return False
                
        except Exception as e:
            self.logger.error(f"Error finding/connecting to load balancer: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def _setup_replica_tracking(self):
        """Register this broker as a replica and setup watch for other replicas."""
        # Register this instance as a replica with proper formatting for the load balancer
        replica_node = f"{self.replicas_path}/{self.broker_address}"
        try:
            # Store the broker address in a format that the load balancer can use
            # Make sure it's just the raw address:port with no extra data
            self.zk.create(replica_node, self.broker_address.encode(), ephemeral=True)
            self.logger.info(f"Registered replica node: {replica_node} with data: {self.broker_address}")
            
            # Verify the data was stored correctly
            try:
                data, _ = self.zk.get(replica_node)
                if data:
                    self.logger.info(f"Verified replica data: {data.decode()}")
            except Exception as e:
                self.logger.error(f"Error verifying replica data: {str(e)}")
                
        except NodeExistsError:
            # If node already exists, update it to ensure correct data format
            self.zk.delete(replica_node)
            self.zk.create(replica_node, self.broker_address.encode(), ephemeral=True)
            self.logger.info(f"Updated replica node: {replica_node} with data: {self.broker_address}")
        except Exception as e:
            self.logger.error(f"Error registering replica: {str(e)}")

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
            self.logger.info(f"BrokerAppln::_attempt_leadership_election - Trying to become leader at {self.leader_path}")
            # Create the ephemeral node with initial lease data
            self.zk.create(self.leader_path, leader_data.encode(), ephemeral=True)
            self.logger.info(f"BrokerAppln::_attempt_leadership_election - Successfully created leader node {self.leader_path}")
            # Proceed to become primary ONLY if creation succeeded
            self._become_primary(lease_expiry)

        except NodeExistsError:
            self.logger.info(f"BrokerAppln::_attempt_leadership_election - Leader node {self.leader_path} already exists.")
            self._become_backup()
        except Exception as e:
             self.logger.error(f"BrokerAppln::_attempt_leadership_election - Error creating leader node {self.leader_path}: {str(e)}")
             # Ensure we are definitely not primary if creation failed
             if self.is_primary:
                 self._relinquish_leadership() # Clean up if state is inconsistent
             else:
                 self.is_primary = False
                 self.mw_obj.update_primary_status(False)
        
        # No need to register in flat structure - only use the standard nested broker structure

    def _become_primary(self, lease_expiry):
        """Handle transition to primary role."""
        # Double-check if the node still exists before fully committing to primary role
        if not self.zk.exists(self.leader_path):
             self.logger.warning(f"BrokerAppln::_become_primary - Leader node {self.leader_path} disappeared before primary transition completed. Aborting.")
             self.is_primary = False
             if self.mw_obj: self.mw_obj.update_primary_status(False)
             # Optionally, attempt election again or become backup
             # self._attempt_leadership_election()
             return

        self.is_primary = True
        self.leader_start_time = time.time()
        self.logger.info(f"Instance {self.broker_address} became primary. Lease expires around {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(lease_expiry))}.")
        
        # Before passing to middleware, verify the leader node still exists
        if self.zk.exists(self.leader_path):
            # Pass the correct broker address for renewal data
            self.start_lease_renewal(self.broker_address)
            
            # Explicitly set middleware to primary with extra logging 
            if self.mw_obj:
                self.logger.info(f"Setting middleware to primary status")
                self.mw_obj.update_primary_status(True)
                self.logger.info(f"Middleware primary status is now: {self.mw_obj.is_primary}")
            else:
                self.logger.error(f"Cannot set middleware primary status: middleware object is None")
        else:
            self.logger.error(f"Leader node disappeared during _become_primary! Not updating middleware.")
            self.is_primary = False

    def _become_backup(self):
        """Handle backup role and watch for leader changes."""
        # Ensure we are not marked as primary
        if self.is_primary:
             self.logger.warning("BrokerAppln::_become_backup - Was marked as primary, correcting status.")
             self.is_primary = False
             if self.mw_obj: self.mw_obj.update_primary_status(False)
             # Stop lease renewal if it was running
             # (Need a way to signal the thread to stop, or just let it exit on next check)

        self.logger.info(f"Instance {self.broker_address} running as backup for group {self.group}. Watching {self.leader_path}.")

        # Setup the watch only if the leader path exists
        if self.zk.exists(self.leader_path):
            @self.zk.DataWatch(self.leader_path)
            def watch_leader(data, stat, event):
                # Check if the node was deleted
                if event is not None and event.type == "DELETED":
                    self._handle_leader_deletion()
                # Optionally handle data changes if needed (e.g., primary changed address)
                # elif event is not None and event.type == "CHANGED":
                #    self.logger.info(f"Leader data changed: {data}")
        else:
            # If leader path doesn't exist when becoming backup, attempt election immediately
            self.logger.warning(f"Leader path {self.leader_path} does not exist while becoming backup. Attempting election.")
            self._attempt_leadership_election()


    def _handle_leader_deletion(self):
        """Handle the situation when the leader znode is deleted."""
        self.logger.info(f"Detected deletion of leader node {self.leader_path}. Attempting to become primary...")
        # Short delay to potentially avoid thundering herd
        time.sleep(random.uniform(0.1, 0.5))

        try:
            # Ensure the parent path structure exists (should generally exist)
            parent_path = self.leader_path.rsplit('/', 1)[0]
            self.zk.ensure_path(parent_path)

            # Attempt to create the node again
            new_expiry = time.time() + self.lease_duration
            new_data = f"{self.broker_address}|{new_expiry}"
            self.logger.info(f"Attempting to recreate leader node {self.leader_path}")
            self.zk.create(self.leader_path, new_data.encode(), ephemeral=True)
            self.logger.info(f"Successfully recreated leader node {self.leader_path}")
            # Transition to primary
            self._become_primary(new_expiry)

        except NodeExistsError:
            # Node was recreated by someone else while we waited/tried
            self.logger.info(f"Leader node {self.leader_path} already recreated by another instance. Becoming backup.")
            self._become_backup() # Transition to backup state properly

        except Exception as e:
            self.logger.error(f"Error during leadership transition after deletion of {self.leader_path}: {str(e)}")
            # Ensure we are backup if anything goes wrong
            self._become_backup()


    def _renew_lease(self, broker_addr_port): # Changed parameter name for clarity
        """Renew the leadership lease."""
        # Ensure we are still supposed to be primary before renewing
        if not self.is_primary:
            self.logger.warning("Attempted to renew lease while not primary. Stopping renewal thread.")
            # Signal the thread to stop (or let it exit naturally)
            return # Exit the renewal loop

        new_expiry = time.time() + self.lease_duration
        new_data = f"{broker_addr_port}|{new_expiry}"
        try:
            # Check if node still exists before setting
            if self.zk.exists(self.leader_path):
                self.zk.set(self.leader_path, new_data.encode())
                self.logger.info(f"Lease renewed for {self.leader_path}; new expiry ~{time.strftime('%H:%M:%S', time.localtime(new_expiry))}")
            else:
                # Node disappeared unexpectedly (e.g., ZK issue, manual deletion)
                self.logger.error(f"Leader node {self.leader_path} disappeared unexpectedly during lease renewal. Relinquishing leadership.")
                self._relinquish_leadership()

        except Exception as e:
            self.logger.error(f"Failed to renew lease for {self.leader_path}: {str(e)}. Relinquishing leadership.")
            # If renewal fails, we must assume we lost leadership
            self._relinquish_leadership() # Call the proper cleanup function

    def _relinquish_leadership(self):
        """Voluntarily give up leadership role."""
        # Only perform actions if currently primary
        if not self.is_primary:
            return

        self.logger.info(f"Instance {self.broker_address} relinquishing primary role for group {self.group}.")
        self.is_primary = False # Set status first
        if self.mw_obj: self.mw_obj.update_primary_status(False)

        # Stop the lease renewal thread (important!)
        # Need a mechanism like a threading.Event or checking self.is_primary in the loop

        try:
            # Delete the leader node associated with this instance
            if self.zk.exists(self.leader_path):
                 # Optional: Check if the data is still ours before deleting?
                 # current_data, _ = self.zk.get(self.leader_path)
                 # if self.broker_address in current_data.decode():
                 self.zk.delete(self.leader_path)
                 self.logger.info(f"Deleted leader znode {self.leader_path} to relinquish leadership.")
                 # else:
                 #    self.logger.warning(f"Did not delete {self.leader_path} as data did not match our address.")
            else:
                 self.logger.warning(f"Leader znode {self.leader_path} did not exist when trying to relinquish leadership.")

        except Exception as e:
            self.logger.error(f"Error deleting leader znode {self.leader_path}: {str(e)}")

        # After relinquishing, become a backup and watch for new leader
        self._become_backup()

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
        
        spawn_lock_path = f"{self.zk_paths['brokers']}/spawn_lock"
        
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
                     f"-l {self.args.loglevel} -g {self.group}")
        
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
        """Voluntarily give up leadership role."""
        # Only perform actions if currently primary
        if not self.is_primary:
            return

        self.logger.info(f"Instance {self.broker_address} relinquishing primary role for group {self.group}.")
        self.is_primary = False # Set status first
        if self.mw_obj: self.mw_obj.update_primary_status(False)

        # Stop the lease renewal thread (important!)
        # Need a mechanism like a threading.Event or checking self.is_primary in the loop

        try:
            # Delete the leader node associated with this instance
            if self.zk.exists(self.leader_path):
                 # Optional: Check if the data is still ours before deleting?
                 # current_data, _ = self.zk.get(self.leader_path)
                 # if self.broker_address in current_data.decode():
                 self.zk.delete(self.leader_path)
                 self.logger.info(f"Deleted leader znode {self.leader_path} to relinquish leadership.")
                 # else:
                 #    self.logger.warning(f"Did not delete {self.leader_path} as data did not match our address.")
            else:
                 self.logger.warning(f"Leader znode {self.leader_path} did not exist when trying to relinquish leadership.")

        except Exception as e:
            self.logger.error(f"Error deleting leader znode {self.leader_path}: {str(e)}")

        # After relinquishing, become a backup and watch for new leader
        self._become_backup()

    def _setup_lb_watch(self):
        """Set up a watch for load balancer nodes in ZooKeeper"""
        try:
            # Make sure the path exists
            lb_path = "/load_balancers"
            if not self.zk.exists(lb_path):
                self.logger.info(f"Creating load balancer path in ZooKeeper: {lb_path}")
                self.zk.ensure_path(lb_path)
            
            # Set up the watch function
            @self.zk.ChildrenWatch(lb_path)
            def watch_lb(children):
                self.logger.info(f"Load balancers changed: {children}")
                if children and not self.mw_obj.using_lb:
                    self.logger.info("Load balancer detected after startup, attempting to connect")
                    self._find_and_connect_to_lb()
            
            self.logger.info("Set up watch for load balancers")
            return True
        except Exception as e:
            self.logger.error(f"Error setting up load balancer watch: {str(e)}")
            return False

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
        
        # Relinquish leadership if we are primary
        if hasattr(self, 'is_primary') and self.is_primary:
            self._relinquish_leadership()
        
        # Clean up middleware
        if self.mw_obj:
            self.mw_obj.cleanup()
            
        # Close ZooKeeper connection
        if self.zk:
            self.zk.stop()
            self.zk.close()


def parseCmdLineArgs():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Broker Application")
    parser.add_argument("-n", "--name", default="broker", help="Broker name")
    parser.add_argument("-p", "--port", type=int, default=6000, help="Broker port")
    parser.add_argument("-a", "--addr", default="localhost", help="Broker's advertised address") 
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", help="ZooKeeper Address")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level (default=INFO)")
    parser.add_argument("-g", "--group", default="default_group", help="Broker group name")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file path")
    
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
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down...")
        broker_app.cleanup()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Exception in main: {str(e)}")
        broker_app.cleanup()
        sys.exit(1)


if __name__ == "__main__":
    main()

