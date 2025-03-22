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


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 

import sys
import time
import argparse
import logging
import configparser
import signal
import subprocess
import os
import threading
import zmq
import socket

from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock
from kazoo.exceptions import NodeExistsError
from CS6381_MW.BrokerMW import BrokerMW  
from CS6381_MW import discovery_pb2  


# Publishers -> Broker; Broker -> Subscribers

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
        self.name = None
        self.zk = None
        self.zk_path = "/brokers"
        self.agent = None  # Reference to broker agent if running in agent mode
        signal.signal(signal.SIGINT, self.signal_handler)
        # Lease settings (in seconds)
        self.lease_duration = 30          # How long each lease is valid
        self.lease_renew_interval = 10    # How frequently to renew the lease
        self.max_leader_duration = 120    # Maximum time this instance can remain primary
        self.leader_start_time = None     # Time when this instance became leader
        self.lease_thread = None          # Thread for lease renewal
        self.args = None                # Will store command-line args
        self.bootstrap_complete = False # Flag to indicate bootstrapping is complete
        # election stuff
        self.is_primary = False
        self.leader_path = "/brokers/leader"
        self.replicas_path = "/brokers/replicas"

    ########################################
    # Quorum Check Helper
    ########################################
    def quorum_met(self):
        """Return True if at least 3 Broker replicas are registered."""
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
        """Attempt to spawn a new broker replica using a global lock to ensure only one spawns."""
        self.logger.info("Attempting to spawn a new Broker replica to restore quorum.")
        
        # Check if there's a stale lock before trying to acquire
        spawn_lock_path = "/brokers/spawn_lock"
        
        # First check if the lock node exists
        if self.zk.exists(f"{spawn_lock_path}/lock"):
            try:
                # Get lock data and creation time
                data, stat = self.zk.get(f"{spawn_lock_path}/lock")
                lock_age = time.time() - (stat.ctime / 1000)  # ZK time is in milliseconds
                
                # If lock is older than 60 seconds, consider it stale
                if lock_age > 60:
                    self.logger.warning(f"Detected stale lock (age: {lock_age:.1f}s). Attempting to clean up.")
                    try:
                        self.zk.delete(f"{spawn_lock_path}/lock")
                        self.logger.info("Successfully cleaned up stale lock node.")
                        # Give ZK a moment to fully process the deletion
                        time.sleep(1)
                    except Exception as e:
                        self.logger.error(f"Failed to clean up stale lock: {str(e)}")
            except Exception as e:
                self.logger.error(f"Error checking lock age: {str(e)}")
        
        # Now try to acquire the lock with a retry mechanism
        max_retries = 2
        for attempt in range(max_retries):
            try:
                spawn_lock = Lock(self.zk, spawn_lock_path)
                
                # Use a shorter timeout initially, then longer
                timeout = 5 if attempt == 0 else 10
                
                self.logger.info(f"Attempting to acquire spawn lock (attempt {attempt+1}/{max_retries}, timeout: {timeout}s)")
                if spawn_lock.acquire(timeout=timeout):
                    try:
                        # Re-check quorum status with lock held
                        replicas = self.zk.get_children(self.replicas_path)
                        if len(replicas) >= 3:
                            self.logger.info("Quorum restored while waiting for lock; no need to spawn.")
                            return
                        
                        # Get free port for new replica
                        free_port = get_free_port()
                        self.logger.info(f"Spawning new replica on port {free_port}")
                        
                        # Build command, carefully handling optional arguments
                        cmd = ["gnome-terminal", "--", "bash", "-c"]
                        spawn_cmd = f"python3 {sys.argv[0]} -p {free_port} -a {self.args.addr} -z {self.args.zookeeper} -n broker_replica_{free_port} -l {self.args.loglevel}"
                        
                        # Add config flag only if it was provided
                        if hasattr(self.args, 'config') and self.args.config:
                            spawn_cmd += f" -c {self.args.config}"
                        
                        spawn_cmd += "; exec bash"
                        cmd.append(spawn_cmd)
                        
                        # Launch the new broker process
                        subprocess.Popen(cmd)
                        self.logger.info(f"Spawned new Broker replica with command: {spawn_cmd}")
                        return  # Successfully spawned
                    except Exception as e:
                        self.logger.error(f"Error while spawn lock was held: {str(e)}")
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
                            time.sleep(5)
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

    def configure(self, args):
        self.logger.info("BrokerAppln::configure")

        try:
            self.name = args.name 
            self.args = args
            # config_obj = configparser.ConfigParser()
            # config_obj.read(args.config)

            # Connect to ZooKeeper
            self.logger.info(f"BrokerAppln::configure - Connecting to ZooKeeper at {args.zookeeper}")
            self.zk = KazooClient(hosts=args.zookeeper)
            self.zk.start()
            self.logger.info("BrokerAppln::configure - Connected to ZooKeeper")

            # Ensure base paths exist
            self.zk.ensure_path(self.zk_path)
            self.zk.ensure_path(self.replicas_path)
            
            # register this instance as a replica
            broker_address = f"{args.addr}:{args.port}"
            replica_node = f"{self.replicas_path}/{broker_address}"
            try:
                self.zk.create(replica_node, broker_address.encode(), ephemeral=True)
                self.logger.info(f"Registered replica node: {replica_node}")
            except NodeExistsError:
                self.zk.delete(replica_node)
                self.zk.create(replica_node, broker_address.encode(), ephemeral=True)
                self.logger.info(f"Updated replica node: {replica_node}")

            @self.zk.ChildrenWatch(self.replicas_path)
            def watch_replicas(children):
                num = len(children)
                self.logger.info(f"Replica watch: {num} replicas present.")
                if self.bootstrap_complete and num < 3:
                    self.logger.info("Quorum not met: fewer than 3 replicas active.")
                    self.spawn_replica()
            self.wait_for_bootstrap()

            lease_expiry = time.time() + self.lease_duration
            leader_data = f"{broker_address}|{lease_expiry}"
            try:
                self.zk.create(self.leader_path, leader_data.encode(), ephemeral=True)
                self.is_primary = True
                self.leader_start_time = time.time()  # Record when we became leader
                self.logger.info(f"Instance {broker_address} became primary with lease expiring at {lease_expiry}.")
                self.start_lease_renewal(broker_address)   
            except NodeExistsError:
                self.is_primary = False
                self.logger.info("A leader already exists. Running as backup.")
                @self.zk.DataWatch(self.leader_path)
                def watch_leader(data, stat, event):
                    if event is not None and event.type == "DELETED":
                        self.logger.info("Leader znode deleted. Attempting to become primary...")
                        time.sleep(1)
                        try:
                            self.zk.ensure_path(self.zk_path)
                            if not self.zk.exists(self.leader_path):
                                new_expiry = time.time() + self.lease_duration
                                new_data = f"{broker_address}|{new_expiry}"
                                self.zk.create(self.leader_path, new_data.encode(), ephemeral=True)
                                self.is_primary = True
                                self.leader_start_time = time.time()  # Reset the leader start time
                                self.logger.info(f"This instance has now become the primary with lease expiring at {new_expiry}!")
                                self.start_lease_renewal(broker_address)
                            else:
                                self.logger.info("Leader node already recreated by another instance.")
                                self.is_primary = False
                        except NodeExistsError:
                            self.logger.info("Another instance became primary while we were trying.")
                            self.is_primary = False
                        except Exception as e:
                            self.logger.error(f"Error during leadership transition: {str(e)}")
                            self.is_primary = False
            
            # Check if we are the leader
            if self.zk.exists(self.leader_path):
                data, _ = self.zk.get(self.leader_path)
                self.logger.info(f"Current leader in ZooKeeper: {data.decode()}")
            else:
                self.logger.error("Leader node does not exist after attempted creation.")

            # Initialize middleware - don't register ourselves in ZK here since the middleware will do it
            self.logger.debug("BrokerAppln::configure - Initializing middleware")
            self.mw_obj = BrokerMW(self.logger, self.zk, False)  
            self.mw_obj.configure(args)
            
            # Register with discovery service
            self.mw_obj.register(self.name)
            
            self.logger.info("BrokerAppln::configure - completed")
        except Exception as e:
            self.logger.error(f"BrokerAppln::configure - Exception: {str(e)}")
            raise e
          
    def driver(self):
        # start the broker event loop
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
        """ Invoke operation for message forwarding """
        try:
            # We don't need to explicitly call forward_messages anymore since
            # the event_loop will handle forwarding when messages are received
            return None
        except Exception as e:
            self.logger.error(f"BrokerAppln::invoke_operation - error: {str(e)}")
            return None
    
    def signal_handler(self, signum, frame):
        """ Handle shutdown when interrupt signal is received """
        self.logger.info(f"BrokerAppln::signal_handler - received signal {signum}")
        self.cleanup()
        sys.exit(0)

    def cleanup(self):
        """ Cleanup the middleware """
        self.logger.info("BrokerAppln::cleanup")
        if self.mw_obj:
            self.mw_obj.cleanup()

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Broker Application")

    parser.add_argument("-n", "--name", default="broker", help="Broker name")
    parser.add_argument("-p", "--port", type=int, default=6000, help="Broker port")
    parser.add_argument("-a", "--addr", default="localhost", help="Broker's advertised address") 
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", help="ZooKeeper Address")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level (default=INFO)")
    
    return parser.parse_args()

###################################
#
# Main program
#
###################################

def main():
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

