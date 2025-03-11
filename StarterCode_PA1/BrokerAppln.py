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
import signal
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from CS6381_MW.BrokerMW import BrokerMW  
from CS6381_MW import discovery_pb2  


# Publishers -> Broker; Broker -> Subscribers



class BrokerAppln():
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None  
        self.name = None
        self.zk = None
        self.zk_path = "/brokers"
        self.leader_path = "/brokers/leader"
        self.is_primary = False  # Indicates if this broker is the primary
        
        # Lease settings (in seconds)
        self.lease_duration = 10         # How long each lease is valid
        self.lease_renew_interval = 10   # How frequently to renew the lease
        self.max_leader_duration = 30    # Maximum time this instance can remain primary
        self.leader_start_time = None    # Time when this instance became leader
        self.lease_thread = None         # Thread for lease renewal
        
        signal.signal(signal.SIGINT, self.signal_handler)

    def configure(self, args):
        self.logger.info("BrokerAppln::configure")
        self.name = args.name 

        # connect to ZooKeeper
        self.zk = KazooClient(hosts=args.zookeeper)
        self.zk.start()

        # create /brokers path
        self.zk.ensure_path(self.zk_path)
        broker_address = f"{args.addr}:{args.port}"
        
        # Attempt to become the leader broker by creating the ephemeral leader node
        lease_expiry = time.time() + self.lease_duration
        leader_data = f"{broker_address}|{lease_expiry}"
        
        try:
            self.zk.create(self.leader_path, leader_data.encode(), ephemeral=True)
            self.is_primary = True
            self.leader_start_time = time.time()  # Record when we became leader
            self.logger.info(f"Broker {self.name} became primary with lease expiring at {lease_expiry}.")
            self.start_lease_renewal(broker_address)
        except NodeExistsError:
            self.is_primary = False
            self.logger.info("A leader broker already exists. Running as backup.")
            
            # Watch for leader znode deletion so backup can try to become primary
            @self.zk.DataWatch(self.leader_path)
            def watch_leader(data, stat, event):
                if event is not None and event.type == "DELETED":
                    self.logger.info("Leader broker znode deleted. Attempting to become primary...")
                    # Add a small delay to avoid race conditions
                    time.sleep(1)
                    try:
                        # Re-ensure the brokers path exists
                        self.zk.ensure_path(self.zk_path)
                        
                        # Check if the leader node still doesn't exist
                        if not self.zk.exists(self.leader_path):
                            new_expiry = time.time() + self.lease_duration
                            new_data = f"{broker_address}|{new_expiry}"
                            self.zk.create(self.leader_path, new_data.encode(), ephemeral=True)
                            self.is_primary = True
                            self.leader_start_time = time.time()  # Reset the leader start time
                            self.logger.info(f"This broker has now become the primary with lease expiring at {new_expiry}!")
                            self.start_lease_renewal(broker_address)
                        else:
                            self.logger.info("Leader node already recreated by another broker.")
                            self.is_primary = False
                    except NodeExistsError:
                        self.logger.info("Another broker became primary while we were trying.")
                        self.is_primary = False
                    except Exception as e:
                        self.logger.error(f"Error during leadership transition: {str(e)}")
                        self.is_primary = False
        
        # Log current leader information for debugging
        if self.zk.exists(self.leader_path):
            data, _ = self.zk.get(self.leader_path)
            self.logger.info(f"Current leader broker in ZooKeeper: {data.decode()}")
        else:
            self.logger.error("Leader broker node does not exist after attempted creation.")

        # Register this broker node (whether leader or backup)
        broker_node_path = f"{self.zk_path}/{self.name}"
        if self.zk.exists(broker_node_path):
            self.zk.delete(broker_node_path)
        self.zk.create(broker_node_path, broker_address.encode(), ephemeral=True)
        self.logger.info(f"Broker registered in ZooKeeper at {broker_node_path}")
        
        # initialize middleware
        self.mw_obj = BrokerMW(self.logger, self.zk, self.is_primary)  
        self.mw_obj.configure(args)  
        self.logger.info(f"BrokerAppln::configure - completed (Primary status: {self.is_primary})")

    def start_lease_renewal(self, broker_address):
        """Start a background thread to renew the lease periodically, but relinquish leadership after max duration."""
        import threading
        
        def renewal_task():
            self.logger.info("Lease renewal thread started")
            start_time = time.time()
            
            while True:
                # Check if we've been the leader too long
                elapsed_time = time.time() - start_time
                if elapsed_time >= self.max_leader_duration:
                    self.logger.info(f"Max leadership duration ({self.max_leader_duration}s) reached, giving up leadership")
                    try:
                        if self.zk.exists(self.leader_path):
                            self.zk.delete(self.leader_path)
                        self.is_primary = False
                        # Update our middleware's primary status
                        if self.mw_obj:
                            self.mw_obj.update_primary_status(False)
                        return  # Exit the thread
                    except Exception as e:
                        self.logger.error(f"Error giving up leadership: {str(e)}")
                        return
                
                # Otherwise continue with lease renewal
                try:
                    if not self.is_primary:
                        self.logger.info("No longer primary, stopping lease renewal")
                        return  # Exit the thread if we're not primary anymore
                    
                    if self.zk.exists(self.leader_path):
                        new_expiry = time.time() + self.lease_duration
                        new_data = f"{broker_address}|{new_expiry}"
                        self.zk.set(self.leader_path, new_data.encode())
                        self.logger.debug(f"Lease renewed, new expiry: {new_expiry}")
                    else:
                        self.logger.warning("Leader node doesn't exist during renewal, attempting to recreate")
                        new_expiry = time.time() + self.lease_duration
                        new_data = f"{broker_address}|{new_expiry}"
                        self.zk.create(self.leader_path, new_data.encode(), ephemeral=True)
                except Exception as e:
                    self.logger.error(f"Error renewing lease: {str(e)}")
                    self.is_primary = False
                    if self.mw_obj:
                        self.mw_obj.update_primary_status(False)
                    return  # Exit on error
                
                # Sleep until next renewal
                time.sleep(self.lease_renew_interval)
        
        # Start the renewal thread
        self.lease_thread = threading.Thread(target=renewal_task, daemon=True)
        self.lease_thread.start()

    def update_primary_status(self, is_primary):
        """Update the primary status and notify middleware"""
        if is_primary != self.is_primary:
            self.is_primary = is_primary
            self.logger.info(f"Broker primary status updated to: {is_primary}")
            if self.mw_obj:
                self.mw_obj.update_primary_status(is_primary)

    def driver(self):
        # Starting event Loop
        try:
            self.logger.info(f"BrokerAppln::driver - starting event loop (Primary: {self.is_primary})")
            self.mw_obj.set_upcall_handle(self)
            
            # Main event loop
            while True:
                # Process any network events (with timeout)
                self.mw_obj.event_loop(timeout=100)  # 100ms timeout
                
                # Explicitly check for messages to forward (non-blocking)
                self.invoke_operation()
                
                # Small sleep to prevent CPU spinning
                time.sleep(0.01)
                
        except Exception as e:
            self.logger.error(f"BrokerAppln::driver - error: {str(e)}")
            self.cleanup()

    def invoke_operation(self):
        """ Invoke operation for message forwarding """
        try:
            # Only the primary broker should forward messages
            if not self.is_primary:
                return None
                
            # Check for and forward any available messages (non-blocking)
            result = self.mw_obj.forward_messages()
            if result:
                self.logger.debug("BrokerAppln::invoke_operation - message forwarded successfully")
            return None
        except Exception as e:
            self.logger.error(f"BrokerAppln::invoke_operation - error: {str(e)}")
            return None
    
    def dump_zk_nodes(self):
        """Debug method to dump ZooKeeper node structure"""
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
    parser.add_argument("-p", "--port", type=int, default=6000, help="Broker port")# broker dispatch message to subscriber
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery Service IP:Port")
    parser.add_argument("--publisher_ip", default="localhost", help="Publisher IP Address") # Publisher ip. Publisher send message to broker
    parser.add_argument("--publisher_port", type=int, default=6001, help="Publisher Port")
    parser.add_argument("--addr", default="localhost", help="Broker's advertised address") 
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", help="ZooKeeper Address")
    return parser.parse_args()
    
###################################
#
# Main program
#
###################################

def main():
    logging.getLogger("kazoo").setLevel(logging.WARNING)
    logger = logging.getLogger("BrokerAppln")
    args = parseCmdLineArgs()
    broker_app = BrokerAppln(logger)
    broker_app.configure(args)
    broker_app.driver()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()

