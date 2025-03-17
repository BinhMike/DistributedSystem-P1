###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Broker Middleware with warm-passive replication
#
# Created: Spring 2023
#
###############################################

import zmq
import logging
import time
import threading
from CS6381_MW import discovery_pb2

class BrokerMW:
    def __init__(self, logger):
        self.logger = logger
        self.context = zmq.Context()
        self.sub = None          # SUB socket to receive from publishers
        self.pub = None          # PUB socket to send to subscribers
        self.req = None          # REQ socket for Discovery communications
        self.poller = None       # ZMQ poller for event handling
        self.upcall_obj = None   # Application object for callbacks
        self.addr = None         # Our IP address
        self.port = None         # Our port
        self.zk = None           # ZooKeeper client (shared with app)
        self.is_primary = False  # Whether this instance is the primary
        
        # Replication sockets
        self.repl_pub = None     # PUB socket for sending replication data (primary)
        self.repl_sub = None     # SUB socket for receiving replication data (backup)
    
    def configure(self, args, is_primary, zk_client):
        """Initialize the Broker Middleware"""
        try:
            self.logger.info("BrokerMW::configure")
            
            # Save arguments
            self.addr = args.addr
            self.port = args.port
            self.is_primary = is_primary
            self.zk = zk_client
            
            # Initialize ZMQ poller
            self.poller = zmq.Poller()
            
            # Create PUB socket for subscribers
            self.pub = self.context.socket(zmq.PUB)
            self.pub.bind(f"tcp://*:{args.port}")
            self.logger.info(f"BrokerMW::configure - PUB socket bound to port {args.port}")
            
            # Create SUB socket for receiving from publishers
            self.sub = self.context.socket(zmq.SUB)
            self.sub.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all topics
            self.poller.register(self.sub, zmq.POLLIN)
            self.logger.info("BrokerMW::configure - SUB socket created")
            
            # Set up replication based on role
            if self.is_primary:
                self.setup_primary_replication()
            else:
                self.setup_backup_replication()
            
            # Connect to all existing publishers
            self.connect_to_publishers()
            
            # Set up publisher watcher
            self.zk.ChildrenWatch("/publishers", self.handle_publisher_change)
            
            self.logger.info("BrokerMW::configure - Configuration complete")
            
        except Exception as e:
            self.logger.error(f"BrokerMW::configure - Exception: {str(e)}")
            raise e
    
    def setup_primary_replication(self):
        """Set up replication as primary"""
        try:
            # Create a PUB socket for replicating messages to backups
            self.repl_port = self.port + 1000  # Use port+1000 for replication
            self.repl_pub = self.context.socket(zmq.PUB)
            self.repl_pub.bind(f"tcp://*:{self.repl_port}")
            self.logger.info(f"BrokerMW::setup_primary_replication - Replication PUB socket bound to port {self.repl_port}")
            
            # Clean up any backup subscription if it exists
            if hasattr(self, 'repl_sub') and self.repl_sub:
                self.poller.unregister(self.repl_sub)
                self.repl_sub.close()
                self.repl_sub = None
            
        except Exception as e:
            self.logger.error(f"BrokerMW::setup_primary_replication - Exception: {str(e)}")
            raise e
    
    def setup_backup_replication(self):
        """Set up replication as backup"""
        try:
            # Clean up any primary publication socket if it exists
            if hasattr(self, 'repl_pub') and self.repl_pub:
                self.repl_pub.close()
                self.repl_pub = None
                
            # Get leader address
            leader_data = None
            if self.zk.exists("/broker/leader"):
                data, _ = self.zk.get("/broker/leader")
                leader_data = data.decode()
                
            if leader_data and '|' in leader_data:
                leader_addr = leader_data.split('|')[0]  # Extract address from leader data
                if ':' in leader_addr:
                    addr, port = leader_addr.split(':')
                    repl_port = int(port) + 1000  # Primary's replication port
                    
                    # Create SUB socket to receive replicated messages
                    if hasattr(self, 'repl_sub') and self.repl_sub:
                        self.poller.unregister(self.repl_sub)
                        self.repl_sub.close()
                        
                    self.repl_sub = self.context.socket(zmq.SUB)
                    self.repl_sub.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all topics
                    self.repl_sub.connect(f"tcp://{addr}:{repl_port}")
                    self.poller.register(self.repl_sub, zmq.POLLIN)
                    self.logger.info(f"BrokerMW::setup_backup_replication - Connected to primary's replication at {addr}:{repl_port}")
                    
            else:
                self.logger.warning("BrokerMW::setup_backup_replication - No leader found or invalid leader data")
                
        except Exception as e:
            self.logger.error(f"BrokerMW::setup_backup_replication - Exception: {str(e)}")
    
    def connect_to_publishers(self):
        """Connect to all registered publishers"""
        try:
            self.logger.info("BrokerMW::connect_to_publishers - Connecting to publishers")
            
            if self.zk.exists("/publishers"):
                publishers = self.zk.get_children("/publishers")
                self.logger.info(f"BrokerMW::connect_to_publishers - Found {len(publishers)} publishers")
                
                # Connect to each publisher
                for pub_id in publishers:
                    pub_path = f"/publishers/{pub_id}"
                    if self.zk.exists(pub_path):
                        data, _ = self.zk.get(pub_path)
                        pub_addr = data.decode()
                        if pub_addr:
                            conn_str = f"tcp://{pub_addr}"
                            self.sub.connect(conn_str)
                            self.logger.info(f"BrokerMW::connect_to_publishers - Connected to {pub_id} at {conn_str}")
                
        except Exception as e:
            self.logger.error(f"BrokerMW::connect_to_publishers - Exception: {str(e)}")
    
    def handle_publisher_change(self, publishers):
        """Handle changes to the publisher list in ZooKeeper"""
        try:
            self.logger.info(f"BrokerMW::handle_publisher_change - Publisher list changed: {publishers}")
            
            # Since ZMQ SUB can't easily disconnect from specific endpoints,
            # we need to create a new socket and reconnect to all publishers
            new_sub = self.context.socket(zmq.SUB)
            new_sub.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all topics
            
            # Connect to all current publishers
            for pub_id in publishers:
                try:
                    pub_path = f"/publishers/{pub_id}"
                    if self.zk.exists(pub_path):
                        data, _ = self.zk.get(pub_path)
                        pub_addr = data.decode()
                        if pub_addr:
                            conn_str = f"tcp://{pub_addr}"
                            new_sub.connect(conn_str)
                            self.logger.info(f"BrokerMW::handle_publisher_change - Connected to {pub_id} at {conn_str}")
                except Exception as e:
                    self.logger.error(f"BrokerMW::handle_publisher_change - Error connecting to {pub_id}: {str(e)}")
            
            # Update poller and close old socket
            if self.sub:
                self.poller.unregister(self.sub)
                self.sub.close()
            
            self.sub = new_sub
            self.poller.register(self.sub, zmq.POLLIN)
            self.logger.info("BrokerMW::handle_publisher_change - Updated publisher connections")
            
        except Exception as e:
            self.logger.error(f"BrokerMW::handle_publisher_change - Exception: {str(e)}")
    
    def event_loop(self, timeout=None):
        """Process events for the specified timeout"""
        try:
            # Poll for events
            events = dict(self.poller.poll(timeout=timeout))
            
            # Handle incoming messages from publishers
            if self.sub in events:
                message = self.sub.recv()
                
                # Forward message to subscribers
                if self.pub:
                    self.pub.send(message)
                
                # If primary, replicate to backups
                if self.is_primary and self.repl_pub:
                    self.repl_pub.send(message)
                    
                # Let application know we processed something
                if self.upcall_obj:
                    self.upcall_obj.invoke_operation()
            
            # Handle replicated messages from primary (if backup)
            elif hasattr(self, 'repl_sub') and self.repl_sub and self.repl_sub in events:
                message = self.repl_sub.recv()
                
                # Forward replicated message to subscribers
                if self.pub:
                    self.pub.send(message)
                
                # Let application know we processed something
                if self.upcall_obj:
                    self.upcall_obj.invoke_operation()
                
        except Exception as e:
            self.logger.error(f"BrokerMW::event_loop - Exception: {str(e)}")
    
    def set_upcall_handle(self, upcall_obj):
        """Set the upcall object"""
        self.upcall_obj = upcall_obj
        self.logger.info("BrokerMW::set_upcall_handle - Upcall handle set")
    
    def update_role(self, is_primary):
        """Update our role (primary or backup)"""
        if self.is_primary != is_primary:
            self.logger.info(f"BrokerMW::update_role - Role changing from {'primary' if self.is_primary else 'backup'} to {'primary' if is_primary else 'backup'}")
            self.is_primary = is_primary
            
            # Reconfigure replication based on new role
            if self.is_primary:
                self.setup_primary_replication()
            else:
                self.setup_backup_replication()
    
    def cleanup(self):
        """Clean up all resources"""
        try:
            self.logger.info("BrokerMW::cleanup - Cleaning up resources")
            
            # Close sockets
            if self.sub:
                self.poller.unregister(self.sub)
                self.sub.close()
                
            if self.pub:
                self.pub.close()
                
            if hasattr(self, 'repl_pub') and self.repl_pub:
                self.repl_pub.close()
                
            if hasattr(self, 'repl_sub') and self.repl_sub:
                self.poller.unregister(self.repl_sub)
                self.repl_sub.close()
                
            # Terminate ZMQ context
            self.context.term()
            
        except Exception as e:
            self.logger.error(f"BrokerMW::cleanup - Exception: {str(e)}")

    def driver(self):
        try:
            self.logger.info("BrokerAppln::driver - Starting event loop")
            
            # Run the main event loop
            while True:
                self.mw_obj.event_loop(timeout=100)  # 100ms timeout
                time.sleep(0.01)  # Small sleep to prevent CPU spinning
                
        except KeyboardInterrupt:
            self.logger.info("BrokerAppln::driver - KeyboardInterrupt received")
            self.cleanup()
        except Exception as e:
            self.logger.error(f"BrokerAppln::driver - Exception: {str(e)}")
            self.cleanup()

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


