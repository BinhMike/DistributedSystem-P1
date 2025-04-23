###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.


import zmq
import logging
import time
import traceback
import threading
from CS6381_MW import discovery_pb2  

# Constants
REPLICATION_PORT_OFFSET = 1000
DEDUPLICATION_CACHE_SIZE = 1000
NOTIFICATION_INTERVAL = 0.1
ZK_BROKER_BASE_PATH = "/brokers"
ZK_PUBLISHER_PATH = "/publishers"
ZK_LOAD_BALANCER_PATH = "/load_balancers"
ZK_REPLICA_ROLE = "replica"
ZK_PRIMARY_ROLE = "primary"

class BrokerMW():
    ########################################
    # Constructor
    ########################################
    def __init__(self, logger, zk_client, is_primary=False):
        self.logger = logger
        self.zk = zk_client
        self.context = zmq.Context()
        self.sub = None          # SUB socket to receive from publishers
        self.pub = None          # PUB socket to send to subscribers
        self.req = None          # REQ socket for Discovery communications
        self.poller = None
        self.upcall_obj = None   # Application logic handle
        self.addr = None
        self.port = None
        self.is_primary = is_primary  # Flag indicating if this is the primary broker
        self.socket_lock = threading.Lock()  # Lock for thread-safe socket operations
        self.subscriptions = {}  # Map of topics to subscriber identities
        
        # Initialize recent messages cache for deduplication
        self.recent_messages = set()
        self.dedup_cache_size = DEDUPLICATION_CACHE_SIZE # Use constant
        
        # Store last notification time to avoid tight loop
        self._last_notify_time = 0
        self._notify_interval = NOTIFICATION_INTERVAL # Use constant
        
        # Initialize load balancer connection attributes
        self.using_lb = False    # Flag to indicate if we're using a load balancer
        self.lb_push = None      # PUSH socket for communication with load balancer
        self.lb_addr = None      # Load balancer address
        self.lb_port = None      # Load balancer port
        self.group_name = None   # Broker group name
        
    ########################################
    # Configure Middleware
    ########################################
    def configure(self, args):
        """Initialize and configure the broker middleware"""
        try:
            self.logger.info("BrokerMW::configure")
            self.args = args
            self.addr = args.addr  
            self.port = args.port
            
            # Store group name from args if available
            if hasattr(args, 'group'):
                self.group_name = args.group
                self.logger.info(f"BrokerMW::configure - Using group name: {self.group_name}")
            
            # Initialize ZMQ sockets
            self.poller = zmq.Poller()
            
            # Setup publisher and subscriber sockets
            self._configure_pub_socket()
            self._configure_sub_socket()
            
            # Set up publisher path and initial subscriptions
            self.publisher_path = ZK_PUBLISHER_PATH # Use constant
            self.subscribe_to_publishers()
            
            # Set up watch for publisher changes
            self._configure_publisher_watch()
            
            # Configure replication sockets based on primary status
            self._configure_replication()
            
            # Automatically look for and connect to load balancer if available
            self._setup_lb_watch()
            if self._find_and_connect_to_lb():
                self.logger.info("BrokerMW::configure - Connected to load balancer automatically")
            else:
                self.logger.info("BrokerMW::configure - No load balancer found, operating in standalone mode")
            
            self.logger.info("BrokerMW::configure - Configuration complete")
            
        except Exception as e:
            self.logger.error(f"BrokerMW::configure - Exception: {str(e)}")
            raise e

    def _configure_pub_socket(self):
        """Configure the publication socket for subscribers"""
        self.pub = self.create_socket(zmq.PUB)
        self.pub.bind(f"tcp://*:{self.port}")
        self.logger.info(f"BrokerMW::_configure_pub_socket - PUB socket bound to tcp://*:{self.port}")

    def _configure_sub_socket(self):
        """Configure the subscription socket for publishers"""
        self.sub = self.create_socket(zmq.SUB, [(zmq.SUBSCRIBE, "")])
        self.poller.register(self.sub, zmq.POLLIN)
        self.logger.info("BrokerMW::_configure_sub_socket - SUB socket ready for publishers")

    def _configure_publisher_watch(self):
        """Configure ZooKeeper watch for publisher changes"""
        @self.zk.ChildrenWatch(self.publisher_path)
        def watch_publishers(children):
            self.logger.info(f"BrokerMW::_configure_publisher_watch - Publisher list changed: {children}")
            self.handle_publisher_change(children)

    def _configure_replication(self):
        """Configure replication sockets based on primary/follower role"""
        repl_port = self.port + REPLICATION_PORT_OFFSET # Use constant
        
        if self.is_primary:
            # Primary broker's replication socket (PUB for followers)
            self.replication_socket = self.create_socket(zmq.PUB)
            self.replication_socket.bind(f"tcp://*:{repl_port}")
            self.logger.info(f"BrokerMW::_configure_replication - Replication PUB socket bound to tcp://*:{repl_port}")
        else:
            # Follower broker's replication listener (SUB from primary)
            self.replication_listener = self.create_socket(zmq.SUB, [(zmq.SUBSCRIBE, "")])
            self.poller.register(self.replication_listener, zmq.POLLIN)
            self.logger.info("BrokerMW::_configure_replication - Replication SUB socket ready for primary")
    
    ########################################
    # Connect Socket to Publishers (Helper)
    ########################################
    def _connect_socket_to_publishers(self, sub_socket):
        """Connects the given SUB socket to all registered publishers found in ZooKeeper."""
        try:
            self.logger.info(f"BrokerMW::_connect_socket_to_publishers - Connecting socket {sub_socket}")
            if self.zk.exists(self.publisher_path): # publisher_path uses constant
                publishers = self.zk.get_children(self.publisher_path)
                self.logger.info(f"BrokerMW::_connect_socket_to_publishers - Found {len(publishers)} publishers: {publishers}")
                
                connected_count = 0
                for pub_id in publishers:
                    # skip our own broker-registration node if we have one
                    if hasattr(self, 'broker_name') and pub_id == self.broker_name:
                        continue
                        
                    pub_node_path = f"{self.publisher_path}/{pub_id}"
                    if self.zk.exists(pub_node_path):
                        try:
                            pub_data, _ = self.zk.get(pub_node_path)
                            data_str = pub_data.decode()
                            # skip broker registrations (they include "group=")
                            if "group=" in data_str:
                                continue
                            pub_address = data_str
                            
                            # Extract just the address part if there's topic mapping data
                            if "|" in pub_address:
                                pub_address = pub_address.split("|")[0]
                                self.logger.debug(f"BrokerMW::_connect_socket_to_publishers - Extracted address {pub_address} from publisher data")
                            
                            connection_url = f"tcp://{pub_address}"
                            sub_socket.connect(connection_url)
                            self.logger.info(f"BrokerMW::_connect_socket_to_publishers - Connected socket to Publisher {pub_id} at {connection_url}")
                            connected_count += 1
                        except Exception as connect_e:
                            self.logger.error(f"BrokerMW::_connect_socket_to_publishers - Error connecting to publisher {pub_id}: {str(connect_e)}")
                self.logger.info(f"BrokerMW::_connect_socket_to_publishers - Connected socket to {connected_count} publishers.")
            else:
                self.logger.warning(f"BrokerMW::_connect_socket_to_publishers - Path {self.publisher_path} doesn't exist yet") # publisher_path uses constant
        except Exception as e:
            self.logger.error(f"BrokerMW::_connect_socket_to_publishers - Error: {str(e)}")

    ########################################
    # Subscribe to Publishers
    ########################################
    def subscribe_to_publishers(self):
        """Subscribe to all registered publishers"""
        try:
            self.logger.info("BrokerMW::subscribe_to_publishers - Checking for publishers in ZooKeeper")
            # Use the helper method to connect the main SUB socket
            self._connect_socket_to_publishers(self.sub)
            
        except Exception as e:
            self.logger.error(f"BrokerMW::subscribe_to_publishers - Error: {str(e)}")

    ########################################
    # Handle Publisher Changes
    ########################################
    def handle_publisher_change(self, children):
        """Handle changes in the publisher list"""
        try:
            self.logger.info(f"BrokerMW::handle_publisher_change - Publisher list changed: {children}")
            
            # Create new socket before closing old one
            new_sub = self.context.socket(zmq.SUB)
            new_sub.setsockopt_string(zmq.SUBSCRIBE, "")  # Receive all topics
            
            # Connect the new socket to all current publishers using the helper method
            self._connect_socket_to_publishers(new_sub)
            
            # Hold a reference to the old socket
            old_sub = self.sub
            
            # Use lock when modifying poller and sockets
            with self.socket_lock:
                # Now safely unregister the old socket BEFORE updating the reference
                try:
                    if old_sub:
                        self.poller.unregister(old_sub)
                except Exception as e:
                    self.logger.error(f"BrokerMW::handle_publisher_change - Error unregistering old socket: {str(e)}")
                
                # Register the new socket with the poller
                self.poller.register(new_sub, zmq.POLLIN)
                
                # Make the new socket the current one AFTER updating the poller
                self.sub = new_sub
            
            # Now close the old socket
            try:
                if old_sub:
                    old_sub.close()
            except Exception as e:
                self.logger.error(f"BrokerMW::handle_publisher_change - Error closing old socket: {str(e)}")
            
        except Exception as e:
            self.logger.error(f"BrokerMW::handle_publisher_change - Error: {str(e)}")

    ########################################
    # Connect to Primary (for followers)
    ########################################
    def connect_to_primary(self, primary_addr, primary_port):
        """Connect follower's replication listener to primary's replication socket"""
        if not hasattr(self, 'replication_listener'):
            self.logger.error("BrokerMW::connect_to_primary - No replication listener available")
            return False
        
        try:
            repl_port = primary_port + REPLICATION_PORT_OFFSET # Use constant
            primary_endpoint = f"tcp://{primary_addr}:{repl_port}"
            
            # If we already have existing connections, disconnect first
            # Create a copy of the keys to avoid "dictionary changed during iteration" error
            endpoints = list(self.replication_listener._endpoints.keys())
            for endpoint in endpoints:
                self.replication_listener.disconnect(endpoint)
                self.logger.info(f"BrokerMW::connect_to_primary - Disconnected from old primary at {endpoint}")
            
            # Connect to the new primary
            self.replication_listener.connect(primary_endpoint)
            self.logger.info(f"BrokerMW::connect_to_primary - Connected to primary's replication at {primary_endpoint}")
            return True
            
        except Exception as e:
            self.logger.error(f"BrokerMW::connect_to_primary - Failed to connect: {str(e)}")
            return False

    ########################################
    # Connect to Load Balancer
    ########################################
    def connect_to_lb(self, lb_addr, lb_port, group_name):
        """Connect to the load balancer for managed message routing"""
        try:
            self.logger.info(f"BrokerMW::connect_to_lb - Connecting to LB at {lb_addr}:{lb_port}")
            
            # 1. PUSH socket for sending messages to load balancer's PULL socket
            self.lb_push = self.create_socket(zmq.PUSH)
            self.lb_push.setsockopt(zmq.LINGER, 1000)
            self.lb_push.setsockopt(zmq.SNDTIMEO, 5000)
            lb_push_endpoint = f"tcp://{lb_addr}:{lb_port}"
            self.lb_push.connect(lb_push_endpoint)
            self.logger.info(f"Connected PUSH socket to LB at {lb_push_endpoint}")
            
            # Store group name and LB info for identification
            self.group_name = group_name
            self.lb_addr = lb_addr
            self.lb_port = int(lb_port)
            self.using_lb = True
            
            # Send a test message to confirm connection
            try:
                test_msg = f"{group_name}_BROKER_ONLINE".encode()
                self.lb_push.send(test_msg)
                self.logger.info("Test message sent to LB")
            except Exception as e:
                self.logger.warning(f"Test message send failed: {str(e)}")
                # Continue anyway - non-critical
            
            return True
        except Exception as e:
            self.logger.error(f"BrokerMW::connect_to_lb - Error: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    ########################################
    # Check Load Balancer Connection
    ########################################
    def check_lb_connection(self):
        """Check if we're still connected to load balancer"""
        if not self.using_lb:
            return True  # Not using LB, so no connection issues
            
        if not hasattr(self, 'lb_push') or self.lb_push is None:
            self.logger.warning("LB PUSH socket not initialized")
            self.using_lb = False
            return False
            
        try:
            # Send a heartbeat to LB
            test_msg = f"{self.group_name}_BROKER_HEARTBEAT".encode()
            self.lb_push.send(test_msg, zmq.NOBLOCK)
            return True
        except Exception as e:
            self.logger.warning(f"LB connection may be down: {str(e)}")
            return False

    ########################################
    # Update Primary Status
    ########################################
    def update_primary_status(self, is_primary):
        """Update whether this broker is primary or not"""
        if self.is_primary == is_primary:
            return  # No change in status
            
        self.logger.info(f"BrokerMW::update_primary_status - Changing primary status to: {is_primary}")
        
        # Handle socket transitions based on new role
        if is_primary:
            self._transition_to_primary()
        else:
            self._transition_to_follower()
        
        # Update our status
        self.is_primary = is_primary
        
        # Update ZooKeeper registration based on new status
        self._update_publisher_registration(is_primary)

    def _transition_to_primary(self):
        """Handle transition from follower to primary role"""
        # Initialize replication socket if needed
        if not hasattr(self, 'replication_socket'):
            repl_port = self.port + REPLICATION_PORT_OFFSET # Use constant
            self.replication_socket = self.create_socket(zmq.PUB)
            self.replication_socket.bind(f"tcp://*:{repl_port}")
            self.logger.info(f"BrokerMW::_transition_to_primary - Bound replication socket to port {repl_port}")
        
        # Close replication listener if it exists
        if hasattr(self, 'replication_listener') and self.replication_listener:
            self.safe_socket_close(self.replication_listener)
            del self.replication_listener
            self.logger.info("BrokerMW::_transition_to_primary - Closed replication listener")

    def _transition_to_follower(self):
        """Handle transition from primary to follower role"""
        # Create replication listener if needed
        if not hasattr(self, 'replication_listener') or not self.replication_listener:
            self.replication_listener = self.create_socket(zmq.SUB, [(zmq.SUBSCRIBE, "")])
            self.poller.register(self.replication_listener, zmq.POLLIN)
            self.logger.info("BrokerMW::_transition_to_follower - Created replication listener")
        
        # Close replication socket if it exists
        if hasattr(self, 'replication_socket') and self.replication_socket:
            self.safe_socket_close(self.replication_socket, unregister=False)
            del self.replication_socket
            self.logger.info("BrokerMW::_transition_to_follower - Closed replication socket")

    ########################################
    # Event Loop
    ########################################
    def event_loop(self, timeout=None):
        """Process events for specified timeout then return control to application"""
        try:
            # Make sure we have valid sockets to poll
            if not self.sub:
                time.sleep(0.01)  # Short sleep to prevent CPU spinning
                return
                
            # Use lock when accessing poller
            with self.socket_lock:
                # Poll for events with the specified timeout
                events = dict(self.poller.poll(timeout=timeout))
            
            if not events and self.upcall_obj:
                # No events within timeout, let application decide what to do
                return
            
            # Process events from different sockets
            if self.sub in events:
                self._handle_publisher_message()
                
            if hasattr(self, 'replication_listener') and self.replication_listener in events:
                self._handle_replication_message()
                
        except Exception as e:
            self.logger.error(f"BrokerMW::event_loop - Exception: {str(e)}")

    def _handle_publisher_message(self):
        """Handle messages from publishers"""
        message = self.sub.recv_string()
        parts = message.split(":", 1)
        topic = parts[0]
        
        # Add a message identifier to track duplicates
        message_id = hash(message + str(time.time()))
        
        # Check if we've recently seen this message (implement a deduplication cache)
        if message in self.recent_messages:
            self.logger.debug(f"BrokerMW::_handle_publisher_message - Ignoring duplicate message on topic [{topic}]")
            return
        
        # Store message in recent cache
        self.recent_messages.add(message)
        # Limit cache size to prevent memory issues
        if len(self.recent_messages) > self.dedup_cache_size: # Use constant
            # Efficiently remove an arbitrary element if using set
            try:
                self.recent_messages.pop()
            except KeyError: # Handle case where set might be empty unexpectedly
                pass 
            
        self.logger.info(f"BrokerMW::_handle_publisher_message - Received message on topic [{topic}]")
        
        # Primary broker forwards to subscribers directly and replicates
        if self.is_primary and self.pub:
            # Forward the complete original message
            self.pub.send_string(message)
            self.logger.info(f"BrokerMW::_handle_publisher_message - Primary forwarded message on topic [{topic}]")
            
            # Replicate to follower brokers
            self._replicate_message(message)
        
        # Let application know we processed something, but avoid tight loop
        # Only notify application occasionally to prevent rapid re-polling
        if time.time() - self._last_notify_time > self._notify_interval: # Use constant
            self._notify_application()
            self._last_notify_time = time.time()

    def _handle_replication_message(self):
        """Handle messages from primary broker (replication)"""
        message = self.replication_listener.recv_string()
        parts = message.split(":", 1)
        topic = parts[0]
        
        self.logger.info(f"BrokerMW::_handle_replication_message - Follower received replicated message on topic [{topic}]")
        
        # Forward replicated message to subscribers
        if self.pub:
            self.pub.send_string(message)
        
        # Let application know we processed something
        self._notify_application()

    def _replicate_message(self, message):
        """Replicate a message to follower brokers"""
        if hasattr(self, 'replication_socket') and self.replication_socket:
            try:
                self.replication_socket.send_string(message)
                self.logger.debug("BrokerMW::_replicate_message - Replicated message to followers")
            except Exception as e:
                self.logger.error(f"BrokerMW::_replicate_message - Failed to replicate message: {str(e)}")

    def _notify_application(self):
        """Notify the application layer of an event"""
        if self.upcall_obj:
            self.upcall_obj.invoke_operation()

    ########################################
    # Set Upcall Handle
    ########################################
    def set_upcall_handle(self, upcall_obj):
        """Set the upcall object for application-level callbacks"""
        self.logger.info("BrokerMW::set_upcall_handle - Setting upcall handle")
        self.upcall_obj = upcall_obj

    ########################################
    # Cleanup
    ########################################
    def cleanup(self):
        """Clean up all resources"""
        try:
            self.logger.info("BrokerMW::cleanup - Cleaning up resources")
            
            # Make sure we're no longer primary and remove from ZK
            if self.is_primary:
                self._update_publisher_registration(False)
                self.is_primary = False
            
            # Clean up sockets
            if self.sub:
                self.safe_socket_close(self.sub)
                self.sub = None
                
            if self.pub:
                self.safe_socket_close(self.pub, unregister=False)
                self.pub = None
            
            if hasattr(self, 'replication_socket') and self.replication_socket:
                self.safe_socket_close(self.replication_socket, unregister=False)
                self.replication_socket = None
            
            if hasattr(self, 'replication_listener') and self.replication_listener:
                self.safe_socket_close(self.replication_listener)
                self.replication_listener = None
                
            # Add cleanup for load balancer sockets
            if hasattr(self, 'lb_push') and self.lb_push:
                self.safe_socket_close(self.lb_push)
                self.lb_push = None
        
            # Terminate ZMQ context
            if self.context:
                self.context.term()
                self.context = None
            
            # Add explicit ZooKeeper cleanup
            if self.zk:
                self.logger.info("BrokerMW::cleanup - Closing ZooKeeper connection")
                self.zk.stop()
                self.zk.close()
            
            self.logger.info("BrokerMW::cleanup - Cleanup complete")
            
        except Exception as e:
            self.logger.error(f"BrokerMW::cleanup - Error: {str(e)}")

    ########################################
    # Register with Discovery Service
    ########################################
    def register(self, name, topic_list=None):
        """Register the broker with the discovery service via ZooKeeper"""
        try:
            self.logger.info(f"BrokerMW::register - Registering broker {name}")
            self.broker_name = name  # Store the name for future use
            
            # Create and register broker node in the group structure
            broker_base_path = ZK_BROKER_BASE_PATH # Use constant
            self.ensure_path_exists(broker_base_path)
            
            # Use group name from args directly instead of parsing from name
            if hasattr(self.args, 'group'):
                group_name = self.args.group
                self.logger.info(f"BrokerMW::register - Using group name from args: {group_name}")
            else:
                # Fallback to parsing from name 
                group_name = "default_group"
                if "group" in name:
                    parts = name.split("group")
                    if len(parts) > 1 and parts[1]:
                        group_name = f"group{parts[1].split('_')[0]}"
                self.logger.info(f"BrokerMW::register - Parsed group name from broker name: {group_name}")
            
            # Store group name for future use
            self.group_name = group_name
            
            # Create group path
            group_path = f"{broker_base_path}/{group_name}"
            self.ensure_path_exists(group_path)
            
            # Ensure replicas path exists
            replicas_path = f"{group_path}/replicas"
            self.ensure_path_exists(replicas_path)
            
            # Register this broker as a replica with additional metadata
            replica_node = f"{replicas_path}/{self.addr}:{self.port}"
            
            # Include more detailed information in the replica node
            # This helps subscribers with direct connections if needed
            role = ZK_PRIMARY_ROLE if self.is_primary else ZK_REPLICA_ROLE # Use constants
            address_str = f"{self.addr}:{self.port}:{role}"
            
            # If we have group information, include it
            if hasattr(self, 'group_name') and self.group_name:
                address_str += f"|group={self.group_name}"
                
            self.zk_update_node(replica_node, address_str, ephemeral=True)
            self.logger.info(f"BrokerMW::register - Registered as replica at {replica_node} with data {address_str}")
            
            # Only register as a "publisher" if primary (for discovery compatibility if needed)
            self._update_publisher_registration(self.is_primary)
            
            return True
        except Exception as e:
            self.logger.error(f"BrokerMW::register - Error: {str(e)}")
            return False

    def _update_publisher_registration(self, register=True):
        """Update or remove broker's registration as a publisher"""
        if not hasattr(self, 'broker_name'):
            self.logger.warning("BrokerMW::_update_publisher_registration - No broker name available")
            return False
            
        try:
            # Ensure publisher path exists
            self.ensure_path_exists(self.publisher_path)
            broker_as_pub_node = f"{self.publisher_path}/{self.broker_name}"
            
            if register:
                # Register as a publisher with extra metadata to help subscribers
                # Include role and group info in the registration data
                if hasattr(self, 'group_name'):
                    address_str = f"{self.addr}:{self.port}|group={self.group_name}"
                else:
                    address_str = f"{self.addr}:{self.port}"
                
                self.zk_update_node(broker_as_pub_node, address_str, ephemeral=True)
                self.logger.info(f"BrokerMW::_update_publisher_registration - Registered as publisher: {broker_as_pub_node} with data {address_str}")
            else:
                # Remove publisher registration
                self.zk_delete_node(broker_as_pub_node)
                self.logger.info(f"BrokerMW::_update_publisher_registration - Removed publisher registration: {broker_as_pub_node}")
            
            return True
        except Exception as e:
            self.logger.error(f"BrokerMW::_update_publisher_registration - Error: {str(e)}")
            return False

    ########################################
    # Create Socket
    ########################################
    def create_socket(self, socket_type, socket_options=None):
        """Create and configure a ZMQ socket of the specified type"""
        try:
            socket = self.context.socket(socket_type)
            
            # Apply any socket options if provided
            if socket_options:
                for option, value in socket_options:
                    if isinstance(value, str):
                        socket.setsockopt_string(option, value)
                    else:
                        socket.setsockopt(option, value)
            
            return socket
        except Exception as e:
            self.logger.error(f"BrokerMW::create_socket - Error: {str(e)}")
            raise e

    ########################################
    # Safe Socket Close
    ########################################
    def safe_socket_close(self, socket, unregister=True):
        """Safely close a socket and unregister from poller if needed"""
        if not socket:
            return
            
        try:
            if unregister and self.poller:
                try:
                    self.poller.unregister(socket)
                except Exception as e:
                    self.logger.error(f"BrokerMW::safe_socket_close - Unregister error: {str(e)}")
            
            socket.close()
        except Exception as e:
            self.logger.error(f"BrokerMW::safe_socket_close - Close error: {str(e)}")

    ########################################
    # Ensure Path Exists
    ########################################
    def ensure_path_exists(self, path):
        """Create ZooKeeper path if it doesn't exist"""
        try:
            if not self.zk.exists(path):
                self.zk.create(path, b"", makepath=True)
                self.logger.info(f"BrokerMW::ensure_path_exists - Created path: {path}")
            return True
        except Exception as e:
            self.logger.error(f"BrokerMW::ensure_path_exists - Error: {str(e)}")
            return False

    ########################################
    # ZooKeeper Update Node
    ########################################
    def zk_update_node(self, path, data, ephemeral=True):
        """Create or update a ZooKeeper node with data. Handles ephemeral node updates correctly."""
        try:
            encoded_data = data.encode() if isinstance(data, str) else data
                
            if self.zk.exists(path):
                # For ephemeral nodes, you cannot directly update them if the session differs.
                # A common pattern is delete-then-create if the update needs to be ephemeral 
                # and tied to the current session, especially if the original creator might be gone.
                # However, if the SAME process is updating its OWN ephemeral node, set() works.
                # Assuming the broker updates its own registration, set() is fine.
                # If this method could be called to update nodes created by others, 
                # a delete/create might be safer for ephemeral nodes.
                self.zk.set(path, encoded_data)
                self.logger.info(f"BrokerMW::zk_update_node - Updated: {path}")
            else:
                # Create the node, ensuring parent paths exist if necessary (makepath=True)
                self.zk.create(path, encoded_data, ephemeral=ephemeral, makepath=True)
                self.logger.info(f"BrokerMW::zk_update_node - Created: {path} (ephemeral={ephemeral})")
            return True
        except Exception as e:
            # Log specific Kazoo exceptions if helpful
            self.logger.error(f"BrokerMW::zk_update_node - Error with {path}: {str(e)}")
            return False

    ########################################
    # ZooKeeper Delete Node
    ########################################
    def zk_delete_node(self, path, recursive=False):
        """Delete a ZooKeeper node if it exists."""
        try:
            if self.zk.exists(path):
                # Use recursive=True if you need to delete children as well
                self.zk.delete(path, recursive=recursive)
                self.logger.info(f"BrokerMW::zk_delete_node - Deleted: {path} (recursive={recursive})")
            else:
                self.logger.info(f"BrokerMW::zk_delete_node - Node not found, skipping deletion: {path}")
            return True
        except Exception as e:
            self.logger.error(f"BrokerMW::zk_delete_node - Error with {path}: {str(e)}")
            return False

    ########################################
    # Setup Load Balancer Watch
    ########################################
    def _setup_lb_watch(self):
        """Set up a watch for load balancer nodes in ZooKeeper"""
        try:
            # Make sure the path exists
            lb_path = ZK_LOAD_BALANCER_PATH # Use constant
            if not self.zk.exists(lb_path):
                self.logger.info(f"BrokerMW::_setup_lb_watch - Creating load balancer path in ZooKeeper: {lb_path}")
                self.zk.ensure_path(lb_path)
            
            # Set up the watch function
            @self.zk.ChildrenWatch(lb_path)
            def watch_lb(children):
                self.logger.info(f"BrokerMW::_setup_lb_watch - Load balancers changed: {children}")
                if children and not self.using_lb:
                    self.logger.info("BrokerMW::_setup_lb_watch - Load balancer detected after startup, attempting to connect")
                    self._find_and_connect_to_lb()
            
            self.logger.info("BrokerMW::_setup_lb_watch - Set up watch for load balancers")
            return True
        except Exception as e:
            self.logger.error(f"BrokerMW::_setup_lb_watch - Error setting up load balancer watch: {str(e)}")
            return False
            
    ########################################
    # Find and Connect to Load Balancer
    ########################################
    def _find_and_connect_to_lb(self):
        """Find load balancer in ZooKeeper and connect to it"""
        try:
            lb_path = ZK_LOAD_BALANCER_PATH # Use constant
            if not self.zk.exists(lb_path):
                self.logger.info("BrokerMW::_find_and_connect_to_lb - Load balancer path doesn't exist yet")
                return False
            
            # Get list of load balancers
            lb_nodes = self.zk.get_children(lb_path)
            if not lb_nodes:
                self.logger.info("BrokerMW::_find_and_connect_to_lb - No load balancer nodes found")
                return False
            
            # Pick the first one
            lb_node = lb_nodes[0]
            lb_data_path = f"{lb_path}/{lb_node}"
            
            # Get load balancer connection info
            if not self.zk.exists(lb_data_path):
                self.logger.warning(f"BrokerMW::_find_and_connect_to_lb - Load balancer node {lb_node} disappeared")
                return False
                
            lb_data, _ = self.zk.get(lb_data_path)
            if not lb_data:
                self.logger.warning("BrokerMW::_find_and_connect_to_lb - Load balancer data is empty")
                return False
                
            # Parse connection info
            lb_info = lb_data.decode().split(":")
            if len(lb_info) < 3:
                self.logger.error(f"BrokerMW::_find_and_connect_to_lb - Invalid load balancer data format: {lb_data.decode()}")
                return False
                
            lb_addr, pub_port, sub_port = lb_info[0], lb_info[1], lb_info[2]
            self.logger.info(f"BrokerMW::_find_and_connect_to_lb - Found load balancer at {lb_addr}:{pub_port}")
            
            # Connect to load balancer using the group name from args
            group = self.group_name or "default_group"
            result = self.connect_to_lb(lb_addr, int(pub_port), group)
            
            return result
                
        except Exception as e:
            self.logger.error(f"BrokerMW::_find_and_connect_to_lb - Error finding/connecting to load balancer: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
