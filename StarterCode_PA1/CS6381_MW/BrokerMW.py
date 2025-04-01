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
            
            # Initialize ZMQ sockets
            self.poller = zmq.Poller()
            
            # Setup publisher and subscriber sockets
            self._configure_pub_socket()
            self._configure_sub_socket()
            
            # Set up publisher path and initial subscriptions
            self.publisher_path = "/publishers"
            self.subscribe_to_publishers()
            
            # Set up watch for publisher changes
            self._configure_publisher_watch()
            
            # Configure replication sockets based on primary status
            self._configure_replication()
            
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
        repl_port = self.port + 1000  # Use different port for replication
        
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
    # Subscribe to Publishers
    ########################################
    def subscribe_to_publishers(self):
        """Subscribe to all registered publishers"""
        try:
            self.logger.info("BrokerMW::subscribe_to_publishers - Checking for publishers in ZooKeeper")
            
            if self.zk.exists(self.publisher_path):
                publishers = self.zk.get_children(self.publisher_path)
                self.logger.info(f"BrokerMW::subscribe_to_publishers - Found {len(publishers)} publishers: {publishers}")
                
                for pub_id in publishers:
                    pub_node_path = f"{self.publisher_path}/{pub_id}"
                    if self.zk.exists(pub_node_path):
                        pub_data, _ = self.zk.get(pub_node_path)
                        pub_address = pub_data.decode()
                        connection_url = f"tcp://{pub_address}"
                        self.sub.connect(connection_url)
                        self.logger.info(f"BrokerMW::subscribe_to_publishers - Connected to Publisher {pub_id} at {connection_url}")
            else:
                self.logger.warning(f"BrokerMW::subscribe_to_publishers - Path {self.publisher_path} doesn't exist yet")
                
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
            
            # Connect to all publishers with new socket
            if self.zk.exists(self.publisher_path):
                publishers = self.zk.get_children(self.publisher_path)
                for pub_id in publishers:
                    try:
                        pub_node_path = f"{self.publisher_path}/{pub_id}"
                        if self.zk.exists(pub_node_path):
                            pub_data, _ = self.zk.get(pub_node_path)
                            pub_address = pub_data.decode()
                            new_sub.connect(f"tcp://{pub_address}")
                            self.logger.info(f"BrokerMW::handle_publisher_change - Connected to Publisher {pub_id} at {pub_address}")
                    except Exception as e:
                        self.logger.error(f"BrokerMW::handle_publisher_change - Error connecting to publisher {pub_id}: {str(e)}")
            
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
            repl_port = primary_port + 1000  # Replication port is base port + 1000
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
            repl_port = self.port + 1000
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
            self.logger.error(f"BrokerMW::event_loop - Traceback: {traceback.format_exc()}")

    def _handle_publisher_message(self):
        """Handle messages from publishers"""
        message = self.sub.recv_string()
        parts = message.split(":", 1)
        topic = parts[0]
        
        # Add a message identifier to track duplicates
        message_id = hash(message + str(time.time()))
        
        # Check if we've recently seen this message (implement a deduplication cache)
        if hasattr(self, 'recent_messages') and message in self.recent_messages:
            self.logger.debug(f"BrokerMW::_handle_publisher_message - Ignoring duplicate message on topic [{topic}]")
            return
        
        # Store message in recent cache
        if not hasattr(self, 'recent_messages'):
            self.recent_messages = set()
        self.recent_messages.add(message)
        # Limit cache size to prevent memory issues
        if len(self.recent_messages) > 1000:
            self.recent_messages.pop()
            
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
        if not hasattr(self, '_last_notify_time') or time.time() - self._last_notify_time > 0.1:
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
            if hasattr(self, 'is_primary') and self.is_primary:
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
        
            # Terminate ZMQ context
            if hasattr(self, 'context') and self.context:
                self.context.term()
                self.context = None
            
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
            
            # Create and register broker node
            broker_path = "/brokers"
            self.ensure_path_exists(broker_path)
            
            broker_node = f"{broker_path}/{name}"
            address_str = f"{self.addr}:{self.port}"
            self.zk_update_node(broker_node, address_str, ephemeral=True)
            
            # Only register as a "publisher" if primary
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
                # Register as a publisher
                address_str = f"{self.addr}:{self.port}"
                self.zk_update_node(broker_as_pub_node, address_str, ephemeral=True)
                self.logger.info(f"BrokerMW::_update_publisher_registration - Registered as publisher: {broker_as_pub_node}")
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
        """Create or update a ZooKeeper node with data"""
        try:
            if isinstance(data, str):
                encoded_data = data.encode()
            else:
                encoded_data = data
                
            if self.zk.exists(path):
                self.zk.set(path, encoded_data)
                self.logger.info(f"BrokerMW::zk_update_node - Updated: {path}")
            else:
                self.zk.create(path, encoded_data, ephemeral=ephemeral)
                self.logger.info(f"BrokerMW::zk_update_node - Created: {path}")
            return True
        except Exception as e:
            self.logger.error(f"BrokerMW::zk_update_node - Error with {path}: {str(e)}")
            return False

    ########################################
    # ZooKeeper Delete Node
    ########################################
    def zk_delete_node(self, path):
        """Delete a ZooKeeper node if it exists"""
        try:
            if self.zk.exists(path):
                self.zk.delete(path)
                self.logger.info(f"BrokerMW::zk_delete_node - Deleted: {path}")
            return True
        except Exception as e:
            self.logger.error(f"BrokerMW::zk_delete_node - Error with {path}: {str(e)}")
            return False
