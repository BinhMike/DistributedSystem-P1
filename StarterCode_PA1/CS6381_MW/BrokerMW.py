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
        
    ########################################
    # Configure Middleware
    ########################################
    def configure(self, args):
        try:
            self.logger.info("BrokerMW::configure")
            self.args = args
            self.addr = args.addr  
            self.port = args.port
            
            # Initialize ZMQ sockets
            self.poller = zmq.Poller()
            
            # Set up SUB socket to receive from publishers
            self.sub = self.context.socket(zmq.SUB)
            self.sub.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all topics
            self.poller.register(self.sub, zmq.POLLIN)
            self.logger.info("BrokerMW::configure - subscribed to all topics")
            
            # Set up PUB socket for subscribers
            self.pub = self.context.socket(zmq.PUB)
            self.pub.bind(f"tcp://*:{args.port}")
            self.logger.info(f"BrokerMW::configure - PUB socket bound to tcp://*:{args.port}")
            
            # Subscribe to current Publishers - use ZK watch in the application
            self.publisher_path = "/publishers"
            self.subscribe_to_publishers()
            
            # Initialize replication sockets if needed
            if self.is_primary:
                # Primary broker's replication socket (PUB for followers)
                self.replication_socket = self.context.socket(zmq.PUB)
                repl_port = self.port + 1000  # Use a different port for replication
                self.replication_socket.bind(f"tcp://*:{repl_port}")
                self.logger.info(f"BrokerMW::configure - Replication PUB socket bound to tcp://*:{repl_port}")
            else:
                # Follower broker's replication listener (SUB from primary)
                self.replication_listener = self.context.socket(zmq.SUB)
                self.replication_listener.setsockopt_string(zmq.SUBSCRIBE, "")
                self.poller.register(self.replication_listener, zmq.POLLIN)
                self.logger.info("BrokerMW::configure - Replication SUB socket ready for primary")
            
            self.logger.info("BrokerMW::configure - Configuration complete")
            
        except Exception as e:
            self.logger.error(f"BrokerMW::configure - Exception: {str(e)}")
            raise e
    
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
            
            # Update the poller to use the new socket
            try:
                if self.sub:
                    self.poller.unregister(self.sub)
                    self.sub.close()
            except Exception as e:
                self.logger.error(f"BrokerMW::handle_publisher_change - Error unregistering old socket: {str(e)}")
                
            # Assign new socket and register with poller
            self.sub = new_sub
            self.poller.register(self.sub, zmq.POLLIN)
            
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
            for endpoint in self.replication_listener._endpoints.keys():
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
        self.is_primary = is_primary
        
        # If becoming primary, initialize replication socket if it doesn't exist
        if is_primary and not hasattr(self, 'replication_socket'):
            self.replication_socket = self.context.socket(zmq.PUB)
            repl_port = self.port + 1000
            self.replication_socket.bind(f"tcp://*:{repl_port}")
            self.logger.info(f"BrokerMW::update_primary_status - Bound replication socket to port {repl_port}")
        
        # If becoming follower, make sure we have a replication listener
        elif not is_primary and not hasattr(self, 'replication_listener'):
            self.replication_listener = self.context.socket(zmq.SUB)
            self.replication_listener.setsockopt_string(zmq.SUBSCRIBE, "")
            self.poller.register(self.replication_listener, zmq.POLLIN)
            self.logger.info("BrokerMW::update_primary_status - Created replication listener")

    ########################################
    # Event Loop
    ########################################
    def event_loop(self, timeout=None):
        """Process events for specified timeout then return control to application"""
        try:
            # Poll for events with the specified timeout
            events = dict(self.poller.poll(timeout=timeout))
            
            if not events and self.upcall_obj:
                # No events within timeout, let application decide what to do
                return
            
            # Handle messages from publishers
            if self.sub and self.sub in events:
                # Receive the message
                topic_and_message = self.sub.recv_string()
                topic, message = topic_and_message.split(":", 1)
                
                self.logger.info(f"BrokerMW::event_loop - Received topic [{topic}] with message [{message}]")
                
                # Primary broker forwards to subscribers directly and replicates
                if self.is_primary and self.pub:
                    self.pub.send_string(topic_and_message)
                    self.logger.info(f"BrokerMW::event_loop - Primary forwarded message on topic [{topic}]")
                    
                    # Replicate to follower brokers
                    if hasattr(self, 'replication_socket') and self.replication_socket:
                        try:
                            self.replication_socket.send_string(topic_and_message)
                            self.logger.debug(f"BrokerMW::event_loop - Replicated message to followers")
                        except Exception as e:
                            self.logger.error(f"BrokerMW::event_loop - Failed to replicate message: {str(e)}")
                
                # Let application know we processed something
                if self.upcall_obj:
                    self.upcall_obj.invoke_operation()
            
            # Handle replication messages (if follower)
            elif hasattr(self, 'replication_listener') and self.replication_listener in events:
                replica_message = self.replication_listener.recv_string()
                topic, message = replica_message.split(":", 1)
                
                self.logger.info(f"BrokerMW::event_loop - Follower received replicated topic [{topic}] with message [{message}]")
                
                # Forward replicated message to subscribers
                if self.pub:
                    self.pub.send_string(replica_message)
                
                # Let application know we processed something
                if self.upcall_obj:
                    self.upcall_obj.invoke_operation()
                
        except Exception as e:
            self.logger.error(f"BrokerMW::event_loop - Exception: {str(e)}")
            # Log the full traceback for debugging
            self.logger.error(f"BrokerMW::event_loop - Traceback: {traceback.format_exc()}")
    
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
            
            # Make sure we're no longer primary
            self.is_primary = False
            
            # Clean up sockets
            if self.sub:
                self.poller.unregister(self.sub)
                self.sub.close()
                
            if self.pub:
                self.pub.close()
                
            if hasattr(self, 'replication_socket') and self.replication_socket:
                self.replication_socket.close()
                
            if hasattr(self, 'replication_listener') and self.replication_listener:
                self.poller.unregister(self.replication_listener)
                self.replication_listener.close()
            
            # Terminate ZMQ context
            self.context.term()
            
        except Exception as e:
            self.logger.error(f"BrokerMW::cleanup - Error: {str(e)}")
