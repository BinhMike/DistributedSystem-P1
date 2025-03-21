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
        self.discovery_addr = None  # Add this to track Discovery service address
        
        # Message queue for tracking received messages
        self.message_queue = []
        
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
            
            # Add REQ socket for Discovery communications
            self.req = self.context.socket(zmq.REQ)
            discovery_url = f"tcp://{args.addr}:{5555}"  # Default Discovery port
            self.req.connect(discovery_url)
            self.logger.info(f"BrokerMW::configure - Connected to Discovery at {discovery_url}")
            self.discovery_addr = f"{args.addr}:{5555}"
            
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
                
            # Get leader address - Fix path to match the new path structure
            leader_data = None
            if self.zk.exists("/brokers/leader"):  # Changed from "/broker/leader"
                data, _ = self.zk.get("/brokers/leader")  # Changed from "/broker/leader"
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
            
            # Save the old socket first to avoid race conditions
            old_sub = self.sub
            
            # Safely update poller and socket - register new socket first
            self.poller.register(new_sub, zmq.POLLIN)
            self.sub = new_sub
            
            # Then unregister and close old socket
            if old_sub:
                try:
                    self.poller.unregister(old_sub)
                    old_sub.close()
                except Exception as e:
                    self.logger.error(f"BrokerMW::handle_publisher_change - Error closing old socket: {str(e)}")
            
            self.logger.info("BrokerMW::handle_publisher_change - Updated publisher connections")
            
        except Exception as e:
            self.logger.error(f"BrokerMW::handle_publisher_change - Exception: {str(e)}")
    
    def forward_messages(self):
        """Process and forward any messages from publishers to subscribers"""
        try:
            # If there are no buffered messages, try to receive some (non-blocking)
            if not self.message_queue:
                try:
                    # Poll for events
                    events = dict(self.poller.poll(timeout=0))  # Non-blocking poll
                    
                    # Handle incoming messages from publishers (with safety checks)
                    if self.sub and self.sub in events:
                        message = self.sub.recv(zmq.NOBLOCK)
                        self.message_queue.append(message)
                        self.logger.debug(f"BrokerMW::forward_messages - Received message, added to queue")
                    
                    # Handle replicated messages from primary (if backup)
                    elif hasattr(self, 'repl_sub') and self.repl_sub and self.repl_sub in events:
                        message = self.repl_sub.recv(zmq.NOBLOCK)
                        self.message_queue.append(message)
                        self.logger.debug(f"BrokerMW::forward_messages - Received replicated message, added to queue")
                
                except zmq.ZMQError as e:
                    if e.errno == zmq.EAGAIN:
                        # No message available right now
                        return False
                    else:
                        self.logger.error(f"BrokerMW::forward_messages - ZMQ error: {str(e)}")
                        return False
            
            # If we have messages to process, forward them
            if self.message_queue:
                message = self.message_queue.pop(0)
                
                # Forward message to subscribers
                if self.pub:
                    self.pub.send(message)
                    self.logger.debug(f"BrokerMW::forward_messages - Forwarded message to subscribers")
                
                # If primary, also replicate to backups
                if self.is_primary and self.repl_pub:
                    self.repl_pub.send(message)
                    self.logger.debug(f"BrokerMW::forward_messages - Replicated message to backups")
                
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"BrokerMW::forward_messages - Exception: {str(e)}")
            return False
    
    def event_loop(self, timeout=None):
        """Process events for the specified timeout - simplified to focus on collection"""
        try:
            # Poll for events, but defer processing to forward_messages
            events = dict(self.poller.poll(timeout=timeout))
            
            # Just collect messages for the queue, don't process them here
            if self.sub and self.sub in events:
                try:
                    message = self.sub.recv()
                    self.message_queue.append(message)
                    self.logger.debug(f"BrokerMW::event_loop - Received message, added to queue")
                    
                    # Let application know we received something
                    if self.upcall_obj:
                        self.upcall_obj.invoke_operation()
                except zmq.ZMQError as e:
                    self.logger.error(f"BrokerMW::event_loop - ZMQ error on SUB socket: {str(e)}")
            
            # Handle replicated messages from primary (if backup)
            elif hasattr(self, 'repl_sub') and self.repl_sub and self.repl_sub in events:
                try:
                    message = self.repl_sub.recv()
                    self.message_queue.append(message)
                    self.logger.debug(f"BrokerMW::event_loop - Received replicated message, added to queue")
                    
                    # Let application know we received something
                    if self.upcall_obj:
                        self.upcall_obj.invoke_operation()
                except zmq.ZMQError as e:
                    self.logger.error(f"BrokerMW::event_loop - ZMQ error on replication SUB socket: {str(e)}")
                
        except Exception as e:
            self.logger.error(f"BrokerMW::event_loop - Exception: {str(e)}")
            # Don't re-raise here - just log error and continue
            return
    
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
    
    def register(self, name, addr_port):
        """Register the broker with the Discovery service"""
        try:
            self.logger.info(f"BrokerMW::register - Registering broker {name} at {addr_port}")
            
            # Create the protobuf registration request
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            
            # Split address and port
            if ":" in addr_port:
                addr, port_str = addr_port.split(":")
                reg_info.addr = addr
                reg_info.port = int(port_str)
            
            # Create registration request
            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_BOTH  # Broker is both publisher and subscriber
            register_req.info.CopyFrom(reg_info)
            
            # Add to all topics (or specify topics if needed)
            register_req.topiclist.extend(["*"])  # Wildcard to indicate all topics
            
            # Create overall Discovery request
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)
            
            # Send registration request
            self.req.send(disc_req.SerializeToString())
            
            # Wait for response (with timeout)
            try:
                response_bytes = self.req.recv(timeout=5000)  # 5-second timeout
                disc_resp = discovery_pb2.DiscoveryResp()
                disc_resp.ParseFromString(response_bytes)
                
                if disc_resp.register_resp.status == discovery_pb2.STATUS_SUCCESS:
                    self.logger.info("BrokerMW::register - Successfully registered with Discovery")
                else:
                    reason = disc_resp.register_resp.reason
                    self.logger.warning(f"BrokerMW::register - Registration failed: {reason}")
                
            except Exception as e:
                self.logger.error(f"BrokerMW::register - Error receiving response: {str(e)}")
        
        except Exception as e:
            self.logger.error(f"BrokerMW::register - Exception: {str(e)}")
            raise e
    
    def cleanup(self):
        """Clean up all resources"""
        try:
            self.logger.info("BrokerMW::cleanup - Cleaning up resources")
            
            # Close sockets
            if hasattr(self, 'sub') and self.sub:
                try:
                    self.poller.unregister(self.sub)
                    self.sub.close()
                    self.logger.info("BrokerMW::cleanup - Closed SUB socket")
                except Exception as e:
                    self.logger.error(f"BrokerMW::cleanup - Error closing SUB socket: {str(e)}")
            
            if hasattr(self, 'pub') and self.pub:
                try:
                    self.pub.close()
                    self.logger.info("BrokerMW::cleanup - Closed PUB socket")
                except Exception as e:
                    self.logger.error(f"BrokerMW::cleanup - Error closing PUB socket: {str(e)}")
            
            # Clean up REQ socket
            if hasattr(self, 'req') and self.req:
                self.req.close()
            
            # Clean up replication sockets
            if hasattr(self, 'repl_pub') and self.repl_pub:
                try:
                    self.repl_pub.close()
                    self.logger.info("BrokerMW::cleanup - Closed replication PUB socket")
                except Exception as e:
                    self.logger.error(f"BrokerMW::cleanup - Error closing replication PUB socket: {str(e)}")
                    
            if hasattr(self, 'repl_sub') and self.repl_sub:
                try:
                    if hasattr(self, 'poller') and self.poller:
                        self.poller.unregister(self.repl_sub)
                    self.repl_sub.close()
                    self.logger.info("BrokerMW::cleanup - Closed replication SUB socket")
                except Exception as e:
                    self.logger.error(f"BrokerMW::cleanup - Error closing replication SUB socket: {str(e)}")
            
            self.logger.info("BrokerMW::cleanup - Cleanup complete")
        except Exception as e:
            self.logger.error(f"BrokerMW::cleanup - Exception during cleanup: {str(e)}")

    # Note: Removed duplicated cleanup() method and misplaced signal_handler() method
    # that were accidentally copied from BrokerAppln class


