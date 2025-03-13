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
import threading
import subprocess
import os
from kazoo import exceptions
from kazoo.exceptions import NodeExistsError
from CS6381_MW import discovery_pb2  

class BrokerMW():
    ########################################
    # Constructor
    ########################################
    def __init__(self, logger, zk_client, is_leader=False):
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
        
        # ZK paths
        self.broker_path = "/brokers"
        self.leader_path = "/brokers/leader"
        self.publisher_path = "/publishers"
        self.quorum_path = "/brokers/quorum"  # New path for quorum management
        
        # Leadership and replication
        self.is_primary = is_leader  # Initialize with provided value
        self.primary_start_time = None
        self.follower_brokers = []
        
        # Lease settings (in seconds)
        self.lease_duration = 10
        self.lease_renew_interval = 5
        self.max_leader_duration = 30
        self.lease_thread = None
        
        # Quorum management
        self.quorum_size = 3  # Target number of replicas
        self.quorum_check_interval = 5  # Seconds between quorum checks
        self.quorum_thread = None
        self.reviving = False  # Flag to indicate if we're currently reviving a replica

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
            
            # Ensure ZooKeeper paths exist
            self.ensure_zk_paths()
            
            # Register broker in ZooKeeper
            self.register_with_zk()
            
            # Set up leadership election
            self.setup_leadership()
            
            # Subscribe to current Publishers
            self.subscribe_to_publishers()
            
            # Watch for publisher changes
            self.zk.ChildrenWatch(self.publisher_path, self.handle_publisher_change)
            
            # Start quorum monitoring (only for primary)
            if self.is_primary:
                self.start_quorum_monitoring()
            
            self.logger.info("BrokerMW::configure - Configuration complete")
            
        except Exception as e:
            self.logger.error(f"BrokerMW::configure - Exception: {str(e)}")
            raise e
    
    ########################################
    # Ensure ZooKeeper paths exist
    ########################################
    def ensure_zk_paths(self):
        """Ensure all required ZooKeeper paths exist"""
        try:
            self.logger.info("BrokerMW::ensure_zk_paths - Creating ZK paths if needed")
            self.zk.ensure_path(self.broker_path)
            self.zk.ensure_path(self.publisher_path)
        except Exception as e:
            self.logger.error(f"BrokerMW::ensure_zk_paths - Error: {str(e)}")
            raise e
    
    ########################################
    # Register with ZooKeeper
    ########################################
    def register_with_zk(self):
        """Register this broker with ZooKeeper"""
        try:
            broker_address = f"{self.addr}:{self.port}"
            broker_node_path = f"{self.broker_path}/{self.args.name}"
            
            # Clean up old node if it exists
            if self.zk.exists(broker_node_path):
                self.zk.delete(broker_node_path)
            
            # Create new ephemeral node
            self.zk.create(broker_node_path, broker_address.encode(), ephemeral=True)
            self.logger.info(f"BrokerMW::register_with_zk - Registered in ZooKeeper at {broker_node_path}")
        except Exception as e:
            self.logger.error(f"BrokerMW::register_with_zk - Error: {str(e)}")
            raise e

    ########################################
    # Setup Leadership Election
    ########################################
    def setup_leadership(self):
        """Set up leadership election mechanism"""
        try:
            self.logger.info("BrokerMW::setup_leadership - Setting up leadership mechanism")
            
            # Try to become the leader
            broker_address = f"{self.addr}:{self.port}"
            
            # Prepare leader data: address and lease expiry timestamp
            lease_expiry = time.time() + self.lease_duration
            leader_data = f"{broker_address}|{lease_expiry}"
            
            try:
                # Make sure the parent path exists
                self.zk.ensure_path(self.broker_path)
                
                # Try to create the leader node (ephemeral)
                self.zk.create(self.leader_path, leader_data.encode(), ephemeral=True)
                self.is_primary = True
                self.primary_start_time = time.time()
                self.logger.info(f"BrokerMW::setup_leadership - Became primary with lease expiring at {lease_expiry}")
                
                # Start lease renewal
                self.start_lease_renewal(broker_address)
                
                # Set up replication as leader
                self.setup_leader_replication()
                
            except NodeExistsError:
                self.is_primary = False
                self.logger.info("BrokerMW::setup_leadership - A leader already exists, running as backup")
                
                # Watch the leader node for changes and deletions
                self.watch_leader_node(broker_address)
                
                # Setup follower replication
                self.setup_follower_replication()
            
            # Log current leader information
            if self.zk.exists(self.leader_path):
                data, _ = self.zk.get(self.leader_path)
                self.logger.info(f"BrokerMW::setup_leadership - Current leader in ZooKeeper: {data.decode()}")
            else:
                self.logger.error("BrokerMW::setup_leadership - Leader node does not exist after setup")
                
        except Exception as e:
            self.logger.error(f"BrokerMW::setup_leadership - Error: {str(e)}")
            raise e
    
    ########################################
    # Watch Leader Node
    ########################################
    def watch_leader_node(self, broker_address):
        """Watch the leader node for changes or deletion"""
        @self.zk.DataWatch(self.leader_path)
        def watch_leader(data, stat, event):
            if event is not None and event.type == "DELETED":
                self.logger.info("BrokerMW::watch_leader_node - Leader node deleted, attempting to become primary...")
                # Add delay to avoid race conditions
                time.sleep(1)
                
                try:
                    # Re-ensure the broker path exists
                    self.zk.ensure_path(self.broker_path)
                    
                    # Check if leader node still doesn't exist
                    if not self.zk.exists(self.leader_path):
                        new_expiry = time.time() + self.lease_duration
                        new_data = f"{broker_address}|{new_expiry}"
                        self.zk.create(self.leader_path, new_data.encode(), ephemeral=True)
                        self.is_primary = True
                        self.primary_start_time = time.time()
                        
                        self.logger.info(f"BrokerMW::watch_leader_node - This instance has now become the primary with lease expiring at {new_expiry}")
                        
                        # Start lease renewal and leader replication
                        self.start_lease_renewal(broker_address)
                        self.setup_leader_replication()
                        
                        # Start quorum monitoring (now that we're primary)
                        self.start_quorum_monitoring()
                    else:
                        self.logger.info("BrokerMW::watch_leader_node - Leader node already recreated by another instance")
                        self.is_primary = False
                except NodeExistsError:
                    self.logger.info("BrokerMW::watch_leader_node - Another instance became primary while we were trying")
                    self.is_primary = False
                except Exception as e:
                    self.logger.error(f"BrokerMW::watch_leader_node - Error during leadership transition: {str(e)}")
                    self.is_primary = False

    ########################################
    # Start Lease Renewal Thread
    ########################################
    def start_lease_renewal(self, broker_address):
        """Start a background thread for lease renewal"""
        def renewal_task():
            while self.is_primary:
                try:
                    # Check if we've been primary too long
                    current_time = time.time()
                    if self.primary_start_time and (current_time - self.primary_start_time) > self.max_leader_duration:
                        self.logger.info("BrokerMW::lease_renewal - Max leader duration reached, stepping down")
                        # Delete our own leader node to trigger re-election
                        if self.zk.exists(self.leader_path):
                            self.zk.delete(self.leader_path)
                        self.is_primary = False
                        break
                    
                    # Renew the lease
                    if self.zk.exists(self.leader_path):
                        new_expiry = time.time() + self.lease_duration
                        new_data = f"{broker_address}|{new_expiry}"
                        self.zk.set(self.leader_path, new_data.encode())
                        self.logger.debug(f"BrokerMW::lease_renewal - Lease renewed, new expiry: {new_expiry}")
                    else:
                        self.logger.warning("BrokerMW::lease_renewal - Leader node doesn't exist, recreating...")
                        try:
                            new_expiry = time.time() + self.lease_duration
                            new_data = f"{broker_address}|{new_expiry}"
                            self.zk.ensure_path(self.broker_path)
                            self.zk.create(self.leader_path, new_data.encode(), ephemeral=True)
                        except NodeExistsError:
                            self.logger.warning("BrokerMW::lease_renewal - Another instance became leader while renewing")
                            self.is_primary = False
                            break
                        except Exception as e:
                            self.logger.error(f"BrokerMW::lease_renewal - Failed to recreate leader node: {str(e)}")
                            self.is_primary = False
                            break
                
                except Exception as e:
                    self.logger.error(f"BrokerMW::lease_renewal - Error: {str(e)}")
                    self.is_primary = False
                    break
                
                # Sleep before next renewal
                time.sleep(self.lease_renew_interval)
            
            self.logger.info("BrokerMW::lease_renewal - Lease renewal thread exiting")
            
            # Clean up replication resources if we're no longer primary
            if hasattr(self, 'replication_socket') and self.replication_socket:
                try:
                    self.replication_socket.close()
                    delattr(self, 'replication_socket')
                except Exception as e:
                    self.logger.error(f"BrokerMW::lease_renewal - Error closing replication socket: {str(e)}")
            
            # Setup as follower instead
            try:
                self.setup_follower_replication()
            except Exception as e:
                self.logger.error(f"BrokerMW::lease_renewal - Error setting up as follower: {str(e)}")
        
        # Start the renewal thread
        self.lease_thread = threading.Thread(target=renewal_task, daemon=True)
        self.lease_thread.start()
        self.logger.info("BrokerMW::start_lease_renewal - Lease renewal thread started")
    
    ########################################
    # Setup Leader Replication
    ########################################
    def setup_leader_replication(self):
        """Configure replication as a leader broker"""
        try:
            # Clean up any existing replication listener if we're transitioning from follower to leader
            if hasattr(self, 'replication_listener') and self.replication_listener:
                try:
                    self.poller.unregister(self.replication_listener)
                    self.replication_listener.close()
                    delattr(self, 'replication_listener')
                except Exception as e:
                    self.logger.error(f"BrokerMW::setup_leader_replication - Error cleaning up listener: {str(e)}")
            
            # Create replication PUB socket for followers
            self.replication_socket = self.context.socket(zmq.PUB)
            self.replication_socket.bind("tcp://*:7000")
            self.logger.info("BrokerMW::setup_leader_replication - Replication socket started on port 7000")
            
            # Start thread to periodically update follower list
            self.get_follower_brokers()
            self.sync_followers_task = threading.Thread(target=self.periodic_follower_sync, daemon=True)
            self.sync_followers_task.start()
            
        except Exception as e:
            self.logger.error(f"BrokerMW::setup_leader_replication - Error: {str(e)}")

    ########################################
    # Setup Follower Replication
    ########################################
    def setup_follower_replication(self):
        """Configure replication as a follower broker"""
        try:
            # Clean up any existing replication socket if we're transitioning from leader to follower
            if hasattr(self, 'replication_socket') and self.replication_socket:
                try:
                    self.replication_socket.close()
                    delattr(self, 'replication_socket')
                except Exception as e:
                    self.logger.error(f"BrokerMW::setup_follower_replication - Error cleaning up socket: {str(e)}")
            
            # Get current leader address
            if self.zk.exists(self.leader_path):
                leader_data, _ = self.zk.get(self.leader_path)
                leader_data_str = leader_data.decode()
                
                # Parse the leader data (should be in format "address:port|expiry")
                if "|" in leader_data_str:
                    leader_addr = leader_data_str.split("|")[0]
                    leader_ip = leader_addr.split(":")[0]
                    
                    # Create SUB socket to listen to leader
                    if hasattr(self, 'replication_listener') and self.replication_listener:
                        self.poller.unregister(self.replication_listener)
                        self.replication_listener.close()
                    
                    self.replication_listener = self.context.socket(zmq.SUB)
                    self.replication_listener.connect(f"tcp://{leader_ip}:7000")
                    self.replication_listener.setsockopt_string(zmq.SUBSCRIBE, "")
                    self.poller.register(self.replication_listener, zmq.POLLIN)
                    self.logger.info(f"BrokerMW::setup_follower_replication - Listening to leader at {leader_ip}:7000")
                else:
                    self.logger.error(f"BrokerMW::setup_follower_replication - Invalid leader data format: {leader_data_str}")
            else:
                self.logger.warning("BrokerMW::setup_follower_replication - No leader found")
                
        except Exception as e:
            self.logger.error(f"BrokerMW::setup_follower_replication - Error: {str(e)}")

    ########################################
    # Get Follower Brokers
    ########################################
    def get_follower_brokers(self):
        """Get list of follower brokers"""
        self.follower_brokers = []
        
        try:
            if self.zk.exists(self.broker_path):
                brokers = self.zk.get_children(self.broker_path)
                
                for broker_id in brokers:
                    if broker_id == "leader":
                        continue
                        
                    broker_path = f"{self.broker_path}/{broker_id}"
                    if self.zk.exists(broker_path):
                        broker_data, _ = self.zk.get(broker_path)
                        broker_address = broker_data.decode()
                        
                        # Avoid adding our own address to follower list
                        own_address = f"{self.addr}:{self.port}"
                        if broker_address != own_address:
                            self.follower_brokers.append(broker_address)
                
                self.logger.info(f"BrokerMW::get_follower_brokers - Found {len(self.follower_brokers)} followers: {self.follower_brokers}")
        except Exception as e:
            self.logger.error(f"BrokerMW::get_follower_brokers - Error: {str(e)}")
    
    ########################################
    # Periodic Follower Sync
    ########################################
    def periodic_follower_sync(self):
        """Periodically update the follower broker list"""
        while self.is_primary:
            try:
                self.get_follower_brokers()
                time.sleep(5)
            except Exception as e:
                self.logger.error(f"BrokerMW::periodic_follower_sync - Error: {str(e)}")
                if not self.is_primary:
                    break

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
    # Event Loop
    ########################################
    def event_loop(self, timeout=None):
        """Process events for specified timeout then return control to application"""
        try:
            # Check if quorum revival is in progress
            revival_flag_path = f"{self.quorum_path}/revival_in_progress"
            if self.zk.exists(revival_flag_path):
                self.logger.warning("BrokerMW::event_loop - Quorum revival in progress, operations blocked")
                time.sleep(1)  # Sleep briefly and return
                return
            
            # Normal processing continues when not in revival
            # Poll for events with the specified timeout
            events = dict(self.poller.poll(timeout=timeout))
            
            if not events and self.upcall_obj:
                # No events within timeout, let application decide what to do
                return
            
            # Handle messages from publishers
            if self.sub in events:
                # Receive the message
                topic_and_message = self.sub.recv_string()
                topic, message = topic_and_message.split(":", 1)
                
                self.logger.info(f"BrokerMW::event_loop - Received topic [{topic}] with message [{message}]")
                
                # Primary broker forwards to subscribers directly and replicates
                if self.is_primary:
                    self.pub.send_string(topic_and_message)
                    self.logger.info(f"BrokerMW::event_loop - Primary forwarded message on topic [{topic}]")
                    
                    # Replicate to follower brokers
                    if hasattr(self, 'replication_socket') and self.replication_socket:
                        self.replication_socket.send_string(topic_and_message)
                        self.logger.debug(f"BrokerMW::event_loop - Replicated message to followers")
                
                # Let application know we processed something
                if self.upcall_obj:
                    self.upcall_obj.invoke_operation()
            
            # Handle replication messages (if follower)
            elif hasattr(self, 'replication_listener') and self.replication_listener in events:
                replica_message = self.replication_listener.recv_string()
                topic, message = replica_message.split(":", 1)
                
                self.logger.info(f"BrokerMW::event_loop - Follower received replicated topic [{topic}] with message [{message}]")
                
                # Forward replicated message to subscribers
                self.pub.send_string(replica_message)
                
                # Let application know we processed something
                if self.upcall_obj:
                    self.upcall_obj.invoke_operation()
                
        except Exception as e:
            self.logger.error(f"BrokerMW::event_loop - Exception: {str(e)}")
    
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
            
            # Remove quorum revival flag if we created it
            revival_flag_path = f"{self.quorum_path}/revival_in_progress"
            if self.zk.exists(revival_flag_path):
                try:
                    data, stat = self.zk.get(revival_flag_path)
                    self.zk.delete(revival_flag_path)
                except Exception as e:
                    self.logger.error(f"BrokerMW::cleanup - Error removing revival flag: {str(e)}")
                
            # Terminate ZMQ context
            self.context.term()
            
        except Exception as e:
            self.logger.error(f"BrokerMW::cleanup - Error: {str(e)}")

    ########################################
    # Start Quorum Monitoring
    ########################################
    def start_quorum_monitoring(self):
        """Start monitoring the broker quorum to maintain 3 replicas"""
        def quorum_monitoring_task():
            self.logger.info("BrokerMW::quorum_monitoring_task - Starting quorum monitoring")
            
            while self.is_primary:
                try:
                    # Get current broker count
                    broker_count = 0
                    if self.zk.exists(self.broker_path):
                        brokers = self.zk.get_children(self.broker_path)
                        # Filter out the "leader" node which isn't a broker instance
                        broker_count = len([b for b in brokers if b != "leader"])
                    
                    self.logger.info(f"BrokerMW::quorum_monitoring_task - Current broker count: {broker_count}")
                    
                    # Check if we need to revive brokers
                    if broker_count < self.quorum_size and not self.reviving:
                        self.logger.warning(f"BrokerMW::quorum_monitoring_task - Broker count below quorum ({broker_count}/{self.quorum_size})")
                        
                        # Number of brokers to revive
                        to_revive = self.quorum_size - broker_count
                        
                        # Lock operations during revival
                        self.reviving = True
                        
                        # Create a flag in ZooKeeper to indicate revival in progress
                        self.zk.ensure_path(self.quorum_path)
                        revival_flag_path = f"{self.quorum_path}/revival_in_progress"
                        if not self.zk.exists(revival_flag_path):
                            self.zk.create(revival_flag_path, b"true", ephemeral=True)
                        
                        # Start revival in a separate thread to avoid blocking monitor
                        revival_thread = threading.Thread(
                            target=self.revive_broker_replicas, 
                            args=(to_revive, revival_flag_path),
                            daemon=True
                        )
                        revival_thread.start()
                    
                except Exception as e:
                    self.logger.error(f"BrokerMW::quorum_monitoring_task - Error: {str(e)}")
                
                # Sleep before next check
                time.sleep(self.quorum_check_interval)
            
            self.logger.info("BrokerMW::quorum_monitoring_task - Quorum monitoring stopped")
        
        # Start the monitoring thread
        self.quorum_thread = threading.Thread(target=quorum_monitoring_task, daemon=True)
        self.quorum_thread.start()
        self.logger.info("BrokerMW::start_quorum_monitoring - Quorum monitoring thread started")

    ########################################
    # Revive Broker Replicas
    ########################################
    def revive_broker_replicas(self, count, revival_flag_path):
        """Revive the specified number of broker replicas"""
        try:
            self.logger.info(f"BrokerMW::revive_broker_replicas - Reviving {count} broker replicas")
            
            # Get the next available port numbers
            base_port = self.port + 1
            
            # Revive each replica
            for i in range(count):
                new_port = base_port + i
                replica_name = f"broker_replica_{int(time.time())}_{i}"
                
                self.logger.info(f"BrokerMW::revive_broker_replicas - Starting new replica {replica_name} on port {new_port}")
                
                # Prepare command to start a new broker instance
                # Assuming BrokerAppln.py is in the same directory as this file
                current_dir = os.path.dirname(os.path.abspath(__file__))
                parent_dir = os.path.dirname(current_dir)
                broker_script = os.path.join(parent_dir, "BrokerAppln.py")
                
                # Build command with appropriate arguments
                cmd = [
                    "python3", 
                    broker_script,
                    "-n", replica_name,
                    "-a", self.addr,
                    "-p", str(new_port),
                    "-z", self.args.zookeeper if hasattr(self.args, 'zookeeper') else "localhost:2181"
                ]
                
                # Start the new broker process
                subprocess.Popen(cmd)
                self.logger.info(f"BrokerMW::revive_broker_replicas - Started process: {' '.join(cmd)}")
                
                # Wait briefly to allow the new broker to initialize
                time.sleep(2)
            
            # Wait for brokers to register in ZooKeeper
            self.logger.info("BrokerMW::revive_broker_replicas - Waiting for new brokers to register...")
            time.sleep(5)
            
            # Check if we have reached the quorum
            if self.zk.exists(self.broker_path):
                brokers = self.zk.get_children(self.broker_path)
                broker_count = len([b for b in brokers if b != "leader"])
                if broker_count >= self.quorum_size:
                    self.logger.info(f"BrokerMW::revive_broker_replicas - Quorum restored ({broker_count}/{self.quorum_size})")
                else:
                    self.logger.warning(f"BrokerMW::revive_broker_replicas - Quorum not fully restored ({broker_count}/{self.quorum_size})")
            
        except Exception as e:
            self.logger.error(f"BrokerMW::revive_broker_replicas - Error: {str(e)}")
        finally:
            # Remove revival flag and reset reviving state
            if self.zk.exists(revival_flag_path):
                self.zk.delete(revival_flag_path)
            self.reviving = False


