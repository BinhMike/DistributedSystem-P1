import sys
import time
import argparse
import logging
import signal
import threading
import traceback
import zmq

from kazoo.client import KazooClient

class BrokerLoadBalancer:
    """Load balancer for broker instances that preserves the existing quorum and replication logic."""
    
    def __init__(self, logger):
        self.logger = logger
        self.zk = None
        self.context = zmq.Context()
        
        # Frontend sockets (facing clients)
        self.pub_frontend = None    # ROUTER socket for publishers
        self.sub_frontend = None    # ROUTER socket for subscribers
        
        # Backend socket (facing brokers)
        self.backend = None         # DEALER socket for brokers
        
        # ZooKeeper paths
        self.brokers_path = "/brokers"
        
        # Group management
        self.broker_groups = {}  # Dict of group_name -> {"leader": address, "replicas": set()}
        self.topic_to_group = {}  # Dict to map topics to broker groups
        
        # Default group for unknown topics
        self.default_group = "group1"
        
        # For round-robin load balancing within each group
        self.last_used_indices = {}  # group -> index 
        
        # Signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def configure(self, args):
        """Configure the load balancer."""
        try:
            self.logger.info("BrokerLoadBalancer::configure")
            
            # Connect to ZooKeeper
            self.zk = KazooClient(hosts=args.zookeeper)
            self.zk.start()
            self.logger.info(f"Connected to ZooKeeper at {args.zookeeper}")
            
            # Set default group
            self.default_group = args.default_group
            
            # Process topic to group mapping
            if args.topic_map:
                self._process_topic_mapping(args.topic_map)
            
            # Set up broker monitoring
            self._setup_broker_monitoring()
            
            # Set up frontend sockets
            self._setup_frontend_sockets(args.pub_port, args.sub_port)
            
            # Set up backend socket
            self._setup_backend_socket()
            
            # Register the load balancer in ZooKeeper for auto-discovery
            self._register_in_zookeeper(args.addr or "localhost", args.pub_port, args.sub_port)
            
            self.logger.info("BrokerLoadBalancer::configure - Configuration complete")
            return True
            
        except Exception as e:
            self.logger.error(f"BrokerLoadBalancer::configure - Exception: {str(e)}")
            self.cleanup()
            return False

    def _process_topic_mapping(self, mapping_str):
        """Process the topic to group mapping string."""
        try:
            mappings = mapping_str.split(',')
            for mapping in mappings:
                if ':' in mapping:
                    topic, group = mapping.split(':')
                    self.topic_to_group[topic.strip()] = group.strip()
                    self.logger.info(f"Mapped topic '{topic.strip()}' to broker group '{group.strip()}'")
        except Exception as e:
            self.logger.error(f"Error processing topic mapping: {str(e)}")
    
    def _setup_broker_monitoring(self):
        """Set up ZooKeeper watches to monitor broker groups."""
        # Watch for broker group changes
        @self.zk.ChildrenWatch(self.brokers_path)
        def watch_groups(children):
            self.logger.info(f"Broker groups changed: {children}")
            
            # Setup watches for each group
            for group in children:
                self._setup_group_watches(group)
    
    def _setup_group_watches(self, group):
        """Setup watches for a specific broker group."""
        if group not in self.broker_groups:
            self.broker_groups[group] = {"leader": None, "replicas": set()}
        
        group_path = f"{self.brokers_path}/{group}"
        leader_path = f"{group_path}/leader"
        replicas_path = f"{group_path}/replicas"
        
        # Initialize leader data immediately if it exists, don't wait for watch event
        if self.zk.exists(leader_path):
            try:
                data, _ = self.zk.get(leader_path)
                if data:
                    leader_info = data.decode().split('|')[0]
                    self.broker_groups[group]["leader"] = leader_info
                    self.logger.info(f"Group {group} primary broker initialized: {leader_info}")
            except Exception as e:
                self.logger.error(f"Error initializing leader for {group}: {str(e)}")
        
        # Initialize replica data immediately if it exists, don't wait for watch event
        if self.zk.exists(replicas_path):
            try:
                children = self.zk.get_children(replicas_path)
                self.logger.info(f"Initial scan: Group {group} has {len(children)} replica nodes")
                
                for broker_id in children:
                    try:
                        broker_path = f"{replicas_path}/{broker_id}"
                        data, _ = self.zk.get(broker_path)
                        broker_addr = data.decode()
                        self.broker_groups[group]["replicas"].add(broker_addr)
                        self.logger.info(f"Added broker replica: {broker_addr} for group {group}")
                        self._connect_to_broker(broker_addr)
                    except Exception as e:
                        self.logger.error(f"Error processing replica {broker_id}: {str(e)}")
            except Exception as e:
                self.logger.error(f"Error initializing replicas for {group}: {str(e)}")
        
        # Watch for leader changes
        @self.zk.DataWatch(leader_path)
        def watch_leader(data, stat, event):
            if data:
                leader_info = data.decode().split('|')[0]
                self.broker_groups[group]["leader"] = leader_info
                self.logger.info(f"Group {group} primary broker updated: {leader_info}")
        
        # Watch for replica changes
        @self.zk.ChildrenWatch(replicas_path)
        def watch_replicas(children):
            self.logger.info(f"Group {group} broker replicas changed: {len(children)} replicas")
            new_brokers = set()
            
            for broker_id in children:
                try:
                    broker_path = f"{replicas_path}/{broker_id}"
                    data, _ = self.zk.get(broker_path)
                    broker_addr = data.decode()
                    new_brokers.add(broker_addr)
                except Exception as e:
                    self.logger.error(f"Error getting broker data: {str(e)}")
            
            # Handle changed broker list
            old_brokers = self.broker_groups[group]["replicas"].copy()
            self.broker_groups[group]["replicas"] = new_brokers
            
            # Connect to new brokers
            for broker in new_brokers - old_brokers:
                self._connect_to_broker(broker)
            
            # Disconnect from removed brokers
            for broker in old_brokers - new_brokers:
                self._disconnect_from_broker(broker)
    
    def _setup_frontend_sockets(self, pub_port, sub_port):
        """Set up frontend sockets for publishers and subscribers."""
        # Socket for publishers (ROUTER)
        self.pub_frontend = self.context.socket(zmq.ROUTER)
        self.pub_frontend.bind(f"tcp://*:{pub_port}")
        self.logger.info(f"Publisher frontend bound to port {pub_port}")
        
        # Socket for subscribers (ROUTER)
        self.sub_frontend = self.context.socket(zmq.ROUTER)
        self.sub_frontend.bind(f"tcp://*:{sub_port}")
        self.logger.info(f"Subscriber frontend bound to port {sub_port}")
    
    def _setup_backend_socket(self):
        """Set up backend socket to connect to broker instances."""
        self.backend = self.context.socket(zmq.DEALER)
        self.logger.info("Backend socket created for broker connections")
    
    def _connect_to_broker(self, broker_addr):
        """Connect to a broker instance."""
        try:
            # Ensure the broker address is properly formatted for ZMQ connection
            # Sometimes broker_addr might contain additional data or wrong format
            if not broker_addr:
                self.logger.error(f"Invalid broker address: empty")
                return
                
            # Remove any trailing information (like lease data after '|')
            if '|' in broker_addr:
                broker_addr = broker_addr.split('|')[0]
            
            # Ensure the address has both host and port
            if ':' not in broker_addr:
                self.logger.error(f"Invalid broker address format: {broker_addr}")
                return
                
            # Format the connection URL
            connection_url = f"tcp://{broker_addr}"
            
            # Log connection attempt with full details
            self.logger.info(f"Connecting to broker at {connection_url} (raw addr: {broker_addr})")
            
            try:
                # Check if already connected
                if hasattr(self.backend, '_endpoints') and connection_url in self.backend._endpoints:
                    self.logger.info(f"Already connected to {connection_url}")
                    return
            except Exception as e:
                self.logger.warning(f"Error checking existing connections: {str(e)}")
            
            # Connect to broker
            self.backend.connect(connection_url)
            self.logger.info(f"Successfully connected to broker at {connection_url}")
            
        except Exception as e:
            self.logger.error(f"Error connecting to broker {broker_addr}: {str(e)}")
            self.logger.error(traceback.format_exc())
    
    def _disconnect_from_broker(self, broker_addr):
        """Disconnect from a broker instance."""
        try:
            connection_url = f"tcp://{broker_addr}"
            self.backend.disconnect(connection_url)
            self.logger.info(f"Disconnected from broker at {connection_url}")
        except Exception as e:
            self.logger.error(f"Error disconnecting from broker {broker_addr}: {str(e)}")
    
    def _register_in_zookeeper(self, addr, pub_port, sub_port):
        """Register the load balancer in ZooKeeper for auto-discovery."""
        try:
            # Create the load balancer path if it doesn't exist
            lb_path = "/load_balancers"
            self.zk.ensure_path(lb_path)
            
            # Create a node for this load balancer instance
            lb_id = f"lb_{addr}_{pub_port}"
            lb_node_path = f"{lb_path}/{lb_id}"
            
            # Store connection information in the format addr:pub_port:sub_port
            lb_data = f"{addr}:{pub_port}:{sub_port}"
            
            # Create or update the node (using ephemeral node that will be removed if LB crashes)
            if self.zk.exists(lb_node_path):
                self.zk.delete(lb_node_path)
            
            self.zk.create(lb_node_path, lb_data.encode(), ephemeral=True)
            self.logger.info(f"Registered load balancer in ZooKeeper at {lb_node_path}")
            
            return True
        except Exception as e:
            self.logger.error(f"Error registering in ZooKeeper: {str(e)}")
            return False
    
    def run(self):
        """Run the load balancer proxy."""
        try:
            self.logger.info("BrokerLoadBalancer::run - Starting load balancer")
            
            # Start two proxy threads, one for publishers and one for subscribers
            pub_thread = threading.Thread(target=self._run_proxy, 
                                         args=(self.pub_frontend, self.backend, "PUB"), 
                                         daemon=True)
            sub_thread = threading.Thread(target=self._run_proxy, 
                                         args=(self.sub_frontend, self.backend, "SUB"), 
                                         daemon=True)
            
            pub_thread.start()
            sub_thread.start()
            
            # Main thread monitors and maintains broker connections
            while True:
                # Enhanced debug logging to diagnose broker connectivity issues
                self.logger.info("---- Broker Group Status Check ----")
                if self.broker_groups:
                    for group_name, group_data in self.broker_groups.items():
                        self.logger.info(f"Group: {group_name}")
                        self.logger.info(f"  Leader: {group_data['leader']}")
                        self.logger.info(f"  Replicas: {group_data['replicas']}")
                        
                        # Check if the leader node exists in ZooKeeper
                        leader_path = f"{self.brokers_path}/{group_name}/leader"
                        if self.zk.exists(leader_path):
                            try:
                                data, _ = self.zk.get(leader_path)
                                self.logger.info(f"  Leader ZK data: {data.decode() if data else 'None'}")
                            except Exception as e:
                                self.logger.error(f"  Error getting leader data: {str(e)}")
                        else:
                            self.logger.warning(f"  Leader path {leader_path} does not exist!")
                            
                        # Check if replicas exist
                        replicas_path = f"{self.brokers_path}/{group_name}/replicas"
                        if self.zk.exists(replicas_path):
                            try:
                                children = self.zk.get_children(replicas_path)
                                self.logger.info(f"  Replica nodes in ZK: {children}")
                                for child in children:
                                    child_path = f"{replicas_path}/{child}"
                                    data, _ = self.zk.get(child_path)
                                    self.logger.info(f"    Replica {child}: {data.decode() if data else 'None'}")
                            except Exception as e:
                                self.logger.error(f"  Error getting replica data: {str(e)}")
                        else:
                            self.logger.warning(f"  Replicas path {replicas_path} does not exist!")
                else:
                    # Dump the current state of the /brokers directory
                    self.logger.warning("No broker groups found in self.broker_groups!")
                    try:
                        if self.zk.exists(self.brokers_path):
                            children = self.zk.get_children(self.brokers_path)
                            self.logger.info(f"ZooKeeper {self.brokers_path} contains: {children}")
                            for child in children:
                                if child.startswith("group"):
                                    group_path = f"{self.brokers_path}/{child}"
                                    group_children = self.zk.get_children(group_path)
                                    self.logger.info(f"  {child} contains: {group_children}")
                        else:
                            self.logger.warning(f"{self.brokers_path} path does not exist in ZooKeeper!")
                    except Exception as e:
                        self.logger.error(f"Error inspecting ZooKeeper: {str(e)}")
                
                time.sleep(5)  # Increased for less frequent logs
                
        except KeyboardInterrupt:
            self.logger.info("BrokerLoadBalancer::run - Interrupted")
        except Exception as e:
            self.logger.error(f"BrokerLoadBalancer::run - Error: {str(e)}")
        finally:
            self.cleanup()
    
    def _run_proxy(self, frontend, backend, proxy_type):
        """Run a ZMQ proxy between frontend and backend sockets."""
        try:
            self.logger.info(f"Starting {proxy_type} proxy thread")
            
            poller = zmq.Poller()
            poller.register(frontend, zmq.POLLIN)
            poller.register(backend, zmq.POLLIN)
            
            while True:
                events = dict(poller.poll(1000))  # 1 second timeout
                
                if frontend in events:
                    # Get all message parts
                    message = frontend.recv_multipart()
                    
                    # First frame is client identity
                    identity = message[0]
                    
                    # Extract topic from message for routing
                    topic = self._extract_topic(message, proxy_type)
                    target_group = self._get_group_for_topic(topic)
                    
                    # Route based on message type
                    if proxy_type == "PUB":
                        # For publishers, ensure we send to primary broker in the group
                        primary = self._get_primary_for_group(target_group)
                        if primary:
                            self.logger.debug(f"Routing '{topic}' publication to primary broker in group {target_group}")
                            backend.send_multipart(message)
                        else:
                            self.logger.warning(f"No primary broker available for {target_group}, message dropped: {topic}")
                    else:
                        # For subscribers, use round-robin load balancing within group
                        broker = self._get_next_broker_for_group(target_group)
                        if broker:
                            self.logger.debug(f"Routing '{topic}' subscription to broker in group {target_group}")
                            backend.send_multipart(message)
                        else:
                            self.logger.warning(f"No brokers available for {target_group}, message dropped: {topic}")
                
                if backend in events:
                    # Simply pass messages from backend to frontend
                    message = backend.recv_multipart()
                    frontend.send_multipart(message)
        
        except Exception as e:
            self.logger.error(f"{proxy_type} proxy thread error: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
    def _extract_topic(self, message, proxy_type):
        """Extract topic from message parts based on message type."""
        try:
            if len(message) < 2:
                return None
                
            # Skip identity frame
            content = message[1:]
            
            if not content or not content[0]:
                return None
                
            data = content[0]
            
            if proxy_type == "SUB":
                # For SUB messages, the first part is typically the subscription topic
                # or could be a command like SUBSCRIBE
                topic = data.decode() if isinstance(data, bytes) else str(data)
                # If it's a subscription command, the real topic is in the next frame
                if topic == "SUBSCRIBE" and len(content) > 1:
                    topic = content[1].decode() if isinstance(content[1], bytes) else str(content[1])
                return topic
            else:
                # For PUB messages, decode and extract topic from the format "topic:timestamp:data"
                data_str = data.decode() if isinstance(data, bytes) else str(data)
                parts = data_str.split(":", 1)
                topic = parts[0] if parts else None
                return topic
        except Exception as e:
            self.logger.error(f"Error extracting topic: {str(e)}")
            return None

    def _get_group_for_topic(self, topic):
        """Determine which broker group should handle a given topic."""
        if not topic or topic not in self.topic_to_group:
            return self.default_group
        return self.topic_to_group.get(topic, self.default_group)

    def _get_primary_for_group(self, group):
        """Get the primary broker for a specific group."""
        if group in self.broker_groups and self.broker_groups[group]["leader"]:
            return self.broker_groups[group]["leader"]
        return None

    def _get_next_broker_for_group(self, group):
        """Get the next broker in round-robin fashion for a specific group."""
        if group not in self.broker_groups or not self.broker_groups[group]["replicas"]:
            return None
        
        replicas = list(self.broker_groups[group]["replicas"])
        if not replicas:
            return None
        
        if group not in self.last_used_indices:
            self.last_used_indices[group] = 0
        else:
            self.last_used_indices[group] = (self.last_used_indices[group] + 1) % len(replicas)
        
        return replicas[self.last_used_indices[group]]
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down")
        self.cleanup()
        sys.exit(0)
    
    def cleanup(self):
        """Clean up resources."""
        self.logger.info("BrokerLoadBalancer::cleanup")
        
        # Close ZMQ sockets
        if hasattr(self, 'pub_frontend') and self.pub_frontend:
            self.pub_frontend.close()
            
        if hasattr(self, 'sub_frontend') and self.sub_frontend:
            self.sub_frontend.close()
            
        if hasattr(self, 'backend') and self.backend:
            self.backend.close()
        
        # Close ZMQ context
        if hasattr(self, 'context') and self.context:
            self.context.term()
        
        # Close ZooKeeper connection
        if self.zk:
            self.zk.stop()
            self.zk.close()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Broker Load Balancer")
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", 
                      help="ZooKeeper connection string")
    parser.add_argument("--pub-port", type=int, default=7000, 
                      help="Publisher frontend port")
    parser.add_argument("--sub-port", type=int, default=7001, 
                      help="Subscriber frontend port")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, 
                      help="Logging level")
    parser.add_argument("-d", "--default-group", default="group1",
                      help="Default broker group for topics without explicit mapping")
    parser.add_argument("-m", "--topic-map", default="",
                      help="Topic to group mapping in format topic1:group1,topic2:group2")
    parser.add_argument("--addr", default="localhost",
                      help="Address of the load balancer for ZooKeeper registration")
    return parser.parse_args()


def main():
    """Main entry point."""
    # Configure logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("BrokerLoadBalancer")
    
    # Parse arguments
    args = parse_args()
    logger.setLevel(args.loglevel)
    
    # Create and configure load balancer
    lb = BrokerLoadBalancer(logger)
    if lb.configure(args):
        # Run load balancer
        lb.run()


if __name__ == "__main__":
    main()