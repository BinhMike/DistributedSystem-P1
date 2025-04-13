import sys
import time
import argparse
import logging
import signal
import threading
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
            connection_url = f"tcp://{broker_addr}"
            self.backend.connect(connection_url)
            self.logger.info(f"Connected to broker at {connection_url}")
        except Exception as e:
            self.logger.error(f"Error connecting to broker {broker_addr}: {str(e)}")
    
    def _disconnect_from_broker(self, broker_addr):
        """Disconnect from a broker instance."""
        try:
            connection_url = f"tcp://{broker_addr}"
            self.backend.disconnect(connection_url)
            self.logger.info(f"Disconnected from broker at {connection_url}")
        except Exception as e:
            self.logger.error(f"Error disconnecting from broker {broker_addr}: {str(e)}")
    
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
                if not any(group["replicas"] for group in self.broker_groups.values()):
                    self.logger.warning("No active brokers available")
                time.sleep(2)
                
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
                    message = frontend.recv_multipart()
                    # First frame is client identity, preserve it
                    identity = message[0]
                    # Remaining frames are the actual message
                    data = message[1:]
                    
                    # Get topic from message
                    topic = None
                    if len(data) > 0:
                        try:
                            if proxy_type == "SUB":
                                # For SUB messages, extract topic from subscription message
                                # Assuming subscription messages are formatted with topic as first frame
                                topic_bytes = data[0]
                                topic = topic_bytes.decode()
                            else:
                                # For PUB messages, extract topic from publication format
                                topic_bytes = data[0]
                                topic = topic_bytes.decode().split(':')[0]  # Publication format: "topic:data"
                        except Exception:
                            topic = None
                    
                    # Determine target group based on topic
                    target_group = self._get_group_for_topic(topic)
                    
                    # Route message based on type and load balancing strategy
                    if proxy_type == "PUB":
                        # Publications go to primary broker of the target group
                        primary = self._get_primary_for_group(target_group)
                        if primary:
                            self.logger.debug(f"Routing {topic} publication to primary broker {primary} in group {target_group}")
                            backend.send_multipart([identity] + data)
                        else:
                            self.logger.warning(f"No primary available for group {target_group}, message dropped")
                    else:
                        # For subscriptions, ONLY send to brokers that handle the requested topics
                        broker = self._get_next_broker_for_group(target_group)
                        if broker:
                            self.logger.debug(f"Routing {topic} subscription to broker {broker} in group {target_group}")
                            backend.send_multipart([identity] + data)
                        else:
                            self.logger.warning(f"No brokers available for group {target_group}, message dropped")
                
                if backend in events:
                    message = backend.recv_multipart()
                    # First frame is client identity
                    identity = message[0]
                    # Remaining frames are the actual message
                    data = message[1:]
                    frontend.send_multipart([identity] + data)
        
        except Exception as e:
            self.logger.error(f"{proxy_type} proxy thread error: {str(e)}")

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