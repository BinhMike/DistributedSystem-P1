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
    
    # ZooKeeper paths
    BROKERS_PATH = "/brokers"
    LOAD_BALANCERS_PATH = "/load_balancers"
    
    # Subscription command
    SUBSCRIBE_COMMAND = b"SUBSCRIBE"
    
    # Status update interval (seconds)
    STATUS_UPDATE_INTERVAL = 10
    
    def __init__(self, logger):
        self.logger = logger
        self.zk = None
        self.context = zmq.Context()
        
        # Frontend sockets (facing clients)
        self.pub_frontend = None    # PULL socket for publishers
        self.sub_frontend = None    # XPUB socket for subscribers
        self.sub_heartbeat = None   # PULL socket for subscriber heartbeats
        
        # Backend socket (facing brokers)
        self.backend = None         # XSUB socket for brokers
        
        # Group management
        self.broker_groups = {}     # Dict of group_name -> {"leader": address, "replicas": set()}
        self.topic_to_group = {}    # Dict to map topics to broker groups
        
        # Default group for unknown topics
        self.default_group = "group1"
        
        # For round-robin load balancing within each group
        self.last_used_indices = {} # group -> index 
        
        # Signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def configure(self, args):
        """
        Configure the load balancer with command line arguments.
        
        Args:
            args: Command line arguments parsed by argparse
            
        Returns:
            bool: True if configuration was successful, False otherwise
        """
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
            
            # Set up sockets first to ensure they're ready for broker connections
            self._setup_frontend_sockets(args.pub_port, args.sub_port)
            self._setup_backend_socket()
            
            # Only now set up broker monitoring which might trigger connections
            self._setup_broker_monitoring()
            
            # Register the load balancer in ZooKeeper for auto-discovery
            self._register_in_zookeeper(args.addr or "localhost", args.pub_port, args.sub_port)
            
            self.logger.info("BrokerLoadBalancer::configure - Configuration complete")
            return True
            
        except Exception as e:
            self.logger.error(f"BrokerLoadBalancer::configure - Exception: {str(e)}")
            self.logger.error(traceback.format_exc())
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
        @self.zk.ChildrenWatch(self.BROKERS_PATH)
        def watch_groups(children):
            self.logger.info(f"Broker groups changed: {children}")
            for group in children:
                self._setup_group_watches(group)
    
    def _setup_group_watches(self, group):
        """Setup watches for a specific broker group."""
        if group not in self.broker_groups:
            self.broker_groups[group] = {"leader": None, "replicas": set()}
        
        group_path = f"{self.BROKERS_PATH}/{group}"
        leader_path = f"{group_path}/leader"
        replicas_path = f"{group_path}/replicas"
        
        if not self.zk.exists(group_path):
            self.logger.error(f"Group path {group_path} doesn't exist")
            return
            
        if self.zk.exists(leader_path):
            try:
                data, _ = self.zk.get(leader_path)
                if data:
                    leader_info = data.decode().split('|')[0]
                    self.broker_groups[group]["leader"] = leader_info
                    self.logger.info(f"Group {group} primary broker initialized: {leader_info}")
            except Exception as e:
                self.logger.error(f"Error initializing leader for {group}: {str(e)}")
        else:
            self.logger.warning(f"Leader path {leader_path} doesn't exist yet")
        
        if self.zk.exists(replicas_path):
            try:
                children = self.zk.get_children(replicas_path)
                self.logger.info(f"Initial scan: Group {group} has {len(children)} replica nodes")
                
                for broker_id in children:
                    try:
                        broker_path = f"{replicas_path}/{broker_id}"
                        data, _ = self.zk.get(broker_path)
                        if data:
                            broker_addr = data.decode()
                            self.broker_groups[group]["replicas"].add(broker_addr)
                            self.logger.info(f"Added broker replica: {broker_addr} for group {group}")
                            self._connect_to_broker(broker_addr)
                    except Exception as e:
                        self.logger.error(f"Error processing replica {broker_id}: {str(e)}")
            except Exception as e:
                self.logger.error(f"Error initializing replicas for {group}: {str(e)}")
        else:
            self.logger.warning(f"Replicas path {replicas_path} doesn't exist yet")
        
        @self.zk.DataWatch(leader_path)
        def watch_leader(data, stat, event):
            if data:
                try:
                    leader_info = data.decode().split('|')[0]
                    self.broker_groups[group]["leader"] = leader_info
                    self.logger.info(f"Group {group} primary broker updated: {leader_info}")
                except Exception as e:
                    self.logger.error(f"Error processing leader update for {group}: {str(e)}")
            elif event and event.type == "DELETED":
                self.logger.warning(f"Leader node for {group} was deleted")
                self.broker_groups[group]["leader"] = None
        
        @self.zk.ChildrenWatch(replicas_path)
        def watch_replicas(children):
            self.logger.info(f"Group {group} broker replicas changed: {len(children)} replicas")
            try:
                new_brokers = set()
                
                for broker_id in children:
                    try:
                        broker_path = f"{replicas_path}/{broker_id}"
                        data, _ = self.zk.get(broker_path)
                        if data:
                            broker_addr = data.decode()
                            new_brokers.add(broker_addr)
                    except Exception as e:
                        self.logger.error(f"Error getting broker data: {str(e)}")
                
                old_brokers = self.broker_groups[group]["replicas"].copy()
                self.broker_groups[group]["replicas"] = new_brokers
                
                for broker in new_brokers - old_brokers:
                    self._connect_to_broker(broker)
                
                for broker in old_brokers - new_brokers:
                    self._disconnect_from_broker(broker)
            except Exception as e:
                self.logger.error(f"Error processing replica updates for {group}: {str(e)}")
    
    def _setup_frontend_sockets(self, pub_port, sub_port):
        """Set up frontend sockets for publishers and subscribers."""
        # For publishers: Use PULL to receive from PUB clients
        self.pub_frontend = self.context.socket(zmq.PULL)
        self.pub_frontend.bind(f"tcp://*:{pub_port}")
        self.logger.info(f"Publisher frontend bound to port {pub_port}")
        
        # For subscribers: Use XPUB to manage subscriptions from SUB clients
        self.sub_frontend = self.context.socket(zmq.XPUB) 
        self.sub_frontend.bind(f"tcp://*:{sub_port}")
        self.logger.info(f"Subscriber frontend bound to port {sub_port}")
        
        heartbeat_port = sub_port + 100
        self.sub_heartbeat = self.context.socket(zmq.PULL)
        self.sub_heartbeat.bind(f"tcp://*:{heartbeat_port}")
        self.logger.info(f"Subscriber heartbeat socket bound to port {heartbeat_port}")
        
        self.heartbeat_port = heartbeat_port
    
    def _setup_backend_socket(self):
        """Set up backend socket to connect to broker instances."""
        # Use XSUB for sending to brokers - allows for subscription forwarding
        self.backend = self.context.socket(zmq.XSUB)
        # Set socket options to avoid slow subscriber issues
        self.backend.setsockopt(zmq.RCVHWM, 10000)    # Increase receive high water mark
        self.backend.setsockopt(zmq.SNDHWM, 10000)    # Increase send high water mark
        self.logger.info("Backend socket created for broker connections")
        return None  # No need to bind, we're connecting to brokers
    
    def _connect_to_broker(self, broker_addr):
        """Connect to a broker instance."""
        try:
            # Check if backend socket is initialized
            if self.backend is None:
                self.logger.error("Backend socket not initialized, cannot connect to broker")
                return False
                
            # Parse the broker address to extract just host:port
            # It can come in formats like "host:port" or "host:port:role" 
            parts = broker_addr.split(":")
            if len(parts) >= 2:
                clean_addr = f"{parts[0]}:{parts[1]}"
            else:
                clean_addr = broker_addr
                
            connection_url = f"tcp://{clean_addr}"
            self.logger.info(f"Connecting to broker at {connection_url} (raw addr: {broker_addr})")
            
            # Connect to the broker
            self.backend.connect(connection_url)
            self.logger.info(f"Connected to broker at {connection_url}")
                
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to broker {broker_addr}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
    
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
            self.zk.ensure_path(self.LOAD_BALANCERS_PATH)
            
            lb_id = f"lb_{addr}_{pub_port}"
            lb_node_path = f"{self.LOAD_BALANCERS_PATH}/{lb_id}"
            
            # Use a simpler format that the broker expects (only addr:pub_port:sub_port)
            lb_data = f"{addr}:{pub_port}:{sub_port}"
            
            if self.zk.exists(lb_node_path):
                self.zk.delete(lb_node_path)
            
            self.zk.create(lb_node_path, lb_data.encode(), ephemeral=True)
            self.logger.info(f"Registered load balancer in ZooKeeper at {lb_node_path} with data: {lb_data}")
            
            return True
        except Exception as e:
            self.logger.error(f"Error registering in ZooKeeper: {str(e)}")
            return False
    
    def _handle_subscription(self, frontend, backend, message):
        """Handle subscription messages from XPUB socket."""
        try:
            # XPUB provides subscription messages as: [1, topic] for subscribe
            # and [0, topic] for unsubscribe
            if len(message) < 1:
                self.logger.warning("Received empty subscription message")
                return

            is_subscribe = message[0] == 1
            topic = message[1:].decode() if len(message) > 1 else ""
            
            if is_subscribe:
                self.logger.info(f"New subscription for topic: '{topic}'")
            else:
                self.logger.info(f"Unsubscribe from topic: '{topic}'")
            
            # Find the appropriate broker group for this topic
            target_group = self.topic_to_group.get(topic, self.default_group)
            self.logger.info(f"Routing {topic} subscription to broker group '{target_group}'")
            
            # Forward the subscription to the appropriate broker
            if target_group in self.broker_groups:
                # Check if backend socket exists
                if backend is None:
                    self.logger.error("Backend socket not initialized")
                    return
                    
                # Forward subscription to backend - for XSUB socket
                try:
                    backend.send(message)
                    self.logger.info(f"Forwarded subscription to broker group '{target_group}'")
                except Exception as e:
                    self.logger.error(f"Error sending subscription: {str(e)}")
            else:
                self.logger.warning(f"No broker group '{target_group}' available for topic '{topic}'")
        except Exception as e:
            self.logger.error(f"Error handling subscription: {str(e)}")
            self.logger.error(traceback.format_exc())
    
    def _get_next_broker_for_group(self, group):
        """Get the next broker for a group using round-robin load balancing."""
        if group not in self.broker_groups or not self.broker_groups[group]['replicas']:
            return None
            
        replicas = list(self.broker_groups[group]['replicas'])
        
        # If we have a primary/leader, prioritize it
        if self.broker_groups[group]['leader']:
            return self.broker_groups[group]['leader']
            
        # No leader, use round-robin among replicas
        if group not in self.last_used_indices:
            self.last_used_indices[group] = 0
        else:
            self.last_used_indices[group] = (self.last_used_indices[group] + 1) % len(replicas)
            
        return replicas[self.last_used_indices[group]]
    
    def run(self):
        """Run the load balancer proxy."""
        try:
            self.logger.info("BrokerLoadBalancer::run - Starting load balancer")
            
            # Check if all required sockets are initialized
            if not all([self.pub_frontend, self.sub_frontend, self.backend]):
                self.logger.error("Not all sockets are initialized. Aborting.")
                return
            
            # Setup pollers
            frontend_poller = zmq.Poller()
            frontend_poller.register(self.pub_frontend, zmq.POLLIN)
            frontend_poller.register(self.sub_frontend, zmq.POLLIN)
            
            # Also poll the backend for any messages from brokers
            backend_poller = zmq.Poller()
            backend_poller.register(self.backend, zmq.POLLIN)
            
            heartbeat_poller = zmq.Poller()
            heartbeat_poller.register(self.sub_heartbeat, zmq.POLLIN)
            
            self.subscribers = {"count": 0, "identities": set()}
            
            # Track last status update time
            last_status_time = time.time()
            
            self.logger.info("BrokerLoadBalancer::run - Main event loop starting")
            while True:
                # Check frontend events
                events = dict(frontend_poller.poll(100))  # Short timeout
                
                # Handle publisher messages (PULL socket)
                if self.pub_frontend in events:
                    try:
                        message = self.pub_frontend.recv()
                        # Extract topic from message for routing
                        topic_end = message.find(b":")
                        if topic_end != -1:
                            topic = message[:topic_end].decode()
                            target_group = self.topic_to_group.get(topic, self.default_group)
                            self.logger.debug(f"Routing message on topic '{topic}' to broker group '{target_group}'")
                            
                            # Forward to backends using round-robin within target group
                            if target_group in self.broker_groups and self.broker_groups[target_group]:
                                broker = self._get_next_broker_for_group(target_group)
                                if broker:
                                    try:
                                        self.logger.debug(f"Forwarding message to broker: {broker}")
                                        # For XSUB socket, no need for multipart
                                        self.backend.send(message)
                                    except Exception as e:
                                        self.logger.error(f"Error forwarding message to broker: {str(e)}")
                                else:
                                    self.logger.warning(f"No available broker for group '{target_group}'")
                            else:
                                self.logger.warning(f"No broker group '{target_group}' available")
                        else:
                            self.logger.warning("Received message with invalid format (no topic delimiter)")
                    except Exception as e:
                        self.logger.error(f"Error handling publisher message: {str(e)}")
                
                # Handle subscriber messages (XPUB socket)
                if self.sub_frontend in events:
                    try:
                        message = self.sub_frontend.recv()
                        # Handle subscription message
                        self._handle_subscription(self.sub_frontend, self.backend, message)
                    except Exception as e:
                        self.logger.error(f"Error handling subscriber message: {str(e)}")
                
                # Check backend for messages from brokers (may need to forward to subscribers)
                events = dict(backend_poller.poll(1))
                if self.backend in events:
                    try:
                        message = self.backend.recv()
                        self.logger.debug("Received message from broker")
                        # Forward to subscribers
                        self.sub_frontend.send(message)
                    except Exception as e:
                        self.logger.error(f"Error handling message from broker: {str(e)}")
                
                # Check for heartbeat messages
                events = dict(heartbeat_poller.poll(1))
                if self.sub_heartbeat in events:
                    try:
                        message = self.sub_heartbeat.recv()
                        self.logger.debug("Received heartbeat message")
                    except Exception as e:
                        self.logger.error(f"Error processing heartbeat: {str(e)}")
                
                # Periodic status update
                current_time = time.time()
                if current_time - last_status_time > self.STATUS_UPDATE_INTERVAL:
                    self.logger.info(f"Load balancer status: {len(self.broker_groups)} broker groups")
                    for group, info in self.broker_groups.items():
                        self.logger.info(f"Group {group}: Leader={info['leader']}, {len(info['replicas'])} replicas")
                    last_status_time = current_time
                    
                # Short sleep to prevent tight loop
                time.sleep(0.01)
                    
        except KeyboardInterrupt:
            self.logger.info("BrokerLoadBalancer::run - Interrupted")
        except Exception as e:
            self.logger.error(f"BrokerLoadBalancer::run - Error: {str(e)}")
            self.logger.error(traceback.format_exc())
        finally:
            self.cleanup()
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down")
        self.cleanup()
        sys.exit(0)
    
    def cleanup(self):
        """Clean up resources."""
        self.logger.info("BrokerLoadBalancer::cleanup")
        
        if hasattr(self, 'pub_frontend') and self.pub_frontend:
            self.pub_frontend.close()
            
        if hasattr(self, 'sub_frontend') and self.sub_frontend:
            self.sub_frontend.close()
            
        if hasattr(self, 'sub_heartbeat') and self.sub_heartbeat:
            self.sub_heartbeat.close()
            
        if hasattr(self, 'backend') and self.backend:
            self.backend.close()
        
        if hasattr(self, 'context') and self.context:
            self.context.term()
        
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
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("BrokerLoadBalancer")
    
    args = parse_args()
    logger.setLevel(args.loglevel)
    
    lb = BrokerLoadBalancer(logger)
    if lb.configure(args):
        lb.run()


if __name__ == "__main__":
    main()