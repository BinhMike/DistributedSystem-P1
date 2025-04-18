import time
import argparse
import logging
from kazoo.client import KazooClient
from topic_selector import TopicSelector  # Importing TopicSelector
from CS6381_MW.SubscriberMW import SubscriberMW
from CS6381_MW import discovery_pb2

class SubscriberAppln:
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None
        self.zk = None
        self.discovery_addr = None
        self.name = None
        self.topiclist = None  # Will be initialized using TopicSelector
        self.dissemination_strategy = None  # Will be set during configuration
        
        # ZooKeeper paths
        self.zk_paths = {
            "discovery_leader": "/discovery/leader",
            "discovery_replicas": "/discovery/replicas",
            "publishers": "/publishers",
            "brokers": "/brokers",
            "subscribers": "/subscribers",
            "load_balancers": "/load_balancers"
        }

    def configure(self, args):
        """ Configure the subscriber application """
        self.logger.info("SubscriberAppln::configure")
        self.name = args.name

        # Read configuration file to determine dissemination strategy
        import configparser
        config = configparser.ConfigParser()
        config.read(args.config if hasattr(args, 'config') and args.config else "config.ini")
        self.dissemination_strategy = config.get("Dissemination", "Strategy", fallback="Direct")
        self.logger.info(f"Dissemination strategy: {self.dissemination_strategy}")
        
        # Initialize topic list using TopicSelector
        ts = TopicSelector()
        self.topiclist = ts.interest(args.num_topics)  # Get random topics
        self.logger.info(f"SubscriberAppln:: Selected topics: {self.topiclist}")

        # Initialize middleware first before setting ZooKeeper watch
        self.mw_obj = SubscriberMW(self.logger)

        # Connect to ZooKeeper
        self.logger.info("Connecting to ZooKeeper at {}".format(args.zookeeper))
        self.zk = KazooClient(hosts=args.zookeeper)
        self.zk.start()

        # Create initial ZooKeeper structure if needed
        self._ensure_zk_paths_exist()

        # Watch for discovery leader node
        self.zk.DataWatch(self.zk_paths["discovery_leader"], self.update_discovery_info)

        # Wait until we have a discovery service address
        self._wait_for_discovery()

        # Configure middleware with ZooKeeper client
        self.mw_obj.configure(self.discovery_addr, self.zk)

        self.logger.info("SubscriberAppln::configure - Configuration complete")

    def _ensure_zk_paths_exist(self):
        """Ensure base ZooKeeper paths exist"""
        try:
            # Create all necessary base paths
            for path in self.zk_paths.values():
                if not self.zk.exists(path):
                    self.zk.ensure_path(path)
                    self.logger.info(f"Created ZooKeeper path: {path}")
        except Exception as e:
            self.logger.error(f"Error creating ZooKeeper paths: {str(e)}")

    def _wait_for_discovery(self):
        """Wait until a discovery service address is available"""
        while self.discovery_addr is None:
            self.logger.info("Waiting for Discovery service address...")
            # Check if leader node exists directly
            if self.zk.exists(self.zk_paths["discovery_leader"]):
                data, _ = self.zk.get(self.zk_paths["discovery_leader"])
                if data:
                    self.discovery_addr = data.decode().split('|')[0]  # Extract address from "addr:port|expiry"
                    self.logger.info(f"Found Discovery service at: {self.discovery_addr}")
                    break
            time.sleep(2)

    def update_discovery_info(self, data, stat, event=None):
        """ Update Discovery service address if it changes """
        if data:
            new_addr = data.decode("utf-8")
            # Extract just the address:port part if it includes expiry info
            if '|' in new_addr:
                new_addr = new_addr.split('|')[0]
                
            if new_addr != self.discovery_addr:
                self.logger.info(f"Discovery changed to {new_addr}, reconnecting...")
                self.discovery_addr = new_addr
                # Only reconnect if middleware has been configured (i.e. req is not None)
                if self.mw_obj and getattr(self.mw_obj, 'req', None):
                    self.mw_obj.connect_to_discovery(self.discovery_addr)
                    self.register()

    def driver(self):
        """ Start the subscriber event loop """
        self.logger.info("SubscriberAppln::driver")
        self.mw_obj.set_upcall_handle(self)
        self.register()
        self.mw_obj.event_loop()

    def register(self):
        """ Register with Discovery and lookup publishers """
        self.logger.info("SubscriberAppln::register")
        self.mw_obj.register(self.name, self.topiclist)

    def register_response(self, reg_resp):
        """Handle registration response and lookup publishers"""
        if reg_resp.status == discovery_pb2.Status.STATUS_SUCCESS:
            self.logger.info("SubscriberAppln::register_response - Registration successful")
            self.mw_obj.lookup_publishers(self.topiclist)
        elif reg_resp.status == discovery_pb2.Status.STATUS_NOT_PRIMARY:
            # If the current discovery is not primary, try to find the correct one
            self.logger.info("Not connected to primary Discovery. Attempting to locate primary...")
            self._locate_primary_discovery()
        return None

    def _locate_primary_discovery(self):
        """Attempt to directly find primary discovery through ZooKeeper"""
        try:
            # Read leader node again
            if self.zk.exists(self.zk_paths["discovery_leader"]):
                data, _ = self.zk.get(self.zk_paths["discovery_leader"])
                if data:
                    new_addr = data.decode().split('|')[0]  # Extract address from "addr:port|expiry"
                    if new_addr != self.discovery_addr:
                        self.discovery_addr = new_addr
                        self.logger.info(f"Found primary Discovery at: {self.discovery_addr}")
                        self.mw_obj.connect_to_discovery(self.discovery_addr)
                        self.register()
                    else:
                        self.logger.warning("Already connected to the leader address. Waiting for leader state to stabilize.")
                        # Wait briefly before retrying
                        time.sleep(2)
                        self.register()
        except Exception as e:
            self.logger.error(f"Error locating primary: {str(e)}")

    def lookup_response(self, lookup_resp):
        """ Subscribe to publishers/brokers that disseminate the selected topics """
        """ Subscriber receive either publisher or broker address msg, which depends on discovery."""
        try:
            self.logger.info("SubscriberAppln::lookup_response")
            
            # In viaBroker mode, we should always connect to load balancers first
            # This ensures we follow the proper architecture
            if hasattr(self, 'dissemination_strategy') and self.dissemination_strategy == "ViaBroker":
                self.logger.info("ViaBroker mode detected, prioritizing load balancer connection")
                # Always try to connect to load balancers first in viaBroker mode
                if self._try_connect_to_load_balancer():
                    self.logger.info("Successfully connected to load balancer, skipping direct connections")
                    return None
            
            # Check if we got any publishers/brokers
            if not lookup_resp.publishers:
                self.logger.info("No publishers/brokers found in discovery response")
                # Try to connect to brokers directly as a fallback
                self._try_connect_to_brokers()
                return 1

            # First, attempt to detect brokers based on ID format
            # Either by group identifier or by broker name prefix
            broker_publishers = []
            normal_publishers = []
            
            for pub in lookup_resp.publishers:
                # If it has a group identifier or starts with "broker", consider it a broker
                if ((":" in pub.id and "grp=" in pub.id) or 
                    pub.id.startswith("broker") or 
                    "broker" in pub.id.lower()):
                    broker_publishers.append(pub)
                else:
                    normal_publishers.append(pub)
            
            broker_connected = False
            
            # If broker publishers are available, prefer them over direct publishers
            if broker_publishers:
                self.logger.info(f"Using broker mode with {len(broker_publishers)} brokers")
                for pub in broker_publishers:
                    if pub.addr and pub.port:
                        pub_address = f"tcp://{pub.addr}:{pub.port}"
                        self.logger.info(f"Connecting to broker at {pub_address} (ID: {pub.id})")
                        self.mw_obj.subscribe_to_topics(pub_address, self.topiclist)
                        broker_connected = True
            
            # If no brokers connected, use direct publishers
            if not broker_connected and normal_publishers:
                self.logger.info(f"Using direct publisher mode with {len(normal_publishers)} publishers")
                for pub in normal_publishers:
                    if pub.addr and pub.port:
                        pub_address = f"tcp://{pub.addr}:{pub.port}"
                        self.logger.info(f"Connecting to publisher at {pub_address}")
                        self.mw_obj.subscribe_to_topics(pub_address, self.topiclist)
            
            # If we haven't connected to anything at this point, try direct ZooKeeper lookup
            if not broker_connected and not normal_publishers:
                self.logger.info("No successful connections made through discovery, trying direct ZooKeeper lookup")
                self._try_connect_to_brokers()
            
            self.logger.info("Moving to LISTENING state")
            return None

        except Exception as e:
            self.logger.error(f"Error in lookup_response: {str(e)}")
            raise e
            
    def _try_connect_to_load_balancer(self):
        """Connect to a load balancer directly through ZooKeeper, used primarily for viaBroker mode"""
        try:
            # Check for load balancers registered in ZooKeeper
            if self.zk.exists(self.zk_paths["load_balancers"]):
                self.logger.info("Checking for load balancers in ZooKeeper")
                lb_nodes = self.zk.get_children(self.zk_paths["load_balancers"])
                
                if lb_nodes:
                    self.logger.info(f"Found load balancers: {lb_nodes}")
                    for lb_node in lb_nodes:
                        lb_path = f"{self.zk_paths['load_balancers']}/{lb_node}"
                        data, _ = self.zk.get(lb_path)
                        
                        if data:
                            # Load balancer data format is "addr:pub_port:sub_port"
                            lb_info = data.decode()
                            self.logger.info(f"Load balancer info: {lb_info}")
                            
                            parts = lb_info.split(":")
                            if len(parts) >= 3:
                                lb_addr = parts[0]
                                lb_sub_port = parts[2]  # Use the subscriber port from the LB
                                pub_address = f"tcp://{lb_addr}:{lb_sub_port}"
                                
                                self.logger.info(f"Connecting to load balancer at {pub_address}")
                                self.mw_obj.subscribe_to_topics(pub_address, self.topiclist)
                                return True  # Successfully connected to a load balancer
            
            self.logger.info("No load balancers found in ZooKeeper")
            return False
                
        except Exception as e:
            self.logger.error(f"Error connecting to load balancer: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False

    def _try_connect_to_brokers(self):
        """Try to discover and connect to load balancers or brokers directly through ZooKeeper"""
        try:
            # First, try to find and connect to load balancers
            if self.zk.exists(self.zk_paths["load_balancers"]):
                self.logger.info("Checking for load balancers in ZooKeeper")
                lb_nodes = self.zk.get_children(self.zk_paths["load_balancers"])
                
                if lb_nodes:
                    self.logger.info(f"Found load balancers: {lb_nodes}")
                    for lb_node in lb_nodes:
                        lb_path = f"{self.zk_paths['load_balancers']}/{lb_node}"
                        data, _ = self.zk.get(lb_path)
                        
                        if data:
                            # Load balancer data format is "addr:pub_port:sub_port"
                            lb_info = data.decode()
                            self.logger.info(f"Load balancer info: {lb_info}")
                            
                            parts = lb_info.split(":")
                            if len(parts) >= 3:
                                lb_addr = parts[0]
                                lb_sub_port = parts[2]  # Use the subscriber port from the LB
                                pub_address = f"tcp://{lb_addr}:{lb_sub_port}"
                                
                                self.logger.info(f"Connecting to load balancer at {pub_address}")
                                self.mw_obj.subscribe_to_topics(pub_address, self.topiclist)
                                return  # Successfully connected to a load balancer
            
            # If no load balancer found, fall back to direct broker connection
            self.logger.info("No load balancers found, trying direct broker connection")
            
            if not self.zk.exists(self.zk_paths["brokers"]):
                self.logger.info("No broker groups found in ZooKeeper")
                return
                
            # Find all broker groups
            children = self.zk.get_children(self.zk_paths["brokers"])
            # Filter out non-group nodes like 'leader', 'replicas', 'spawn_lock'
            broker_groups = [child for child in children if child.startswith("group")] 
            
            if not broker_groups:
                self.logger.info("No broker groups found under /brokers")
                return
                
            self.logger.info(f"Found broker groups: {broker_groups}. Attempting direct connection.")
                
            for group in broker_groups:
                group_path = f"{self.zk_paths['brokers']}/{group}"
                leader_path = f"{group_path}/leader"
                
                # If this group has a leader, connect to it
                if self.zk.exists(leader_path):
                    self.logger.info(f"Checking leader for group {group} at {leader_path}")
                    data = None
                    retries = 3 # Try up to 3 times
                    for attempt in range(retries):
                        data, stat = self.zk.get(leader_path)
                        if data:
                            self.logger.debug(f"Got data on attempt {attempt+1} for {leader_path}")
                            break # Got data, proceed
                        else:
                            self.logger.warning(f"Broker leader node {leader_path} exists but has no data (attempt {attempt+1}/{retries}). Retrying shortly...")
                            if attempt < retries - 1:
                                 time.sleep(0.2 * (attempt + 1)) # Exponential backoff delay
                            else:
                                 self.logger.error(f"Broker leader node {leader_path} still has no data after {retries} attempts. Skipping group {group}.")
                                 continue
                    
                    # Check if data was successfully retrieved after retries
                    if not data:
                        continue # Skip to the next group if no data after retries

                    # Decode and extract addr:port, handling potential lease info
                    try:
                        decoded_data = data.decode()
                        broker_addr_port = decoded_data.split('|')[0]  # Extract addr:port part
                    except Exception as decode_err:
                         self.logger.error(f"Error decoding data from {leader_path}: {decode_err}. Data: {data}. Skipping.")
                         continue

                    # Add check for empty or invalid address format
                    if not broker_addr_port or ':' not in broker_addr_port:
                        self.logger.error(f"Invalid broker address data found in {leader_path}: '{broker_addr_port}'. Skipping.")
                        continue
                        
                    pub_address = f"tcp://{broker_addr_port}"
                    
                    self.logger.info(f"Connecting directly to broker leader at {pub_address} for group {group}")
                    self.mw_obj.subscribe_to_topics(pub_address, self.topiclist)                     
                    
        except Exception as e:
            self.logger.error(f"Error connecting to load balancers or brokers: {str(e)}")
            # Log detailed traceback for debugging
            import traceback
            self.logger.error(traceback.format_exc())

    def process_message(self, topic, content):
        """ Process received messages from publishers """
        self.logger.info(f"SubscriberAppln::process_message - {topic}::{content}")
        return None

    def invoke_operation(self):
        # This method will be invoked on timeout or error
        # Add periodic logging to confirm we're still listening for messages
        if hasattr(self, 'last_status_time') and time.time() - self.last_status_time < 10:
            # Don't log too frequently - only every 10 seconds
            return 1000  # Return timeout in ms
            
        self.last_status_time = time.time()
        
        # Log connection status
        if hasattr(self.mw_obj, 'connected_publishers') and self.mw_obj.connected_publishers:
            self.logger.info(f"SubscriberAppln::invoke_operation - Actively listening for messages from {len(self.mw_obj.connected_publishers)} sources")
            for addr in self.mw_obj.connected_publishers:
                is_lb = "load_balancer" in addr.lower() or ":700" in addr
                source_type = "load balancer" if is_lb else "publisher/broker"
                self.logger.info(f"  - Connected to {source_type} at {addr}")
                
            # If connected to a load balancer, check if broker groups exist in ZooKeeper
            if any("load_balancer" in addr.lower() or ":700" in addr for addr in self.mw_obj.connected_publishers):
                try:
                    if self.zk and self.zk.exists(self.zk_paths["brokers"]):
                        broker_groups = [g for g in self.zk.get_children(self.zk_paths["brokers"]) if g.startswith("group")]
                        if broker_groups:
                            self.logger.info(f"  - Load balancer is connected to broker groups: {broker_groups}")
                        else:
                            self.logger.warning("  - No broker groups found. Messages may not be received until brokers are started")
                except Exception as e:
                    self.logger.error(f"Error checking broker groups: {str(e)}")
        else:
            self.logger.warning("SubscriberAppln::invoke_operation - Not connected to any publishers or brokers")
        
        return 1000  # Return timeout in ms

    def cleanup(self):
        """Clean up resources"""
        try:
            self.logger.info("SubscriberAppln::cleanup")
            
            # Clean up middleware
            if self.mw_obj:
                self.mw_obj.cleanup()
                
            # Close ZooKeeper connection
            if self.zk:
                self.zk.stop()
                self.zk.close()
                
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
            raise e

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Subscriber Application")
    parser.add_argument("-n", "--name", default="sub1", help="Unique Subscriber Name")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to subscribe")
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", help="ZooKeeper host:port")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level")
    parser.add_argument("-c", "--config", help="Path to configuration file", default="config.ini")
    return parser.parse_args()

def main():
    try:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger = logging.getLogger("SubscriberAppln")
        
        # Parse command line arguments
        args = parseCmdLineArgs()
        logger.setLevel(args.loglevel)
        
        # Create subscriber application
        subscriber = SubscriberAppln(logger)
        
        # Configure application
        subscriber.configure(args)
        
        # Start event loop
        subscriber.driver()
        
    except KeyboardInterrupt:
        logger.info("Subscriber application interrupted. Shutting down...")
        if 'subscriber' in locals():
            subscriber.cleanup()
        
    except Exception as e:
        logger.error(f"Exception in Subscriber application: {str(e)}")
        if 'subscriber' in locals():
            subscriber.cleanup()

if __name__ == "__main__":
    main()
