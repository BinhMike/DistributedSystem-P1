import time
import argparse
import logging
import traceback  # Import traceback
from enum import Enum  # Import Enum
from kazoo.client import KazooClient
from topic_selector import TopicSelector  # Importing TopicSelector
from CS6381_MW.SubscriberMW import SubscriberMW
from CS6381_MW import discovery_pb2

class SubscriberAppln:
    # Add the State enum definition
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        LOOKUP = 3,  # Added LOOKUP state
        RUNNING = 4,
        COMPLETED = 5

    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None
        self.zk = None
        self.discovery_addr = None
        self.name = None
        self.topiclist = None  # Will be initialized using TopicSelector
        self.dissemination_strategy = None  # Will be set during configuration
        self.state = self.State.INITIALIZE  # Initialize state
        self.received_count = 0  # Track received messages
        self.iters = 0  # Number of messages to receive (0 for infinite)
        self.requested_history = {}  # topic -> required history length


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
        
        for t in self.topiclist:
            self.requested_history[t] = 20
        self.logger.info(f"SubscriberAppln:: Requested history: {self.requested_history}")

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
        """Handle the response for a lookup request"""
        try:
            self.logger.info("SubscriberAppln::lookup_response")

            # Check if any publishers/brokers were found
            if not lookup_resp.publishers:
                self.logger.warning("No publishers/brokers found for the requested topics.")
                # Decide how to handle this - maybe retry lookup later?
                return 10000 # Retry lookup after 10 seconds

            # Determine if we are in broker mode or direct publisher mode
            # A simple heuristic: if the ID contains "broker", assume broker mode.
            # A more robust approach might involve checking ZK paths or specific metadata.
            is_broker_mode = any("broker" in pub.id.lower() for pub in lookup_resp.publishers)
            is_lb_mode = any("lb_" in pub.id.lower() for pub in lookup_resp.publishers) # Check for load balancer

            if is_lb_mode:
                 self.logger.info("Load Balancer mode detected.")
                 # Connect to the first load balancer found
                 lb_info = lookup_resp.publishers[0] # Assuming only one LB for now
                 lb_addr = f"{lb_info.addr}:{lb_info.port}"
                 self.logger.info(f"Attempting to connect to Load Balancer at {lb_addr}")
                 # Use the new subscribe method - it handles connection and filtering
                 if self.mw_obj.subscribe(self.topiclist):
                      self.logger.info(f"Successfully subscribed to topics via Load Balancer {lb_addr}")
                      self.state = self.State.RUNNING
                      return 0 # Success, proceed with normal operation timeout
                 else:
                      self.logger.error(f"Failed to subscribe via Load Balancer {lb_addr}")
                      return 5000 # Retry connection/subscription

            elif is_broker_mode:
                self.logger.info(f"Broker mode detected with {len(lookup_resp.publishers)} brokers/groups found.")
                # Use the new subscribe method - it handles ZK lookup, connection, and filtering
                if self.mw_obj.subscribe(self.topiclist):
                     self.logger.info("Successfully subscribed to topics via Brokers.")
                     self.state = self.State.RUNNING
                     return 0 # Success
                else:
                     self.logger.error("Failed to subscribe via Brokers.")
                     return 5000 # Retry

            else: # Direct Publisher mode
                self.logger.info(f"Direct Publisher mode detected with {len(lookup_resp.publishers)} publishers found.")
                filtered_publishers = []
                for pub_info in lookup_resp.publishers:
                    try:
                        if "|" not in pub_info.id:
                            continue
                        pub_id, metadata = pub_info.id.split("|", 1)
                        topics = metadata.split(",")
                        history_ok = True
                        for item in topics:
                            if ":" in item:
                                topic, grp_hist = item.split(":")
                                if "." in grp_hist:
                                    _, hist = grp_hist.split(".")
                                    required = self.requested_history.get(topic.strip(), 0)
                                    if int(hist) < required:
                                        self.logger.info(f"Skipping publisher {pub_id} - insufficient history for topic '{topic}'")
                                        history_ok = False
                                        break
                        if history_ok:
                            filtered_publishers.append(pub_info)
                    except Exception as e:
                        self.logger.warning(f"Failed to parse publisher metadata: {e}")


                connected_count = 0
                for pub_info in lookup_resp.publishers:
                    pub_addr = f"{pub_info.addr}:{pub_info.port}"
                    # Use the new subscribe method - it handles connection and filtering
                    # We subscribe to all topics with each publisher found in this simple model
                    if self.mw_obj.subscribe(self.topiclist): # subscribe handles the connection logic now
                         self.logger.info(f"Successfully subscribed to topics via Publisher {pub_addr}")
                         connected_count +=1
                    else:
                         self.logger.warning(f"Failed to subscribe via Publisher {pub_addr}")

                if connected_count > 0:
                    self.state = self.State.RUNNING
                    return 0 # Success
                else:
                    self.logger.error("Failed to connect to any publishers.")
                    return 5000 # Retry

        except Exception as e:
            self.logger.error(f"Error in lookup_response: {e}")
            self.logger.error(traceback.format_exc())
            return 5000 # Retry on error

    def process_message(self, topic, content, latency):
        """Process received messages (upcall from middleware)"""
        self.logger.info(f"Received message for topic \'{topic}\': {content} (Latency: {latency:.6f}s)")
        # Add any application-specific processing here
        self.received_count += 1
        if self.iters > 0 and self.received_count >= self.iters:
             self.logger.info(f"Received {self.received_count} messages, completing.")
             self.state = self.State.COMPLETED

    def invoke_operation(self):
        """ Invoke operations based on the current state """
        try:
            if self.state == self.State.REGISTER:
                self.logger.info("SubscriberAppln::invoke_operation - Registering")
                self.mw_obj.register(self.name, self.topiclist)
                # We will transition state in the register_response callback
                return None # Wait for response

            elif self.state == self.State.LOOKUP:
                self.logger.info("SubscriberAppln::invoke_operation - Looking up publishers/brokers")
                self.mw_obj.lookup_publishers(self.topiclist)
                # We will transition state in the lookup_response callback
                return None # Wait for response

            elif self.state == self.State.RUNNING:
                # In the running state, we just wait for messages.
                # The event loop timeout is handled by handle_subscription or handle_reply.
                # We can return a default timeout here if needed, e.g., for periodic checks.
                # self.logger.debug("SubscriberAppln::invoke_operation - Running, waiting for messages")
                return 1000 # Default timeout while running

            elif self.state == self.State.COMPLETED:
                self.logger.info("SubscriberAppln::invoke_operation - Completed")
                # Signal the middleware event loop to stop
                self.mw_obj.disable_event_loop()
                return None # No further timeout needed

            else: # Includes INITIALIZE, CONFIGURE
                 # These states should transition quickly, but return a short timeout just in case
                 return 100

        except Exception as e:
            self.logger.error(f"Exception in invoke_operation: {e}")
            self.logger.error(traceback.format_exc())
            # Consider stopping or retrying based on the error
            self.mw_obj.disable_event_loop() # Stop on error for now
            return None

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
