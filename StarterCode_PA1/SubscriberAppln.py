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

    def configure(self, args):
        """ Configure the subscriber application """
        self.logger.info("SubscriberAppln::configure")
        self.name = args.name

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

        # Watch for discovery leader node
        self.zk.DataWatch("/discovery/leader", self.update_discovery_info)

        while self.discovery_addr is None:
            time.sleep(2)

        self.mw_obj.configure(self.discovery_addr)

        self.logger.info("SubscriberAppln::configure - Configuration complete")

    def update_discovery_info(self, data, stat, event=None):
        """ Update Discovery service address if it changes """
        if data:
            new_addr = data.decode("utf-8")
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
        if reg_resp.status == discovery_pb2.STATUS_SUCCESS:
            self.logger.info("SubscriberAppln::register_response - Registration successful")
            self.mw_obj.lookup_publishers(self.topiclist)
        return None

    def lookup_response(self, lookup_resp):
        """ Subscribe to publishers that disseminate the selected topics """
        try:
            self.logger.info("SubscriberAppln::lookup_response")
            
            # Example using Direct strategy (like your pre-ZK version)
            if not lookup_resp.publishers:
                self.logger.error("No publishers found")
                return 1

            # Connect to each publisher
            for pub in lookup_resp.publishers:
                if pub.addr and pub.port:
                    pub_address = f"tcp://{pub.addr}:{pub.port}"
                    self.logger.info(f"Connecting to publisher at {pub_address}")
                    self.mw_obj.subscribe_to_topics(pub_address, self.topiclist)
            
            self.logger.info("Moving to LISTENING state")
            return None

        except Exception as e:
            self.logger.error(f"Error in lookup_response: {str(e)}")
            raise e

    def process_message(self, topic, content):
        """ Process received messages from publishers """
        self.logger.info(f"Received topic: {topic} | Content: {content}")

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Subscriber Application")
    parser.add_argument("-n", "--name", default="sub1", help="Subscriber Name")
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", help="ZooKeeper Address")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1, 10), default=1, help="Number of topics")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level")
    return parser.parse_args()

def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("SubscriberAppln")
    args = parseCmdLineArgs()
    app = SubscriberAppln(logger)
    app.configure(args)
    app.driver()

if __name__ == "__main__":
    main()
