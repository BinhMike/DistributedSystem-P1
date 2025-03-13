###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 

import sys
import time
import argparse
import logging
import signal
from kazoo.client import KazooClient
from CS6381_MW.BrokerMW import BrokerMW  
from CS6381_MW import discovery_pb2  


# Publishers -> Broker; Broker -> Subscribers

class BrokerAppln():
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None  
        self.name = None
        self.zk = None
        self.zk_path = "/brokers"
        signal.signal(signal.SIGINT, self.signal_handler)
        
    def configure(self, args):
        self.logger.info("BrokerAppln::configure")
        self.name = args.name 

        # Connect to ZooKeeper
        self.logger.info(f"BrokerAppln::configure - Connecting to ZooKeeper at {args.zookeeper}")
        self.zk = KazooClient(hosts=args.zookeeper)
        self.zk.start()
        self.logger.info("BrokerAppln::configure - Connected to ZooKeeper")

        # Ensure base paths exist
        self.zk.ensure_path(self.zk_path)
        
        # Initialize middleware - don't register ourselves in ZK here since the middleware will do it
        self.mw_obj = BrokerMW(self.logger, self.zk, False)  
        self.mw_obj.configure(args)
        
        self.logger.info("BrokerAppln::configure - completed")
        
    def driver(self):
        # Starting event Loop
        try:
            self.logger.info("BrokerAppln::driver - starting event loop")
            self.mw_obj.set_upcall_handle(self)
            
            # Main event loop
            while True:
                # Process any network events (with timeout)
                self.mw_obj.event_loop(timeout=100)  # 100ms timeout
                
                # Small sleep to prevent CPU spinning
                time.sleep(0.01)
                
        except Exception as e:
            self.logger.error(f"BrokerAppln::driver - error: {str(e)}")
            self.cleanup()
            return 

    def invoke_operation(self):
        """ Invoke operation for message forwarding """
        try:
            # We don't need to explicitly call forward_messages anymore since
            # the event_loop will handle forwarding when messages are received
            return None
        except Exception as e:
            self.logger.error(f"BrokerAppln::invoke_operation - error: {str(e)}")
            return None
    
    def signal_handler(self, signum, frame):
        """ Handle shutdown when interrupt signal is received """
        self.logger.info(f"BrokerAppln::signal_handler - received signal {signum}")
        self.cleanup()
        sys.exit(0)

    def cleanup(self):
        """ Cleanup the middleware """
        self.logger.info("BrokerAppln::cleanup")
        if self.mw_obj:
            self.mw_obj.cleanup()

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Broker Application")

    parser.add_argument("-n", "--name", default="broker", help="Broker name")
    parser.add_argument("-p", "--port", type=int, default=6000, help="Broker port")
    parser.add_argument("-a", "--addr", default="localhost", help="Broker's advertised address") 
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", help="ZooKeeper Address")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level (default=INFO)")
    return parser.parse_args()

###################################
#
# Main program
#
###################################

def main():
    # Configure logger
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.getLogger("kazoo").setLevel(logging.WARNING)
    logger = logging.getLogger("BrokerAppln")
    
    # Parse command line arguments
    args = parseCmdLineArgs()
    logger.setLevel(args.loglevel)
    
    # Create broker application instance
    broker_app = BrokerAppln(logger)
    
    try:
        # Configure the application
        broker_app.configure(args)
        
        # Start the event loop
        broker_app.driver()
        
    except Exception as e:
        logger.error(f"Exception in main: {str(e)}")
        broker_app.cleanup()
        sys.exit(1)

if __name__ == "__main__":
    main()

