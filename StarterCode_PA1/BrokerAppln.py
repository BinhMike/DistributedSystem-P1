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

        # connect to ZooKeeper
        self.zk = KazooClient(hosts=args.zookeeper)
        self.zk.start()

        # create /brokers 
        self.zk.ensure_path(self.zk_path)
        broker_address = f"{args.addr}:{args.port}"
        broker_node_path = f"{self.zk_path}/{self.name}"
        
        if self.zk.exists(broker_node_path):
            self.zk.delete(broker_node_path)

        self.zk.create(broker_node_path, broker_address.encode(), ephemeral=True)
        self.logger.info(f"Broker registered in ZooKeeper at {broker_node_path}")


        # initialize middleware
        self.mw_obj = BrokerMW(self.logger, self.zk)  
        self.mw_obj.configure(args)  
        self.logger.info("BrokerAppln::configure - completed")

    def driver(self):
        # Starting event Loop
        try:
            self.logger.info("BrokerAppln::driver - starting event loop")
            self.mw_obj.set_upcall_handle(self)
            self.mw_obj.event_loop(timeout=0)  # enter event loop
        except Exception as e:
            self.logger.error(f"BrokerAppln::driver - error: {str(e)}")
            self.cleanup()


    def invoke_operation(self):
        """ Invoke operation for message forwarding """
        try:
            self.logger.info("BrokerAppln::invoke_operation - dispatching messages")
            self.mw_obj.forward_messages()
            return None
        except Exception as e:
            raise e
    

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
    parser.add_argument("-p", "--port", type=int, default=6000, help="Broker port")# broker dispatch message to subscriber
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery Service IP:Port")
    parser.add_argument("--publisher_ip", default="localhost", help="Publisher IP Address") # Publisher ip. Publisher send message to broker
    parser.add_argument("--publisher_port", type=int, default=6001, help="Publisher Port")
    parser.add_argument("--addr", default="localhost", help="Broker's advertised address") 
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", help="ZooKeeper Address")
    return parser.parse_args()
    
###################################
#
# Main program
#
###################################

def main():
    logger = logging.getLogger("BrokerAppln")
    args = parseCmdLineArgs()
    broker_app = BrokerAppln(logger)
    broker_app.configure(args)
    broker_app.driver()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()

