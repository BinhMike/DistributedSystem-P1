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
from CS6381_MW.BrokerMW import BrokerMW  
from CS6381_MW import discovery_pb2  


# Publishers -> Broker; Broker -> Subscribers



class BrokerAppln():
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None  
        self.state = "REGISTER"  
        self.name = None  

    def configure(self, args):
        self.logger.info("BrokerAppln::configure")
        self.name = args.name 
        self.mw_obj = BrokerMW(self.logger)  
        self.mw_obj.configure(args)  
        self.logger.info("BrokerAppln::configure - completed")

    def driver(self):
        # Starting event Loop
        try:
            self.logger.info("BrokerAppln::driver - starting event loop")
            self.mw_obj.set_upcall_handle(self)
            self.mw_obj.event_loop(timeout=0)  # enter event loop
        except Exception as e:
            raise e

    def invoke_operation(self):
        ''' Invoke operating depending on state  '''
        try:
            self.logger.info("BrokerAppln::invoke_operation")
            # check state
            if self.state == "REGISTER": # 
                self.logger.debug("BrokerAppln::invoke_operation - registering with Discovery")
                self.mw_obj.register(self.name)
                return None
            elif self.state == "DISPATCH":
                self.logger.debug("BrokerAppln::invoke_operation - dispatching messages")
                self.mw_obj.forward_messages()
                return None
        except Exception as e:
            raise e

    def register_response(self, response):
        self.logger.info("BrokerAppln::register_response")
        if response.status == discovery_pb2.STATUS_SUCCESS:
            self.logger.debug("BrokerAppln::register_response - registration success")
            self.state = "DISPATCH"  # Dispatch staage
            return 0
        else:
            raise ValueError("Broker registration failed")



def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Broker Application")

    parser.add_argument("-n", "--name", default="broker", help="Broker name")
    parser.add_argument("-p", "--port", type=int, default=6000, help="Broker port")# broker dispatch message to subscriber
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery Service IP:Port")
    parser.add_argument("--publisher_ip", default="localhost", help="Publisher IP Address") # Publisher ip. Publisher send message to broker
    parser.add_argument("--publisher_port", type=int, default=6001, help="Publisher Port")

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

