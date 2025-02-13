###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise to the student. Design the logic in a manner similar
# to the PublisherAppln. As in the publisher application, the subscriber application
# will maintain a handle to the underlying subscriber middleware object.
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the publisher, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# let us know of each publisher that publishes the topic of interest to us. Then
# our middleware object will connect its SUB socket to all these publishers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.


from enum import Enum
import os
import sys
import time
import argparse
import logging
import configparser
from CS6381_MW.SubscriberMW import SubscriberMW
from CS6381_MW import discovery_pb2
from topic_selector import TopicSelector  # Import TopicSelector

##################################
#       Subscriber Application Class
##################################
class SubscriberAppln:
    # Define states similar to Publisher
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        REGISTER = 2
        ISREADY = 3
        LOOKUP = 4
        LISTENING = 5  # State where subscriber listens for data

    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None
        self.name = None
        self.topiclist = None
        self.num_topics = None
        self.state = self.State.INITIALIZE  # Initialize state


    ########################################
    # Configure Subscriber Application
    ########################################
    def configure(self, args):
        ''' Configure the Subscriber Application '''
        try:
            self.logger.info("SubscriberAppln::configure")

            # Parse config file
            self.config = configparser.ConfigParser()
            self.config.read(args.config)

            self.name = args.name
            self.num_topics = args.num_topics  

            # Select topics dynamically using TopicSelector
            ts = TopicSelector()
            self.topiclist = ts.interest(self.num_topics)  
            
            self.logger.info(f"SubscriberAppln::configure - Subscribing to topics: {self.topiclist}")

            # Initialize the middleware
            self.mw_obj = SubscriberMW(self.logger)
            self.mw_obj.configure(args)
            self.mw_obj.set_upcall_handle(self)

            self.logger.info("SubscriberAppln::configure - configuration complete")

        except Exception as e:
            raise e

    ########################################
    # Register with Discovery
    ########################################
    def register(self):
        ''' Register with Discovery Service '''
        self.logger.info("SubscriberAppln::registering with Discovery")
        self.mw_obj.register(self.name, self.topiclist)

    ########################################
    # Invoke operation method for Subscriber
    #
    # This method gets called by the middleware's event loop when a timeout occurs.
    # It checks if the system is ready, and if so, transitions to listening mode.
    ########################################
    def invoke_operation(self):
        ''' Invoke operation depending on state '''

        try:
            self.logger.info("SubscriberAppln::invoke_operation")

            # Check the current state of the subscriber
            if self.state == self.State.REGISTER:
                self.logger.debug("SubscriberAppln::invoke_operation - register with Discovery Service")
                self.mw_obj.register(self.name, self.topiclist)
                return None  # Wait for registration response

            elif self.state == self.State.ISREADY:
                self.logger.debug("SubscriberAppln::invoke_operation - check if ready")
                self.mw_obj.is_ready()  # Ask Discovery if the system is ready
                return None  # Wait for response
            
            elif self.state == self.State.LOOKUP:
                self.logger.debug("SubscriberAppln::invoke_operation - lookup publishers")
                self.mw_obj.lookup_publishers(self.topiclist)
                return None

            elif self.state == self.State.LISTENING:
                self.logger.debug("SubscriberAppln::invoke_operation - now listening for messages")
                return None  # Stay in event loop and wait for messages

            # elif self.state == self.State.COMPLETED:
            #     self.logger.debug("SubscriberAppln::invoke_operation - shutting down")
            #     self.mw_obj.disable_event_loop()  # Stop the event loop
            #     return None

            else:
                raise ValueError("Undefined state in SubscriberAppln")

        except Exception as e:
            raise e


    ########################################
    # Handle Lookup Response
    ########################################
    def lookup_response(self, lookup_resp):
        try:
            self.logger.info("SubscriberAppln::lookup_response")
            
            # Check dissemination strategy from config
            strategy = self.config["Dissemination"]["Strategy"]
            
            if strategy == "ViaBroker":
                # Check if broker field exists and has address
                if not lookup_resp.broker or not lookup_resp.broker.addr:
                    self.logger.error("No broker available")
                    return 1
                    
                broker_addr = f"tcp://{lookup_resp.broker.addr}:{lookup_resp.broker.port}"
                self.logger.info(f"Connecting to broker at {broker_addr}")
                self.mw_obj.subscribe_to_topics(broker_addr, None)
                
            else:  # Direct strategy
                # Check if publishers field has entries
                if not lookup_resp.publishers:
                    self.logger.error("No publishers found")
                    return 1
                    
                # Connect to each publisher
                for pub in lookup_resp.publishers:
                    if pub.addr and pub.port:  # Verify both address and port exist
                        pub_address = f"tcp://{pub.addr}:{pub.port}"
                        self.logger.info(f"Connecting to publisher at {pub_address}")
                        self.mw_obj.subscribe_to_topics(pub_address, self.topiclist)

            self.logger.info("Moving to LISTENING state")
            self.state = self.State.LISTENING
            return None

        except Exception as e:
            self.logger.error(f"Error in lookup_response: {str(e)}")
            raise e

    ########################################
    # Handle Register Response (Upcall)
    ########################################
    def register_response(self, reg_resp):
        ''' Handle register response from Discovery Service '''
        try:
            self.logger.info("SubscriberAppln::register_response")

            if reg_resp.status == discovery_pb2.STATUS_SUCCESS:
                self.logger.debug("SubscriberAppln::register_response - registration successful")

                # Move to ISREADY state to check if system is ready
                self.state = self.State.ISREADY

                # Return timeout 0 to immediately check readiness
                return 0
            else:
                self.logger.debug(f"SubscriberAppln::register_response - registration failed: {reg_resp.reason}")
                raise ValueError("Subscriber registration failed")

        except Exception as e:
            raise e

    ########################################
    # Handle IsReady Response (Upcall)
    ########################################

    def isready_response(self, isready_resp):
        ''' Handle response to is_ready request '''
        try:
            self.logger.info("SubscriberAppln::isready_response")

            if not isready_resp.status:
                # Discovery service is not ready yet
                self.logger.debug("SubscriberAppln::isready_response - Discovery not ready, retrying...")
                time.sleep(5)  # Sleep before retrying
            else:
                # Discovery service is ready
                self.logger.info("SubscriberAppln::isready_response - Discovery is ready! Transition to LOOKUP state")
                self.state = self.State.LOOKUP  # Transition to listening for messages

            # Return 0 so event loop calls `invoke_operation` again
            return 0

        except Exception as e:
            raise e

    ########################################
    # Handle Incoming Messages
    ########################################
    def process_message(self, topic, data):
        ''' Handle incoming messages '''
        self.logger.info(f"Received message: {topic} -> {data}")

    ########################################
    # Run the Subscriber
    ########################################
    def run(self):
        ''' Run Subscriber Event Loop '''
        self.register()
        self.mw_obj.event_loop()

###################################
# Parse command line arguments
###################################
def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Subscriber Application")

    parser.add_argument("-n", "--name", default="sub", help="Unique name for the subscriber")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery Service address (default: localhost:5555)")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1, 10), default=2, help="Number of topics to subscribe to (1-9)")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file (default: config.ini)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[10, 20, 30, 40, 50], help="Logging level")
    parser.add_argument("-p", "--port", type=int, default=6000, help="Subscriber PUB port") 
    return parser.parse_args()


###################################
# Main function
###################################
def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("SubscriberAppln")

    args = parseCmdLineArgs()
    logger.setLevel(args.loglevel)

    app = SubscriberAppln(logger)
    app.configure(args)
    app.run()


if __name__ == "__main__":
    main()
