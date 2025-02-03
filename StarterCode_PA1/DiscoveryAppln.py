###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.

import sys
import time
import argparse
import logging
import configparser
from CS6381_MW.DiscoveryMW import DiscoveryMW
from CS6381_MW import discovery_pb2

##################################
#       Discovery Application Class
##################################
class DiscoveryAppln:
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None  # Middleware handle
        self.total_publishers = None
        self.total_subscribers = None
        self.registry = {"publishers": {}, "subscribers": {}}  # Store registered entities

    ########################################
    # Configure Discovery Application
    ########################################
    def configure(self, args):
        ''' Configure the Discovery Application '''
        try:
            self.logger.info("DiscoveryAppln::configure")

            # Parse configuration file
            config = configparser.ConfigParser()
            config.read(args.config)
            self.total_publishers = int(config["Discovery"].get("TotalPublishers", 1))  # Default to 1
            self.total_subscribers = int(config["Discovery"].get("TotalSubscribers", 1))


            # Initialize the middleware
            self.logger.debug("DiscoveryAppln::configure - initializing middleware")
            self.mw_obj = DiscoveryMW(self.logger)
            self.mw_obj.configure(args)
            self.mw_obj.set_upcall_handle(self)

            self.logger.info("DiscoveryAppln::configure - configuration complete")

        except Exception as e:
            raise e

    ########################################
    # Handle Registration Request
    ########################################
    def register(self, register_req):
        ''' Handle publisher/subscriber registration '''
        role = "Publisher" if register_req.role == discovery_pb2.ROLE_PUBLISHER else "Subscriber"
        self.logger.info(f"Registering {role}: {register_req.info.id}")

        # Store registration details
        if register_req.role == discovery_pb2.ROLE_PUBLISHER:
            self.registry["publishers"][register_req.info.id] = {
                "addr": register_req.info.addr,
                "port": register_req.info.port,
                "topics": list(register_req.topiclist)
            }
        else:
            self.registry["subscribers"][register_req.info.id] = {
                "addr": register_req.info.addr,
                "port": register_req.info.port,
                "topics": list(register_req.topiclist)
            }

        # Build response
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_REGISTER
        response.register_resp.status = discovery_pb2.STATUS_SUCCESS

        # DEBUG
        self.logger.info("response: %s", response)

        return response

    ########################################
    # Handle Lookup Request
    ########################################
    def lookup(self, lookup_req):
        ''' Handle subscriber lookup request '''
        self.logger.info(f"Lookup request for topics: {lookup_req.topiclist}")

        matched_publishers = []
        for pub_id, pub_data in self.registry["publishers"].items():
            if any(topic in pub_data["topics"] for topic in lookup_req.topiclist):
                pub_info = discovery_pb2.RegistrantInfo()
                pub_info.id = pub_id
                pub_info.addr = pub_data["addr"]
                pub_info.port = pub_data["port"]
                matched_publishers.append(pub_info)

        # Build response
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_LOOKUP
        response.lookup_resp.publishers.extend(matched_publishers)

        return response

    ########################################
    # Check if System is Ready
    ########################################
    def is_ready(self):
        ''' Check if all publishers and subscribers have registered '''
        ready = (len(self.registry["publishers"]) == self.total_publishers) and \
                (len(self.registry["subscribers"]) == self.total_subscribers)
        
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_ISREADY
        response.isready_resp.status = ready

        # DEBUG
        self.logger.info(f"DiscoveryAppln::is_ready - Publishers registered: {len(self.registry['publishers'])}/{self.total_publishers}")
        self.logger.info(f"DiscoveryAppln::is_ready - Subscribers registered: {len(self.registry['subscribers'])}/{self.total_subscribers}")
        self.logger.info(f"DiscoveryAppln::is_ready - System Ready? {ready}")


        return response

    ########################################
    # Run the Discovery Service
    ########################################
    def run(self):
        ''' Run Discovery Service Event Loop '''
        self.logger.info("DiscoveryAppln::run - entering event loop")
        self.mw_obj.event_loop()


###################################
# Parse command line arguments
###################################
def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Discovery Service")

    parser.add_argument("-p", "--port", type=int, default=5555, help="Port number to run Discovery Service")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file (default: config.ini)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[10, 20, 30, 40, 50], help="Logging level")

    return parser.parse_args()


###################################
# Main function
###################################
def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("DiscoveryAppln")

    args = parseCmdLineArgs()
    logger.setLevel(args.loglevel)

    app = DiscoveryAppln(logger)
    app.configure(args)
    app.run()


if __name__ == "__main__":
    main()

