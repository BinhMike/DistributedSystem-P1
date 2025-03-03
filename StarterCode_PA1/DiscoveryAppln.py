###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Discovery service using ZooKeeper
#
###############################################

import sys
import time
import argparse
import logging
import configparser
from kazoo.client import KazooClient  # ZooKeeper client
from CS6381_MW.DiscoveryMW import DiscoveryMW
from CS6381_MW import discovery_pb2

##################################
#       Discovery Application Class
##################################
class DiscoveryAppln:
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None  # Middleware handle
        self.zk = None  # ZooKeeper client
        self.registry = {"publishers": {}, "subscribers": {}, "brokers": {}}  # Track registered entities

    ########################################
    # Configure Discovery Application
    ########################################
    def configure(self, args):
        ''' Configure the Discovery Application '''
        try:
            self.logger.info("DiscoveryAppln::configure")

            # Initialize ZooKeeper
            self.zk = KazooClient(hosts=args.zookeeper)  # Use the command-line argument for ZooKeeper
            self.zk.start()
            self.zk.ensure_path("/discovery")  # Ensure discovery path exists

            # Register this Discovery service as the leader
            discovery_address = f"{args.addr}:{args.port}"
            self.zk.create("/discovery/leader", discovery_address.encode(), ephemeral=True, makepath=True)
            self.logger.info(f"Discovery service registered in ZooKeeper at {discovery_address}")

            # Initialize the middleware
            self.logger.debug("DiscoveryAppln::configure - initializing middleware")
            self.mw_obj = DiscoveryMW(self.logger, self.zk)
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
        role_map = {
            discovery_pb2.ROLE_PUBLISHER: "Publisher",
            discovery_pb2.ROLE_SUBSCRIBER: "Subscriber",
            discovery_pb2.ROLE_BOTH: "Broker"
        }

        role = role_map.get(register_req.role, "Unknown")
        self.logger.info(f"Registering {role}: {register_req.info.id}")

        entity_path = f"/{role.lower()}s/{register_req.info.id}"
        entity_data = f"{register_req.info.addr}:{register_req.info.port}"

        # Register in ZooKeeper
        self.zk.ensure_path(f"/{role.lower()}s")
        self.zk.create(entity_path, entity_data.encode(), ephemeral=True, makepath=True)

        # Store registration details locally
        self.registry[role.lower() + "s"][register_req.info.id] = {
            "addr": register_req.info.addr,
            "port": register_req.info.port,
            "topics": list(register_req.topiclist)
        }

        # Build response
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_REGISTER
        response.register_resp.status = discovery_pb2.STATUS_SUCCESS

        return response

    ########################################
    # Handle Lookup Request
    ########################################
    def lookup(self, lookup_req):
        ''' Handle subscriber lookup request '''
        self.logger.info(f"Lookup request for topics: {lookup_req.topiclist}")

        # Query ZooKeeper for available publishers
        matched_publishers = []
        publishers = self.zk.get_children("/publishers")

        for pub_id in publishers:
            data, _ = self.zk.get(f"/publishers/{pub_id}")
            addr, port = data.decode().split(":")
            pub_info = discovery_pb2.RegistrantInfo(id=pub_id, addr=addr, port=int(port))
            matched_publishers.append(pub_info)

        # Build response
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
        response.lookup_resp.publishers.extend(matched_publishers)

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
    parser.add_argument("-a", "--addr", default="localhost", help="IP address of Discovery Service")
    parser.add_argument("-z", "--zookeeper", default="127.0.0.1:2181", help="ZooKeeper address (default: 127.0.0.1:2181)")
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
    app.configure(args)  # `args` now includes the ZooKeeper address
    app.run()

if __name__ == "__main__":
    main()

