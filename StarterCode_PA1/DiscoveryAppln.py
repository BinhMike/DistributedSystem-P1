###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Discovery service using ZooKeeper with warm-passive replication
#
###############################################

import sys
import time
import argparse
import logging
import configparser
from kazoo.client import KazooClient  # ZooKeeper client
from kazoo.exceptions import NodeExistsError
from CS6381_MW.DiscoveryMW import DiscoveryMW
from CS6381_MW import discovery_pb2

##################################
#       Discovery Application Class
##################################
class DiscoveryAppln:
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None        # Middleware handle
        self.zk = None            # ZooKeeper client
        self.registry = {"publishers": {}, "subscribers": {}, "brokers": {}}
        self.is_primary = False   # Will be set True if this instance becomes leader
        self.leader_path = "/discovery/leader"

    ########################################
    # Configure Discovery Application
    ########################################
    def configure(self, args):
        ''' Configure the Discovery Application with warm-passive replication '''
        try:
            self.logger.info("DiscoveryAppln::configure")
            # Parse configuration file
            config = configparser.ConfigParser()
            config.read(args.config)

            # Connect to ZooKeeper
            self.logger.info(f"Connecting to ZooKeeper at {args.zookeeper}")
            self.zk = KazooClient(hosts=args.zookeeper)
            self.zk.start(timeout=10)

            # Ensure the base path for discovery exists
            self.logger.info("Ensuring /discovery path exists")
            self.zk.ensure_path("/discovery")

            # Attempt to become the leader by creating the ephemeral leader node.
            discovery_address = f"{args.addr}:{args.port}"
            try:
                self.zk.create(self.leader_path, discovery_address.encode(), ephemeral=True)
                self.is_primary = True
                self.logger.info(f"Instance {discovery_address} became primary (leader).")
            except NodeExistsError:
                self.is_primary = False
                self.logger.info("A leader already exists. Running as backup.")
                # Set a watch on the leader node so that if it disappears we try to become primary.
                @self.zk.DataWatch(self.leader_path)
                def watch_leader(data, stat, event):
                    if event is not None and event.type == "DELETED":
                        self.logger.info("Leader znode deleted. Attempting to become primary...")
                        try:
                            self.zk.create(self.leader_path, discovery_address.encode(), ephemeral=True)
                            self.is_primary = True
                            self.logger.info("This instance has now become the primary!")
                        except NodeExistsError:
                            self.logger.info("Another instance became primary.")
                            self.is_primary = False

            # Log current leader information for debugging
            if self.zk.exists(self.leader_path):
                data, _ = self.zk.get(self.leader_path)
                self.logger.info(f"Current leader in ZooKeeper: {data.decode()}")
            else:
                self.logger.error("Leader node does not exist after attempted creation.")

            # Initialize the middleware
            self.logger.debug("DiscoveryAppln::configure - initializing middleware")
            self.mw_obj = DiscoveryMW(self.logger, self.zk)
            self.mw_obj.configure(args)
            self.mw_obj.set_upcall_handle(self)

            self.logger.info("DiscoveryAppln::configure - configuration complete")
            self.dump_zk_nodes()

        except Exception as e:
            self.logger.error(f"DiscoveryAppln::configure - Exception: {str(e)}")
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
        entity_id = register_req.info.id
        self.logger.info(f"Registering {role}: {entity_id}")

        # If this instance is not primary, then respond with a not-primary status.
        if not self.is_primary:
            self.logger.warning("Not primary; cannot process registration. Inform client to contact the leader.")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_REGISTER
            response.register_resp.status = discovery_pb2.STATUS_NOT_PRIMARY  # Define this status in your proto.
            return response

        # Otherwise, proceed to register the entity in ZooKeeper.
        category = role.lower() + "s"
        entity_path = f"/{category}/{entity_id}"
        entity_data = f"{register_req.info.addr}:{register_req.info.port}"

        try:
            self.zk.ensure_path(f"/{category}")
            if self.zk.exists(entity_path):
                self.logger.info(f"Node {entity_path} already exists, updating it")
                self.zk.delete(entity_path)
            self.zk.create(entity_path, entity_data.encode(), ephemeral=True)
            self.logger.info(f"Created/updated ZooKeeper node {entity_path}")
        except Exception as e:
            self.logger.error(f"Failed to register {role} in ZooKeeper: {str(e)}")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_REGISTER
            response.register_resp.status = discovery_pb2.STATUS_FAILURE
            return response

        # Store registration locally
        self.registry[category][entity_id] = {
            "addr": register_req.info.addr,
            "port": register_req.info.port,
            "topics": list(register_req.topiclist)
        }

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

        if not self.is_primary:
            self.logger.warning("Not primary; lookup request cannot be processed here.")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            # return an empty response or a status indicating non-primary.
            return response

        matched_publishers = []
        try:
            if not self.zk.exists("/publishers"):
                self.logger.info("No publishers registered yet in ZooKeeper.")
            else:
                publishers = self.zk.get_children("/publishers")
                self.logger.info(f"Found {len(publishers)} publishers in ZooKeeper")
                for pub_id in publishers:
                    try:
                        data, _ = self.zk.get(f"/publishers/{pub_id}")
                        addr, port = data.decode().split(":")
                        pub_info = discovery_pb2.RegistrantInfo(id=pub_id, addr=addr, port=int(port))
                        matched_publishers.append(pub_info)
                    except Exception as e:
                        self.logger.error(f"Error retrieving publisher {pub_id}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error during lookup: {str(e)}")

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

    def dump_zk_nodes(self):
        """Debug method to dump ZooKeeper node structure"""
        self.logger.info("Current ZooKeeper nodes:")

        def print_tree(path, level=0):
            try:
                children = self.zk.get_children(path)
            except Exception:
                children = []
            for child in children:
                child_path = f"{path}/{child}" if path != "/" else f"/{child}"
                data = None
                try:
                    data_bytes, _ = self.zk.get(child_path)
                    data = data_bytes.decode() if data_bytes else None
                except Exception:
                    pass
                self.logger.info(f"{'  ' * level}|- {child} {f'({data})' if data else ''}")
                print_tree(child_path, level + 1)
        print_tree("/")


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
