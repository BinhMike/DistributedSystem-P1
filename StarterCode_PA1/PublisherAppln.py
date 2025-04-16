###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the publisher application
#
# Created: Spring 2023
#
###############################################

# import the needed packages
import os
import sys
import time
import argparse
import logging
from kazoo.client import KazooClient
from topic_selector import TopicSelector
from CS6381_MW.PublisherMW import PublisherMW
from CS6381_MW import discovery_pb2
from enum import Enum

class PublisherAppln:
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        DISSEMINATE = 3,
        COMPLETED = 4

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.topiclist = None
        self.iters = None
        self.frequency = None
        self.num_topics = None
        self.mw_obj = None
        self.logger = logger
        self.zk = None
        self.discovery_addr = None
        self.using_broker_lb = False  # Track if we're using broker load balancer mode
        
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
        try:
            self.logger.info("PublisherAppln::configure")
            self.state = self.State.CONFIGURE

            self.name = args.name
            self.iters = args.iters
            self.frequency = args.frequency
            self.num_topics = args.num_topics

            self.logger.debug("PublisherAppln::configure - selecting topic list")
            ts = TopicSelector()
            self.topiclist = ts.interest(self.num_topics)

            # Connect to ZooKeeper
            self.logger.info("Connecting to ZooKeeper at {}".format(args.zookeeper))
            self.zk = KazooClient(hosts=args.zookeeper)
            self.zk.start()
            
            # Create initial ZooKeeper structure if needed
            self._ensure_zk_paths_exist()
            
            # Watch for discovery leader changes
            self.zk.DataWatch(self.zk_paths["discovery_leader"], self.update_discovery_info)

            self.logger.info("Waiting for Discovery address from ZooKeeper...")
            self._wait_for_discovery()

            self.logger.debug("PublisherAppln::configure - initializing middleware")
            self.mw_obj = PublisherMW(self.logger)
            self.mw_obj.configure(self.discovery_addr, args.port, args.addr, self.zk)
            self.logger.info("PublisherAppln::configure - configuration complete")

        except Exception as e:
            raise e
            
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
        max_attempts = 30
        attempts = 0
        
        while self.discovery_addr is None and attempts < max_attempts:
            attempts += 1
            self.logger.info(f"Waiting for Discovery service address... (attempt {attempts}/{max_attempts})")
            # Check if leader node exists directly
            if self.zk.exists(self.zk_paths["discovery_leader"]):
                data, _ = self.zk.get(self.zk_paths["discovery_leader"])
                if data:
                    leader_info = data.decode()
                    # Extract just the address:port part
                    if '|' in leader_info:
                        self.discovery_addr = leader_info.split('|')[0]
                    else:
                        self.discovery_addr = leader_info
                    self.logger.info(f"Found Discovery service at: {self.discovery_addr}")
                    break
            time.sleep(2)
            
        if self.discovery_addr is None:
            raise Exception("Could not find Discovery service after multiple attempts")

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
                # Only attempt to reconnect if middleware is properly configured
                if self.mw_obj and hasattr(self.mw_obj, 'req') and self.mw_obj.req is not None:
                    self.mw_obj.connect_to_discovery(self.discovery_addr)
                    self.register()

    def register(self):
        """Wrapper to register with Discovery via middleware."""
        self.logger.info("PublisherAppln::register - re-registering with Discovery")
        self.mw_obj.register(self.name, self.topiclist)

    def driver(self):
        try:
            self.logger.info("PublisherAppln::driver")
            self.dump()
            self.mw_obj.set_upcall_handle(self)
            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)
            self.logger.info("PublisherAppln::driver completed")
        except Exception as e:
            raise e

    def invoke_operation(self):
        try:
            if self.state == self.State.REGISTER:
                self.logger.info("PublisherAppln::invoke_operation - Registering with Discovery")
                self.mw_obj.register(self.name, self.topiclist)
                return None

            elif self.state == self.State.DISSEMINATE:
                # Check load balancer health before publishing
                if hasattr(self, 'using_broker_lb') and self.using_broker_lb:
                    if not self._check_lb_health():
                        self.logger.warning("Load balancer not healthy, waiting before publishing")
                        return 1000  # Try again in 1 second

                # Rest of the dissemination logic
                self.logger.info("PublisherAppln::invoke_operation - Disseminating")
                
                # If we have iterations left, disseminate one batch of topics
                if hasattr(self, 'current_iteration') and self.current_iteration < self.iters:
                    ts = TopicSelector()
                    for topic in self.topiclist:
                        dissemination_data = ts.gen_publication(topic)
                        self.mw_obj.disseminate(self.name, topic, dissemination_data)
                    
                    self.current_iteration += 1
                    self.logger.info(f"Published iteration {self.current_iteration}/{self.iters}")
                    
                    # Return a timeout based on the frequency (in milliseconds)
                    return int(1000/float(self.frequency))

                # If we're done with all iterations, complete
                elif hasattr(self, 'current_iteration') and self.current_iteration >= self.iters:
                    self.logger.info("PublisherAppln::invoke_operation - Dissemination completed")
                    self.state = self.State.COMPLETED
                    return 0
                
                # First call to disseminate, initialize the counter
                else:
                    self.current_iteration = 0
                    return 0  # Call us right back to start the first iteration

            else:
                self.logger.info("PublisherAppln::invoke_operation - Completed")
                return None

        except Exception as e:
            self.logger.error(f"Exception in invoke_operation: {e}")
            raise e

    def _check_lb_health(self):
        """Check if load balancer is healthy by verifying its ZooKeeper registration"""
        try:
            if self.zk and self.zk.exists(self.zk_paths["load_balancers"]):
                lb_nodes = self.zk.get_children(self.zk_paths["load_balancers"])
                return len(lb_nodes) > 0
        except Exception as e:
            self.logger.error(f"Error checking load balancer health: {e}")
        return False

    def register_response(self, reg_resp):
        if reg_resp.status == discovery_pb2.Status.STATUS_SUCCESS:
            self.logger.info("PublisherAppln::register_response - Registration successful")
            
            # Check if we received group mapping info in the reason field
            if hasattr(reg_resp, "reason") and reg_resp.reason:
                try:
                    group_mapping = {}
                    mapping_str = reg_resp.reason
                    self.logger.info(f"Received group mapping in reason field: {mapping_str}")
                    
                    # Parse the mapping string
                    for pair in mapping_str.split(","):
                        if ":" in pair:
                            topic, group = pair.split(":")
                            group_mapping[topic] = int(group)
                            
                    # Update middleware with group mapping
                    if group_mapping and self.mw_obj:
                        self.mw_obj.update_group_mapping(group_mapping)
                        # If we got a group mapping, we're using the broker load balancer
                        self.using_broker_lb = True
                        self.logger.info("Using broker load balancer mode")
                except Exception as e:
                    self.logger.error(f"Error processing group mapping: {e}")
            
            # Move to dissemination state
            self.state = self.State.DISSEMINATE
            return 0
        else:
            # Check if it's not primary based on reason field instead of status code
            if hasattr(reg_resp, "reason") and reg_resp.reason and "primary" in reg_resp.reason.lower():
                self.logger.warning(f"Not connected to primary Discovery: {reg_resp.reason}")
                self._locate_primary_discovery()
                return 1000  # Retry after a delay
            else:
                self.logger.error(f"Registration failed with status: {reg_resp.status}, reason: {reg_resp.reason}")
                return 5000  # Longer retry for other failures

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
        except Exception as e:
            self.logger.error(f"Error locating primary: {str(e)}")

    def dump(self):
        self.logger.info(f"PublisherAppln:: Name: {self.name}, Topics: {self.topiclist}")
        
    def cleanup(self):
        """Clean up resources"""
        try:
            self.logger.info("PublisherAppln::cleanup")
            
            # Clean up middleware
            if self.mw_obj:
                self.mw_obj.cleanup()
                
            # Close ZooKeeper connection
            if self.zk:
                self.zk.stop()
                self.zk.close()
                
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")


def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Publisher Application")
    parser.add_argument("-n", "--name", default="pub1", help="Unique Publisher Name")
    parser.add_argument("-a", "--addr", default="localhost", help="Publisher IP Address")
    parser.add_argument("-p", "--port", type=int, default=5577, help="Publisher Port")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics")
    parser.add_argument("-f", "--frequency", type=int, default=1, help="Dissemination frequency")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="Number of iterations")
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", help="ZooKeeper Address")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file (default: config.ini)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level, DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50")
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("PublisherAppln")
    args = parseCmdLineArgs()
    logger.setLevel(args.loglevel)
    app = PublisherAppln(logger)
    app.configure(args)
    app.driver()


if __name__ == "__main__":
    main()

