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
            self.zk.DataWatch("/discovery/leader", self.update_discovery_info)

            self.logger.info("Waiting for Discovery address from ZooKeeper...")
            while self.discovery_addr is None:
                time.sleep(2)

            self.logger.debug("PublisherAppln::configure - initializing middleware")
            self.mw_obj = PublisherMW(self.logger)
            self.mw_obj.configure(self.discovery_addr, args.port, args.addr)
            self.logger.info("PublisherAppln::configure - configuration complete")

        except Exception as e:
            raise e

    def update_discovery_info(self, data, stat, event=None):
        """ Update Discovery service address if it changes """
        if data:
            new_addr = data.decode("utf-8")
            if new_addr != self.discovery_addr:
                self.logger.info(f"Discovery changed to {new_addr}, reconnecting...")
                self.discovery_addr = new_addr
                # Only attempt to reconnect if middleware is properly configured
                if self.mw_obj and hasattr(self.mw_obj, 'req') and self.mw_obj.req is not None:
                    self.mw_obj.connect_to_discovery(self.discovery_addr)
                    self.register()

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
                self.logger.info("PublisherAppln::invoke_operation - Start Disseminating")
                ts = TopicSelector()
                for _ in range(self.iters):
                    for topic in self.topiclist:
                        self.mw_obj.disseminate(self.name, topic, ts.gen_publication(topic))
                    time.sleep(1 / float(self.frequency))

                self.state = self.State.COMPLETED
                return 0

        except Exception as e:
            raise e

    def register_response(self, reg_resp):
        if reg_resp.status == discovery_pb2.STATUS_SUCCESS:
            self.logger.info("PublisherAppln::register_response - Registration successful")
            self.state = self.State.DISSEMINATE
            return 0

    def dump(self):
        self.logger.info(f"PublisherAppln:: Name: {self.name}, Topics: {self.topiclist}")

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Publisher Application")
    parser.add_argument("-n", "--name", default="pub1", help="Unique Publisher Name")
    parser.add_argument("-a", "--addr", default="localhost", help="Publisher IP Address")
    parser.add_argument("-p", "--port", type=int, default=5577, help="Publisher Port")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics")
    parser.add_argument("-f", "--frequency", type=int, default=1, help="Dissemination frequency")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="Number of iterations")
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", help="ZooKeeper Address")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file (default: config.ini)")  # ✅ Add this line
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level")
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

