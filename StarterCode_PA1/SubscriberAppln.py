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


import os
import sys
import time
import argparse
import configparser
import logging

from CS6381_MW.SubscriberMW import SubscriberMW
from CS6381_MW import discovery_pb2
from enum import Enum
from topic_selector import TopicSelector



class SubscriberAppln():

    class State (Enum):
        REGISTER = 1
        ISREADY = 2
        DISSEMINATE = 3
        COMPLETED = 4

    def __init__ (self, logger):
        self.logger = logger
        self.state = None
        self.name = None
        self.topiclist = None
        self.num_topics = None
        self.frequency = None
        self.iters = None
        self.lookup = None
        self.dissemination = None
        self.mw_obj = None

    def configure (self, args):
        try:
            self.logger.info ("SubscriberAppln::configure")
            self.state = self.State.REGISTER
            self.name = args.name
            self.num_topics = args.num_topics
            self.frequency = args.frequency
            self.iters = args.iters

            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]
            ts = TopicSelector()
            self.topiclist = ts.interest(self.num_topics)
            self.mw_obj = SubscriberMW(self.logger)
            self.mw_obj.configure(args)
            self.logger.info("SubscriberAppln::configure - configuration complete")
        except Exception as e:
            raise e

    def driver (self):
        try:
            self.logger.info ("SubscriberAppln::driver")
            self.dump ()
            self.logger.debug ("SubscriberAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle (self)
            self.state = self.State.REGISTER
            self.mw_obj.event_loop (timeout=0)
            self.logger.info ("SubscriberAppln::driver completed")
        except Exception as e:
            raise e

    def invoke_operation(self):
        ''' Invoke operating depending on state  '''

        try:
            self.logger.info("SubscriberAppln::invoke_operation")
            if self.state == self.State.REGISTER:
                self.logger.debug("SubscriberAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register(self.name, self.topiclist)
                return None

            elif self.state == self.State.ISREADY:
                self.logger.debug("SubscriberAppln::invoke_operation - check if are ready to go")
                self.mw_obj.is_ready()
                return None

            elif self.state == self.State.DISSEMINATE:
                self.logger.debug("SubscriberAppln::invoke_operation - waiting for publications")
                return None

            elif self.state == self.State.COMPLETED:
                self.mw_obj.disable_event_loop()
                return None

            else:
                raise ValueError("Undefined state of the appln object")
            
            self.logger.info("SubscriberAppln::invoke_operation completed")
        except Exception as e:
            raise e

    def register_response (self, reg_resp):
        try:
            self.logger.info ("SubscriberAppln::register_response")
            if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.debug ("SubscriberAppln::register_response - registration is a success")
                self.state = self.State.ISREADY
                return 0
            else:
                self.logger.debug ("SubscriberAppln::register_response - registration is a failure with reason {}".format (response.reason))
                raise ValueError ("Subscriber needs to have unique id")
        except Exception as e:
            raise e

    def isready_response (self, isready_resp):
        try:
            self.logger.info ("SubscriberAppln::isready_response")
            if not isready_resp.status:
                self.logger.debug ("SubscriberAppln::driver - Not ready yet; check again")
                time.sleep (10)
            else:
                self.state = self.State.DISSEMINATE
            return 0
        except Exception as e:
            raise e

    def dump (self):
        try:
            self.logger.info ("**********************************")
            self.logger.info ("SubscriberAppln::dump")
            self.logger.info ("------------------------------")
            self.logger.info ("     Name: {}".format (self.name))
            self.logger.info ("     Lookup: {}".format (self.lookup))
            self.logger.info ("     Dissemination: {}".format (self.dissemination))
            self.logger.info ("     Num Topics: {}".format (self.num_topics))
            self.logger.info ("     TopicList: {}".format (self.topiclist))
            self.logger.info ("     Iterations: {}".format (self.iters))
            self.logger.info ("     Frequency: {}".format (self.frequency))
            self.logger.info ("**********************************")
        except Exception as e:
            raise e

def parseCmdLineArgs ():
    parser = argparse.ArgumentParser (description="Subscriber Application")
    parser.add_argument ("-n", "--name", default="sub", help="Some name assigned to us. Keep it unique per subscriber")
    parser.add_argument ("-a", "--addr", default="localhost", help="IP addr of this subscriber to advertise (default: localhost)")
    parser.add_argument ("-p", "--port", type=int, default=5577, help="Port number on which our underlying subscriber ZMQ service runs, default=5577")
    parser.add_argument ("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")
    parser.add_argument ("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to subscribe, currently restricted to max of 9")
    parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument ("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")
    parser.add_argument ("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")
    parser.add_argument ("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    return parser.parse_args()

def main ():
    try:
        logging.info ("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger ("SubscriberAppln")
        logger.debug ("Main: parse command line arguments")
        args = parseCmdLineArgs ()
        logger.debug ("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel (args.loglevel)
        logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
        logger.debug ("Main: obtain the subscriber appln object")
        sub_app = SubscriberAppln (logger)
        logger.debug ("Main: configure the subscriber appln object")
        sub_app.configure (args)
        logger.debug ("Main: invoke the subscriber appln driver")
        sub_app.driver ()
    except Exception as e:
        logger.error ("Exception caught in main - {}".format (e))
        return

if __name__ == "__main__":
    logging.basicConfig (level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main ()
