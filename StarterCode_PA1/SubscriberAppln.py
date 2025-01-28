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

from CS6381_MW import SubscriberMW
from CS6381_MW import discovery_pb2
from enum import Enum


class SubscriberAppln():

    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        DISSEMINATE = 4,
        COMPLETED = 5

    def __init__ (self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.topiclist = None
        self.iters = None
        self.frequency = None
        self.num_topics = None
        self.lookup = None
        self.dissemination = None
        self.mw_obj = None
        self.logger = logger

    def configure (self, args):
        try:
            self.logger.info ("SubscriberAppln::configure")
            self.state = self.State.CONFIGURE
            self.name = args.name
            self.iters = args.iters
            self.frequency = args.frequency
            self.num_topics = args.num_topics
            config = configparser.ConfigParser ()
            config.read (args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]
            ts = TopicSelector ()
            self.topiclist = ts.interest (self.num_topics)
            self.mw_obj = SubscriberMW (self.logger)
            self.mw_obj.configure (args)
            self.logger.info ("SubscriberAppln::configure - configuration complete")
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

    def invoke_operation (self):
        try:
            self.logger.info ("SubscriberAppln::invoke_operation")
            if (self.state == self.State.REGISTER):
                self.logger.debug ("SubscriberAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register (self.name, self.topiclist)
                return None
            elif (self.state == self.State.ISREADY):
                self.logger.debug ("SubscriberAppln::invoke_operation - check if are ready to go")
                self.mw_obj.is_ready ()
                return None
            elif (self.state == self.State.DISSEMINATE):
                self.logger.debug ("SubscriberAppln::invoke_operation - start Disseminating")
                ts = TopicSelector ()
                for i in range (self.iters):
                    for topic in self.topiclist:
                        dissemination_data = ts.gen_publication (topic)
                        self.mw_obj.disseminate (self.name, topic, dissemination_data)
                    time.sleep (1/float (self.frequency))
                self.logger.debug ("SubscriberAppln::invoke_operation - Dissemination completed")
                self.state = self.State.COMPLETED
                return 0
            elif (self.state == self.State.COMPLETED):
                self.mw_obj.disable_event_loop ()
                return None
            else:
                raise ValueError ("Undefined state of the appln object")
            self.logger.info ("SubscriberAppln::invoke_operation completed")
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
