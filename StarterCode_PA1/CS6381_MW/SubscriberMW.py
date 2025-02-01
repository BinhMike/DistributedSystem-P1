###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the subscriber side of things.
#
# Remember that the subscriber middleware does not do anything on its own.
# It must be invoked by the application level logic. This middleware object maintains
# the ZMQ sockets and knows how to talk to Discovery service, etc.
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the Discovery service
# and one in the SUB role to receive topic data
# (2) It must, on behalf of the application logic, register the subscriber application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the subscriber appln, it must use the ZMQ setsockopt method to subscribe to all the
# user-supplied topics of interest. 
# (4) Since it is a receiver, the middleware object will maintain a poller and even loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.
#

import os
import sys
import time
import logging
import zmq
from CS6381_MW import discovery_pb2

class SubscriberMW():

    def __init__ (self, logger):
        self.logger = logger
        self.req = None
        self.sub = None
        self.poller = None
        self.addr = None
        self.port = None
        self.upcall_obj = None
        self.handle_events = True

    def configure (self, args):
        try:
            self.logger.info ("SubscriberMW::configure")
            self.port = args.port
            self.addr = args.addr

            self.logger.debug ("SubscriberMW::configure - obtain ZMQ context")
            context = zmq.Context ()

            self.logger.debug ("SubscriberMW::configure - obtain the poller")
            self.poller = zmq.Poller ()

            self.logger.debug ("SubscriberMW::configure - obtain REQ and SUB sockets")
            self.req = context.socket (zmq.REQ)
            self.sub = context.socket (zmq.SUB)
            
            self.logger.debug ("SubscriberMW::configure - register the REQ socket for incoming replies")
            self.poller.register (self.req, zmq.POLLIN)

            self.logger.debug ("SubscriberMW::configure - connect to Discovery service")
            connect_str = "tcp://" + args.discovery
            self.req.connect (connect_str)

            # Connect to the publisher's addresses
            pub_addrs = args.pub_addrs  # This should be a list of publisher addresses
            for pub_addr in pub_addrs:
                sub_str = "tcp://" + pub_addr
                self.sub.connect (sub_str)

            # Set subscription topics
            for topic in args.topiclist:
                self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)

            self.logger.info ("SubscriberMW::configure completed")
        except Exception as e:
            raise e

    def event_loop (self, timeout=None):
        try:
            self.logger.info ("SubscriberMW::event_loop - run the event loop")
            self.poller.register(self.sub, zmq.POLLIN)  # Register the SUB socket for incoming messages
            while self.handle_events:
                events = dict (self.poller.poll (timeout=timeout))
                if not events:
                    timeout = self.upcall_obj.invoke_operation ()
                elif self.req in events:
                    timeout = self.handle_reply ()
                elif self.sub in events:
                    timeout = self.handle_subscription ()
                else:
                    raise Exception ("Unknown event after poll")
            self.logger.info ("SubscriberMW::event_loop - out of the event loop")
        except Exception as e:
            raise e

    def handle_reply (self):
        try:
            self.logger.info ("SubscriberMW::handle_reply")
            bytesRcvd = self.req.recv ()
            disc_resp = discovery_pb2.DiscoveryResp ()
            disc_resp.ParseFromString (bytesRcvd)
            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
                timeout = self.upcall_obj.register_response (disc_resp.register_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
                timeout = self.upcall_obj.isready_response (disc_resp.isready_resp)
            else:
                raise ValueError ("Unrecognized response message")
            return timeout
        except Exception as e:
            raise e

    def handle_subscription (self):
        try:
            self.logger.info ("SubscriberMW::handle_subscription")
            message = self.sub.recv_string()
            self.logger.debug(f"Received message: {message}")
            # Process the message and make an upcall to the application
            self.upcall_obj.handle_publication(message)
            return None
        except Exception as e:
            raise e

    def register (self, name, topiclist):
        try:
            self.logger.info ("SubscriberMW::register")
            reg_info = discovery_pb2.RegistrantInfo ()
            reg_info.id = name
            self.logger.debug ("SubscriberMW::register - done populating the Registrant Info")
            register_req = discovery_pb2.RegisterReq ()
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER
            register_req.info.CopyFrom (reg_info)
            register_req.topiclist[:] = topiclist
            self.logger.debug ("SubscriberMW::register - done populating nested RegisterReq")
            disc_req = discovery_pb2.DiscoveryReq ()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom (register_req)
            self.logger.debug ("SubscriberMW::register - done building the outer message")
            buf2send = disc_req.SerializeToString ()
            self.logger.debug ("Stringified serialized buf = {}".format (buf2send))
            self.req.send (buf2send)
            self.logger.info ("SubscriberMW::register - sent register message and now wait for reply")
        except Exception as e:
            raise e

    def is_ready (self):
        try:
            self.logger.info ("SubscriberMW::is_ready")
            isready_req = discovery_pb2.IsReadyReq ()
            self.logger.debug ("SubscriberMW::is_ready - done populating nested IsReady msg")
            disc_req = discovery_pb2.DiscoveryReq ()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            disc_req.isready_req.CopyFrom (isready_req)
            self.logger.debug ("SubscriberMW::is_ready - done building the outer message")
            buf2send = disc_req.SerializeToString ()
            self.logger.debug ("Stringified serialized buf = {}".format (buf2send))
            self.req.send (buf2send)
            self.logger.info ("SubscriberMW::is_ready - request sent and now wait for reply")
        except Exception as e:
            raise e

    def set_upcall_handle (self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop (self):
        self.handle_events = False