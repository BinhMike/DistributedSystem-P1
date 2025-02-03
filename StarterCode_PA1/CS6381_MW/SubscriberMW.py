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

class SubscriberMW:
    ########################################
    # Constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger
        self.req = None  # REQ socket for Discovery Service communication
        self.sub = None  # SUB socket for receiving topics
        self.poller = None  # Poller for async event handling
        self.upcall_obj = None  # Application logic handle
        self.handle_events = True  # Event loop flag

    ########################################
    # Configure Middleware
    ########################################
    def configure(self, args):
        ''' Initialize the Subscriber Middleware '''
        try:
            self.logger.info("SubscriberMW::configure")

            # Subscriber does not bind to a port, only connects
            self.addr = args.discovery  # Discovery service address

            # Initialize ZMQ context and sockets
            context = zmq.Context()
            self.poller = zmq.Poller()
            self.req = context.socket(zmq.REQ)  # Request socket for Discovery Service
            self.sub = context.socket(zmq.SUB)  # Subscriber socket for topics

            # Register REQ socket with the poller (expecting Discovery responses)
            self.poller.register(self.req, zmq.POLLIN)

            # Connect to Discovery Service
            connect_str = "tcp://" + args.discovery
            self.req.connect(connect_str)
            self.logger.info(f"SubscriberMW::configure - Connected to Discovery at {connect_str}")

        except Exception as e:
            raise e

    ########################################
    # Event Loop
    ########################################
    def event_loop(self, timeout=None):
        ''' Run the event loop waiting for messages from Discovery or Publishers '''
        try:
            self.logger.info("SubscriberMW::event_loop - running")

            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))

                if not events:
                    timeout = self.upcall_obj.invoke_operation()  # Timeout call to application
                elif self.req in events:
                    timeout = self.handle_reply()  # Handle response from Discovery
                elif self.sub in events:
                    self.handle_subscription()  # Handle received topic data
                else:
                    raise Exception("Unknown event after poll")

        except Exception as e:
            raise e

    ########################################
    # Handle Discovery Service Replies
    ########################################
    def handle_reply(self):
        ''' Process replies from Discovery Service '''
        try:
            self.logger.info("SubscriberMW::handle_reply")

            # Receive and deserialize response
            bytes_received = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytes_received)

            # Handle based on message type
            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                timeout = self.upcall_obj.lookup_response(disc_resp.lookup_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
                timeout = self.upcall_obj.isready_response(disc_resp.isready_resp)
            else:
                raise ValueError("Unrecognized response message")

            return timeout

        except Exception as e:
            raise e

    ########################################
    # Register with Discovery Service
    ########################################
    def register(self, name, topiclist):
        ''' Register the subscriber with the Discovery Service '''
        try:
            self.logger.info("SubscriberMW::register")

            # Populate registration request
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name

            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER
            register_req.info.CopyFrom(reg_info)
            register_req.topiclist[:] = topiclist

            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)

            # Serialize and send request
            self.req.send(disc_req.SerializeToString())

        except Exception as e:
            raise e

    ########################################
    # Check if the Discovery service is ready
    #
    # Sends the is_ready message and waits for a response.
    ########################################
    def is_ready(self):
        ''' Check if the system is ready via Discovery Service '''

        try:
            self.logger.info("SubscriberMW::is_ready")

            # Build an IsReady request
            self.logger.debug("SubscriberMW::is_ready - populate the nested IsReady msg")
            isready_req = discovery_pb2.IsReadyReq()  # Empty message
            self.logger.debug("SubscriberMW::is_ready - done populating nested IsReady msg")

            # Build the outer DiscoveryReq message
            self.logger.debug("SubscriberMW::is_ready - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            disc_req.isready_req.CopyFrom(isready_req)
            self.logger.debug("SubscriberMW::is_ready - done building the outer message")

            # Serialize the message
            buf2send = disc_req.SerializeToString()
            self.logger.debug(f"SubscriberMW::is_ready - sending serialized buffer: {buf2send}")

            # Send the request to Discovery
            self.logger.debug("SubscriberMW::is_ready - sending request to Discovery service")
            self.req.send(buf2send)

            # Now the event loop will wait for the response
            self.logger.info("SubscriberMW::is_ready - request sent, waiting for reply")

        except Exception as e:
            raise e


    ########################################
    # Subscribe to Topics
    ########################################
    def subscribe_to_topics(self, pub_address, topiclist):
        ''' Connect to publisher and subscribe to topics '''
        try:
            self.logger.info(f"SubscriberMW::subscribe_to_topics - Connecting to {pub_address}")

            # Connect to publisher
            self.sub.connect(pub_address)

            # Subscribe to topics
            for topic in topiclist:
                self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
                self.logger.info(f"Subscribed to topic: {topic}")

            # Register the SUB socket with poller
            self.poller.register(self.sub, zmq.POLLIN)

        except Exception as e:
            raise e

    ########################################
    # Handle Incoming Subscription Messages
    ########################################
    def handle_subscription(self):
        ''' Process received topic messages '''
        try:
            message = self.sub.recv_string()
            topic, data = message.split(":", 1)
            self.upcall_obj.process_message(topic, data)

        except Exception as e:
            raise e

    ########################################
    # Set Upcall Handle
    ########################################
    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    ########################################
    # Disable Event Loop
    ########################################
    def disable_event_loop(self):
        self.handle_events = False
