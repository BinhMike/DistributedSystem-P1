###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.

import zmq
import logging
from CS6381_MW import discovery_pb2

class DiscoveryMW:
    ########################################
    # Constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger
        self.rep = None  # REP socket for handling requests
        self.registry = {"publishers": {}, "subscribers": {}}  # Store registered entities
        self.upcall_obj = None  # Application logic handle

    ########################################
    # Configure Middleware
    ########################################
    def configure(self, args):
        ''' Initialize the Discovery Middleware '''

        try:
            self.logger.info("DiscoveryMW::configure")

            # Initialize ZMQ context and REP socket
            context = zmq.Context()
            self.rep = context.socket(zmq.REP)

            # Bind to the discovery service port
            bind_str = "tcp://*:" + str(args.port)
            self.rep.bind(bind_str)
            self.logger.info(f"DiscoveryMW::configure - Discovery service bound at {bind_str}")

        except Exception as e:
            raise e

    ########################################
    # Event Loop
    ########################################
    def event_loop(self):
        ''' Forever loop waiting for requests '''
        try:
            self.logger.info("DiscoveryMW::event_loop - running")

            while True:
                # Wait for a request
                bytes_received = self.rep.recv()

                # Deserialize request
                disc_req = discovery_pb2.DiscoveryReq()
                disc_req.ParseFromString(bytes_received)

                # Determine message type and handle accordingly
                if disc_req.msg_type == discovery_pb2.TYPE_REGISTER:
                    response = self.upcall_obj.register(disc_req.register_req)
                elif disc_req.msg_type == discovery_pb2.TYPE_LOOKUP:
                    response = self.upcall_obj.lookup(disc_req.lookup_req)
                elif disc_req.msg_type == discovery_pb2.TYPE_ISREADY:
                    response = self.upcall_obj.is_ready()
                else:
                    self.logger.error("DiscoveryMW::event_loop - Unknown request type")
                    response = discovery_pb2.DiscoveryResp()
                    response.msg_type = discovery_pb2.TYPE_UNKNOWN

                # Serialize and send the response
                self.rep.send(response.SerializeToString())

        except Exception as e:
            raise e

    ########################################
    # Set Upcall Handle
    ########################################
    def set_upcall_handle(self, upcall_obj):
        ''' Save reference to the application logic '''
        self.upcall_obj = upcall_obj