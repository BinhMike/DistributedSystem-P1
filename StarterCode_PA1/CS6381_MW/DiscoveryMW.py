###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Middleware for Discovery Service using ZooKeeper
#
###############################################

import zmq
import logging
from kazoo.client import KazooClient
from CS6381_MW import discovery_pb2

class DiscoveryMW:
    ########################################
    # Constructor
    ########################################
    def __init__(self, logger, zk_client):
        self.logger = logger
        self.zk = zk_client        # ZooKeeper client (shared with the application)
        self.rep = None            # REP socket for handling requests
        self.upcall_obj = None     # Application logic handle

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
                self.logger.debug("DiscoveryMW::event_loop - Received request")
                # Deserialize request
                disc_req = discovery_pb2.DiscoveryReq()
                disc_req.ParseFromString(bytes_received)
                # If this instance is not primary, it may choose not to serve requests.
                if not self.upcall_obj.is_primary:
                    self.logger.warning("Not primary. Rejecting request.")
                    response = discovery_pb2.DiscoveryResp()
                    response.msg_type = disc_req.msg_type
                    # Define a STATUS_NOT_PRIMARY in proto file.
                    response.register_resp.status = discovery_pb2.STATUS_NOT_PRIMARY
                    self.rep.send(response.SerializeToString())
                    continue

                # Process requests if primary
                if disc_req.msg_type == discovery_pb2.TYPE_REGISTER:
                    response = self.upcall_obj.register(disc_req.register_req)
                elif disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                    response = self.upcall_obj.lookup(disc_req.lookup_req)
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

    ########################################
    # Cleanup Middleware
    ########################################
    def cleanup(self):
        """Clean up middleware resources"""
        try:
            self.logger.info("DiscoveryMW::cleanup - Cleaning up resources")
            if self.rep:
                self.rep.close()
        except Exception as e:
            self.logger.error(f"Error during middleware cleanup: {e}")
