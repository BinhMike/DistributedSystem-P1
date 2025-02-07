###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.


import zmq
import logging
import time
from CS6381_MW import discovery_pb2  

class BrokerMW():
    def __init__(self, logger):
        self.logger = logger
        self.context = zmq.Context()
        self.sub = None  
        self.pub = None  

    def configure(self, args):
        self.addr = args.addr  
        self.port = args.port
        self.logger.info("BrokerMW::configure")
        self.sub = self.context.socket(zmq.SUB) # Set sub
        self.pub = self.context.socket(zmq.PUB) # Set pub

        self.logger.debug("BrokerMW::configure - connecting to Publisher")
        self.sub.connect(f"tcp://{args.publisher_ip}:{args.publisher_port}")
        self.sub.setsockopt_string(zmq.SUBSCRIBE, "") # ensure sub socket can receive all kinds of topics

        self.logger.debug("BrokerMW::configure - binding PUB socket")
        self.pub.bind(f"tcp://*:{args.port}")

        #req-rep
        self.req = self.context.socket(zmq.REQ)
        self.logger.debug("BrokerMW::configure - connecting to Discovery Service")
        self.req.connect(f"tcp://{args.discovery}")

    def register(self, name):
        ''' Send registration requirment to Discovery Server '''
        self.logger.info(f"BrokerMW::register - Registering Broker: {name}")
        # Construct registrantInfo
        reg_info = discovery_pb2.RegistrantInfo()
        reg_info.id = name
        reg_info.addr = self.addr  
        reg_info.port = self.port 

        # construct RegisterReq
        register_req = discovery_pb2.RegisterReq()
        register_req.role = discovery_pb2.ROLE_BOTH  # Broker type
        register_req.info.CopyFrom(reg_info) 

        # construct DiscoveryReq
        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.msg_type = discovery_pb2.TYPE_REGISTER
        disc_req.register_req.CopyFrom(register_req)

        # Send registration message
        buf = register_req.SerializeToString()
        self.logger.debug(f"BrokerMW::register - Sending registration request: {buf}")
        self.req.send(buf)

        # wait for response
        response = self.req.recv()
        # parse response
        reg_resp = discovery_pb2.RegisterResp()
        reg_resp.ParseFromString(response) 
        self.logger.debug(f"BrokerMW::register - Received response: {reg_resp.status}")
        return reg_resp.register_resp



    def set_upcall_handle(self, upcall_obj): # upcall function same as in Publisher
        self.logger.info("BrokerMW::set_upcall_handle - setting upcall handle")
        self.upcall_obj = upcall_obj


    def forward_messages(self):
        ''' Listening from Publisher and forward to Subscriber '''
        while True:
            message = self.sub.recv_string() # read message from publisher
            self.logger.info(f"BrokerMW::forward_messages - Forwarding message: {message}")
            self.pub.send_string(message)  # dispatch message to subscriber

    def event_loop(self, timeout=None):
        self.logger.info("BrokerMW::event_loop - running")
        while True:
            self.forward_messages()
