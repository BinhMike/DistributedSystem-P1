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
import time

class BrokerMW():
    def __init__(self, logger, zk_client):
        self.logger = logger
        self.zk = zk_client
        self.context = zmq.Context()
        self.sub = None
        self.pub = None
        self.req = None
        self.poller = None
        self.upcall_obj = None
        self.handle_events = True
        self.addr = None
        self.port = None
        self.zk_path = "/brokers"
        self.publisher_path = "/publishers"

    def configure(self, args):
        self.addr = args.addr  
        self.port = args.port
        self.logger.info("BrokerMW::configure")
        self.sub = self.context.socket(zmq.SUB) # Set sub
        self.pub = self.context.socket(zmq.PUB) # Set pub

        self.logger.debug("BrokerMW::configure - binding PUB socket")
        self.pub.bind(f"tcp://*:{args.port}")

        # register Broker to ZooKeeper
        broker_address = f"{self.addr}:{self.port}"
        broker_node_path = f"{self.zk_path}/{args.name}"

        self.zk.ensure_path(self.zk_path)

        if self.zk.exists(broker_node_path):
            self.zk.delete(broker_node_path)

        self.zk.create(broker_node_path, broker_address.encode(), ephemeral=True)
        self.logger.info(f"Broker registered in ZooKeeper at {broker_node_path}")

        self.poller = zmq.Poller()
        # subscribe to current Publishers
        self.logger.info("Looking for existing Publishers in ZooKeeper")
        self.subscribe_to_publishers()

        # Listening to the change of publisher list. Trigger handle_publisher_change if there is a change
        self.zk.ChildrenWatch(self.publisher_path, self.handle_publisher_change)

        
  
       


    
    def subscribe_to_publishers(self):
        '''  Request and connect to publishers'''
        if self.zk.exists(self.publisher_path):
            publishers = self.zk.get_children(self.publisher_path) # return a publisher ID list
            for pub_id in publishers:
                pub_data, stat = self.zk.get(f"{self.publisher_path}/{pub_id}") # return pubdata and Znodestat
                pub_address = pub_data.decode() # publisher tcp address
                self.sub.connect(f"tcp://{pub_address}")
                self.logger.info(f"Connected to Publisher {pub_id} at {pub_address}")

        self.poller.register(self.sub, zmq.POLLIN)  # listen to publishers


    def handle_publisher_change(self, children):
        ''' reconnect when publishers change '''
        self.logger.info(f"Publisher list changed: {children}")
        self.sub.close()
        self.sub = self.context.socket(zmq.SUB)
        self.sub.setsockopt_string(zmq.SUBSCRIBE, "") # Receive all topics
        self.subscribe_to_publishers()

   



# no need to use req-rep to connect to Discovery

        # #req-rep
        # self.req = self.context.socket(zmq.REQ)
        # self.logger.debug("BrokerMW::configure - connecting to Discovery Service")
        # self.req.connect(f"tcp://{args.discovery}")


    # def register(self, name):
    #     ''' Send registration requirment to Discovery Server '''
    #     self.logger.info(f"BrokerMW::register - Registering Broker: {name}")
        
    #     # Construct registrantInfo
    #     reg_info = discovery_pb2.RegistrantInfo()
    #     reg_info.id = name
    #     reg_info.addr = self.addr  
    #     reg_info.port = self.port 

    #     # construct RegisterReq
    #     register_req = discovery_pb2.RegisterReq()
    #     register_req.role = discovery_pb2.ROLE_BOTH  # Broker type
    #     register_req.info.CopyFrom(reg_info) 

    #     # construct DiscoveryReq
    #     disc_req = discovery_pb2.DiscoveryReq()
    #     disc_req.msg_type = discovery_pb2.TYPE_REGISTER
    #     disc_req.register_req.CopyFrom(register_req)

    #     # Send registration message - Use disc_req instead of register_req
    #     buf = disc_req.SerializeToString()  # Changed from register_req to disc_req
    #     self.logger.debug(f"BrokerMW::register - Sending registration request: {buf}")
    #     self.req.send(buf)

    #     # wait for response
    #     response = self.req.recv()
    #     # parse response 
    #     disc_resp = discovery_pb2.DiscoveryResp()  # Changed to DiscoveryResp
    #     disc_resp.ParseFromString(response)
    #     self.logger.debug(f"BrokerMW::register - Received response: {disc_resp.register_resp.status}")
    #     return disc_resp.register_resp

    def set_upcall_handle(self, upcall_obj): # upcall function same as in Publisher
        self.logger.info("BrokerMW::set_upcall_handle - setting upcall handle")
        self.upcall_obj = upcall_obj


    def forward_messages(self):
        ''' Forward messages with error handling '''
        try:
            # Receive message from publisher
            message = self.sub.recv_string(flags=zmq.NOBLOCK)
            if message:
                # Log and forward message
                self.logger.debug(f"BrokerMW::forward_messages - received: {message}")
                self.pub.send_string(message)
                return True
                
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                # No message available
                pass
            else:
                self.logger.error(f"BrokerMW::forward_messages - ZMQ error: {str(e)}")
        except Exception as e:
            self.logger.error(f"BrokerMW::forward_messages - error: {str(e)}")
            
        return False

    def event_loop(self, timeout=None):
        ''' Improved event loop with ZMQ Poller '''
        try:
            self.logger.info("BrokerMW::event_loop")
            
            while True:
                # Poll for events with timeout
                
                events = dict(self.poller.poll(timeout=timeout))
                
                if not events:
                    # No events, let application handle timeout
                    timeout = self.upcall_obj.invoke_operation()
                elif self.sub in events:
                    # Handle publisher messages
                    self.forward_messages()
                    
        except Exception as e:
            self.logger.error(f"BrokerMW::event_loop - error: {str(e)}")
            raise e

    # def handle_reply(self):
    #     ''' Handle discovery service responses '''
    #     try:
    #         # Receive and parse response
    #         response = self.req.recv()
    #         disc_resp = discovery_pb2.DiscoveryResp()
    #         disc_resp.ParseFromString(response)
            
    #         if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
    #             timeout = self.upcall_obj.register_response(disc_resp.register_resp)
    #         else:
    #             self.logger.warning(f"Unhandled response type: {disc_resp.msg_type}")
    #             timeout = None
                
    #         return timeout
            
    #     except Exception as e:
    #         self.logger.error(f"BrokerMW::handle_reply - error: {str(e)}")
    #         raise e
        
    def cleanup(self):
        ''' Clean shutdown of sockets '''
        try:
            if self.sub:
                self.poller.unregister(self.sub)
                self.sub.close()
            if self.pub:
                self.pub.close()
            # if self.req:
            #     self.poller.unregister(self.req)
            #     self.req.close()
            self.context.term()
        except Exception as e:
            self.logger.error(f"BrokerMW::cleanup - error: {str(e)}")

    
