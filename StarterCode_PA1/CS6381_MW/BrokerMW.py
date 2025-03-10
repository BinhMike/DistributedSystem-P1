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
        ''' Safely reconnect when publishers change '''
        self.logger.info(f"Publisher list changed: {children}")
        
        # Create new socket before closing old one
        new_sub = self.context.socket(zmq.SUB)
        new_sub.setsockopt_string(zmq.SUBSCRIBE, "") # Receive all topics
        
        # Connect to all publishers with new socket
        if self.zk.exists(self.publisher_path):
            publishers = self.zk.get_children(self.publisher_path)
            for pub_id in publishers:
                try:
                    pub_data, stat = self.zk.get(f"{self.publisher_path}/{pub_id}")
                    pub_address = pub_data.decode()
                    new_sub.connect(f"tcp://{pub_address}")
                    self.logger.info(f"Connected to Publisher {pub_id} at {pub_address}")
                except Exception as e:
                    self.logger.error(f"Error connecting to publisher {pub_id}: {str(e)}")
        
        # Update the poller to use the new socket
        try:
            if self.sub:
                self.poller.unregister(self.sub)
                self.sub.close()
        except Exception as e:
            self.logger.error(f"Error unregistering old socket: {str(e)}")
            
        # Assign new socket and register with poller
        self.sub = new_sub
        self.poller.register(self.sub, zmq.POLLIN)

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
            
            while self.handle_events:
                try:
                    # Poll for events with timeout
                    events = dict(self.poller.poll(timeout=timeout))
                    
                    if not events:
                        # No events, let application handle timeout
                        timeout = self.upcall_obj.invoke_operation()
                    elif self.sub in events:
                        # Handle publisher messages
                        self.forward_messages()
                except zmq.ZMQError as e:
                    # Handle socket errors that might occur during polling
                    self.logger.error(f"ZMQ Error in polling: {str(e)}")
                    # Short pause before retrying
                    time.sleep(0.1)
                        
        except Exception as e:
            self.logger.error(f"BrokerMW::event_loop - error: {str(e)}")
            raise e

    def set_upcall_handle(self, upcall_obj): # upcall function same as in Publisher
        self.logger.info("BrokerMW::set_upcall_handle - setting upcall handle")
        self.upcall_obj = upcall_obj

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


