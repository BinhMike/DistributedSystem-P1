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
        
        # Subscribe to all topics by default
        self.sub.setsockopt_string(zmq.SUBSCRIBE, "")
        self.logger.info("BrokerMW::configure - subscribed to all topics")
        
        self.pub = self.context.socket(zmq.PUB) # Set pub

        self.logger.debug("BrokerMW::configure - binding PUB socket")
        self.pub.bind(f"tcp://*:{args.port}")

        # Add more verbose logging about socket setup
        self.logger.info(f"BrokerMW::configure - PUB socket bound to tcp://*:{args.port}")
        self.logger.info(f"BrokerMW::configure - External address advertised as {self.addr}:{self.port}")

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
        self.logger.info("BrokerMW::subscribe_to_publishers - Checking for publishers in ZooKeeper")
    
        if self.zk.exists(self.publisher_path):
            publishers = self.zk.get_children(self.publisher_path) # return a publisher ID list
            self.logger.info(f"BrokerMW::subscribe_to_publishers - Found {len(publishers)} publishers: {publishers}")
        
            for pub_id in publishers:
                pub_data, stat = self.zk.get(f"{self.publisher_path}/{pub_id}") # return pubdata and Znodestat
                pub_address = pub_data.decode() # publisher tcp address
                connection_url = f"tcp://{pub_address}"
                self.sub.connect(connection_url)
                self.logger.info(f"BrokerMW::subscribe_to_publishers - Connected to Publisher {pub_id} at {connection_url}")
        else:
            self.logger.warning(f"BrokerMW::subscribe_to_publishers - Path {self.publisher_path} doesn't exist yet")

        self.poller.register(self.sub, zmq.POLLIN)  # listen to publishers
        self.logger.info("BrokerMW::subscribe_to_publishers - SUB socket registered with poller")


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
                    self.logger.info(f"BrokerMW::handle_publisher_change - Connected to Publisher {pub_id} at {pub_address}")
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
        ''' Forward messages from publishers to subscribers '''
        try:
            # Check if we have any messages to forward (non-blocking)
            events = dict(self.poller.poll(timeout=0))
            
            if self.sub in events:
                # Receive message from publisher
                topic_and_message = self.sub.recv_string()
                
                # Forward the message to all subscribers
                self.logger.info(f"BrokerMW::forward_messages - Forwarding: {topic_and_message}")
                self.pub.send_string(topic_and_message)
                return True
            return False
                
        except Exception as e:
            self.logger.error(f"BrokerMW::forward_messages - error: {str(e)}")
            return False

    def event_loop(self, timeout=None):
        ''' Process events for specified timeout then return control to application '''
        try:
            # Use timeout directly in poll - this will allow the method to return after timeout
            # so the application can do other things if needed
            events = dict(self.poller.poll(timeout=timeout))
            
            if not events and self.upcall_obj:
                # No events within timeout, let application decide what to do
                return
            
            if self.sub in events and events[self.sub] == zmq.POLLIN:
                # Message available, receive it
                topic_and_message = self.sub.recv_string()
                
                # Forward the message immediately
                self.logger.info(f"BrokerMW::event_loop - Received and forwarding: {topic_and_message}")
                self.pub.send_string(topic_and_message)
                
                # Let the application know we processed something
                if self.upcall_obj:
                    self.upcall_obj.invoke_operation()
                    
        except Exception as e:
            self.logger.error(f"BrokerMW::event_loop - Exception: {str(e)}")
            return

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


