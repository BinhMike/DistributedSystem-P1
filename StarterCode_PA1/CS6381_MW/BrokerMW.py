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
import json
import time
from kazoo.client import KazooClient
from CS6381_MW import discovery_pb2

class BrokerMW:
    """Middleware for the Broker application"""
    
    def __init__(self, logger, zk_client, is_primary=False):
        self.logger = logger
        self.zk = zk_client        # ZooKeeper client (shared with the application)
        self.is_primary = is_primary
        self.pub_socket = None     # ZMQ PUB socket for publishing to subscribers
        self.sub_socket = None     # ZMQ SUB socket for receiving from publishers
        self.upcall_obj = None     # Application logic handle
        self.poller = None         # ZMQ Poller
        self.addr = None           # Our address
        self.port = None           # Our port
        self.message_buffer = []   # Buffer for messages when running as backup
        self.message_count = 0     # Count total messages processed

    def configure(self, args):
        """Initialize the Broker Middleware"""
        try:
            self.logger.info("BrokerMW::configure")
            
            # Save our address and port
            self.addr = args.addr
            self.port = args.port
            
            # Initialize ZMQ context
            self.context = zmq.Context()
            
            # Create a poller
            self.poller = zmq.Poller()
            
            # Socket facing publishers (we subscribe to them)
            self.sub_socket = self.context.socket(zmq.SUB)
            connect_str = f"tcp://{args.publisher_ip}:{args.publisher_port}"
            self.sub_socket.connect(connect_str)
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, "") # Subscribe to all topics
            self.logger.info(f"BrokerMW::configure - Connected to publishers at {connect_str}")
            
            # Socket facing subscribers (we publish to them)
            self.pub_socket = self.context.socket(zmq.PUB)
            bind_str = f"tcp://*:{self.port}"
            self.pub_socket.bind(bind_str)
            self.logger.info(f"BrokerMW::configure - Publisher socket bound at {bind_str}")
            
            # Register the SUB socket with the poller
            self.poller.register(self.sub_socket, zmq.POLLIN)
            
            self.logger.info(f"BrokerMW::configure complete (Primary: {self.is_primary})")
            
        except Exception as e:
            self.logger.error(f"BrokerMW::configure - Exception: {str(e)}")
            raise e

    def event_loop(self, timeout=None):
        """Single iteration of the event loop with optional timeout"""
        try:
            # Poll for events
            events = dict(self.poller.poll(timeout))
            
            # Check if we received any message from publishers
            if self.sub_socket in events:
                # Receive multipart message [topic, content]
                # Topic is used for filtering and routing
                multipart_msg = self.sub_socket.recv_multipart()
                
                if len(multipart_msg) == 2:
                    topic = multipart_msg[0].decode()
                    content = multipart_msg[1]
                    
                    if self.is_primary:
                        # Primary broker - immediately forward to subscribers
                        self.pub_socket.send_multipart([multipart_msg[0], multipart_msg[1]])
                        self.message_count += 1
                        self.logger.debug(f"BrokerMW::event_loop - Forwarded message #{self.message_count} on topic '{topic}'")
                    else:
                        # Backup broker - store in buffer
                        self.message_buffer.append((topic, content))
                        self.logger.debug(f"BrokerMW::event_loop - Buffered message on topic '{topic}' (buffer size: {len(self.message_buffer)})")
                else:
                    self.logger.warning(f"BrokerMW::event_loop - Received malformed message: {multipart_msg}")

        except zmq.ZMQError as e:
            self.logger.error(f"BrokerMW::event_loop - ZMQ error: {str(e)}")
        except Exception as e:
            self.logger.error(f"BrokerMW::event_loop - Exception: {str(e)}")
            raise e

    def forward_messages(self):
        """Forward any buffered messages when running as primary"""
        if not self.is_primary:
            return False
            
        # Check if we have buffered messages to forward
        if self.message_buffer:
            count = 0
            # Forward up to 10 messages at once to avoid blocking too long
            messages_to_forward = self.message_buffer[:10]
            self.message_buffer = self.message_buffer[10:]
            
            for topic, content in messages_to_forward:
                self.pub_socket.send_multipart([topic.encode(), content])
                count += 1
                self.message_count += 1
                
            self.logger.info(f"BrokerMW::forward_messages - Forwarded {count} buffered messages, {len(self.message_buffer)} remaining")
            return True
        return False

    def update_primary_status(self, is_primary):
        """Update our primary/backup status"""
        old_status = self.is_primary
        self.is_primary = is_primary
        if old_status != is_primary:
            self.logger.info(f"BrokerMW::update_primary_status - Status changed to: {'PRIMARY' if is_primary else 'BACKUP'}")
            if is_primary:
                self.logger.info(f"BrokerMW::update_primary_status - Now primary, will start forwarding {len(self.message_buffer)} buffered messages")
                # If we just became primary, start forwarding immediately
                self.forward_messages()
            else:
                self.logger.info("BrokerMW::update_primary_status - Now backup, will buffer incoming messages")

    def set_upcall_handle(self, upcall_obj):
        """Set the upcall object"""
        self.upcall_obj = upcall_obj

    def cleanup(self):
        """Clean up ZMQ sockets"""
        self.logger.info("BrokerMW::cleanup")
        if self.pub_socket:
            self.pub_socket.close()
        if self.sub_socket:
            self.sub_socket.close()
        if self.context:
            self.context.term()


