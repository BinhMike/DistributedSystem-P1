###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the publisher middleware code
#
# Created: Spring 2023
#
###############################################
# import the needed packages
import os
import sys
import time
import zmq
import logging
from CS6381_MW import discovery_pb2

class PublisherMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None  # REQ socket for Discovery
        self.pub = None  # PUB socket for topic dissemination
        self.poller = None
        self.discovery_addr = None
        self.upcall_obj = None
        self.handle_events = True
        self.zk = None  # ZooKeeper client reference
        self.topics = []  # List of topics we publish
        self.group_mapping = {}  # Topic to virtual group mapping
        
        # Paths for ZooKeeper structure
        self.zk_paths = {
            "discovery_leader": "/discovery/leader", 
            "publishers": "/publishers",
            "brokers": "/brokers",
            "subscribers": "/subscribers",
            "load_balancers": "/load_balancers"
        }

    def configure(self, discovery_addr, port, ipaddr, zk_client=None):
        try:
            self.logger.info("PublisherMW::configure")

            self.discovery_addr = discovery_addr
            self.port = port
            self.addr = ipaddr
            self.zk = zk_client  # Store ZooKeeper client reference if provided

            self.context = zmq.Context()
            self.poller = zmq.Poller()
            
            # Socket for talking to Discovery service
            self.req = self.context.socket(zmq.REQ)
            self.poller.register(self.req, zmq.POLLIN)
            self.req.connect(f"tcp://{self.discovery_addr}")
            
            # Socket for disseminating topics
            self.pub = self.context.socket(zmq.PUB)
            self.pub.bind(f"tcp://*:{self.port}")
            
            self.logger.info(f"PublisherMW::configure - PUB socket bound to port {self.port}")

        except Exception as e:
            raise e

    def register(self, name, topiclist):
        try:
            self.logger.info("PublisherMW::register")
            self.pub_name = name  # Store publisher name for ZooKeeper registration
            self.topics = topiclist  # Store topics for reference
            
            # Create discovery service registration request
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            reg_info.addr = self.addr
            reg_info.port = self.port
            
            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_PUBLISHER
            register_req.info.CopyFrom(reg_info)
            register_req.topiclist.extend(topiclist)
            
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)
            
            # Send registration request to Discovery
            self.req.send(disc_req.SerializeToString())
            
            # Also register directly in ZooKeeper if available
            if self.zk:
                self.register_in_zookeeper(name, topiclist)
            
        except Exception as e:
            raise e

    def register_in_zookeeper(self, name, topiclist):
        """Register publisher in ZooKeeper for discovery by others"""
        if not self.zk:
            self.logger.warning("PublisherMW::register_in_zookeeper - No ZooKeeper client available")
            return
            
        try:
            # Ensure the publishers path exists
            pub_path = self.zk_paths["publishers"]
            self.zk.ensure_path(pub_path)
                
            # Create or update the publisher's node with its info
            pub_node = f"{pub_path}/{name}"
            
            # Include topic-group mapping if available
            if self.group_mapping:
                mapping_str = ",".join([f"{t}:{self.group_mapping[t]}" for t in self.group_mapping])
                node_data = f"{self.addr}:{self.port}|{mapping_str}"
            else:
                # Just include address:port if no mapping is available yet
                node_data = f"{self.addr}:{self.port}"
                
            if self.zk.exists(pub_node):
                self.zk.delete(pub_node)
                
            self.zk.create(pub_node, node_data.encode(), ephemeral=True)
            self.logger.info(f"PublisherMW::register_in_zookeeper - Registered in ZooKeeper at {pub_node} with data {node_data}")
            
        except Exception as e:
            self.logger.error(f"PublisherMW::register_in_zookeeper - Failed: {str(e)}")

    def update_group_mapping(self, topic_group_mapping):
        """Update the topic to virtual group mapping and update ZooKeeper registration"""
        self.group_mapping.update(topic_group_mapping)
        self.logger.info(f"PublisherMW::update_group_mapping - Updated mapping: {self.group_mapping}")
        
        # If we have ZooKeeper and pub_name, update registration
        if self.zk and hasattr(self, 'pub_name'):
            self.register_in_zookeeper(self.pub_name, self.topics)

    def event_loop(self, timeout=None):
        try:
            self.logger.info("PublisherMW::event_loop - running")
            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))

                if not events:
                    timeout = self.upcall_obj.invoke_operation()
                elif self.req in events:
                    timeout = self.handle_reply()
                else:
                    raise Exception("Unknown event in event loop")
        except Exception as e:
            raise e

    def handle_reply(self):
        try:
            self.logger.info("PublisherMW::handle_reply")
            bytes_received = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytes_received)

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                # If registration response contains group mapping info in the reason field,
                # extract it (since additional_info doesn't exist in the protobuf definition)
                if hasattr(disc_resp.register_resp, "reason") and disc_resp.register_resp.reason:
                    try:
                        mapping_str = disc_resp.register_resp.reason
                        if mapping_str:
                            for pair in mapping_str.split(","):
                                if ":" in pair:
                                    topic, grp = pair.split(":")
                                    self.group_mapping[topic] = int(grp)
                            
                            # Update ZooKeeper registration with new mapping
                            if self.zk and hasattr(self, 'pub_name'):
                                self.register_in_zookeeper(self.pub_name, self.topics)
                    except Exception as e:
                        self.logger.error(f"Error processing group mapping: {e}")
                
                return self.upcall_obj.register_response(disc_resp.register_resp)
            else:
                raise ValueError("Unrecognized response message")
        except Exception as e:
            raise e

    def disseminate (self, id, topic, data):
        try:
            self.logger.info ("PublisherMW::disseminate")

            # Now use the protobuf logic to encode the info and send it.  But for now
            # we are simply sending the string to make sure dissemination is working.
            timestamp = time.time()  # add time stamp
            send_str = f"{topic}:{timestamp}:{data}"  
            self.logger.info (f"PublisherMW::disseminate - {send_str}")

            # send the info as bytes. See how we are providing an encoding of utf-8
            self.pub.send (bytes(send_str, "utf-8"))

            self.logger.info ("PublisherMW::disseminate complete")
        except Exception as e:
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop (self):
        ''' disable event loop '''
        self.handle_events = False
        
    def connect_to_discovery(self, discovery_addr):
        """Connect to a Discovery service instance"""
        try:
            # Extract the address part if a lease expiry is appended
            if "|" in discovery_addr:
                discovery_addr = discovery_addr.split("|")[0]
            self.logger.info(f"PublisherMW::connect_to_discovery - Connecting to {discovery_addr}")
            
            # Only disconnect if we have a previous connection
            if self.discovery_addr and self.req:
                try:
                    self.req.disconnect(f"tcp://{self.discovery_addr}")
                except zmq.error.ZMQError as e:
                    self.logger.warning(f"Error disconnecting from {self.discovery_addr}: {str(e)}")
                    
            # Connect to new Discovery service
            self.req.connect(f"tcp://{discovery_addr}")
            self.discovery_addr = discovery_addr
            self.logger.info(f"Successfully connected to Discovery at {discovery_addr}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Discovery at {discovery_addr}: {str(e)}")
            raise e
            
    def cleanup(self):
        """Clean up resources before shutdown"""
        try:
            self.logger.info("PublisherMW::cleanup - Cleaning up resources")
            
            # Close sockets
            if self.pub:
                self.pub.close()
                
            if self.req:
                self.req.close()
                
            # Close ZMQ context
            if hasattr(self, 'context'):
                self.context.term()
                
            # Note: We don't close the ZooKeeper client here as it's managed by the application
                
        except Exception as e:
            self.logger.error(f"PublisherMW::cleanup - Error: {str(e)}")