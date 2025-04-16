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
from CS6381_MW import discovery_pb2

class DiscoveryMW:
    """Middleware for the Discovery Service"""

    def __init__(self, logger, zk_client=None):
        self.logger = logger
        self.rep = None  # REP socket for discovery service
        self.poller = None  # Poller for event loop
        self.zk = zk_client  # ZooKeeper client
        self.addr = None  # Our address
        self.port = None  # Our port
        self.upcall_obj = None  # Upcall object
        
        # Paths for unified ZooKeeper structure
        self.zk_paths = {
            "discovery_leader": "/discovery/leader",
            "discovery_replicas": "/discovery/replicas",
            "publishers": "/publishers",
            "subscribers": "/subscribers",
            "brokers": "/brokers",
            "load_balancers": "/load_balancers"
        }
        
        # Virtual group settings
        self.num_virtual_groups = 3  # Default number of virtual groups

    def configure(self, args):
        """Configure the middleware"""
        try:
            self.logger.info("DiscoveryMW::configure")
            self.addr = args.addr
            self.port = args.port
            
            # Create ZMQ context and socket
            context = zmq.Context()
            self.poller = zmq.Poller()
            self.rep = context.socket(zmq.REP)
            self.logger.debug("DiscoveryMW::configure - REP socket created")
            
            # Bind socket
            bind_str = f"tcp://*:{self.port}"
            self.rep.bind(bind_str)
            self.logger.debug(f"DiscoveryMW::configure - Bound to {bind_str}")
            
            # Register with poller
            self.poller.register(self.rep, zmq.POLLIN)
            self.logger.debug("DiscoveryMW::configure - Registered with poller")
            
        except Exception as e:
            self.logger.error(f"DiscoveryMW::configure - Exception: {str(e)}")
            raise e

    def set_upcall_handle(self, upcall_obj):
        """Set the upcall object"""
        self.upcall_obj = upcall_obj
        
    def set_virtual_groups(self, num_groups):
        """Set the number of virtual groups for publishers"""
        self.num_virtual_groups = num_groups
        self.logger.info(f"DiscoveryMW::set_virtual_groups - Set to {num_groups}")

    def event_loop(self):
        """Run the event loop"""
        try:
            self.logger.info("DiscoveryMW::event_loop - Starting event loop")
            
            while True:
                events = dict(self.poller.poll())
                
                if self.rep in events:
                    # Received a request, so process it
                    self._handle_request()
                    
        except KeyboardInterrupt:
            self.logger.info("DiscoveryMW::event_loop - KeyboardInterrupt")
        except Exception as e:
            self.logger.error(f"DiscoveryMW::event_loop - Exception: {str(e)}")
            raise e

    def _handle_request(self):
        """Handle incoming requests"""
        try:
            # Receive request
            request_bytes = self.rep.recv()
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.ParseFromString(request_bytes)
            
            # Based on the request type, process it
            if disc_req.msg_type == discovery_pb2.TYPE_REGISTER:
                # Handle registration request
                response = self.upcall_obj.register(disc_req.register_req)
                self.rep.send(response.SerializeToString())
                
            elif disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                # Handle lookup request
                response = self.upcall_obj.lookup(disc_req.lookup_req)
                self.rep.send(response.SerializeToString())
                
            else:
                self.logger.warning(f"Unknown request type: {disc_req.msg_type}")
                # Respond with an error
                response = discovery_pb2.DiscoveryResp()
                response.msg_type = disc_req.msg_type
                self.rep.send(response.SerializeToString())
                
        except Exception as e:
            self.logger.error(f"DiscoveryMW::_handle_request - Exception: {str(e)}")
            # Try to send an error response if possible
            try:
                response = discovery_pb2.DiscoveryResp()
                response.msg_type = discovery_pb2.TYPE_UNKNOWN
                self.rep.send(response.SerializeToString())
            except:
                pass

    def update_publishers_in_zookeeper(self, publishers_dict):
        """Update publisher information in ZooKeeper"""
        if not self.zk:
            self.logger.warning("DiscoveryMW::update_publishers_in_zookeeper - No ZooKeeper client")
            return
            
        try:
            # Ensure the publisher path exists
            self.zk.ensure_path(self.zk_paths["publishers"])
            
            # Update or create each publisher node
            for pub_id, pub_info in publishers_dict.items():
                pub_path = f"{self.zk_paths['publishers']}/{pub_id}"
                
                # Format publisher data with group mapping if available
                if "group_mapping" in pub_info:
                    mapping_str = ",".join([f"{t}:{pub_info['group_mapping'][t]}" for t in pub_info["group_mapping"]])
                    pub_data = f"{pub_info['addr']}:{pub_info['port']}|{mapping_str}"
                else:
                    pub_data = f"{pub_info['addr']}:{pub_info['port']}"
                
                # Create or update the node
                if self.zk.exists(pub_path):
                    self.zk.set(pub_path, pub_data.encode())
                else:
                    self.zk.create(pub_path, pub_data.encode(), ephemeral=True)
                    
            self.logger.info(f"Updated {len(publishers_dict)} publishers in ZooKeeper")
            
        except Exception as e:
            self.logger.error(f"DiscoveryMW::update_publishers_in_zookeeper - Exception: {str(e)}")

    def update_brokers_in_zookeeper(self, brokers_dict):
        """Update broker information in ZooKeeper"""
        if not self.zk:
            self.logger.warning("DiscoveryMW::update_brokers_in_zookeeper - No ZooKeeper client")
            return
            
        try:
            # Ensure the broker path exists
            self.zk.ensure_path(self.zk_paths["brokers"])
            
            # Update or create each broker node
            for broker_id, broker_info in brokers_dict.items():
                # Extract group information from broker_id if available
                group = "default_group"
                if ":" in broker_id:
                    parts = broker_id.split(":")
                    if len(parts) > 1:
                        group_part = parts[-1].strip()
                        if group_part.startswith("group="):
                            group = group_part.split("=")[1]
                
                # Ensure group path exists
                group_path = f"{self.zk_paths['brokers']}/{group}"
                if not self.zk.exists(group_path):
                    self.zk.ensure_path(group_path)
                
                # Create/update broker node in its group
                broker_path = f"{group_path}/{broker_id}"
                broker_data = f"{broker_info['addr']}:{broker_info['port']}"
                
                if self.zk.exists(broker_path):
                    self.zk.set(broker_path, broker_data.encode())
                else:
                    self.zk.create(broker_path, broker_data.encode(), ephemeral=True)
                
                # Also maintain a broker replicas path for quorum management
                replica_path = f"{group_path}/replicas"
                if not self.zk.exists(replica_path):
                    self.zk.ensure_path(replica_path)
                    
                broker_replica_node = f"{replica_path}/{broker_id}"
                if self.zk.exists(broker_replica_node):
                    self.zk.set(broker_replica_node, broker_data.encode())
                else:
                    self.zk.create(broker_replica_node, broker_data.encode(), ephemeral=True)
                
            self.logger.info(f"Updated {len(brokers_dict)} brokers in ZooKeeper")
            
        except Exception as e:
            self.logger.error(f"DiscoveryMW::update_brokers_in_zookeeper - Exception: {str(e)}")

    def update_subscribers_in_zookeeper(self, subscribers_dict):
        """Update subscriber information in ZooKeeper"""
        if not self.zk:
            self.logger.warning("DiscoveryMW::update_subscribers_in_zookeeper - No ZooKeeper client")
            return
            
        try:
            # Ensure the subscribers path exists
            self.zk.ensure_path(self.zk_paths["subscribers"])
            
            # Update or create each subscriber node
            for sub_id, sub_info in subscribers_dict.items():
                sub_path = f"{self.zk_paths['subscribers']}/{sub_id}"
                sub_data = f"{sub_info['addr']}:{sub_info['port']}"
                
                if self.zk.exists(sub_path):
                    self.zk.set(sub_path, sub_data.encode())
                else:
                    self.zk.create(sub_path, sub_data.encode(), ephemeral=True)
                    
            self.logger.info(f"Updated {len(subscribers_dict)} subscribers in ZooKeeper")
            
        except Exception as e:
            self.logger.error(f"DiscoveryMW::update_subscribers_in_zookeeper - Exception: {str(e)}")
            
    def cleanup(self):
        """Cleanup the middleware resources"""
        try:
            self.logger.info("DiscoveryMW::cleanup")
            
            # Close socket (if it exists)
            if self.rep:
                self.rep.close()
                
            # Note: Don't close ZK client here as it's managed by the application
                
        except Exception as e:
            self.logger.error(f"DiscoveryMW::cleanup - Exception: {str(e)}")
