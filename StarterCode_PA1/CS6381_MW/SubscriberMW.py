import os
import time
import zmq
import logging
import csv
import traceback
import threading
from CS6381_MW import discovery_pb2

class SubscriberMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None  # REQ socket for Discovery communication
        self.sub = None  # SUB socket for topic subscriptions
        self.heartbeat_socket = None  # PUSH socket for load balancer heartbeats
        self.poller = None  # Poller for async event handling
        self.upcall_obj = None  # Handle to application logic
        self.discovery_addr = None  # Discovery address from ZooKeeper
        self.connected_publishers = set()  # Track connected publishers
        self.zk = None  # ZooKeeper client reference
        self.topics_to_groups = {}  # Track topic to group mapping
        self.context = None  # ZMQ context
        self.lb_heartbeat_thread = None  # Thread for sending heartbeats
        self.keep_sending_heartbeats = False  # Control heartbeat thread
        
        # Paths for ZooKeeper structure
        self.zk_paths = {
            "discovery_leader": "/discovery/leader",
            "publishers": "/publishers", 
            "brokers": "/brokers",
            "subscribers": "/subscribers",
            "load_balancers": "/load_balancers"
        }

    def configure(self, discovery_addr, zk_client=None):
        ''' Initialize the Subscriber Middleware '''
        try:
            self.logger.info("SubscriberMW::configure")

            self.discovery_addr = discovery_addr
            self.zk = zk_client  # Store ZooKeeper client reference if provided

            self.context = zmq.Context()
            self.poller = zmq.Poller()
            self.req = self.context.socket(zmq.REQ)  # Request socket for Discovery
            self.sub = self.context.socket(zmq.SUB)  # Subscriber socket for topics

            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            self.connect_to_discovery(self.discovery_addr)

        except Exception as e:
            self.logger.error(f"SubscriberMW::configure - Error: {str(e)}")
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
            raise e

    def set_upcall_handle(self, upcall_obj):
        """ Assigns an upcall handle to the application layer """
        self.upcall_obj = upcall_obj

    def connect_to_discovery(self, discovery_addr):
        ''' Connects to Discovery '''
        # Extract the address part if a lease expiry is appended
        if "|" in discovery_addr:
            discovery_addr = discovery_addr.split("|")[0]
        self.logger.info(f"SubscriberMW::connect_to_discovery - Connecting to Discovery at {discovery_addr}")

        # Only disconnect if we have a previous connection
        if self.discovery_addr and self.req:
            try:
                self.logger.info(f"Disconnecting from previous Discovery at {self.discovery_addr}")
                self.req.disconnect(f"tcp://{self.discovery_addr}")
            except zmq.error.ZMQError as e:
                self.logger.warning(f"Failed to disconnect from {self.discovery_addr}: {str(e)}")

        # Connect to new address
        try:
            self.req.connect(f"tcp://{discovery_addr}")
            self.discovery_addr = discovery_addr
            self.logger.info(f"Successfully connected to Discovery at {discovery_addr}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Discovery at {discovery_addr}: {str(e)}")
            raise e

    def event_loop(self, timeout=1000):
        ''' Run the event loop waiting for messages from Discovery or Publishers '''
        try:
            self.logger.info("SubscriberMW::event_loop - running")

            while True:
                events = dict(self.poller.poll(timeout=timeout))

                if not events:
                    timeout = self.upcall_obj.invoke_operation()
                elif self.req in events:
                    timeout = self.handle_reply()
                elif self.sub in events:
                    self.handle_subscription()
                else:
                    raise Exception("Unknown event in event loop")

        except Exception as e:
            raise e

    def handle_reply(self):
        ''' Handle response from Discovery '''
        try:
            self.logger.info("SubscriberMW::handle_reply")

            bytes_received = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytes_received)

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                return self.upcall_obj.register_response(disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                # Store virtual group info from Discovery response
                if disc_resp.lookup_resp.publishers:
                    for pub in disc_resp.lookup_resp.publishers:
                        if ":" in pub.id and "grp=" in pub.id:
                            # Extract group info from ID (format: "pub_name:grp=X")
                            parts = pub.id.split(":")
                            if len(parts) > 1 and "grp=" in parts[-1]:
                                group_id = int(parts[-1].split("=")[1])
                                # Add to our topic-to-group mapping
                                for topic in self.current_topics:
                                    self.topics_to_groups[topic] = group_id
                
                return self.upcall_obj.lookup_response(disc_resp.lookup_resp)
            else:
                raise ValueError(f"Unhandled response type: {disc_resp.msg_type}")

        except Exception as e:
            raise e

    def register(self, name, topiclist):
        ''' Register the subscriber with the Discovery Service '''
        try:
            self.logger.info("SubscriberMW::register")

            reg_info = discovery_pb2.RegistrantInfo(id=name)
            register_req = discovery_pb2.RegisterReq(role=discovery_pb2.ROLE_SUBSCRIBER, info=reg_info)
            register_req.topiclist.extend(topiclist)

            disc_req = discovery_pb2.DiscoveryReq(msg_type=discovery_pb2.TYPE_REGISTER, register_req=register_req)

            self.req.send(disc_req.SerializeToString())

            # Register in ZooKeeper if available
            if self.zk:
                self.register_in_zookeeper(name)

        except Exception as e:
            raise e
            
    def register_in_zookeeper(self, name):
        """Register subscriber in ZooKeeper for discovery by others"""
        if not self.zk:
            self.logger.warning("SubscriberMW::register_in_zookeeper - No ZooKeeper client available")
            return
            
        try:
            # Ensure the subscribers path exists
            sub_path = self.zk_paths["subscribers"]
            if not self.zk.exists(sub_path):
                self.zk.ensure_path(sub_path)
                
            # Create or update the subscriber's node with its info
            sub_node = f"{sub_path}/{name}"
            
            # Create node with empty data (topics are managed via the Discovery service)
            if self.zk.exists(sub_node):
                self.zk.delete(sub_node)
                
            self.zk.create(sub_node, b"", ephemeral=True)
            self.logger.info(f"SubscriberMW::register_in_zookeeper - Registered in ZooKeeper at {sub_node}")
            
        except Exception as e:
            self.logger.error(f"SubscriberMW::register_in_zookeeper - Failed: {str(e)}")

    def lookup_publishers(self, topics):
        """Lookup publishers for topics of interest"""
        try:
            self.logger.info("SubscriberMW::lookup_publishers")
            
            # Store the current topics being looked up
            self.current_topics = topics
            
            lookup_req = discovery_pb2.LookupPubByTopicReq()
            lookup_req.topiclist.extend(topics)
            
            # Create and send discovery request
            disc_req = discovery_pb2.DiscoveryReq(
                msg_type=discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC,
                lookup_req=lookup_req
            )
            self.req.send(disc_req.SerializeToString())
            
        except Exception as e:
            self.logger.error(f"Error in lookup_publishers: {str(e)}")
            raise e

    def lookup_broker_for_topic(self, topic):
        """Find the appropriate broker for a specific topic using ONLY the group-based structure"""
        if not self.zk:
            self.logger.warning("SubscriberMW::lookup_broker_for_topic - No ZooKeeper client available")
            return None
            
        try:
            broker_path = self.zk_paths["brokers"]
            self.logger.info(f"SubscriberMW::lookup_broker_for_topic - Looking for broker groups under {broker_path}")
            
            # Check if the brokers path exists
            if not self.zk.exists(broker_path):
                self.logger.warning(f"Broker path {broker_path} does not exist in ZooKeeper")
                return None
            
            # Get all broker groups
            groups = self.zk.get_children(broker_path)
            # Filter out non-group nodes 
            groups = [group for group in groups if group.startswith("group")]
            
            if not groups:
                self.logger.warning(f"No broker groups found under {broker_path}")
                return None
                
            self.logger.info(f"Found broker groups: {groups}")
            
            # First, try to find a group that matches our topic's group (if available)
            if topic in self.topics_to_groups:
                group_id = self.topics_to_groups[topic]
                group_name = f"group{group_id}"
                
                if group_name in groups:
                    # Try to connect to this specific group's leader
                    leader_path = f"{broker_path}/{group_name}/leader"
                    if self.zk.exists(leader_path):
                        data, _ = self.zk.get(leader_path)
                        broker_info = data.decode()
                        
                        # Handle format with lease expiry
                        if "|" in broker_info:
                            broker_info = broker_info.split("|")[0]
                            
                        self.logger.info(f"Found matching broker group {group_name} for topic {topic}, leader at {broker_info}")
                        return broker_info
            
            # Otherwise, try any available broker group
            for group_name in groups:
                leader_path = f"{broker_path}/{group_name}/leader"
                if self.zk.exists(leader_path):
                    data, _ = self.zk.get(leader_path)
                    broker_info = data.decode()
                    
                    # Handle format with lease expiry
                    if "|" in broker_info:
                        broker_info = broker_info.split("|")[0]
                        
                    self.logger.info(f"Using broker from group {group_name} at {broker_info} for topic {topic}")
                    return broker_info
            
            # If we still haven't found a broker, log an error
            self.logger.error("No broker leaders found in any group")
            return None
            
        except Exception as e:
            self.logger.error(f"Error looking up broker for topic {topic}: {str(e)}")
            return None

    def subscribe_to_topics(self, pub_address_or_topics, topics=None):
        """Subscribe to topics with the publisher/load balancer.
        
        This method supports two calling styles:
        - subscribe_to_topics(topics) - traditional style
        - subscribe_to_topics(pub_address, topics) - new style for load balancer
        
        Args:
            pub_address_or_topics: Either the address of the publisher or the topic list
            topics: List of topics (or None if first param contains topics)
        """
        try:
            # Check which calling style is being used
            if topics is None:
                # Traditional calling style: subscribe_to_topics(topics)
                topics_to_subscribe = pub_address_or_topics
                # Use the already connected publisher
            else:
                # New calling style: subscribe_to_topics(pub_address, topics)
                pub_address = pub_address_or_topics
                topics_to_subscribe = topics
                
                # Connect to the publisher/load balancer
                self.logger.info(f"Connecting to publisher at {pub_address}")
                
                # Check if the address already has the tcp:// prefix
                if pub_address.startswith("tcp://"):
                    connection_url = pub_address
                else:
                    connection_url = f"tcp://{pub_address}"
                    
                self.sub.connect(connection_url)
            
            # Subscribe to all topics
            for topic in topics_to_subscribe:
                topic_bytes = topic.encode()
                self.logger.debug(f"Subscribing to topic: {topic}")
                self.sub.setsockopt(zmq.SUBSCRIBE, topic_bytes)
                
            self.logger.info(f"Subscribed to {len(topics_to_subscribe)} topics")
            return True
            
        except Exception as e:
            self.logger.error(f"Error subscribing to topics: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def send_heartbeats(self):
        """Send heartbeats periodically to the load balancer"""
        try:
            self.logger.info("SubscriberMW::send_heartbeats - Starting heartbeat thread")
            while self.keep_sending_heartbeats:
                self.heartbeat_socket.send_multipart([b"HEARTBEAT"])
                time.sleep(1)  # Send heartbeat every second
        except Exception as e:
            self.logger.error(f"Error in send_heartbeats: {str(e)}")
        finally:
            self.logger.info("SubscriberMW::send_heartbeats - Stopping heartbeat thread")

    def handle_subscription(self):
        try:
            self.logger.info("SubscriberMW::handle_subscription")
            
            # Receive published message
            message = self.sub.recv_string()
            # Original message format: "topic:timestamp:content"
            topic, time_sent_str, content = message.split(":", 2)  
            time_sent = float(time_sent_str)  # parse timestamp
            time_received = time.time()  

            # Calculate latency
            latency = time_received - time_sent 
            
            # Pass to application layer
            self.upcall_obj.process_message(topic, content)
            
            # Get subscriber name from upcall object if available
            subscriber_name = "Unknown"
            if self.upcall_obj and hasattr(self.upcall_obj, 'name'):
                subscriber_name = self.upcall_obj.name

            # Save in CSV
            csv_filename = "subscriber_data.csv"
            file_exists = os.path.exists(csv_filename)
            
            with open(csv_filename, mode="a", newline="") as csv_file:
                csv_writer = csv.writer(csv_file)
                if not file_exists:
                    # Add subscriber_name to header
                    csv_writer.writerow(["timestamp", "subscriber_name", "topic", "latency", "content"])  
                # Add subscriber_name to data row
                csv_writer.writerow([time_received, subscriber_name, topic, latency, content]) 

            self.logger.info(f"Data saved to {csv_filename} for {subscriber_name}, Topic: {topic}, Latency: {latency:.6f} s")
            
        except Exception as e:
            self.logger.error(f"Error handling subscription: {e}") # Log the error
            self.logger.error(traceback.format_exc()) # Log traceback
            # Decide if you want to re-raise or continue
            # raise e # Re-raising might stop the event loop
            
    def cleanup(self):
        """Clean up resources before shutdown"""
        try:
            self.logger.info("SubscriberMW::cleanup - Cleaning up resources")
            
            # Stop heartbeat thread
            self.keep_sending_heartbeats = False
            if self.lb_heartbeat_thread:
                self.lb_heartbeat_thread.join()
            
            # Close ZMQ sockets
            if self.sub:
                self.sub.close()
                
            if self.req:
                self.req.close()
                
            if self.heartbeat_socket:
                self.heartbeat_socket.close()
                
            # ZMQ context cleanup
            if self.context:
                self.context.term()
                
            # Note: We don't close the ZooKeeper client here as it's managed by the application
                
        except Exception as e:
            self.logger.error(f"SubscriberMW::cleanup - Error: {str(e)}")
