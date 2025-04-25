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
import traceback  # Added for better error logging
from kazoo.protocol.states import KazooState  # For ZK session state listener
from CS6381_MW import discovery_pb2
from collections import deque
import threading


# Constants for ZK paths (matching BrokerMW)
ZK_DISCOVERY_LEADER_PATH = "/discovery/leader"
ZK_PUBLISHERS_PATH = "/publishers"
ZK_BROKERS_PATH = "/brokers"
ZK_SUBSCRIBERS_PATH = "/subscribers"
ZK_LOAD_BALANCERS_PATH = "/load_balancers"
ZK_OWNERSHIP_PATH = "/ownership"  # Base path for ownership strength coordination

# Constants for ZMQ options
ZMQ_PUB_HWM = 1000  # High Water Mark for PUB socket
ZMQ_REQ_TIMEOUT = 5000  # Timeout for REQ socket in ms
ZMQ_REQ_RETRIES = 3  # Retries for REQ socket

class PublisherMW:
    def __init__(self, logger):
        self.logger = logger
        # Ownership strength tracking
        self.ownership_nodes = {}      # topic -> own sequential node name
        self.is_leader = {}            # topic -> bool leadership status
        self.ownership_strength = {}    # topic -> current rank (1 is leader)
        self.context = None  # Initialize in configure
        self.req = None  # REQ socket for Discovery
        self.pub = None  # PUB socket for topic dissemination
        self.poller = None  # Initialize in configure
        self.discovery_addr = None
        self.upcall_obj = None
        self.handle_events = True
        self.zk = None  # ZooKeeper client reference
        self.pub_name = None  # Store publisher name
        self.addr = None  # Store publisher address
        self.port = None  # Store publisher port
        self.topics = []  # List of topics we publish
        self.group_mapping = {}  # Topic to virtual group mapping
        self.history_length = {}  # topic -> number of samples to retain
        self.history_buffer = {}
        


        # Paths for ZooKeeper structure using constants
        self.zk_paths = {
            "discovery_leader": ZK_DISCOVERY_LEADER_PATH,
            "publishers": ZK_PUBLISHERS_PATH,
            "brokers": ZK_BROKERS_PATH,
            "subscribers": ZK_SUBSCRIBERS_PATH,
            "load_balancers": ZK_LOAD_BALANCERS_PATH,
            "ownership": ZK_OWNERSHIP_PATH  # Path for ownership strength coordination
        }

    def configure(self, discovery_addr, port, ipaddr, zk_client=None):
        """Initialize ZMQ context, sockets, and poller."""
        try:
            self.logger.info("PublisherMW::configure")
            # Ensure logger prints DEBUG messages for ownership QoS introspection
            try:
                self.logger.setLevel(logging.DEBUG)
                for handler in self.logger.handlers:
                    handler.setLevel(logging.DEBUG)
            except Exception:
                logging.getLogger().setLevel(logging.DEBUG)

            self.discovery_addr = discovery_addr
            self.port = port
            self.addr = ipaddr
            self.zk = zk_client  # Store ZooKeeper client reference if provided
            # Ensure ownership base path exists in ZooKeeper
            if self.zk:
                self.zk.ensure_path(self.zk_paths['ownership'])
                # Add listener for ZooKeeper session state changes to re-register ownership nodes on session loss
                def _zk_state_listener(state):
                    if state == KazooState.LOST:
                        self.logger.warning("PublisherMW - ZooKeeper session lost. Re-registering ownership nodes")
                        # Clear previous ownership nodes and statuses
                        self.ownership_nodes.clear()
                        self.is_leader.clear()
                        self.ownership_strength.clear()
                        # Recreate ownership nodes and watches for current topics
                        self.register_ownership_strength(self.topics)
                    elif state == KazooState.SUSPENDED:
                        self.logger.warning("PublisherMW - ZooKeeper connection suspended")
                    elif state == KazooState.CONNECTED:
                        self.logger.info("PublisherMW - ZooKeeper reconnected")
                self.zk.add_listener(_zk_state_listener)

            # Initialize ZMQ context
            self.context = zmq.Context()
            self.poller = zmq.Poller()

            # Socket for talking to Discovery service
            # Use constants for options
            self.req = self.context.socket(zmq.REQ)
            self.req.setsockopt(zmq.LINGER, 0)  # Don't wait on close
            self.req.setsockopt(zmq.RCVTIMEO, ZMQ_REQ_TIMEOUT)  # Set receive timeout
            self.req.setsockopt(zmq.SNDTIMEO, ZMQ_REQ_TIMEOUT)  # Set send timeout
            self.poller.register(self.req, zmq.POLLIN)
            self.connect_to_discovery(self.discovery_addr)  # Use helper to connect

            # Socket for disseminating topics
            self.pub = self.context.socket(zmq.PUB)
            # Set High Water Mark to prevent message loss if subscribers are slow
            self.pub.setsockopt(zmq.SNDHWM, ZMQ_PUB_HWM)
            self.pub.bind(f"tcp://*:{self.port}")

            self.logger.info(f"PublisherMW::configure - PUB socket bound to tcp://*:{self.port}, REQ connected to {self.discovery_addr}")

            def _periodic_replay():
                while self.handle_events:
                    self.replay_history()
                    time.sleep(5)  # repeat every 5 sec

            replay_thread = threading.Thread(target=_periodic_replay, daemon=True)
            replay_thread.start()
            self.logger.info("PublisherMW::configure - Started periodic history replay thread")

        except Exception as e:
            self.logger.error(f"PublisherMW::configure - Error: {str(e)}")
            self.logger.error(traceback.format_exc())  # Log full traceback
            raise e  # Re-raise the exception

    def register(self, name, topiclist):
        """Register publisher with Discovery service and ZooKeeper."""
        try:
            self.logger.info(f"PublisherMW::register - Registering '{name}' for topics: {topiclist}")
            self.pub_name = name  # Store publisher name
            self.topics = topiclist  # Store topics

            for topic in topiclist:
                self.history_length[topic] = 15 


            # Register directly in ZooKeeper first (if available)
            # This makes the publisher discoverable via ZK immediately
            if self.zk:
                self.register_in_zookeeper()  # Use stored info
                # Setup ownership strength coordination in ZooKeeper
                self.register_ownership_strength(topiclist)

            # Create discovery service registration request (protobuf)
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

            # Send registration request to Discovery via REQ socket
            # Add retry logic for robustness
            success = self._send_discovery_request(disc_req)
            if not success:
                self.logger.error("PublisherMW::register - Failed to send registration request to Discovery service after retries.")
                # Depending on requirements, might raise an error or just log
                # raise RuntimeError("Failed to register with Discovery Service")

        except Exception as e:
            self.logger.error(f"PublisherMW::register - Error: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise e

    def _send_discovery_request(self, disc_req):
        """Send a request to the Discovery service with retries."""
        for attempt in range(ZMQ_REQ_RETRIES):
            try:
                self.req.send(disc_req.SerializeToString())
                self.logger.debug(f"Sent Discovery request (type {disc_req.msg_type}), attempt {attempt+1}")
                # Wait for reply in event loop, don't block here
                return True  # Assume send succeeded if no immediate error
            except zmq.error.Again as e:
                self.logger.warning(f"Discovery request send timed out (attempt {attempt+1}/{ZMQ_REQ_RETRIES}): {e}")
                # Optional: Reconnect before retrying?
                # self.connect_to_discovery(self.discovery_addr)
                time.sleep(0.5 * (attempt + 1))  # Exponential backoff
            except Exception as e:
                self.logger.error(f"Error sending Discovery request (attempt {attempt+1}): {str(e)}")
                self.logger.error(traceback.format_exc())
                # Optional: Reconnect before retrying?
                # self.connect_to_discovery(self.discovery_addr)
                time.sleep(1 * (attempt + 1))  # Longer backoff for other errors
        return False  # Failed after retries

    def register_in_zookeeper(self):
        """Register or update publisher info in ZooKeeper. Uses stored instance variables."""
        if not self.zk:
            self.logger.warning("PublisherMW::register_in_zookeeper - No ZooKeeper client available")
            return False
        if not self.pub_name or not self.addr or self.port is None:
            self.logger.error("PublisherMW::register_in_zookeeper - Missing publisher info (name, addr, port)")
            return False

        try:
            # Ensure the publishers path exists
            pub_path = self.zk_paths["publishers"]
            self.zk.ensure_path(pub_path)

            # Create or update the publisher's node with its info
            pub_node = f"{pub_path}/{self.pub_name}"

            # Include topic-group-history mapping if available
            if self.group_mapping and self.history_length:
                mapping_str = ",".join([
                    f"{t}:{self.group_mapping.get(t, 0)}.{self.history_length.get(t, 0)}"
                    for t in self.topics
                ])
                node_data = f"{self.addr}:{self.port}|{mapping_str}"
            else:
                # Just include address:port if no mapping is available yet
                node_data = f"{self.addr}:{self.port}"

            # Use helper methods for ZK operations (assuming they exist in zk client or BrokerMW style)
            # Simplified example: delete then create for ephemeral nodes ensures ownership
            try:
                if self.zk.exists(pub_node):
                    self.zk.delete(pub_node)
                    self.logger.debug(f"Deleted existing ZK node: {pub_node}")
            except Exception as del_e:
                # Log error but proceed to create - maybe node was deleted by another process
                self.logger.warning(f"Error deleting existing ZK node {pub_node}: {del_e}")

            self.zk.create(pub_node, node_data.encode(), ephemeral=True, makepath=True)
            self.logger.info(f"PublisherMW::register_in_zookeeper - Registered in ZooKeeper at {pub_node} with data {node_data}")
            return True

        except Exception as e:
            self.logger.error(f"PublisherMW::register_in_zookeeper - Failed: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def update_group_mapping(self, topic_group_mapping):
        """Update the topic to virtual group mapping and update ZooKeeper registration"""
        if not isinstance(topic_group_mapping, dict):
            self.logger.error("PublisherMW::update_group_mapping - Input must be a dictionary.")
            return

        self.group_mapping.update(topic_group_mapping)
        self.logger.info(f"PublisherMW::update_group_mapping - Updated mapping: {self.group_mapping}")

        # If we have ZooKeeper and pub_name, update registration
        if self.zk and self.pub_name:
            self.register_in_zookeeper()  # Re-register with updated mapping

    def event_loop(self, timeout=None):
        """Main event loop polling for ZMQ events."""
        try:
            self.logger.info("PublisherMW::event_loop - running")
            while self.handle_events:
                # Poll sockets with a timeout
                # Use timeout provided by invoke_operation or default (e.g., 1000ms)
                poll_timeout = timeout if timeout is not None else 1000
                events = dict(self.poller.poll(timeout=poll_timeout))

                if not events:
                    # Timeout occurred, invoke application logic
                    if self.upcall_obj:
                        timeout = self.upcall_obj.invoke_operation()
                    else:
                        # If no upcall object, just wait again
                        timeout = None  # Reset timeout for next poll
                elif self.req in events:
                    # Handle reply from Discovery service
                    timeout = self.handle_reply()
                else:
                    # Should not happen with current setup
                    self.logger.error(f"Unknown event source in poller: {events.keys()}")
                    # Maybe add a small sleep to prevent tight loop on error
                    time.sleep(0.1)
                    timeout = None  # Reset timeout

        except KeyboardInterrupt:
            self.logger.info("PublisherMW::event_loop - KeyboardInterrupt received.")
            self.handle_events = False  # Signal loop to stop
        except Exception as e:
            self.logger.error(f"PublisherMW::event_loop - Unhandled exception: {str(e)}")
            self.logger.error(traceback.format_exc())
            self.handle_events = False  # Stop loop on error
            raise e  # Re-raise after logging

    def handle_reply(self):
        """Handle replies received on the REQ socket from Discovery."""
        try:
            self.logger.debug("PublisherMW::handle_reply - Receiving reply from Discovery")
            bytes_received = self.req.recv()  # This might block if called without polling, but event_loop ensures it's ready
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytes_received)

            self.logger.info(f"PublisherMW::handle_reply - Received response type: {disc_resp.msg_type}")

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                # Process registration response
                reg_resp = disc_resp.register_resp
                self.logger.info(f"Registration response status: {reg_resp.status}, Reason: {reg_resp.reason}")

                # If registration response contains group mapping info in the reason field, extract it
                # This assumes a specific format like "topic1:grp1,topic2:grp2" in the reason string
                if reg_resp.status == discovery_pb2.STATUS_SUCCESS and reg_resp.reason:
                    try:
                        mapping_str = reg_resp.reason
                        new_mapping = {}
                        for pair in mapping_str.split(","):
                            if ":" in pair:
                                topic, grp_str = pair.split(":", 1)
                                try:
                                    grp_id = int(grp_str)
                                    new_mapping[topic.strip()] = grp_id
                                except ValueError:
                                    self.logger.warning(f"Invalid group ID '{grp_str}' in mapping string: {mapping_str}")
                        if new_mapping:
                            self.logger.info(f"Received group mapping from Discovery: {new_mapping}")
                            self.update_group_mapping(new_mapping)  # Update internal state and ZK

                    except Exception as e:
                        self.logger.error(f"Error processing group mapping from Discovery reason field: {e}")

                # Pass response to application layer via upcall
                if self.upcall_obj:
                    return self.upcall_obj.register_response(reg_resp)
                else:
                    return None  # No upcall object, return default timeout

            # Add handling for other response types if needed (e.g., lookup responses if publisher ever looks up)
            # elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
            #     ...

            else:
                self.logger.warning(f"Unrecognized response message type: {disc_resp.msg_type}")
                return None  # Return default timeout

        except zmq.error.Again:
            # This shouldn't happen if poller indicated POLLIN, but handle defensively
            self.logger.warning("PublisherMW::handle_reply - recv timed out unexpectedly.")
            return None
        except Exception as e:
            self.logger.error(f"PublisherMW::handle_reply - Error processing reply: {str(e)}")
            self.logger.error(traceback.format_exc())
            # Decide how to handle error - maybe return a specific timeout or raise
            raise e  # Re-raise by default

    def disseminate(self, topic, data):
        """Publish data under a specific topic."""
        # Removed 'id' parameter as it wasn't used
        if not isinstance(topic, str) or not topic:
            self.logger.error("PublisherMW::disseminate - Invalid topic provided.")
            return False
        # Data validation could be added here if needed

        try:
            # store message
            if topic not in self.history_buffer:
                maxlen = self.history_length.get(topic, 0)
                self.history_buffer[topic] = deque(maxlen=maxlen)
            self.history_buffer[topic].append((time.time(), data))
            
            # Ownership strength: only leader for topic publishes
            if self.zk and topic in self.ownership_nodes:
                if not self.is_leader.get(topic, False):
                    self.logger.debug(f"PublisherMW::disseminate - Not ownership leader for topic '{topic}', dropping message.")
                    return False
                else:
                    strength = self.ownership_strength.get(topic)
                    self.logger.debug(f"PublisherMW::disseminate - Ownership leader (strength={strength}) confirmed for topic '{topic}', sending message.")
            # Add timestamp for latency calculation
            timestamp = time.time()
            # Format message: "topic:timestamp:data"
            send_str = f"{topic}:{timestamp}:{data}"
            self.logger.debug(f"PublisherMW::disseminate - Publishing on topic '{topic}': {send_str[:100]}...")  # Log snippet

            # Send the message as bytes, using NOBLOCK for safety with HWM
            # If HWM is reached, NOBLOCK prevents blocking indefinitely
            try:
                self.pub.send(bytes(send_str, "utf-8"), zmq.NOBLOCK)
                self.logger.debug(f"PublisherMW::disseminate - Message sent for topic '{topic}'")
                return True
            except zmq.error.Again:
                # High Water Mark likely reached, message not sent
                self.logger.warning(f"PublisherMW::disseminate - Could not send message on topic '{topic}', HWM possibly reached. Message dropped.")
                # Implement retry or buffering here if dropping messages is not acceptable
                return False

        except Exception as e:
            self.logger.error(f"PublisherMW::disseminate - Error publishing on topic '{topic}': {str(e)}")
            self.logger.error(traceback.format_exc())
            # Consider if this should raise or just return False
            return False  # Return False on error

    def register_ownership_strength(self, topics):
        """Create ephemeral sequential nodes and watches for ownership strength per topic."""
        for topic in topics:
            try:
                base = f"{self.zk_paths['ownership']}/{topic}"
                # Ensure base path exists
                self.zk.ensure_path(base)
                # Create ephemeral sequential node for this publisher
                node_path = self.zk.create(f"{base}/strength-", b"", ephemeral=True, sequence=True, makepath=True)
                node_name = node_path.split('/')[-1]
                self.ownership_nodes[topic] = node_name
                self.ownership_strength[topic] = None  # will be set on first watch callback
                self.logger.debug(f"PublisherMW::register_ownership_strength - Created strength node '{node_name}' for topic '{topic}' at path '{node_path}'")
                # Initialize leadership status
                self.is_leader[topic] = False
                # Watch for ownership changes
                @self.zk.ChildrenWatch(base)
                def watch_children(children, topic=topic):
                    self._handle_ownership_change(topic, children)
                # Perform initial fetch of children to set initial leadership status before event loop
                try:
                    initial_children = self.zk.get_children(base)
                    self._handle_ownership_change(topic, initial_children)
                except Exception as e:
                    self.logger.error(f"PublisherMW::register_ownership_strength - Failed initial children fetch for topic '{topic}': {e}")
            except Exception as e:
                self.logger.error(f"PublisherMW::register_ownership_strength - Error for topic '{topic}': {e}")

    def _handle_ownership_change(self, topic, children):
        """Determine ownership leader for a topic based on smallest sequential node."""
        try:
            if not children:
                return
            self.logger.debug(f"PublisherMW::_handle_ownership_change - Children before sort for topic '{topic}': {children}")
            sorted_nodes = sorted(children)
            leader = sorted_nodes[0]
            # Compute strength rank (1-based index in sorted list)
            my_node = self.ownership_nodes.get(topic)
            if my_node in sorted_nodes:
                rank = sorted_nodes.index(my_node) + 1
                self.ownership_strength[topic] = rank
            else:
                rank = None
                self.ownership_strength[topic] = None
            self.logger.debug(f"PublisherMW::_handle_ownership_change - Sorted nodes for topic '{topic}': {sorted_nodes}, my_node: {my_node}, rank: {rank}, leader: {leader}")
            prev = self.is_leader.get(topic, False)
            curr = (leader == self.ownership_nodes.get(topic))
            if curr != prev:
                self.is_leader[topic] = curr
                if curr:
                    self.logger.info(f"PublisherMW - Gained ownership for topic '{topic}'")
                else:
                    self.logger.info(f"PublisherMW - Lost ownership for topic '{topic}'")
        except Exception as e:
            self.logger.error(f"PublisherMW::_handle_ownership_change - {e}")

    def set_upcall_handle(self, upcall_obj):
        """Set the upcall object for application callbacks."""
        self.logger.info("PublisherMW::set_upcall_handle")
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        """Signal the event loop to stop."""
        self.logger.info("PublisherMW::disable_event_loop - Stopping event loop")
        self.handle_events = False

    def connect_to_discovery(self, discovery_addr):
        """Connect or reconnect the REQ socket to a Discovery service instance."""
        try:
            # Extract the address part if a lease expiry is appended (e.g., "host:port|expiry")
            connect_addr = discovery_addr.split("|")[0] if "|" in discovery_addr else discovery_addr
            target_endpoint = f"tcp://{connect_addr}"

            self.logger.info(f"PublisherMW::connect_to_discovery - Attempting to connect REQ socket to {target_endpoint}")

            # Disconnect from the old address *if* it's different and we were connected
            if self.discovery_addr and f"tcp://{self.discovery_addr.split('|')[0]}" != target_endpoint:
                try:
                    old_endpoint = f"tcp://{self.discovery_addr.split('|')[0]}"
                    self.logger.debug(f"Disconnecting from previous Discovery at {old_endpoint}")
                    self.req.disconnect(old_endpoint)
                except zmq.error.ZMQError as e:
                    # Log warning but continue - disconnect might fail if already disconnected
                    self.logger.warning(f"Error disconnecting from {old_endpoint}: {str(e)}")

            # Connect to the new Discovery service
            self.req.connect(target_endpoint)
            self.discovery_addr = discovery_addr  # Store the original address (might include expiry)
            self.logger.info(f"Successfully connected REQ socket to Discovery at {target_endpoint}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect REQ socket to Discovery at {discovery_addr}: {str(e)}")
            self.logger.error(traceback.format_exc())
            # Consider the state - should we raise? Can the publisher operate without discovery?
            # raise e # Re-raise if connection is critical
            return False

    def cleanup(self):
        """Clean up ZMQ resources."""
        try:
            self.logger.info("PublisherMW::cleanup - Cleaning up ZMQ resources")

            # Stop the event loop if running
            self.disable_event_loop()

            # Close sockets safely
            if self.pub:
                self.logger.debug("Closing PUB socket")
                self.pub.close()
                self.pub = None

            if self.req:
                self.logger.debug("Closing REQ socket")
                # Unregister from poller before closing
                if self.poller and self.req in self.poller.sockets:
                    try:
                        self.poller.unregister(self.req)
                    except KeyError:  # Already unregistered
                        pass
                self.req.close()
                self.req = None

            # Terminate ZMQ context
            if self.context:
                self.logger.debug("Terminating ZMQ context")
                self.context.term()
                self.context = None

            # Note: We don't close the ZooKeeper client here as it's managed by the application

            self.logger.info("PublisherMW::cleanup - ZMQ cleanup complete")

        except Exception as e:
            self.logger.error(f"PublisherMW::cleanup - Error during cleanup: {str(e)}")
            self.logger.error(traceback.format_exc())

    def replay_history(self):
        """Replay the latest N history messages for each topic"""
        try:
            self.logger.info("PublisherMW::replay_history - Replaying history to connected subscribers")

            for topic, buffer in self.history_buffer.items():
                for timestamp, data in buffer:
                    send_str = f"{topic}:{timestamp}:{data}:H"
                    try:
                        self.pub.send(bytes(send_str, "utf-8"), zmq.NOBLOCK)
                        self.logger.debug(f"Replayed history message for topic '{topic}': {send_str[:100]}...")
                    except zmq.error.Again:
                        self.logger.warning(f"PublisherMW::replay_history - Failed to send history message for topic '{topic}'")
        except Exception as e:
            self.logger.error(f"PublisherMW::replay_history - Error during replay: {str(e)}")
            self.logger.error(traceback.format_exc())
