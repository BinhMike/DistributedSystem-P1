import os
import time
import zmq
import logging
import csv
import traceback
import threading
from CS6381_MW import discovery_pb2

# Constants for ZK paths
ZK_DISCOVERY_LEADER_PATH = "/discovery/leader"
ZK_PUBLISHERS_PATH = "/publishers"
ZK_BROKERS_PATH = "/brokers"
ZK_SUBSCRIBERS_PATH = "/subscribers"
ZK_LOAD_BALANCERS_PATH = "/load_balancers"

# Constants for ZMQ options
ZMQ_REQ_TIMEOUT = 5000
ZMQ_REQ_RETRIES = 3
ZMQ_SUB_TIMEOUT = 1000

class SubscriberMW:
    def __init__(self, logger):
        self.logger = logger
        self.context = None
        self.req = None
        self.sub = None
        self.poller = None
        self.upcall_obj = None
        self.discovery_addr = None
        self.connected_publishers = set()
        self.zk = None
        self.topics_to_groups = {}
        self.current_topics = set()
        self.handle_events = True

        self.zk_paths = {
            "discovery_leader": ZK_DISCOVERY_LEADER_PATH,
            "publishers": ZK_PUBLISHERS_PATH,
            "brokers": ZK_BROKERS_PATH,
            "subscribers": ZK_SUBSCRIBERS_PATH,
            "load_balancers": ZK_LOAD_BALANCERS_PATH
        }

    def configure(self, discovery_addr, zk_client=None):
        try:
            self.logger.info("SubscriberMW::configure")

            self.discovery_addr = discovery_addr
            self.zk = zk_client

            self.context = zmq.Context()
            self.poller = zmq.Poller()

            self.req = self.context.socket(zmq.REQ)
            self.req.setsockopt(zmq.LINGER, 0)
            self.req.setsockopt(zmq.RCVTIMEO, ZMQ_REQ_TIMEOUT)
            self.req.setsockopt(zmq.SNDTIMEO, ZMQ_REQ_TIMEOUT)
            self.poller.register(self.req, zmq.POLLIN)

            self.sub = self.context.socket(zmq.SUB)
            self.poller.register(self.sub, zmq.POLLIN)

            self.connect_to_discovery(self.discovery_addr)

            self.logger.info("SubscriberMW::configure - Configuration complete")

        except Exception as e:
            self.logger.error(f"SubscriberMW::configure - Error: {str(e)}")
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.logger.info("SubscriberMW::set_upcall_handle")
        self.upcall_obj = upcall_obj

    def connect_to_discovery(self, discovery_addr):
        try:
            connect_addr = discovery_addr.split("|")[0] if "|" in discovery_addr else discovery_addr
            target_endpoint = f"tcp://{connect_addr}"

            self.logger.info(f"SubscriberMW::connect_to_discovery - Attempting to connect REQ socket to {target_endpoint}")

            if self.discovery_addr and f"tcp://{self.discovery_addr.split('|')[0]}" != target_endpoint:
                try:
                    old_endpoint = f"tcp://{self.discovery_addr.split('|')[0]}"
                    self.logger.debug(f"Disconnecting from previous Discovery at {old_endpoint}")
                    self.req.disconnect(old_endpoint)
                except zmq.error.ZMQError as e:
                    self.logger.warning(f"Error disconnecting from {old_endpoint}: {str(e)}")

            self.req.connect(target_endpoint)
            self.discovery_addr = discovery_addr
            self.logger.info(f"Successfully connected REQ socket to Discovery at {target_endpoint}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect REQ socket to Discovery at {discovery_addr}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def event_loop(self, timeout=None):
        try:
            self.logger.info("SubscriberMW::event_loop - running")
            current_timeout = timeout if timeout is not None else 1000

            while self.handle_events:
                events = dict(self.poller.poll(timeout=current_timeout))

                if not events:
                    if self.upcall_obj:
                        current_timeout = self.upcall_obj.invoke_operation()
                    else:
                        current_timeout = 1000
                elif self.req in events:
                    current_timeout = self.handle_reply()
                elif self.sub in events:
                    self.handle_subscription()
                    if self.upcall_obj:
                        current_timeout = self.upcall_obj.invoke_operation()
                    else:
                        current_timeout = 1000
                else:
                    self.logger.error(f"Unknown event source in poller: {events.keys()}")
                    time.sleep(0.1)
                    current_timeout = 1000

        except KeyboardInterrupt:
            self.logger.info("SubscriberMW::event_loop - KeyboardInterrupt received.")
            self.handle_events = False
        except Exception as e:
            self.logger.error(f"SubscriberMW::event_loop - Unhandled exception: {str(e)}")
            self.logger.error(traceback.format_exc())
            self.handle_events = False
            raise e

    def handle_reply(self):
        try:
            self.logger.debug("SubscriberMW::handle_reply - Receiving reply from Discovery")
            bytes_received = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytes_received)

            self.logger.info(f"SubscriberMW::handle_reply - Received response type: {disc_resp.msg_type}")

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                reg_resp = disc_resp.register_resp
                self.logger.info(f"Registration response status: {reg_resp.status}, Reason: {reg_resp.reason}")
                if self.upcall_obj:
                    return self.upcall_obj.register_response(reg_resp)
                else:
                    return None

            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                # Process lookup response
                lookup_resp = disc_resp.lookup_resp
                # self.logger.info(f"Lookup response status: {lookup_resp.status}") # Removed: LookupPubByTopicResp has no status field

                # Clear previous group mapping for the topics we just looked up
                # This prevents stale mappings if a publisher changes groups

                for topic in self.current_topics:
                    if topic in self.topics_to_groups:
                        del self.topics_to_groups[topic]

                if lookup_resp.publishers:
                    self.logger.debug(f"Found {len(lookup_resp.publishers)} potential publishers/brokers.")
                    for pub_info in lookup_resp.publishers:
                        group_id = None
                        if "|" in pub_info.id:
                            parts = pub_info.id.split("|", 1)
                            metadata = parts[1]
                            if metadata.startswith("group="):
                                try:
                                    group_id = int(metadata.split("=")[1])
                                    for topic in self.current_topics:
                                        self.topics_to_groups[topic] = group_id
                                    self.logger.debug(f"Broker {parts[0]} assigned to group {group_id} for topics {self.current_topics}")
                                except (ValueError, IndexError):
                                    self.logger.warning(f"Could not parse group ID from broker metadata: {metadata}")
                            else:
                                try:
                                    topic_mappings = dict(pair.split(":") for pair in metadata.split(","))
                                    for topic, grp_str in topic_mappings.items():
                                        if topic in self.current_topics:
                                            try:
                                                self.topics_to_groups[topic] = int(grp_str)
                                                self.logger.debug(f"Publisher {parts[0]} assigned topic '{topic}' to group {self.topics_to_groups[topic]}")
                                            except ValueError:
                                                self.logger.warning(f"Invalid group ID '{grp_str}' for topic '{topic}' in publisher metadata: {metadata}")
                                except ValueError:
                                    self.logger.warning(f"Could not parse topic-group mapping from publisher metadata: {metadata}")

                self.logger.info(f"Updated topic-to-group mapping: {self.topics_to_groups}")

                if self.upcall_obj:
                    return self.upcall_obj.lookup_response(lookup_resp)
                else:
                    return None
            else:
                self.logger.warning(f"Unhandled response type: {disc_resp.msg_type}")
                return None

        except zmq.error.Again:
            self.logger.warning("SubscriberMW::handle_reply - recv timed out unexpectedly.")
            return None
        except Exception as e:
            self.logger.error(f"SubscriberMW::handle_reply - Error processing reply: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise e

    def register(self, name, topiclist):
        try:
            self.logger.info(f"SubscriberMW::register - Registering '{name}' for topics: {topiclist}")
            self.current_topics.update(topiclist)

            if self.zk:
                self.register_in_zookeeper(name)

            reg_info = discovery_pb2.RegistrantInfo(id=name)
            register_req = discovery_pb2.RegisterReq(role=discovery_pb2.ROLE_SUBSCRIBER, info=reg_info)
            register_req.topiclist.extend(topiclist)

            disc_req = discovery_pb2.DiscoveryReq(msg_type=discovery_pb2.TYPE_REGISTER, register_req=register_req)

            success = self._send_discovery_request(disc_req)
            if not success:
                self.logger.error("SubscriberMW::register - Failed to send registration request to Discovery service after retries.")

        except Exception as e:
            self.logger.error(f"SubscriberMW::register - Error: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise e

    def _send_discovery_request(self, disc_req):
        for attempt in range(ZMQ_REQ_RETRIES):
            try:
                self.req.send(disc_req.SerializeToString())
                self.logger.debug(f"Sent Discovery request (type {disc_req.msg_type}), attempt {attempt+1}")
                return True
            except zmq.error.Again as e:
                self.logger.warning(f"Discovery request send timed out (attempt {attempt+1}/{ZMQ_REQ_RETRIES}): {e}")
                time.sleep(0.5 * (attempt + 1))
            except Exception as e:
                self.logger.error(f"Error sending Discovery request (attempt {attempt+1}): {str(e)}")
                self.logger.error(traceback.format_exc())
                time.sleep(1 * (attempt + 1))
        return False

    def register_in_zookeeper(self, name):
        if not self.zk:
            self.logger.warning("SubscriberMW::register_in_zookeeper - No ZooKeeper client available")
            return False

        try:
            sub_path = self.zk_paths["subscribers"]
            self.zk.ensure_path(sub_path)

            sub_node = f"{sub_path}/{name}"

            node_data = ",".join(sorted(list(self.current_topics)))

            try:
                if self.zk.exists(sub_node):
                    self.zk.delete(sub_node)
                    self.logger.debug(f"Deleted existing ZK subscriber node: {sub_node}")
            except Exception as del_e:
                self.logger.warning(f"Error deleting existing ZK subscriber node {sub_node}: {del_e}")

            self.zk.create(sub_node, node_data.encode(), ephemeral=True, makepath=True)
            self.logger.info(f"SubscriberMW::register_in_zookeeper - Registered in ZooKeeper at {sub_node} with topics: {node_data}")
            return True

        except Exception as e:
            self.logger.error(f"SubscriberMW::register_in_zookeeper - Failed: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def lookup_publishers(self, topics):
        try:
            if isinstance(topics, str):
                topics = [topics]
            elif not isinstance(topics, (list, set)):
                raise TypeError("Topics must be a string, list, or set")

            topics_to_lookup = list(topics)
            self.logger.info(f"SubscriberMW::lookup_publishers - Looking up topics: {topics_to_lookup}")

            self.current_topics.update(topics_to_lookup)

            lookup_req = discovery_pb2.LookupPubByTopicReq()
            lookup_req.topiclist.extend(topics_to_lookup)

            disc_req = discovery_pb2.DiscoveryReq(
                msg_type=discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC,
                lookup_req=lookup_req
            )

            success = self._send_discovery_request(disc_req)
            if not success:
                self.logger.error("SubscriberMW::lookup_publishers - Failed to send lookup request to Discovery service after retries.")
                return False
            return True

        except Exception as e:
            self.logger.error(f"Error in lookup_publishers: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise e

    def lookup_broker_for_topic(self, topic):
        if not self.zk:
            self.logger.warning("SubscriberMW::lookup_broker_for_topic - No ZooKeeper client available")
            return None

        try:
            broker_base_path = self.zk_paths["brokers"]
            self.logger.info(f"SubscriberMW::lookup_broker_for_topic - Looking for broker for topic '{topic}' under {broker_base_path}")

            if not self.zk.exists(broker_base_path):
                self.logger.warning(f"Broker base path {broker_base_path} does not exist in ZooKeeper")
                return None

            target_group_name = None
            if topic in self.topics_to_groups:
                group_id = self.topics_to_groups[topic]
                target_group_name = f"group{group_id}"
                self.logger.debug(f"Topic '{topic}' is mapped to group '{target_group_name}'")

            try:
                all_groups = self.zk.get_children(broker_base_path)
                available_groups = [group for group in all_groups if group.startswith("group")]
            except Exception as e:
                self.logger.error(f"Error getting children from {broker_base_path}: {e}")
                return None

            if not available_groups:
                self.logger.warning(f"No broker groups found under {broker_base_path}")
                return None

            self.logger.debug(f"Available broker groups: {available_groups}")

            if target_group_name and target_group_name in available_groups:
                leader_info = self._get_group_leader_info(broker_base_path, target_group_name)
                if leader_info:
                    self.logger.info(f"Found leader for target group '{target_group_name}' for topic '{topic}': {leader_info}")
                    return leader_info
                else:
                    self.logger.warning(f"Target group '{target_group_name}' exists but no leader found. Will try other groups.")

            self.logger.debug(f"Searching for leader in other available groups: {available_groups}")
            for group_name in available_groups:
                if group_name == target_group_name:
                    continue
                leader_info = self._get_group_leader_info(broker_base_path, group_name)
                if leader_info:
                    self.logger.info(f"Using leader from group '{group_name}' for topic '{topic}': {leader_info}")
                    return leader_info

            self.logger.error(f"No broker leaders found in any available group for topic '{topic}'")
            return None

        except Exception as e:
            self.logger.error(f"Error looking up broker for topic {topic}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return None

    def _get_group_leader_info(self, broker_base_path, group_name):
        try:
            leader_path = f"{broker_base_path}/{group_name}/leader"
            if self.zk.exists(leader_path):
                data, znode_stat = self.zk.get(leader_path)
                if data:
                    broker_info = data.decode()
                    leader_addr_port = broker_info.split("|")[0]
                    return leader_addr_port
                else:
                    self.logger.warning(f"Leader node {leader_path} exists but has no data.")
                    return None
            else:
                return None
        except Exception as e:
            self.logger.error(f"Error getting leader info for group {group_name}: {e}")
            return None

    def subscribe(self, topics):
        try:
            if isinstance(topics, str):
                topics = [topics]
            elif not isinstance(topics, (list, set)):
                raise TypeError("Topics must be a string, list, or set")

            topics_to_subscribe = list(topics)
            self.logger.info(f"SubscriberMW::subscribe - Subscribing to topics: {topics_to_subscribe}")
            self.current_topics.update(topics_to_subscribe)

            endpoints_to_connect = set()
            if self.zk:
                for topic in topics_to_subscribe:
                    broker_addr_port = self.lookup_broker_for_topic(topic)
                    if broker_addr_port:
                        endpoints_to_connect.add(broker_addr_port)
                    else:
                        self.logger.warning(f"Could not find a broker for topic '{topic}' via ZooKeeper.")

            connected_count = 0
            for addr_port in endpoints_to_connect:
                if addr_port not in self.connected_publishers:
                    try:
                        connection_url = f"tcp://{addr_port}"
                        self.logger.info(f"Connecting SUB socket to broker at {connection_url}")
                        self.sub.connect(connection_url)
                        self.connected_publishers.add(addr_port)
                        connected_count += 1
                    except Exception as e:
                        self.logger.error(f"Failed to connect SUB socket to {connection_url}: {e}")
                else:
                    self.logger.debug(f"Already connected to {addr_port}")

            for topic in topics_to_subscribe:
                try:
                    topic_bytes = topic.encode('utf-8')
                    self.logger.debug(f"Applying SUB filter for topic: {topic}")
                    self.sub.setsockopt(zmq.SUBSCRIBE, topic_bytes)
                except Exception as e:
                    self.logger.error(f"Failed to set SUB filter for topic '{topic}': {e}")

            self.logger.info(f"Connected to {connected_count} new endpoints. Applied filters for {len(topics_to_subscribe)} topics.")
            if self.zk and hasattr(self.upcall_obj, 'name'):
                self.register_in_zookeeper(self.upcall_obj.name)
            return True

        except Exception as e:
            self.logger.error(f"Error in subscribe method: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def unsubscribe(self, topics):
        try:
            if isinstance(topics, str):
                topics = [topics]
            elif not isinstance(topics, (list, set)):
                raise TypeError("Topics must be a string, list, or set")

            topics_to_unsubscribe = list(topics)
            self.logger.info(f"SubscriberMW::unsubscribe - Unsubscribing from topics: {topics_to_unsubscribe}")

            for topic in topics_to_unsubscribe:
                try:
                    topic_bytes = topic.encode('utf-8')
                    self.logger.debug(f"Removing SUB filter for topic: {topic}")
                    self.sub.setsockopt(zmq.UNSUBSCRIBE, topic_bytes)
                    if topic in self.current_topics:
                        self.current_topics.remove(topic)
                except Exception as e:
                    self.logger.error(f"Failed to remove SUB filter for topic '{topic}': {e}")

            self.logger.info(f"Removed filters for {len(topics_to_unsubscribe)} topics.")
            if self.zk and hasattr(self.upcall_obj, 'name'):
                self.register_in_zookeeper(self.upcall_obj.name)
            return True

        except Exception as e:
            self.logger.error(f"Error in unsubscribe method: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False

    def handle_subscription(self):
        try:
            message = self.sub.recv_string()
            time_received = time.time()

            try:
                topic, time_sent_str, deadline_str, content = message.split(":", 3)
                time_sent = float(time_sent_str)
                deadline = float(deadline_str)
                latency = time_received - time_sent

                # classify msg with latency>0.5 as history msg
                is_replay = latency > 0.5
                tag = "[REPLAY]" if is_replay else "[LIVE]"
                # Deadline violation check
                missed_deadline = time_received > deadline
                deadline_tag = "[DEADLINE-MISS]" if missed_deadline else "[ONTIME]"

                # Combined log
                self.logger.info(f"{tag} {deadline_tag} Message on topic '{topic}' with latency {latency:.3f}s: {content[:50]}")

            except ValueError as e:
                self.logger.error(f"Could not parse received message: '{message[:100]}...' - Error: {e}")
                return

            if self.upcall_obj:
                self.upcall_obj.process_message(topic, content, latency)

            self._save_latency_data(time_received, topic, latency, content)

        except zmq.error.Again:
            self.logger.warning("SubscriberMW::handle_subscription - recv timed out unexpectedly.")
        except Exception as e:
            self.logger.error(f"Error handling subscription: {e}")
            self.logger.error(traceback.format_exc())

    def _save_latency_data(self, timestamp, topic, latency, content):
        try:
            subscriber_name = "Unknown"
            if self.upcall_obj and hasattr(self.upcall_obj, 'name'):
                subscriber_name = self.upcall_obj.name

            csv_filename = f"subscriber_data_{subscriber_name}.csv"
            file_exists = os.path.exists(csv_filename)

            with open(csv_filename, mode="a", newline="") as csv_file:
                fieldnames = ["timestamp", "subscriber_name", "topic", "latency", "content_preview"]
                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                if not file_exists or os.path.getsize(csv_filename) == 0:
                    csv_writer.writeheader()

                csv_writer.writerow({
                    "timestamp": timestamp,
                    "subscriber_name": subscriber_name,
                    "topic": topic,
                    "latency": f"{latency:.6f}",
                    "content_preview": content[:50]
                })

        except Exception as e:
            self.logger.error(f"Error saving latency data to CSV: {e}")

    def disable_event_loop(self):
        self.logger.info("SubscriberMW::disable_event_loop - Stopping event loop")
        self.handle_events = False

    def cleanup(self):
        try:
            self.logger.info("SubscriberMW::cleanup - Cleaning up resources")

            self.disable_event_loop()

            if self.sub:
                self.logger.debug("Closing SUB socket")
                if self.poller and self.sub in self.poller.sockets:
                    try:
                        self.poller.unregister(self.sub)
                    except KeyError:
                        pass
                self.sub.close()
                self.sub = None

            if self.req:
                self.logger.debug("Closing REQ socket")
                if self.poller and self.req in self.poller.sockets:
                    try:
                        self.poller.unregister(self.req)
                    except KeyError:
                        pass
                self.req.close()
                self.req = None

            if self.context:
                self.logger.debug("Terminating ZMQ context")
                self.context.term()
                self.context = None

            self.logger.info("SubscriberMW::cleanup - ZMQ cleanup complete")

        except Exception as e:
            self.logger.error(f"SubscriberMW::cleanup - Error: {str(e)}")
            self.logger.error(traceback.format_exc())
