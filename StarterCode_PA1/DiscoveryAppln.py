import sys
import time
import argparse
import logging
import configparser
import threading
import subprocess
import socket
from kazoo.recipe.lock import Lock
import signal
import atexit
from kazoo.client import KazooClient  # ZooKeeper client
from kazoo.exceptions import NodeExistsError
from CS6381_MW.DiscoveryMW import DiscoveryMW
from CS6381_MW import discovery_pb2
import hashlib

# Helper function to hash topic to get group id
def consistent_hash(text):
    # Compute the MD5 hash of the text.
    md5_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
    # Convert the hexadecimal digest to an integer.
    return int(md5_hash, 16)

# Helper function for getting new port for quorum spawning
def get_free_port():
    """Bind a temporary socket to port 0 and return the OS-assigned free port."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))  # Let OS choose a free port
    port = s.getsockname()[1]
    s.close()
    return port

##################################
#       Discovery Application Class
##################################
class DiscoveryAppln:
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None          # Middleware handle
        self.zk = None              # ZooKeeper client
        self.registry = {"publishers": {}, "subscribers": {}, "brokers": {}}
        self.is_primary = False     # True if this instance becomes leader
        self.leader_path = "/discovery/leader"
        # Replica-related path for quorum
        self.replicas_path = "/discovery/replicas"
        # Lease settings (in seconds)
        self.lease_duration = 30          # How long each lease is valid
        self.lease_renew_interval = 10    # How frequently to renew the lease
        self.max_leader_duration = 120    # Maximum time this instance can remain primary
        self.leader_start_time = None     # Time when this instance became leader
        self.lease_thread = None          # Thread for lease renewal
        self.args = None                # Will store command-line args
        self.bootstrap_complete = False # Flag to indicate bootstrapping is complete
        # NEW: number of virtual groups to simulate for load balancing.
        self.num_virtual_groups = 3

    ########################################
    # Quorum Check Helper
    ########################################
    def quorum_met(self):
        """Return True if at least 3 Discovery replicas are registered."""
        try:
            replicas = self.zk.get_children(self.replicas_path)
            self.logger.info(f"Quorum check: {len(replicas)} replicas present.")
            return len(replicas) >= 3
        except Exception as e:
            self.logger.error(f"Error checking quorum: {str(e)}")
            return False

    ########################################
    # Spawn a New Replica
    ########################################
    def spawn_replica(self):
        """Attempt to spawn a new Discovery replica using a global lock to ensure only one spawns."""
        self.logger.info("Attempting to spawn a new Discovery replica to restore quorum.")
        
        spawn_lock_path = "/discovery/spawn_lock"
        
        # First, check for a stale lock
        if self.zk.exists(f"{spawn_lock_path}/lock"):
            try:
                data, stat = self.zk.get(f"{spawn_lock_path}/lock")
                lock_age = time.time() - (stat.ctime / 1000)  # ZK time is in ms
                if lock_age > 60:
                    self.logger.warning(f"Detected stale lock (age: {lock_age:.1f}s). Attempting to clean up.")
                    try:
                        self.zk.delete(f"{spawn_lock_path}/lock")
                        self.logger.info("Successfully cleaned up stale lock node.")
                        time.sleep(1)
                    except Exception as e:
                        self.logger.error(f"Failed to clean up stale lock: {str(e)}")
            except Exception as e:
                self.logger.error(f"Error checking lock age: {str(e)}")
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                spawn_lock = Lock(self.zk, spawn_lock_path)
                timeout = 5 if attempt == 0 else 10
                self.logger.info(f"Attempting to acquire spawn lock (attempt {attempt+1}/{max_retries}, timeout: {timeout}s)")
                if spawn_lock.acquire(timeout=timeout):
                    try:
                        replicas = self.zk.get_children(self.replicas_path)
                        if len(replicas) >= 3:
                            self.logger.info("Quorum restored while waiting for lock; no need to spawn.")
                            return
                        
                        free_port = get_free_port()
                        self.logger.info(f"Spawning new replica on port {free_port}")
                        
                        cmd = ["gnome-terminal", "--", "bash", "-c"]
                        spawn_cmd = f"python3 {sys.argv[0]} -p {free_port} -a {self.args.addr} -z {self.args.zookeeper} -l {self.args.loglevel}"
                        if hasattr(self.args, 'config') and self.args.config:
                            spawn_cmd += f" -c {self.args.config}"
                        spawn_cmd += "; exec bash"
                        cmd.append(spawn_cmd)
                        
                        subprocess.Popen(cmd)
                        self.logger.info(f"Spawned new Discovery replica with command: {spawn_cmd}")
                        return
                    except Exception as e:
                        self.logger.error(f"Error while spawn lock was held: {str(e)}")
                    finally:
                        try:
                            spawn_lock.release()
                            self.logger.info("Spawn lock released successfully")
                        except Exception as e:
                            self.logger.error(f"Error releasing spawn lock: {str(e)}")
                else:
                    self.logger.warning(f"Could not acquire spawn lock on attempt {attempt+1}")
            except Exception as e:
                self.logger.error(f"Exception during lock handling on attempt {attempt+1}: {str(e)}")
            if attempt < max_retries - 1:
                backoff = 3 * (attempt + 1)
                self.logger.info(f"Waiting {backoff}s before retry...")
                time.sleep(backoff)
        self.logger.error("Failed to spawn a new replica after multiple attempts")

    ########################################
    # Wait for Bootstrap Completion
    ########################################
    def wait_for_bootstrap(self):
        """Wait until at least 3 replica nodes are registered, then mark bootstrap as complete."""
        self.logger.info("Waiting for bootstrap: expecting at least 3 replicas before enabling auto-spawn...")
        while True:
            try:
                replicas = self.zk.get_children(self.replicas_path)
                self.logger.info(f"Bootstrap check: {len(replicas)} replicas present.")
                if len(replicas) >= 3:
                    self.logger.info("Bootstrap complete: quorum achieved.")
                    break
            except Exception as e:
                self.logger.error(f"Error during bootstrap wait: {str(e)}")
            time.sleep(2)
        self.bootstrap_complete = True

    ########################################
    # Configure Discovery Application
    ########################################
    def configure(self, args):
        """ Configure the Discovery Application with warm-passive replication, lease, and quorum support """
        try:
            self.args = args  # Save args for future use (e.g., spawning replicas)
            self.logger.info("DiscoveryAppln::configure")
            config_obj = configparser.ConfigParser()
            config_obj.read(args.config)

            self.dissemination_strategy = config_obj.get("Dissemination", "Strategy", fallback="Direct")
            self.logger.info(f"Dissemination strategy set to: {self.dissemination_strategy}")

            # Connect to ZooKeeper
            self.logger.info(f"Connecting to ZooKeeper at {args.zookeeper}")
            self.zk = KazooClient(hosts=args.zookeeper)
            self.zk.start(timeout=10)

            # Ensure base paths exist
            self.logger.info("Ensuring /discovery path exists")
            self.zk.ensure_path("/discovery")
            self.logger.info(f"Ensuring {self.replicas_path} path exists")
            self.zk.ensure_path(self.replicas_path)

            # Register this instance as a replica
            discovery_address = f"{args.addr}:{args.port}"
            replica_node = f"{self.replicas_path}/{discovery_address}"
            try:
                self.zk.create(replica_node, discovery_address.encode(), ephemeral=True)
                self.logger.info(f"Registered replica node: {replica_node}")
            except NodeExistsError:
                self.zk.delete(replica_node)
                self.zk.create(replica_node, discovery_address.encode(), ephemeral=True)
                self.logger.info(f"Updated replica node: {replica_node}")

            # Do not spawn new replicas until bootstrapping is complete.
            @self.zk.ChildrenWatch(self.replicas_path)
            def watch_replicas(children):
                num = len(children)
                self.logger.info(f"Replica watch: {num} replicas present.")
                if self.bootstrap_complete and num < 3:
                    self.logger.warning("Quorum not met: fewer than 3 replicas active.")
                    self.spawn_replica()

            # Wait for bootstrap
            self.wait_for_bootstrap()

            # Attempt to become the leader by creating the ephemeral leader node.
            lease_expiry = time.time() + self.lease_duration
            leader_data = f"{discovery_address}|{lease_expiry}"
            try:
                self.zk.create(self.leader_path, leader_data.encode(), ephemeral=True)
                self.is_primary = True
                self.leader_start_time = time.time()
                self.logger.info(f"Instance {discovery_address} became primary with lease expiring at {lease_expiry}.")
                self.start_lease_renewal(discovery_address)
            except NodeExistsError:
                self.is_primary = False
                self.logger.info("A leader already exists. Running as backup.")
                @self.zk.DataWatch(self.leader_path)
                def watch_leader(data, stat, event):
                    if event is not None and event.type == "DELETED":
                        self.logger.info("Leader znode deleted. Attempting to become primary...")
                        time.sleep(1)
                        try:
                            self.zk.ensure_path("/discovery")
                            if not self.zk.exists(self.leader_path):
                                new_expiry = time.time() + self.lease_duration
                                new_data = f"{discovery_address}|{new_expiry}"
                                self.zk.create(self.leader_path, new_data.encode(), ephemeral=True)
                                self.is_primary = True
                                self.leader_start_time = time.time()
                                self.logger.info(f"This instance has now become the primary with lease expiring at {new_expiry}!")
                                self.start_lease_renewal(discovery_address)
                            else:
                                self.logger.info("Leader node already recreated by another instance.")
                                self.is_primary = False
                        except NodeExistsError:
                            self.logger.info("Another instance became primary while we were trying.")
                            self.is_primary = False
                        except Exception as e:
                            self.logger.error(f"Error during leadership transition: {str(e)}")
                            self.is_primary = False

            if self.zk.exists(self.leader_path):
                data, _ = self.zk.get(self.leader_path)
                self.logger.info(f"Current leader in ZooKeeper: {data.decode()}")
            else:
                self.logger.error("Leader node does not exist after attempted creation.")

            # Initialize the middleware
            self.logger.debug("DiscoveryAppln::configure - initializing middleware")
            self.mw_obj = DiscoveryMW(self.logger, self.zk)
            self.mw_obj.configure(args)
            self.mw_obj.set_upcall_handle(self)

            self.logger.info("DiscoveryAppln::configure - configuration complete")
            self.dump_zk_nodes()

        except Exception as e:
            self.logger.error(f"DiscoveryAppln::configure - Exception: {str(e)}")
            raise e

    ########################################
    # Lease Renewal Thread
    ########################################
    def start_lease_renewal(self, discovery_address):
        """Start a background thread to renew the lease periodically, but relinquish leadership after max duration."""
        if self.lease_thread and self.lease_thread.is_alive():
            self.logger.info("Lease renewal thread already running.")
            return

        def renew_lease():
            while self.is_primary:
                if time.time() - self.leader_start_time >= self.max_leader_duration:
                    self.logger.info("Max leader duration reached. Relinquishing primary role.")
                    try:
                        if self.zk.exists(self.leader_path):
                            self.zk.delete(self.leader_path)
                            self.logger.info("Deleted leader znode to relinquish leadership.")
                            time.sleep(5)
                    except Exception as e:
                        self.logger.error(f"Error deleting leader znode: {str(e)}")
                    self.is_primary = False
                    break

                time.sleep(self.lease_renew_interval)
                new_expiry = time.time() + self.lease_duration
                new_data = f"{discovery_address}|{new_expiry}"
                try:
                    self.zk.set(self.leader_path, new_data.encode())
                    self.logger.info(f"Lease renewed; new expiry time: {new_expiry}")
                except Exception as e:
                    self.logger.error(f"Failed to renew lease: {str(e)}")
                    break

        import threading
        self.lease_thread = threading.Thread(target=renew_lease, daemon=True)
        self.lease_thread.start()

    ########################################
    # Handle Registration Request with Virtual Group Mapping
    ########################################
    def register(self, register_req):
        """Handle publisher/subscriber registration, with virtual group assignment for publishers."""
        if not self.quorum_met():
            self.logger.warning("Quorum not met. Rejecting registration.")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.MsgTypes.TYPE_REGISTER
            response.register_resp.status = discovery_pb2.Status.STATUS_CHECK_AGAIN
            response.register_resp.reason = "Quorum not met; please wait until all replicas are active."
            return response

        role_map = {
            discovery_pb2.Role.ROLE_PUBLISHER: "Publisher",
            discovery_pb2.Role.ROLE_SUBSCRIBER: "Subscriber",
            discovery_pb2.Role.ROLE_BOTH: "Broker"
        }
        role = role_map.get(register_req.role, "Unknown")
        entity_id = register_req.info.id
        self.logger.info(f"Registering {role}: {entity_id}")

        if not self.is_primary:
            self.logger.warning("Not primary; cannot process registration. Inform client to contact the leader.")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.MsgTypes.TYPE_REGISTER
            response.register_resp.status = discovery_pb2.Status.STATUS_FAILURE
            if self.leader_path and self.zk.exists(self.leader_path):
                try:
                    leader_data, _ = self.zk.get(self.leader_path)
                    leader_addr = leader_data.decode().split('|')[0]  # Extract address from lease info
                    response.register_resp.reason = f"Not primary. Please contact primary at {leader_addr}"
                except Exception as e:
                    self.logger.error(f"Error getting leader info: {str(e)}")
                    response.register_resp.reason = "Not primary. Unable to determine primary; please retry."
            else:
                response.register_resp.reason = "Not primary. No primary available; please retry later."
            return response

        # For publishers, assign topics to virtual groups using consistent hashing
        topic_group_info = {}
        if role == "Publisher" and register_req.topiclist:
            # Assign each topic to a virtual group (deterministically)
            for topic in register_req.topiclist:
                group = consistent_hash(topic) % self.num_virtual_groups
                topic_group_info[topic] = group
                self.logger.info(f"Assigned topic '{topic}' to virtual group {group}")
            
            # Format as topic1:group1,topic2:group2
            topic_mapping = ",".join([f"{t}:{g}" for t, g in topic_group_info.items()])
            
            # Store mapping for broker lookup purposes
            entity_id_with_group = f"{entity_id}:grp={list(topic_group_info.values())[0]}"
            self.logger.info(f"Publisher ID with group: {entity_id_with_group}")
            
            # Format entity data with virtual group mapping
            entity_data = f"{register_req.info.addr}:{register_req.info.port}|{topic_mapping}"
        else:
            topic_mapping = ""
            entity_id_with_group = entity_id
            entity_data = f"{register_req.info.addr}:{register_req.info.port}"
            # Log that no virtual group assignment is performed for subscribers or brokers.
            self.logger.info(f"{role} {entity_id} registered without virtual group assignment.")

        # Determine the ZooKeeper path category.
        if role == "Broker":
            category = "brokers"
        else:
            category = role.lower() + "s"  # "publishers" or "subscribers"
        entity_path = f"/{category}/{entity_id}"

        try:
            self.zk.ensure_path(f"/{category}")
            if self.zk.exists(entity_path):
                self.logger.info(f"Node {entity_path} already exists, updating it")
                self.zk.delete(entity_path)
            self.zk.create(entity_path, entity_data.encode(), ephemeral=True)
            self.logger.info(f"Created/updated ZooKeeper node {entity_path} with data: {entity_data}")
            
            # For brokers, also register in the replicas path for quorum management.
            if role == "Broker" and category == "brokers":
                broker_replica_path = f"/brokers/replicas/{entity_id}"
                self.zk.ensure_path("/brokers/replicas")
                if self.zk.exists(broker_replica_path):
                    self.zk.delete(broker_replica_path)
                self.zk.create(broker_replica_path, entity_data.encode(), ephemeral=True)
                self.logger.info(f"Registered broker in replica path: {broker_replica_path}")
        except Exception as e:
            self.logger.error(f"Failed to register {role} in ZooKeeper: {str(e)}")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.MsgTypes.TYPE_REGISTER
            response.register_resp.status = discovery_pb2.Status.STATUS_FAILURE
            response.register_resp.reason = f"Failed to register in ZooKeeper: {str(e)}"
            return response

        # Update the in-memory registry.
        entry = {
            "addr": register_req.info.addr,
            "port": register_req.info.port,
            "topics": list(register_req.topiclist)
        }
        if role == "Publisher":
            entry["group_mapping"] = topic_group_info
        self.registry[category][entity_id if role != "Publisher" else entity_id_with_group] = entry

        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.MsgTypes.TYPE_REGISTER
        response.register_resp.status = discovery_pb2.Status.STATUS_SUCCESS
        
        # If this is a publisher, include group mapping in the reason field
        if role == "Publisher" and topic_mapping:
            response.register_resp.reason = topic_mapping
            self.logger.info(f"Sending topic mapping in response reason: {topic_mapping}")
            
        return response

    ########################################
    # Handle Lookup Request with Filtering by Virtual Group
    ########################################
    def lookup(self, lookup_req):
        """Handle subscriber lookup request and properly handle broker groups in viaBroker mode."""
        self.logger.info(f"Lookup request for topics: {lookup_req.topiclist}")

        if not self.quorum_met():
            self.logger.warning("Quorum not met. Rejecting lookup request.")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            response.lookup_resp.reason = "Quorum not met; please wait until all replicas are active."
            return response

        if not self.is_primary:
            self.logger.warning("Not primary; lookup request cannot be processed here.")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            return response

        matched_nodes = []
        if self.dissemination_strategy == "Direct":
            # Direct mode: Find publishers that handle the requested topics
            self._lookup_direct_publishers(lookup_req.topiclist, matched_nodes)
        else:
            # ViaBroker mode: Find appropriate brokers, accounting for group structure
            self._lookup_brokers_with_group_structure(lookup_req.topiclist, matched_nodes)

        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
        response.lookup_resp.publishers.extend(matched_nodes)
        self.logger.info(f"Returning {len(matched_nodes)} matches for topics {lookup_req.topiclist}")
        return response
        
    def _lookup_direct_publishers(self, topics, matched_nodes):
        """Find publishers that match the requested topics."""
        path = "/publishers"
        self.logger.info("Direct mode: Looking up publishers")
        
        # For each topic requested, compute expected virtual group
        topic_to_group = {}
        for topic in topics:
            expected_group = consistent_hash(topic) % self.num_virtual_groups
            topic_to_group[topic] = expected_group
            self.logger.info(f"Subscriber lookup: For topic '{topic}' expected virtual group is {expected_group}")

        try:
            if self.zk.exists(path):
                nodes = self.zk.get_children(path)
                self.logger.info(f"Found {len(nodes)} nodes in ZooKeeper under {path}")
                
                for node_id in nodes:
                    if node_id in ["leader", "replicas", "spawn_lock"]:
                        self.logger.warning(f"Skipping system node '{node_id}' in {path}")
                        continue
                        
                    try:
                        data, _ = self.zk.get(f"{path}/{node_id}")
                        node_data = data.decode()
                        
                        if "|" in node_data:
                            # Data format: "addr:port|topic1:group1,topic2:group2,..."
                            addr_port, mapping_str = node_data.split("|")
                            addr_parts = addr_port.split(":")
                            if len(addr_parts) != 2:
                                self.logger.error(f"Invalid address format in node {node_id}: {addr_port}")
                                continue
                                
                            addr = addr_parts[0]
                            port = int(addr_parts[1])
                            
                            # Build a mapping dictionary
                            mapping = {}
                            for pair in mapping_str.split(","):
                                if ":" in pair:
                                    topic_name, grp = pair.split(":")
                                    mapping[topic_name] = int(grp)
                                    
                            # Match publisher to topics based on group
                            for topic in topics:
                                expected_group = topic_to_group[topic]
                                if topic in mapping and mapping[topic] == expected_group:
                                    node_info = discovery_pb2.RegistrantInfo(
                                        id=f"{node_id}:grp={mapping[topic]}",
                                        addr=addr,
                                        port=port
                                    )
                                    matched_nodes.append(node_info)
                                    self.logger.info(f"Added publisher {node_id} (group {mapping[topic]}) for topic '{topic}'")
                                    break  # Only add publisher once even if it serves multiple topics
                                    
                        else:
                            # If no virtual group mapping is present, fall back to original format
                            if ":" in node_data:
                                addr, port = node_data.split(":")
                                node_info = discovery_pb2.RegistrantInfo(id=node_id, addr=addr, port=int(port))
                                matched_nodes.append(node_info)
                                self.logger.info(f"Added {node_id} at {addr}:{port} to response")
                            else:
                                self.logger.error(f"Node {node_id} has invalid data format: {node_data}")
                                
                    except Exception as e:
                        self.logger.error(f"Error retrieving node {node_id}: {str(e)}")
            else:
                self.logger.warning(f"Path {path} does not exist in ZooKeeper")
                
        except Exception as e:
            self.logger.error(f"Error during publisher lookup: {str(e)}")
            
    def _lookup_brokers_with_group_structure(self, topics, matched_nodes):
        """Find appropriate brokers using the group structure, with fallback to flat structure."""
        broker_base_path = "/brokers"
        self.logger.info("ViaBroker mode: Looking up brokers in group structure")
        self.logger.info(f"DEBUG: Broker base path is {broker_base_path}")

        # For each topic requested, compute expected virtual group
        topic_to_group = {}
        for topic in topics:
            expected_group = consistent_hash(topic) % self.num_virtual_groups
            topic_to_group[topic] = expected_group
            self.logger.info(f"Broker lookup: Topic '{topic}' maps to virtual group {expected_group}")

        try:
            # Check if base path exists
            if not self.zk.exists(broker_base_path):
                self.logger.warning(f"DEBUG: Broker path {broker_base_path} does not exist in ZooKeeper. Cannot find brokers.")
                return

            # Get all children under /brokers
            children = self.zk.get_children(broker_base_path)
            if not children:
                self.logger.warning(f"DEBUG: No children found under {broker_base_path}")
                return

            self.logger.info(f"DEBUG: Found children under /brokers: {children}")

            # Separate groups from potential flat broker nodes
            groups = [child for child in children if child.startswith("group")]
            flat_brokers = [child for child in children if not child.startswith("group") and child not in ["leader", "replicas", "spawn_lock"]]

            self.logger.info(f"DEBUG: Identified groups: {groups}")
            self.logger.info(f"DEBUG: Identified potential flat brokers: {flat_brokers}")

            # Dump entire ZooKeeper structure for debugging
            self.logger.info("DEBUG: Current ZooKeeper broker structure:")
            self.dump_zk_path(broker_base_path)

            # --- Primary Strategy: Look for group leaders ---
            self.logger.info("DEBUG: Attempting lookup via group structure...")
            found_in_groups = False
            for topic in topics:
                expected_group = topic_to_group[topic]
                group_name = f"group{expected_group}"
                self.logger.info(f"DEBUG: Looking for group {group_name} for topic {topic}")

                # Check if this group exists
                if group_name in groups:
                    self.logger.info(f"DEBUG: Found matching group {group_name}")
                    # Look for leader in this group
                    leader_path = f"{broker_base_path}/{group_name}/leader"
                    self.logger.info(f"DEBUG: Checking for leader at {leader_path}")

                    if self.zk.exists(leader_path):
                        self.logger.info(f"DEBUG: Leader exists at {leader_path}")
                        data, _ = self.zk.get(leader_path)
                        broker_info = data.decode()
                        self.logger.info(f"DEBUG: Leader data: {broker_info}")

                        # Handle format with lease expiry
                        if "|" in broker_info:
                            broker_info = broker_info.split("|")[0]

                        if ":" in broker_info:
                            addr, port_str = broker_info.split(":")
                            try:
                                port = int(port_str)
                                # Create broker info with the group ID attached
                                node_info = discovery_pb2.RegistrantInfo(
                                    id=f"broker_{group_name}:grp={expected_group}",
                                    addr=addr,
                                    port=port
                                )

                                # Check if we've already added this broker
                                already_added = any(node.addr == addr and node.port == port for node in matched_nodes)

                                if not already_added:
                                    matched_nodes.append(node_info)
                                    self.logger.info(f"DEBUG: Added broker {addr}:{port} from group {group_name} for topic '{topic}'")
                                    found_in_groups = True
                                else:
                                     self.logger.info(f"DEBUG: Broker {addr}:{port} from group {group_name} already added.")

                            except ValueError:
                                self.logger.error(f"DEBUG: Invalid port number in leader data: {port_str}")
                        else:
                            self.logger.error(f"DEBUG: Invalid format for leader data: {broker_info}")
                    else:
                        self.logger.info(f"DEBUG: No leader found at {leader_path}")
                else:
                    self.logger.info(f"DEBUG: Group {group_name} not found in available groups {groups}")

            # --- Fallback Strategy: Use flat broker nodes if no group leaders were found ---
            if not found_in_groups and flat_brokers:
                self.logger.info("DEBUG: No specific group leaders found or matched. Falling back to flat broker nodes.")
                for broker_id in flat_brokers:
                    broker_path = f"{broker_base_path}/{broker_id}"
                    try:
                        if self.zk.exists(broker_path):
                            data, _ = self.zk.get(broker_path)
                            broker_info = data.decode()
                            self.logger.info(f"DEBUG: Checking flat broker {broker_id} with data: {broker_info}")
                            if ":" in broker_info:
                                # Handle potential extra data like topic mapping
                                addr_port = broker_info.split("|")[0]
                                addr, port_str = addr_port.split(":")
                                try:
                                    port = int(port_str)
                                    node_info = discovery_pb2.RegistrantInfo(
                                        id=broker_id,
                                        addr=addr,
                                        port=port
                                    )
                                    # Check if already added
                                    already_added = any(node.addr == addr and node.port == port for node in matched_nodes)
                                    if not already_added:
                                        matched_nodes.append(node_info)
                                        self.logger.info(f"DEBUG: Added flat broker {broker_id} at {addr}:{port} as fallback")
                                        # In fallback, often adding one is enough, but let's add all found flat ones
                                except ValueError:
                                     self.logger.error(f"DEBUG: Invalid port number in flat broker data: {port_str}")
                            else:
                                self.logger.error(f"DEBUG: Invalid format for flat broker data: {broker_info}")
                    except Exception as e:
                        self.logger.error(f"DEBUG: Error processing flat broker {broker_id}: {str(e)}")

            # --- Final Fallback: Use *any* group leader if still nothing found ---
            if not matched_nodes and groups:
                 self.logger.info("DEBUG: Still no brokers found. Falling back to *any* available group leader.")
                 for group_name in groups:
                    leader_path = f"{broker_base_path}/{group_name}/leader"
                    self.logger.info(f"DEBUG: Checking final fallback leader at {leader_path}")
                    if self.zk.exists(leader_path):
                        self.logger.info(f"DEBUG: Final fallback leader exists at {leader_path}")
                        data, _ = self.zk.get(leader_path)
                        broker_info = data.decode()
                        self.logger.info(f"DEBUG: Final fallback leader data: {broker_info}")

                        if "|" in broker_info: broker_info = broker_info.split("|")[0]

                        if ":" in broker_info:
                            addr, port_str = broker_info.split(":")
                            try:
                                port = int(port_str)
                                node_info = discovery_pb2.RegistrantInfo(
                                    id=f"broker_{group_name}_fallback",
                                    addr=addr,
                                    port=port
                                )
                                # Check if already added
                                already_added = any(node.addr == addr and node.port == port for node in matched_nodes)
                                if not already_added:
                                    matched_nodes.append(node_info)
                                    self.logger.info(f"DEBUG: Added broker from group {group_name} as final fallback")
                                    break # Found one, that's enough for final fallback
                            except ValueError:
                                self.logger.error(f"DEBUG: Invalid port number in final fallback leader data: {port_str}")
                        else:
                            self.logger.error(f"DEBUG: Invalid format for final fallback leader data: {broker_info}")
                    else:
                        self.logger.info(f"DEBUG: No final fallback leader found at {leader_path}")


        except Exception as e:
            self.logger.error(f"DEBUG: Error during broker lookup: {str(e)}")
            # Log the stack trace for detailed debugging
            import traceback
            self.logger.error(traceback.format_exc())

        # Final check
        if not matched_nodes:
             self.logger.error("DEBUG: After all attempts, no brokers were found to return.")
            
    def dump_zk_path(self, path, level=0):
        """Debug method to dump a specific ZooKeeper path structure."""
        try:
            children = self.zk.get_children(path)
            for child in children:
                child_path = f"{path}/{child}"
                data = None
                try:
                    data_bytes, _ = self.zk.get(child_path)
                    data = data_bytes.decode() if data_bytes else None
                except Exception:
                    pass
                self.logger.info(f"{'  ' * level}|- {child} {f'({data})' if data else ''}")
                self.dump_zk_path(child_path, level + 1)
        except Exception as e:
            self.logger.error(f"Error dumping path {path}: {str(e)}")

    ########################################
    # Run the Discovery Service
    ########################################
    def run(self):
        """Run Discovery Service Event Loop."""
        self.logger.info("DiscoveryAppln::run - entering event loop")
        try:
            atexit.register(self.cleanup)
            self.mw_obj.event_loop()
        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt received, shutting down")
        except Exception as e:
            self.logger.error(f"Exception in event loop: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources and deregister from ZooKeeper."""
        try:
            self.logger.info("DiscoveryAppln::cleanup - Performing cleanup operations")
            self.is_primary = False
            if self.lease_thread and self.lease_thread.is_alive():
                self.lease_thread.join(timeout=2)
            if self.zk and self.zk.connected and self.leader_path:
                if self.zk.exists(self.leader_path):
                    try:
                        data, _ = self.zk.get(self.leader_path)
                        our_addr = f"{self.args.addr}:{self.args.port}" if self.args else None
                        if data and our_addr and our_addr in data.decode():
                            self.logger.info("Deleting our leader node from ZooKeeper")
                            self.zk.delete(self.leader_path)
                    except Exception as e:
                        self.logger.warning(f"Error deleting leader node: {e}")
            if self.zk:
                self.logger.info("Closing ZooKeeper connection")
                self.zk.stop()
                self.zk.close()
            if self.mw_obj and hasattr(self.mw_obj, 'cleanup'):
                self.mw_obj.cleanup()
            self.logger.info("Cleanup complete")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def dump_zk_nodes(self):
        """Debug method to dump ZooKeeper node structure."""
        self.logger.info("Current ZooKeeper nodes:")
        def print_tree(path, level=0):
            try:
                children = self.zk.get_children(path)
            except Exception:
                children = []
            for child in children:
                child_path = f"{path}/{child}" if path != "/" else f"/{child}"
                data = None
                try:
                    data_bytes, _ = self.zk.get(child_path)
                    data = data_bytes.decode() if data_bytes else None
                except Exception:
                    pass
                self.logger.info(f"{'  ' * level}|- {child} {f'({data})' if data else ''}")
                print_tree(child_path, level + 1)
        print_tree("/")

########################################
# Parse command line arguments
########################################
def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Discovery Service")
    parser.add_argument("-p", "--port", type=int, default=5555, help="Port number to run Discovery Service")
    parser.add_argument("-a", "--addr", default="localhost", help="IP address of Discovery Service")
    parser.add_argument("-z", "--zookeeper", default="127.0.0.1:2181", help="ZooKeeper address (default: 127.0.0.1:2181)")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file (default: config.ini)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[10,20,30,40,50], help="Logging level")
    return parser.parse_args()

########################################
# Main function
########################################
def main():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("DiscoveryAppln")
    args = parseCmdLineArgs()
    logger.setLevel(args.loglevel)
    app = DiscoveryAppln(logger)
    app.configure(args)
    
    # Register signal handlers
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down gracefully")
        if 'discovery' in locals() and discovery:
            discovery.cleanup()
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        app.run()
    except Exception as e:
        logger.error(f"Exception in main: {e}")
        if 'discovery' in locals() and discovery:
            discovery.cleanup()

if __name__ == "__main__":
    main()
