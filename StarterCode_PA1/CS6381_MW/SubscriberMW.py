import zmq
import logging
from CS6381_MW import discovery_pb2

class SubscriberMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None  # REQ socket for Discovery communication
        self.sub = None  # SUB socket for topic subscriptions
        self.poller = None  # Poller for async event handling
        self.upcall_obj = None  # Handle to application logic
        self.discovery_addr = None  # Discovery address from ZooKeeper
        self.connected_publishers = set()  # Track connected publishers

    def configure(self, discovery_addr):
        ''' Initialize the Subscriber Middleware '''
        try:
            self.logger.info("SubscriberMW::configure")

            self.discovery_addr = discovery_addr

            context = zmq.Context()
            self.poller = zmq.Poller()
            self.req = context.socket(zmq.REQ)  # Request socket for Discovery
            self.sub = context.socket(zmq.SUB)  # Subscriber socket for topics

            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            self.connect_to_discovery(self.discovery_addr)

        except Exception as e:
            raise e

    def set_upcall_handle(self, upcall_obj):
        """ Assigns an upcall handle to the application layer """
        self.upcall_obj = upcall_obj

    def connect_to_discovery(self, discovery_addr):
        ''' Connects to Discovery '''
        self.logger.info(f"Connecting to Discovery at {discovery_addr}")

        if self.req:
            try:
                self.req.disconnect(f"tcp://{self.discovery_addr}")
            except zmq.error.ZMQError:
                self.logger.warning(f"Failed to disconnect from {self.discovery_addr}")

        self.req = zmq.Context().socket(zmq.REQ)
        self.req.connect(f"tcp://{discovery_addr}")
        self.discovery_addr = discovery_addr

    def event_loop(self, timeout=None):
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

        except Exception as e:
            raise e

    def lookup_publishers(self, topics):
        """Lookup publishers for topics of interest"""
        try:
            self.logger.info("SubscriberMW::lookup_publishers")
            
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

    def subscribe_to_topics(self, publisher_addr, topics):
        """Connect to a publisher and subscribe to topics"""
        try:
            self.logger.info(f"SubscriberMW::subscribe_to_topics - {publisher_addr}, topics: {topics}")
            
            # Connect if not already connected
            if publisher_addr not in self.connected_publishers:
                self.sub.connect(publisher_addr)
                self.connected_publishers.add(publisher_addr)
                
            # Subscribe to each topic
            for topic in topics:
                self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
                self.logger.info(f"Subscribed to topic: {topic}")
                
        except Exception as e:
            self.logger.error(f"Failed to subscribe: {str(e)}")
            raise e

    def handle_subscription(self):
        """Process received publication"""
        try:
            message = self.sub.recv_string()
            # Split the message into topic and content
            topic, content = message.split(":", 1)
            self.logger.debug(f"Received: {topic} - {content}")
            return self.upcall_obj.process_message(topic, content)
        except Exception as e:
            self.logger.error(f"Error handling subscription: {str(e)}")
            raise e
