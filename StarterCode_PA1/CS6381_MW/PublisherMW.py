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

    def configure(self, discovery_addr, pub_port, pub_addr):
        ''' Configure middleware '''
        try:
            self.logger.info("PublisherMW::configure")

            self.discovery_addr = discovery_addr
            self.pub_port = pub_port
            self.pub_addr = pub_addr

            context = zmq.Context()
            self.poller = zmq.Poller()
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.PUB)

            self.poller.register(self.req, zmq.POLLIN)
            self.connect_to_discovery(self.discovery_addr)

            bind_str = f"tcp://*:{self.pub_port}"
            self.pub.bind(bind_str)
            self.logger.info(f"PublisherMW::configure - Publishing on {bind_str}")

        except Exception as e:
            raise e

    def connect_to_discovery(self, discovery_addr):
      ''' Connects to Discovery '''
      self.logger.info(f"Connecting to Discovery at {discovery_addr}")

      # Only disconnect if the socket was already connected before
      if self.discovery_addr:
          self.logger.info(f"Disconnecting from previous Discovery at {self.discovery_addr}")
          try:
              self.req.disconnect(f"tcp://{self.discovery_addr}")
          except zmq.error.ZMQError as e:
              self.logger.warning(f"Failed to disconnect from {self.discovery_addr}: {e}")

      # Connect to the new Discovery service
      self.req.connect(f"tcp://{discovery_addr}")
      self.discovery_addr = discovery_addr

    def event_loop(self, timeout=None):
        try:
            self.logger.info("PublisherMW::event_loop - running")
            while True:
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
                return self.upcall_obj.register_response(disc_resp.register_resp)
            else:
                raise ValueError("Unrecognized response message")
        except Exception as e:
            raise e

    def register(self, name, topiclist):
        ''' Register Publisher '''
        try:
            self.logger.info("PublisherMW::register")
            reg_info = discovery_pb2.RegistrantInfo(id=name, addr=self.pub_addr, port=self.pub_port)
            register_req = discovery_pb2.RegisterReq(role=discovery_pb2.ROLE_PUBLISHER, info=reg_info)
            register_req.topiclist.extend(topiclist)

            disc_req = discovery_pb2.DiscoveryReq(msg_type=discovery_pb2.TYPE_REGISTER, register_req=register_req)
            self.req.send(disc_req.SerializeToString())

        except Exception as e:
            raise e

    def disseminate(self, id, topic, data):
        ''' Publish topic updates '''
        try:
            send_str = f"{topic}:{data}"
            self.pub.send(send_str.encode("utf-8"))
            self.logger.debug(f"PublisherMW::disseminate - {send_str}")
        except Exception as e:
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj
