###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 

import sys
import time
import argparse
import logging
import signal
import subprocess
import os
import threading
import zmq
from kazoo.client import KazooClient
from CS6381_MW.BrokerMW import BrokerMW  
from CS6381_MW import discovery_pb2  


# Publishers -> Broker; Broker -> Subscribers

class BrokerAgent:
    """Agent that runs to spawn brokers on demand"""
    
    def __init__(self, logger, host, port, zk_conn_str):
        self.logger = logger
        self.host = host
        self.port = port
        self.zk_conn_str = zk_conn_str
        self.zk = None
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.running = False
        self.broker_processes = {}
        
    def start(self):
        """Start the broker agent service"""
        self.logger.info(f"Starting Broker Agent on {self.host}:{self.port}")
        
        # Connect to ZooKeeper
        self.zk = KazooClient(hosts=self.zk_conn_str)
        self.zk.start()
        
        # Register this agent in ZooKeeper
        self.register_with_zk()
        
        # Start listening for spawn requests
        self.socket.bind(f"tcp://*:{self.port}")
        self.running = True
        
        # Start event loop in a separate thread
        self.agent_thread = threading.Thread(target=self.event_loop, daemon=True)
        self.agent_thread.start()
        
        self.logger.info("BrokerAgent started successfully")
        return self.agent_thread
    
    def register_with_zk(self):
        """Register this agent with ZooKeeper"""
        agent_path = "/broker_agents"
        self.zk.ensure_path(agent_path)
        
        node_path = f"{agent_path}/{self.host}"
        agent_data = f"{self.host}:{self.port}"
        
        if self.zk.exists(node_path):
            self.zk.delete(node_path)
            
        self.zk.create(node_path, agent_data.encode(), ephemeral=True)
        self.logger.info(f"Registered agent in ZooKeeper at {node_path}")
    
    def event_loop(self):
        """Main event loop to process spawn requests"""
        while self.running:
            try:
                # Wait for requests with a timeout so we can check if we should terminate
                if self.socket.poll(1000) == 0:  # 1 second timeout
                    continue
                    
                # Receive spawn request
                message = self.socket.recv_json()
                self.logger.info(f"Received request: {message}")
                
                if message['action'] == 'spawn_broker':
                    result = self.spawn_broker(message)
                    self.socket.send_json(result)
                elif message['action'] == 'status':
                    result = {'status': 'ok', 'brokers': list(self.broker_processes.keys())}
                    self.socket.send_json(result)
                else:
                    self.socket.send_json({'status': 'error', 'message': 'Unknown action'})
                    
            except Exception as e:
                self.logger.error(f"Error in event loop: {str(e)}")
                try:
                    self.socket.send_json({'status': 'error', 'message': str(e)})
                except:
                    pass
    
    def spawn_broker(self, request):
        """Spawn a new broker process with the given parameters"""
        try:
            broker_name = request.get('name', f"broker_{int(time.time())}")
            broker_port = request.get('port', 5556)  # Default port
            
            # Path to broker application
            current_dir = os.path.dirname(os.path.abspath(__file__))
            broker_script = os.path.join(current_dir, "BrokerAppln.py")
            
            # Build command
            cmd = [
                "python3", 
                broker_script,
                "-n", broker_name,
                "-a", self.host,
                "-p", str(broker_port),
                "-z", self.zk_conn_str
            ]
            
            # Start broker process
            self.logger.info(f"Spawning broker: {' '.join(cmd)}")
            process = subprocess.Popen(cmd)
            self.broker_processes[broker_name] = {
                'pid': process.pid,
                'port': broker_port,
                'start_time': time.time()
            }
            
            return {
                'status': 'ok', 
                'broker_name': broker_name, 
                'pid': process.pid,
                'host': self.host,
                'port': broker_port
            }
            
        except Exception as e:
            self.logger.error(f"Error spawning broker: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def cleanup(self):
        """Clean up resources"""
        self.running = False
        
        # Close ZMQ socket
        if self.socket:
            self.socket.close()
        
        # Close ZooKeeper connection
        if self.zk:
            self.zk.stop()
            self.zk.close()


class BrokerAppln():
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None  
        self.name = None
        self.zk = None
        self.zk_path = "/brokers"
        self.agent = None  # Reference to broker agent if running in agent mode
        signal.signal(signal.SIGINT, self.signal_handler)
        
    def configure(self, args):
        self.logger.info("BrokerAppln::configure")
        self.name = args.name 

        # Check if we're running in agent mode
        if args.agent_mode:
            self.logger.info("BrokerAppln::configure - Starting in agent mode")
            self.start_agent(args)
            return
            
        # Otherwise continue with normal broker setup
        # Connect to ZooKeeper
        self.logger.info(f"BrokerAppln::configure - Connecting to ZooKeeper at {args.zookeeper}")
        self.zk = KazooClient(hosts=args.zookeeper)
        self.zk.start()
        self.logger.info("BrokerAppln::configure - Connected to ZooKeeper")

        # Ensure base paths exist
        self.zk.ensure_path(self.zk_path)
        
        # Initialize middleware - don't register ourselves in ZK here since the middleware will do it
        self.mw_obj = BrokerMW(self.logger, self.zk, False)  
        self.mw_obj.configure(args)
        
        self.logger.info("BrokerAppln::configure - completed")

    def start_agent(self, args):
        """Start this application in broker agent mode"""
        self.logger.info(f"BrokerAppln::start_agent - Starting broker agent on {args.addr}:{args.agent_port}")
        
        # Create and start the agent
        self.agent = BrokerAgent(
            self.logger, 
            args.addr, 
            args.agent_port, 
            args.zookeeper
        )
        self.agent_thread = self.agent.start()
        
    def driver(self):
        # If running in agent mode, just wait for the agent thread to complete
        if self.agent:
            self.logger.info("BrokerAppln::driver - Running in agent mode, waiting for agent thread")
            while True:
                time.sleep(1)  # Just keep the main thread alive
            return
        
        # Otherwise, start the broker event loop
        try:
            self.logger.info("BrokerAppln::driver - starting event loop")
            self.mw_obj.set_upcall_handle(self)
            
            # Main event loop
            while True:
                # Process any network events (with timeout)
                self.mw_obj.event_loop(timeout=100)  # 100ms timeout
                
                # Small sleep to prevent CPU spinning
                time.sleep(0.01)
                
        except Exception as e:
            self.logger.error(f"BrokerAppln::driver - error: {str(e)}")
            self.cleanup()
            return 

    def invoke_operation(self):
        """ Invoke operation for message forwarding """
        try:
            # We don't need to explicitly call forward_messages anymore since
            # the event_loop will handle forwarding when messages are received
            return None
        except Exception as e:
            self.logger.error(f"BrokerAppln::invoke_operation - error: {str(e)}")
            return None
    
    def signal_handler(self, signum, frame):
        """ Handle shutdown when interrupt signal is received """
        self.logger.info(f"BrokerAppln::signal_handler - received signal {signum}")
        self.cleanup()
        sys.exit(0)

    def cleanup(self):
        """ Cleanup the middleware """
        self.logger.info("BrokerAppln::cleanup")
        if self.mw_obj:
            self.mw_obj.cleanup()
            
        # If running in agent mode, clean up the agent
        if self.agent:
            self.agent.cleanup()

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Broker Application")

    parser.add_argument("-n", "--name", default="broker", help="Broker name")
    parser.add_argument("-p", "--port", type=int, default=6000, help="Broker port")
    parser.add_argument("-a", "--addr", default="localhost", help="Broker's advertised address") 
    parser.add_argument("-z", "--zookeeper", default="localhost:2181", help="ZooKeeper Address")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level (default=INFO)")
    
    # Agent mode arguments
    parser.add_argument("--agent", dest="agent_mode", action="store_true", help="Run as broker agent")
    parser.add_argument("--agent-port", type=int, default=5555, help="Port for agent to listen on")
    
    return parser.parse_args()

###################################
#
# Main program
#
###################################

def main():
    # Configure logger
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.getLogger("kazoo").setLevel(logging.WARNING)
    logger = logging.getLogger("BrokerAppln")
    
    # Parse command line arguments
    args = parseCmdLineArgs()
    logger.setLevel(args.loglevel)
    
    # Create broker application instance
    broker_app = BrokerAppln(logger)
    
    try:
        # Configure the application
        broker_app.configure(args)
        
        # Start the event loop
        broker_app.driver()
        
    except Exception as e:
        logger.error(f"Exception in main: {str(e)}")
        broker_app.cleanup()
        sys.exit(1)

if __name__ == "__main__":
    main()

