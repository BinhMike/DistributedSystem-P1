#!/usr/bin/env python3

import zmq
import sys
import os
import subprocess
import time
import signal
import logging
import argparse
from kazoo.client import KazooClient

class BrokerAgent:
    """Agent that runs on each machine to spawn brokers on demand"""
    
    def __init__(self, host, port, zk_conn_str):
        # Set up logging
        self.logger = logging.getLogger("BrokerAgent")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        # Initialize properties
        self.host = host
        self.port = port
        self.zk_conn_str = zk_conn_str
        self.zk = KazooClient(hosts=zk_conn_str)
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.running = False
        self.broker_processes = {}
        
    def start(self):
        """Start the broker agent service"""
        self.logger.info(f"Starting Broker Agent on {self.host}:{self.port}")
        
        # Connect to ZooKeeper
        self.zk.start()
        
        # Register this agent in ZooKeeper
        self.register_with_zk()
        
        # Start listening for spawn requests
        self.socket.bind(f"tcp://*:{self.port}")
        self.running = True
        
        # Set up signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.event_loop()
    
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
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, shutting down")
        self.cleanup()
        sys.exit(0)
    
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

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Broker Agent')
    parser.add_argument('-a', '--addr', type=str, default='localhost', help='IP address')
    parser.add_argument('-p', '--port', type=int, default=5555, help='Port')
    parser.add_argument('-z', '--zookeeper', type=str, default='localhost:2181', help='ZooKeeper connection string')
    return parser.parse_args()

def main():
    """Main function"""
    args = parse_args()
    agent = BrokerAgent(args.addr, args.port, args.zookeeper)
    agent.start()

if __name__ == "__main__":
    main()