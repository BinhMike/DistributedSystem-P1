#!/usr/bin/python

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
from mininet.nodelib import NAT
from mininet.cli import CLI
from mininet.node import OVSController, RemoteController, OVSKernelSwitch


def run():
    # Set higher log level for debugging
    setLogLevel('info')

    print("Creating Mininet with OVSController")
    net = Mininet(switch=OVSKernelSwitch, controller=OVSController)

    print("Adding controller")
    c0 = net.addController('c0')

    # Create hosts
    print("Creating hosts")
    h1 = net.addHost('h1')  # Discovery 1
    h2 = net.addHost('h2')  # Discovery 2
    h3 = net.addHost('h3')  # Discovery 3
    h4 = net.addHost('h4')  # Broker 1
    h5 = net.addHost('h5')  # Broker 2
    h6 = net.addHost('h6')  # Broker 3
    h7 = net.addHost('h7')  # Publisher
    h8 = net.addHost('h8')  # Subscriber
    h9 = net.addHost('h9')  # Zookeeper host

    # Create switch
    print("Creating switch")
    s1 = net.addSwitch('s1')

    # Link hosts to switch
    print("Creating links")
    for h in [h1, h2, h3, h4, h5, h6, h7, h8, h9]:
        net.addLink(h, s1)

    print("Starting network")
    net.start()
    
    # Extended network testing
    print("Network interfaces on h1:")
    print(h1.cmd('ifconfig'))
    
    print("Network interfaces on h2:")
    print(h2.cmd('ifconfig'))
    
    print("Switch flow tables:")
    print(s1.cmd('ovs-ofctl dump-flows s1'))
    # Get IPs (after connectivity verification)
    h1_ip = h1.IP()
    h2_ip = h2.IP()
    h3_ip = h3.IP()
    h4_ip = h4.IP()
    h5_ip = h5.IP()
    h6_ip = h6.IP()
    h7_ip = h7.IP()
    h8_ip = h8.IP()
    zookeeper_ip = h9.IP()
    
    # Start Zookeeper first on h9
    #print("Starting Zookeeper on", zookeeper_ip)
    #h9.cmd('xterm -hold -e "zkServer.sh start-foreground" &')
    
    # Wait for Zookeeper to initialize
    import time
    time.sleep(5)
    
    # Start Discovery Services
    h1.cmd(f'xterm -hold -e "python3 DiscoveryAppln.py -p 5555 -a {h1_ip} -z {zookeeper_ip}:2181" &')
    h2.cmd(f'xterm -hold -e "python3 DiscoveryAppln.py -p 5556 -a {h2_ip} -z {zookeeper_ip}:2181" &')
    h3.cmd(f'xterm -hold -e "python3 DiscoveryAppln.py -p 5557 -a {h3_ip} -z {zookeeper_ip}:2181" &')

    # Start Brokers
    h4.cmd(f'xterm -hold -e "python3 BrokerAppln.py -n broker1 -p 6000 --addr {h4_ip} -z {zookeeper_ip}:2181" &')
    h5.cmd(f'xterm -hold -e "python3 BrokerAppln.py -n broker2 -p 6001 --addr {h5_ip} -z {zookeeper_ip}:2181" &')
    h6.cmd(f'xterm -hold -e "python3 BrokerAppln.py -n broker3 -p 6002 --addr {h6_ip} -z {zookeeper_ip}:2181" &')

    # Start Publisher
    h7.cmd(f'xterm -hold -e "python3 PublisherAppln.py -n pub1 -a {h7_ip} -p 5577 -z {zookeeper_ip}:2181 -T 2 -f 1 -i 1000 -l 20" &')

    # Start Subscriber
    h8.cmd(f'xterm -hold -e "python3 SubscriberAppln.py -n sub1 -z {zookeeper_ip}:2181 -T 9 -l 20" &')

    print("Dumping host connections")
    dumpNodeConnections(net.hosts)
    
    print("\n===== NETWORK SHOULD BE READY =====")
    print("Starting Mininet CLI. Try testing connectivity with 'h1 ping h2'")
    CLI(net)
    
    print("Stopping network")
    net.stop()

if __name__ == '__main__':
    run()
