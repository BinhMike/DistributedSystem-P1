#!/usr/bin/python

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
from mininet.nodelib import NAT
from mininet.cli import CLI


def run():
    zookeeper_ip = "172.16.52.133"  # Your VM's IP address

    net = Mininet()

    # Create hosts
    h1 = net.addHost('h1')  # Discovery 1
    h2 = net.addHost('h2')  # Discovery 2
    h3 = net.addHost('h3')  # Discovery 3
    h4 = net.addHost('h4')  # Broker 1
    h5 = net.addHost('h5')  # Broker 2
    h6 = net.addHost('h6')  # Broker 3
    h7 = net.addHost('h7')  # Publisher
    h8 = net.addHost('h8')  # Subscriber
    h9 = net.addHost('h9')  # (Optional extra host)

    # Create switch
    s1 = net.addSwitch('s1')

    # Link hosts to switch
    for h in [h1,h2,h3,h4,h5,h6,h7,h8,h9]:
        net.addLink(h, s1)

    nat = net.addNAT().configDefault()  # This sets up iptables and routing
    net.start()

    # Get IPs
    h1_ip = h1.IP()
    h2_ip = h2.IP()
    h3_ip = h3.IP()
    h4_ip = h4.IP()
    h5_ip = h5.IP()
    h6_ip = h6.IP()
    h7_ip = h7.IP()
    h8_ip = h8.IP()

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

    CLI(net)

    input("Press Enter to stop simulation...")
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()
