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

    zk_path = "/opt/zookeeper/bin"
    
    # Create hosts
    print("Creating hosts")
    h1 = net.addHost('h1') #zookeeper
    h2 = net.addHost('h2')  
    h3 = net.addHost('h3')  
    h4 = net.addHost('h4')  
    h5 = net.addHost('h5')  
    h6 = net.addHost('h6') 
    h7 = net.addHost('h7')  
    h8 = net.addHost('h8')  
    h9 = net.addHost('h9')  
    h10 = net.addHost('h10')  
    h11 = net.addHost('h11')  
    h12 = net.addHost('h12')  
    h13 = net.addHost('h13')  

    #h9.cmd(f'{zk_path}/zkServer.sh stop &')
    #h9.cmd(f'{zk_path}/zkServer.sh start &')
    #print(h9.cmd('jps'))

    # Create switch
    print("Creating switch")
    s1 = net.addSwitch('s1')

    # Link hosts to switch
    print("Creating links")
    for h in [h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12, h13]:
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
    zookeeper_ip = h1.IP()
    h2_ip = h2.IP()
    h3_ip = h3.IP()
    h4_ip = h4.IP()
    h5_ip = h5.IP()
    h6_ip = h6.IP()
    h7_ip = h7.IP()
    h8_ip = h8.IP()
    h9_ip = h9.IP()
    h10_ip = h10.IP()
    h11_ip = h11.IP()
    h12_ip = h12.IP()
    h13_ip = h13.IP()

    
    # Configure ZooKeeper
    h1.cmd(f"echo 'clientPortAddress={zookeeper_ip}' > /tmp/zoo.cfg")
    h1.cmd(f"echo 'dataDir=/tmp/zookeeper' >> /tmp/zoo.cfg")
    h1.cmd(f"echo 'clientPort=2181' >> /tmp/zoo.cfg")
    h1.cmd(f"mkdir -p /tmp/zookeeper")

    # Stop any running ZooKeeper instance
    h1.cmd(f'{zk_path}/zkServer.sh stop')

    # Start ZooKeeper with the custom config
    h1.cmd(f'{zk_path}/zkServer.sh start-foreground /tmp/zoo.cfg &')
    h1.cmd(f'xterm -fa "Monospace" -fs 12 -geometry 100x30 -hold -e {zk_path}/zkCli.sh &')

    # Wait longer for ZooKeeper to initialize
    import time
    time.sleep(10)
    
    # Start Discovery Services
    h2.cmd(f'xterm -fa "Monospace" -fs 12 -geometry 100x30 -hold -e "python3 DiscoveryAppln.py -p 5555 -a {h2_ip} -z {zookeeper_ip}:2181" &')
    h3.cmd(f'xterm -fa "Monospace" -fs 12 -geometry 100x30 -hold -e "python3 DiscoveryAppln.py -p 5556 -a {h3_ip} -z {zookeeper_ip}:2181" &')
    h4.cmd(f'xterm -fa "Monospace" -fs 12 -geometry 100x30 -hold -e "python3 DiscoveryAppln.py -p 5557 -a {h4_ip} -z {zookeeper_ip}:2181" &')

    # Start Brokers
    h5.cmd(f'xterm -fa "Monospace" -fs 12 -geometry 100x30 -hold -e "python3 BrokerAppln.py -n broker1 -p 6000 --addr {h5_ip} -z {zookeeper_ip}:2181 -g group1" &')
    h6.cmd(f'xterm -fa "Monospace" -fs 12 -geometry 100x30 -hold -e "python3 BrokerAppln.py -n broker2 -p 6001 --addr {h6_ip} -z {zookeeper_ip}:2181 -g group1" &')
    h7.cmd(f'xterm -fa "Monospace" -fs 12 -geometry 100x30 -hold -e "python3 BrokerAppln.py -n broker3 -p 6002 --addr {h7_ip} -z {zookeeper_ip}:2181 -g group1" &')

    # h8.cmd(f'xterm -fa "Monospace" -fs 12 -geometry 100x30 -hold -e "python3 BrokerAppln.py -n broker4 -p 6000 --addr {h8_ip} -z {zookeeper_ip}:2181 -g group2" &')
    # h9.cmd(f'xterm -fa "Monospace" -fs 12 -geometry 100x30 -hold -e "python3 BrokerAppln.py -n broker5 -p 6001 --addr {h9_ip} -z {zookeeper_ip}:2181 -g group2" &')
    # h10.cmd(f'xterm -fa "Monospace" -fs 12 -geometry 100x30 -hold -e "python3 BrokerAppln.py -n broker6 -p 6002 --addr {h10_ip} -z {zookeeper_ip}:2181 -g group2" &')

    # Start Publisher
    h11.cmd(f'xterm -fa "Monospace" -fs 12 -geometry 100x30 -hold -e "python3 PublisherAppln.py -n pub1 -a {h11_ip} -p 5577 -z {zookeeper_ip}:2181 -T 2 -f 1 -i 1000 -l 20" &')

    # Start Subscriber
    h12.cmd(f'xterm -fa "Monospace" -fs 12 -geometry 100x30 -hold -e "python3 SubscriberAppln.py -n sub1 -z {zookeeper_ip}:2181 -T 9 -l 20" &')

    print("Dumping host connections")
    dumpNodeConnections(net.hosts)
    
    print("\n===== NETWORK SHOULD BE READY =====")
    print("Starting Mininet CLI. Try testing connectivity with 'h1 ping h2'")
    CLI(net)
    
    print("Stopping network")
    net.stop()

if __name__ == '__main__':
    run()
