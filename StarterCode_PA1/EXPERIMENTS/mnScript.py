#!/usr/bin/python

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
from mininet.cli import CLI
from mininet.node import OVSController, OVSKernelSwitch
import time
import signal
import os


def run():
    # Set higher log level for debugging
    setLogLevel('info')

    print("Creating Mininet with OVSController")
    net = Mininet(switch=OVSKernelSwitch, controller=OVSController)

    print("Adding controller")
    c0 = net.addController('c0')

    zk_path = "/home/haonan/Downloads/apache-zookeeper-3.8.4-bin/bin"
    
    # Create hosts with descriptive names
    print("Creating hosts")
    zk_host = net.addHost('zk')      # ZooKeeper
    discovery = []
    for i in range(3):
        discovery.append(net.addHost(f'disc{i+1}'))  # Discovery services
    
    lb = net.addHost('lb')          # Load balancer
    
    brokers_group1 = []
    for i in range(3):
        brokers_group1.append(net.addHost(f'brk1_{i+1}'))  # Broker group 1
    
    brokers_group2 = []
    for i in range(3):
        brokers_group2.append(net.addHost(f'brk2_{i+1}'))  # Broker group 2
    
    publishers = []
    for i in range(5):
        publishers.append(net.addHost(f'pub{i+1}'))  # Publishers
    
    subscribers = []
    for i in range(5):
        subscribers.append(net.addHost(f'sub{i+1}'))  # Subscribers
    
    # Monitor host (for watching and killing processes to test failover)
    monitor = net.addHost('monitor')
    
    # Create switch
    print("Creating switch")
    s1 = net.addSwitch('s1')

    # Link hosts to switch
    print("Creating links")
    all_hosts = [zk_host] + discovery + [lb] + brokers_group1 + brokers_group2 + publishers + subscribers + [monitor]
    for h in all_hosts:
        net.addLink(h, s1)

    print("Starting network")
    net.start()
    
    # Get IPs for all hosts
    zk_ip = zk_host.IP()
    discovery_ips = [h.IP() for h in discovery]
    lb_ip = lb.IP()
    broker1_ips = [h.IP() for h in brokers_group1]
    broker2_ips = [h.IP() for h in brokers_group2]
    publisher_ips = [h.IP() for h in publishers]
    subscriber_ips = [h.IP() for h in subscribers]
    monitor_ip = monitor.IP()

    # Print IP addresses for reference
    print("\n=== Network IP Addresses ===")
    print(f"ZooKeeper: {zk_ip}")
    print(f"Discovery Services: {', '.join(discovery_ips)}")
    print(f"Load Balancer: {lb_ip}")
    print(f"Broker Group 1: {', '.join(broker1_ips)}")
    print(f"Broker Group 2: {', '.join(broker2_ips)}")
    print(f"Publishers: {', '.join(publisher_ips)}")
    print(f"Subscribers: {', '.join(subscriber_ips)}")
    print(f"Monitor: {monitor_ip}\n")
    
    # Configure ZooKeeper
    zk_host.cmd(f"echo 'clientPortAddress={zk_ip}' > /tmp/zoo.cfg")
    zk_host.cmd(f"echo 'dataDir=/tmp/zookeeper' >> /tmp/zoo.cfg")
    zk_host.cmd(f"echo 'clientPort=2181' >> /tmp/zoo.cfg")
    # Ensure a fresh ZooKeeper data directory
    zk_host.cmd(f"rm -rf /tmp/zookeeper") 
    zk_host.cmd(f"mkdir -p /tmp/zookeeper")

    # Stop any running ZooKeeper instance
    zk_host.cmd(f'{zk_path}/zkServer.sh stop')

    # Start ZooKeeper in a separate terminal with distinctive title
    zk_host.cmd(f'xterm -T "ZooKeeper Server" -fa "Monospace" -fs 12 -geometry 100x30 -bg black -fg green -e "{zk_path}/zkServer.sh start-foreground /tmp/zoo.cfg" &')
    
    # ZooKeeper client for monitoring - explicitly specify the server IP
    zk_host.cmd(f'xterm -T "ZooKeeper Client ({zk_ip})" -fa "Monospace" -fs 12 -geometry 100x30 -bg black -fg yellow -e "{zk_path}/zkCli.sh -server {zk_ip}:2181" &')

    print("\nWaiting for ZooKeeper to initialize...")
    time.sleep(10)
    
    # Start Discovery Services - color coded with blue background
    print("\nStarting Discovery Services...")
    for i, (disc, ip) in enumerate(zip(discovery, discovery_ips)):
        port = 5555 + i
        title = f"Discovery {i+1} ({ip}:{port})"
        disc.cmd(f'xterm -T "{title}" -fa "Monospace" -fs 12 -geometry 100x30 -bg blue -fg white -hold -e "python3 DiscoveryAppln.py -p {port} -a {ip} -z {zk_ip}:2181 -l 20" &')
        time.sleep(1)  # Stagger the starts

    # Start Brokers Group 1 - red background
    print("\nStarting Broker Group 1...")
    for i, (broker, ip) in enumerate(zip(brokers_group1, broker1_ips)):
        port = 6000 + i
        title = f"Broker Group 1-{i+1} ({ip}:{port})"
        broker.cmd(f'xterm -T "{title}" -fa "Monospace" -fs 12 -geometry 100x30 -bg red -fg white -hold -e "python3 BrokerAppln.py -n broker1_{i+1} -p {port} --addr {ip} -z {zk_ip}:2181 -g group1 -l 20" &')
        time.sleep(1)  # Give each broker time to initialize

    # Start Brokers Group 2 - orange background
    print("\nStarting Broker Group 2...")
    for i, (broker, ip) in enumerate(zip(brokers_group2, broker2_ips)):
        port = 6200 + i  # Using different port range to avoid conflicts
        title = f"Broker Group 2-{i+1} ({ip}:{port})"
        broker.cmd(f'xterm -T "{title}" -fa "Monospace" -fs 12 -geometry 100x30 -bg orange -fg black -hold -e "python3 BrokerAppln.py -n broker2_{i+1} -p {port} --addr {ip} -z {zk_ip}:2181 -g group2 -l 20" &')
        time.sleep(1)  # Give each broker time to initialize

    # # Wait for brokers to fully initialize and register with ZooKeeper
    print("\nWaiting for broker groups to initialize in ZooKeeper...")
    time.sleep(5)

    # Start Broker Load Balancer with topic mapping - purple background
    print("\nStarting Broker Load Balancer...")
    title = f"Load Balancer ({lb_ip})"
    # Added topic mapping and verbose logging
    lb.cmd(f'xterm -T "{title}" -fa "Monospace" -fs 12 -geometry 100x30 -bg purple -fg white -hold -e "python3 BrokerLB.py --addr {lb_ip} -z {zk_ip}:2181 -l 10 -d group1 -m weather:group1,light:group2,altitude:group1,humidity:group2,location:group1,temperature:group2,pressure:group1,airquality:group2,sound:group1" &')
    time.sleep(2)

    # Start Publishers - green background
    print("\nStarting Publishers...")
    for i, (pub, ip) in enumerate(zip(publishers, publisher_ips)):
        port = 5577 + i
        title = f"Publisher {i+1} ({ip}:{port})"
        pub.cmd(f'xterm -T "{title}" -fa "Monospace" -fs 12 -geometry 100x30 -bg green -fg black -hold -e "python3 PublisherAppln.py -n pub{i+1} -a {ip} -p {port} -z {zk_ip}:2181 -T 2 -f 1 -i 1000 -l 20" &')
        time.sleep(1)

    # Start Subscribers - cyan background
    print("\nStarting Subscribers...")

    # wait 10 sec after starting publisher
    time.sleep(10)
    for i, (sub, ip) in enumerate(zip(subscribers, subscriber_ips)):
        title = f"Subscriber {i+1} ({ip})"
        sub.cmd(f'xterm -T "{title}" -fa "Monospace" -fs 12 -geometry 100x30 -bg cyan -fg black -hold -e "python3 SubscriberAppln.py -n sub{i+1} -z {zk_ip}:2181 -T 9 -l 20" &')
        time.sleep(1)

    # Set up monitor station for testing failover scenarios
    monitor.cmd(f'xterm -T "Test Monitor Station" -fa "Monospace" -fs 12 -geometry 120x40 -bg white -fg black -hold -e "echo \'Welcome to the Test Monitor Station\n\nZooKeeper: {zk_ip}:2181\nDiscovery: {discovery_ips}\nBrokers Group 1: {broker1_ips}\nBrokers Group 2: {broker2_ips}\nPublishers: {publisher_ips}\nSubscribers: {subscriber_ips}\n\nExample commands:\n- Check broker leaders: echo stat | nc {zk_ip} 2181 | grep brokers\n- Kill primary broker: ssh {broker1_ips[0]} pkill -f BrokerAppln\n- Kill discovery leader: ssh {discovery_ips[0]} pkill -f DiscoveryAppln\n- Check status: ps -ef | grep -E \"(Broker|Discovery|Publisher|Subscriber)Appln\" | grep -v grep\n\nUse this monitor to test failover scenarios\'; bash" &')

    print("\n===== NETWORK IS READY =====")
    print("Color coding:")
    print("  - ZooKeeper: Green text")
    print("  - Discovery Services: Blue background")
    print("  - Load Balancer: Purple background")
    print("  - Broker Group 1: Red background")
    print("  - Broker Group 2: Orange background")  
    print("  - Publishers: Green background")
    print("  - Subscribers: Cyan background")
    print("  - Test Monitor: White background (use for testing failover)")
    
    print("\nTEST SCENARIOS TO TRY:")
    print("1. Verify that publishers are publishing and subscribers are receiving messages")
    print("2. Kill the primary broker in Group 1 and verify failover")
    print("3. Kill the discovery leader and verify that a new leader is elected")
    print("4. Kill two brokers in a group and verify quorum-based auto-spawning of new replicas")
    print("\nStarting Mininet CLI. Try commands like 'zk ip', 'brk1_1 ip', 'monitor ip'")
    CLI(net)
    
    # Cleanup process
    print("\nShutting down the test environment...")
    
    def cleanup_processes():
        hosts_to_clean = all_hosts
        print("Killing all application processes...")

        # Graceful shutdown
        for host in hosts_to_clean:
            host.cmd("pkill -INT -f '[D]iscoveryAppln'")
            host.cmd("pkill -INT -f '[B]rokerAppln'")
            host.cmd("pkill -INT -f '[B]rokerLB'")
            host.cmd("pkill -INT -f '[P]ublisherAppln'")
            host.cmd("pkill -INT -f '[S]ubscriberAppln'")

        print("Waiting for processes to shut down gracefully...")
        time.sleep(3)

        # Force kill only your application processes
        for host in hosts_to_clean:
            host.cmd("pkill -9 -f '[Pp]ython3 .*DiscoveryAppln'")
            host.cmd("pkill -9 -f '[Pp]ython3 .*BrokerAppln'")
            host.cmd("pkill -9 -f '[Pp]ython3 .*BrokerLB'")
            host.cmd("pkill -9 -f '[Pp]ython3 .*PublisherAppln'")
            host.cmd("pkill -9 -f '[Pp]ython3 .*SubscriberAppln'")
            # DO NOT kill all xterms or shells

        # Stop ZooKeeper
        print("Stopping ZooKeeper...")
        zk_host.cmd(f"{zk_path}/zkServer.sh stop")
        time.sleep(2)
        zk_host.cmd("pkill -9 -f '[Zz]ooKeeper'")

        print("All application processes terminated")
    
    # Run cleanup with a timeout to avoid hanging
    import threading
    cleanup_thread = threading.Thread(target=cleanup_processes)
    cleanup_thread.daemon = True  # Allow the script to exit even if thread is running
    cleanup_thread.start()
    
    # Wait for cleanup with a timeout
    cleanup_thread.join(timeout=4)  # Wait up to 15 seconds for cleanup
    if cleanup_thread.is_alive():
        print("Warning: Cleanup taking too long, forcing exit")
    
    print("Stopping network")
    net.stop()

if __name__ == '__main__':
    run()
