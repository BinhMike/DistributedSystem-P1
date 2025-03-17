# DistributedSystem Group 10

For PA2:

zookeeper: lets just say vm1 handles zookeeper at ip: 192.168.5.91

Start your Discovery service:
(vm1)
python3 DiscoveryAppln.py -p 5555 -a 192.168.5.91 -z localhost:2181
(vm2)
python3 DiscoveryAppln.py -p 5555 -a 192.168.5.55 -z 192.168.5.91:2181
(vm3)
python3 DiscoveryAppln.py -p 5555 -a 192.168.5.230 -z 192.168.5.91:2181


(localhost)
python3 DiscoveryAppln.py -p 5555 -a localhost -z localhost:2181
python3 DiscoveryAppln.py -p 5556 -a localhost -z localhost:2181
python3 DiscoveryAppln.py -p 5557 -a localhost -z localhost:2181


we only have 7 so we'll just use vm7 for pub and sub

Run the Publisher:

(vm7) 
python3 PublisherAppln.py -n pub1 -a 192.168.5.249 -p 5577 -z 192.168.5.91:2181 -T 2 -f 1 -i 1000 -l 20
python3 PublisherAppln.py -n pub2 -a 192.168.5.249 -p 5578 -z 192.168.5.91:2181 -T 2 -f 1 -i 1000 -l 20



(localhost)
python3 PublisherAppln.py -n pub1 -a localhost -p 5577 -z localhost:2181 -T 2 -f 1 -i 1000 -l 20

python3 PublisherAppln.py -n pub2 -a localhost -p 5578 -z localhost:2181 -T 2 -f 1 -i 1000 -l 20


Run the broker:

(vm4)
python3 BrokerAppln.py -n broker1 -a 192.168.5.166 -p 5554 -z 192.168.5.91:2181

(vm5)
python3 BrokerAppln.py -n broker2 -a 192.168.5.226 -p 5555 -z 192.168.5.91:2181

(vm6)
python3 BrokerAppln.py -n broker3 -a 192.168.5.234 -p 5556 -z 192.168.5.91:2181


(localhost)
python3 BrokerAppln.py -n broker1 -a localhost -p 6000 -z localhost:2181
python3 BrokerAppln.py -n broker2 -a localhost -p 6001 -z localhost:2181
python3 BrokerAppln.py -n broker3 -a localhost -p 6002 -z localhost:2181


Run the subscriber：
(vm7)
python3 SubscriberAppln.py -n sub1 -z 192.168.5.91:2181 -T 9 -l 20
python3 SubscriberAppln.py -n sub2 -z 192.168.5.91:2181 -T 9 -l 20


(localhost)
python3 SubscriberAppln.py -n sub1 -z localhost:2181 -T 9 -l 20
python3 SubscriberAppln.py -n sub2 -z localhost:2181 -T 9 -l 20


Start Zookeeper:
To start a new Zookeeper:

./zkServer.sh start

To run Zookeeper:
./zkCli.sh

ls /discovery/leader
ls /broker/leader
ls /subscribers
ls /publishers

If you want to see the data stored in a particular node:
get /discovery/your_node_name

Summary
stat shows you the current state of the connected ZooKeeper server (including whether it is the leader).
ls <path> lists the children of a given znode where your discovery service may have registered its information.
get <path> displays the data stored in a znode.


**Testing Quorum and Leadership Failover Locally:

1. Start three instances of Discovery service:
python3 DiscoveryAppln.py -p 5555 -a localhost -z localhost:2181
python3 DiscoveryAppln.py -p 5556 -a localhost -z localhost:2181
python3 DiscoveryAppln.py -p 5557 -a localhost -z localhost:2181

2. Start three instances of Broker service:
python3 BrokerAppln.py -n broker1 -a localhost -p 6000 -z localhost:2181
python3 BrokerAppln.py -n broker2 -a localhost -p 6001 -z localhost:2181
python3 BrokerAppln.py -n broker3 -a localhost -p 6002 -z localhost:2181

3. Run Publisher and Subscriber:
python3 PublisherAppln.py -n pub1 -a localhost -p 5577 -z localhost:2181 -T 2 -f 1 -i 1000 -l 20
python3 SubscriberAppln.py -n sub1 -z localhost:2181 -T 9 -l 20

4. Testing Failover:
   - Manually terminate the primary Discovery or Broker instance (the one showing "became primary" in logs)
   - Watch as another instance takes over leadership
   - Verify a new replica is spawned automatically to maintain the quorum of 3


