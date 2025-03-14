# DistributedSystem Group 10

For PA2:

zookeeper: lets just say vm1 handles zookeeper at ip: 192.168.5.91

Start your Discovery service:
(vm1)
python3 DiscoveryAppln.py -p 5555 -a 192.168.5.91 -z localhost:2181
(vm2)
python3 DiscoveryAppln.py -p 5555 -a 192.168.5.55 -z 192.168.5.91:2181
(vm3)
fill this out plz

(localhost)
python3 DiscoveryAppln.py -p 5555 -a localhost -z localhost:2181
python3 DiscoveryAppln.py -p 5556 -a localhost -z localhost:2181


Run the Publisher:
(localhost)
python3 PublisherAppln.py -n pub1 -a localhost -p 5577 -z localhost:2181 -T 2 -f 1 -i 1000 -l 20

python3 PublisherAppln.py -n pub2 -a localhost -p 5578 -z localhost:2181 -T 2 -f 1 -i 1000 -l 20


Run the broker:

(vm4)
(primary) (need to run brokerAppln with --agent on all machines first, then start regular broker on 1 machine)
python3 BrokerAppln.py -n broker1 -a 192.168.5.166 -p 5554 -z 192.168.5.91:2181

python3 BrokerAppln.py --agent -a 192.168.5.166 -p 5555 -z 192.168.5.91:2181

(vm5)
python3 BrokerAppln.py --agent -a 129.114.25.181 -p 5555 -z 192.168.5.91:2181

(vm6)
python3 BrokerAppln.py --agent -a 192.168.5.234 -p 5555 -z 192.168.5.91:2181


(localhost)
python3 BrokerAppln.py -n broker1 -p 6000 --addr localhost -z localhost:2181

python3 BrokerAppln.py -n broker2 -p 6001 --addr localhost -z localhost:2181

python3 BrokerAppln.py -n broker3 -p 6002 --addr localhost -z localhost:2181


Run the subscriber：
python3 SubscriberAppln.py -n sub1 -z localhost:2181 -T 9 -l 20

python3 SubscriberAppln.py -n sub2 -z localhost:2181 -T 9 -l 20


Start Zookeeper:
To start a new Zookeeper:

./zkServer.sh start

To run Zookeeper:
./zkCli.sh

ls /discovery/leader

ls /subscribers

ls /publishers

If you want to see the data stored in a particular node:
get /discovery/your_node_name

Summary
stat shows you the current state of the connected ZooKeeper server (including whether it is the leader).
ls <path> lists the children of a given znode where your discovery service may have registered its information.
get <path> displays the data stored in a znode.
