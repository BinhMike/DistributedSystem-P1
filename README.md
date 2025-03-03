# DistributedSystem Group 10

For PA2:

Start your Discovery service:
python3 DiscoveryAppln.py -p 5555 -a localhost -z localhost:2181

Run the Publisher:
python3 PublisherAppln.py -n pub1 -a localhost -p 5577 -z localhost:2181 -T 2 -f 1 -i 1000 -l 20

Run the Subscriber:
python3 SubscriberAppln.py -n sub1 -z localhost:2181 -T 2 -l 20
