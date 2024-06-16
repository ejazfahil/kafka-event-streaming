"""Consumer lag monitor 2024-06-16"""
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer
def get_consumer_lag(brokers:str,group_id:str,topics:list)->dict:
    admin=AdminClient({"bootstrap.servers":brokers})
    c=Consumer({"bootstrap.servers":brokers,"group.id":group_id})
    lag={}
    for topic in topics:
        partitions=admin.list_topics(topic).topics[topic].partitions
        for pid in partitions:
            committed=c.committed([{"topic":topic,"partition":pid}])
            watermarks=c.get_watermark_offsets({"topic":topic,"partition":pid})
            lag[f"{topic}:{pid}"]=watermarks[1]-(committed[0].offset or 0)
    c.close(); return lag
