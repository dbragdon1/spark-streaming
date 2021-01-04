from kafka import KafkaProducer
import feedparser
import time
from datetime import datetime

class HNFeed:
    def __init__(self):
        self.rss_link = r'https://hnrss.org/newcomments'
        self.last_modified = None

        self.producer = KafkaProducer(bootstrap_servers = 'localhost:9092',
                                      key_serializer = lambda k: bytes(k, encoding = 'utf-8'),
                                      value_serializer = lambda v: bytes(v, encoding = 'utf-8'))
    
    def get_data(self):
            """
            Update parser instance and check for new comments
            """
            self.feed = feedparser.parse(self.rss_link, modified = self.last_modified)
            self.last_modified = self.feed.modified
            if len(self.feed.entries) > 0:
                print('Found {} new entries after {}.'.format(len(self.feed.entries), self.last_modified))
        
    
    def send_data(self):
        """
        Send new comments as messages to Kafka Topic
        """
        #TODO: Gather more info than just comments (user_id, time submitted, parent comment, etc)
        for entry in self.feed.entries:
            #id_ = entry['id'].split('=')[1]
            timeposted = entry['published_parsed']
            timeformat = datetime.fromtimestamp(time.mktime(timeposted))
            text = entry['summary']
            self.producer.send(topic = 'hncomments',
                               key = str(timeformat),
                               value = text)


    def run(self):
        while True:
            self.get_data()
            self.send_data()
            time.sleep(10)

    
if __name__ == "__main__":
    parser = HNFeed()
    parser.run()