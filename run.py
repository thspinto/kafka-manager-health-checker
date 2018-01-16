#!/usr/bin/python3
import logging
import requests
import sys
from datetime import datetime
import os
from apscheduler.schedulers.background import BlockingScheduler
from slackclient import SlackClient
import pypd

LOGGING_FORMAT = ('%(asctime)s - %(name)s - %(threadName)s - '
                  '%(levelname)s - %(message)s')
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format=LOGGING_FORMAT,
                    datefmt='%m/%d/%Y %H:%M:%S')

LOGGER = logging.getLogger(__name__)
scheduler = BlockingScheduler({'apscheduler.timezone': 'UTC'})
interval = 10

class HealthChecker():

    def __init__(self):
        #Configs
        self.excludeConsumers = os.getenv('EXCLUDED_CONSUMERS', '').split(',')
        self.slack_token = os.getenv('SLACK_API_TOKEN')
        self.credentials = (os.getenv('USER'), os.getenv('PASSWORD'))
        self.url = os.getenv('URL', '')+'/api/status/5a-kafka'
        self.expectedBrokers = int(os.getenv('LIVE_BROKERS', '3'))
        self.maxLag = int(os.getenv('MAX_LAG', '40'))
        self.alertChannel = os.getenv('SLACK_ALERT_CHANNEL')
        self.alerted = False
        self.incidentKey = None

    def start(self):
        scheduler.add_job(self.check, 'interval', seconds=interval, id='healthChecker')
        print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass

    def check(self):
        liveBrokers = self.liveBrokersCheck()
        topics = self.getTopics()
        LOGGER.info("Topics: " + str(topics))
        unavailableTopics = self.unavailablePartitionsCheck(topics)
        consumerSummary = self.getConsumerSummary()
        LOGGER.info("Consummers:" + str(consumerSummary))
        laggingConsumers = self.laggingConsumer(consumerSummary)

        send, alertText = self.processAlert([liveBrokers, unavailableTopics, laggingConsumers])
        if(send):
            self.sendAlert(alertText)
            if(not self.alerted):
                self.pagerduty_publish(alertText)
            scheduler.reschedule_job('healthChecker', trigger='interval', seconds=300)
            self.alerted = True
        else:
            if(self.alerted):
                self.pagerduty_publish(alertText, 'resolve')
                self.sendAlert('Back to normal :beauty:')
                scheduler.reschedule_job('healthChecker', trigger='interval', seconds=interval)
                self.incidentKey = None
                self.alerted = False

    def processAlert(self, alertDicts):
        alertText = ''
        sendAlert = False
        for alert in alertDicts:
            if alert['sendAlert']:
                sendAlert = True
                alertText += alert['text']
        return sendAlert, alertText

    def liveBrokersCheck(self):
        alertDict = { 'text': '', 'sendAlert': False }
        endpoint = self.url+'/brokers'
        r = requests.get(endpoint, auth=self.credentials)

        if(r.status_code != 200):
            LOGGER.error('Request ' + endpoint + ' failed with status '+ str (r.status_code))
        else:
            LOGGER.info("Brokers: " + str(r.json()['brokers']))
            liveBrokers = r.json()['brokers']
            if(len(liveBrokers) < self.expectedBrokers):
                alertDict['text'] = 'Broker down!\n'
                alertDict['sendAlert'] = True
        return alertDict


    def getTopics(self):
        endpoint = self.url+'/topics'
        r = requests.get(endpoint, auth=self.credentials)
        return r.json()['topics']

    def getConsumerSummary(self):
        endpoint = self.url+'/consumersSummary'
        r = requests.get(endpoint, auth=self.credentials)
        return r.json()['consumers']

    def laggingConsumer(self, consumerSummary):
        alertDict={ 'text': 'Lagging Consumer!\n', 'sendAlert': False}
        laggingConsumers = []
        for consumer in consumerSummary:
            if consumer['name'] in self.excludeConsumers:
                print("Ignoring consumer: " + consumer['name'])
                continue
            for topic in consumer['topics']:
                endpoint = self.url+'/'+consumer['name']+'/'+topic+'/'+consumer['type']+'/'+'topicSummary'
                r = requests.get(endpoint, auth=self.credentials)
                if(r.status_code != 200):
                    LOGGER.error('Request ' + endpoint + ' failed with status '+ str (r.status_code))
                    break
                if(r.json()['totalLag'] > self.maxLag):
                    laggingConsumers.append(consumer)
                    alertDict['sendAlert'] = True

        LOGGER.info("laggingConsumers: " + str(laggingConsumers))
        alertDict['text'] += str(laggingConsumers) + '\n'
        return alertDict

    def underReplicatedPartitionsCheck(self, topics):
        alertDict = { 'text': 'Under replicated topics!\n', 'sendAlert': False }
        underRepicatedTopics = []
        for topic in topics:
            endpoint = self.url+'/'+topic+'/underReplicatedPartitions'
            r = requests.get(endpoint, auth=self.credentials)
            if(r.status_code != 200):
                LOGGER.error('Request ' + endpoint + ' failed with status '+ str (r.status_code))
                break
            if(len(r.json()['underReplicatedPartitions']) > 0):
                underRepicatedTopics.append(topic)
                alertDict['sendAlert'] = True

        LOGGER.info("Under Replicated Topics: " + str(underRepicatedTopics))
        alertDict['text'] += str(underRepicatedTopics) + '\n'
        return alertDict

    def unavailablePartitionsCheck(self, topics):
        alertDict = { 'text': 'Unavailable topics!\n', 'sendAlert': False }
        unavailableTopics = []
        for topic in topics:
         endpoint = self.url+'/'+topic+'/unavailablePartitions'
         r = requests.get(endpoint, auth=self.credentials)
         if(r.status_code != 200):
             LOGGER.error('Request ' + endpoint + ' failed with status '+ str (r.status_code))
             break
         if(len(r.json()['unavailablePartitions']) > 0):
             unavailableTopics.append(topic)
             alertDict['sendAlert'] = True

        LOGGER.info("Unavailable Topics: " + str(unavailableTopics))
        alertDict['text'] += str(unavailableTopics) + '\n'
        return alertDict

    def sendAlert(self, alertText):
        sc = SlackClient(self.slack_token)
        LOGGER.info('Alert sent: ' + alertText)
        sc.api_call(
          "chat.postMessage",
          channel=self.alertChannel,
          text=":fire: " + alertText
        )

    def pagerduty_publish(self, message, action='trigger'):
        r = pypd.EventV2.create(data={
            'routing_key': os.environ['SERVICE_KEY'],
            'event_action': action,
            'dedup_key': self.incidentKey,
            'payload': {
                'summary': os.environ['DESCRIPTION'],
                'severity': 'warning',
                'source': self.url,
                'class': 'Kafka warning alert',
                'custom_details': {
                    'node_info': message
                }
            }
        })
        LOGGER.info(r)
        self.incidentKey = r['dedup_key']

if __name__ == '__main__':
    HealthChecker().start()
