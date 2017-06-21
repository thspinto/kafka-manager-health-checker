#!/usr/bin/python3
import logging
import requests
import sys
from datetime import datetime
import os
from apscheduler.schedulers.background import BlockingScheduler
from slackclient import SlackClient

LOGGING_FORMAT = ('%(asctime)s - %(name)s - %(threadName)s - '
                  '%(levelname)s - %(message)s')
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format=LOGGING_FORMAT,
                    datefmt='%m/%d/%Y %H:%M:%S')

#Configs
slack_token = os.getenv("SLACK_API_TOKEN")
credentials = (os.getenv('USER'), os.getenv('PASSWORD'))
url = os.getenv('URL')+'/api/status/5a-kafka'
expectedBrokers = int(os.getenv('LIVE_BROKERS', '3'))
alertChannel = os.getenv("SLACK_ALERT_CHANNEL")

def healthChecker():
    liveBrokers = liveBrokersCheck(url, credentials)
    topics = getTopics(url, credentials)
    LOGGER.info("Topics: " + str(topics))
    underReplicatedTopics = underReplicatedPartitionsCheck(topics, url, credentials)
    unavailableTopics = unavailablePartitionsCheck(topics, url, credentials)

    alertText = liveBrokers['text'] + underReplicatedTopics['text'] + unavailableTopics['text']
    send = liveBrokers['sendAlert'] or  underReplicatedTopics['sendAlert'] or unavailableTopics['sendAlert']
    if(send):
        sendAlert(alertText)

def liveBrokersCheck(url, credentials):
    alertDict = { 'text': '', 'sendAlert': False }
    endpoint = url+'/brokers'
    r = requests.get(endpoint, auth=credentials)

    if(r.status_code != 200):
        LOGGER.error('Request ' + endpoint + ' failed with status '+ str (r.status_code))
    else:
        LOGGER.info("Brokers: " + str(r.json()['brokers']))
        liveBrokers = r.json()['brokers']
        if(len(liveBrokers) < expectedBrokers):
            alertDict['text'] = 'Broker down!\n'
            alertDict['sendAlert'] = True
    return alertDict


def getTopics(url, credentials):
    endpoint = url+'/topics'
    r = requests.get(endpoint, auth=credentials)
    return r.json()['topics']

def underReplicatedPartitionsCheck(topics, url, credentials):
    alertDict = { 'text': 'Under replicated topics!\n', 'sendAlert': False }
    underRepicatedTopics = []
    for topic in topics:
        endpoint = url+'/'+topic+'/underReplicatedPartitions'
        r = requests.get(endpoint, auth=credentials)
        if(r.status_code != 200):
            LOGGER.error('Request ' + endpoint + ' failed with status '+ str (r.status_code))
            break
        if(len(r.json()['underReplicatedPartitions']) > 0):
            underRepicatedTopics.append(topic)
            alertDict['sendAlert'] = True

    LOGGER.info("Under Replicated Topics: " + str(underRepicatedTopics))
    alertDict['text'] += str(underRepicatedTopics) + '\n'
    return alertDict

def unavailablePartitionsCheck(topics, url, credentials):
    alertDict = { 'text': 'Unavailable topics!\n', 'sendAlert': False }
    unavailableTopics = []
    for topic in topics:
     endpoint = url+'/'+topic+'/unavailablePartitions'
     r = requests.get(endpoint, auth=credentials)
     if(r.status_code != 200):
         LOGGER.error('Request ' + endpoint + ' failed with status '+ str (r.status_code))
         break
     if(len(r.json()['unavailablePartitions']) > 0):
         unavailableTopics.append(topic)
         alertDict['sendAlert'] = True

    LOGGER.info("Unavailable Topics: " + str(unavailableTopics))
    alertDict['text'] += str(unavailableTopics) + '\n'
    return alertDict

def sendAlert(alertText):
    sc = SlackClient(slack_token)
    sc.api_call(
      "chat.postMessage",
      channel=alertChannel,
      text=":fire: " + alertText
    )

if __name__ == '__main__':
    LOGGER = logging.getLogger(__name__)
    scheduler = BlockingScheduler({'apscheduler.timezone': 'UTC'})
    scheduler.add_job(healthChecker, 'interval', seconds=10)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
