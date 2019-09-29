#!/usr/bin/env python3

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import datetime
import time
import sys
import signal
import json

# This holds the parsed arguments for the entire script
parserArgs = None
# This lets the message received event handler know that the DB connection is ready
dbConn = None
# This is the MQTT client object
client = None

Address = None

# This is the Subscriber
def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("DataTopic")
  client.subscribe("SrcValueOutTopic")
  startInfluxDB()

def on_message(client, userdata, msg): 
   global Address
   #print("Mqtt data:")
   #print(msg.topic, str(msg.payload))
   topic =str( msg.topic)

   if topic == 'SrcValueOutTopic':
	print("Node Address:")
    	#print(msg.payload.decode()) 
        Address =str(msg.payload)
	print(Address)
   
   if topic == 'DataTopic':
    #client.disconnect()
#    	current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	current_time= time.ctime() 
   	#print("current time:")
	#print(str(current_time))
	print("creating json object")
	json_body = [
    	{
        	"measurement": "temperature",
        	"tags": {
            		"Node": str(Address),
        	},
        	"time": str(current_time),
        	"fields": {
            		"value": str(msg.payload)
           	}
    	}
    	]
	print("created json object")
    	print("Saving data on DB")
	dbConn.write_points(json_body)
    	print("Saved data on DB")
	print("Data Rx:")
    	print(msg.topic+" "+str(msg.payload))

def _sigIntHandler(signum, frame):
    '''This function handles Ctrl+C for graceful shutdown of the programme'''
    print("Received Ctrl+C. Exiting.")
    stopMQTT()
    stopInfluxDB()
    exit(0)

def setupSigInt():
    '''Sets up our Ctrl + C handler'''
    signal.signal(signal.SIGINT, _sigIntHandler)
    print("Installed Ctrl+C handler.")

def startInfluxDB():
    '''This function sets up our InfluxDB connection'''
    global dbConn
    
    try:
        dbConn =  InfluxDBClient('localhost', 8086, username="pi", password="dau2quel", database='collectd_db')
        print("Connected to InfluxDB.")
        dbConn.create_database('collectd_db')
    	print("Created database")
    except InfluxDBClientError as e:
        print("Could not connect to Influxdb. Message: " + e.content)
        stopMQTT()
        exit(1)
    except:
        print("Could not connect to InfluxDB.")
        stopMQTT()
        exit(1) 

def stopMQTT():
    '''This function stops the MQTT client service'''
    global client
    if client is not None:
        #client.loop_stop()
        client.disconnect()
        print("Disconnected from MQTT broker.")
        client = None
    else:
        print("Attempting to disconnect without first connecting to MQTT broker.")


def stopInfluxDB():
    '''This functions closes our InfluxDB connection'''
    dbConn = None
    print("Disconnected from InfluxDB.") 

def startMQTT():
    '''This function starts the MQTT connection and listens for messages'''
    client = mqtt.Client()
    client.username_pw_set ("pi", "dau2quel")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("192.168.0.107",1883,60)
    #client.connect(parserArgs.mqtthost, parserArgs.mqttport, 60)
    client.loop_forever()

#influx_client = InfluxDBClient('localhost', 8086, username="pi", password="dau2quel", database='collectd_db')


def main():
    # Process our command line arguments first
    #global parserArgs
    #parserArgs = processArgs()

    # Now setup logging
    #setupLogging()

    # Setup our interrupt handler
    setupSigInt()

    # Open up a connection to our MQTT server and subscribe
    startMQTT()

    # Stay here forever
    #global client
    #while True:
    #client.loop_forever()

    # Got here somehow? Okay, clean-up and exit.
    stopInfluxDB()
    stopMQTT()

if __name__ == "__main__":
    main()

