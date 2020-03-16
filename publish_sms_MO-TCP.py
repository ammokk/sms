#!/usr/bin/env python
""
name: publish_sms_MO-TCP.py
author: Ettore Eliseo Pelliccioni
email: ettore.pelliccioni@techniclite.com
last modified: 2015/09/03

This program receive a stream from TCP / PORT 11007 like this line (carriage return ended): 
  
  TEXT;XXXXXXXXXXXX;2015090111595232;

where the fields are separated by comma ";"

TEXT: is the text written from mobile phone number to SHORTCODE
XXXXXXXXXXXX is the mobile phone number writing to SHORTCODE (src_addr)
2015090111595232 is the timestamp from the msg (YYYYMMDDHHMMSSMS)

and publish as SMS-MO directly on RabbitMQ as an upstream source to Jasmin SMS Gateway

TODO: 
      - NOT FOR PRODUCTION SYSTEMS: deliver_sm_resp issue, no managment for sequence number!!!
      - Add logging to debug messages.
      
""
import pickle

from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol,reactor
from twisted.internet.protocol import ClientCreator
from twisted.python import log
from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec

from jasmin.vendor.smpp.pdu.pdu_types import (EsmClass, EsmClassMode, EsmClassType,
                                RegisteredDelivery, RegisteredDeliveryReceipt,
                                AddrTon, AddrNpi, PriorityFlag, ReplaceIfPresentFlag)

from jasmin.vendor.smpp.pdu.operations import  DeliverSMResp,DeliverSM
from jasmin.managers.content import  DeliverSmContent

@inlineCallbacks
def gotConnection(conn, username, password,src,dest,text):
   yield conn.authenticate(username, password)
   #print 'Connected to RabbitMQ'
 
   chan = yield conn.channel(1)
   yield chan.channel_open()
    
   DeliverSmPDU = DeliverSM( 	source_addr = src, 
  	source_addr_ton = AddrTon.NATIONAL,
  	source_addr_npi = AddrNpi.ISDN,
  	destination_addr = dest, 
  	dest_addr_ton = AddrTon.NATIONAL,
  	dest_addr_npi = AddrNpi.ISDN,
  	esm_class=EsmClass(EsmClassMode.DEFAULT, EsmClassType.DEFAULT),
  	protocol_id=0,
  	priority_flag=PriorityFlag.LEVEL_0,
  	registered_delivery=RegisteredDelivery(RegisteredDeliveryReceipt.NO_SMSC_DELIVERY_RECEIPT_REQUESTED),
  	replace_if_present_flag=ReplaceIfPresentFlag.DO_NOT_REPLACE,
  	short_message = text, 
	seqNum = 1,)

   content = DeliverSmContent(DeliverSmPDU, 'SIMULATOR' )
    
   #print DeliverSmPDU
   yield chan.basic_publish(exchange='messaging', routing_key='deliver.sm.SIMULATOR', content=content)
    
   # A clean way to tear down and stop
   yield chan.channel_close()
    
   chan0 = yield conn.channel(0)
   yield chan0.connection_close()
       

class Server(protocol.Protocol):
    


    def dataReceived(self, data):

        self.transport.write(data)
	campo  = data.split(";")
	#print campo[0],";",campo[1],";",campo[2]

	# Send an SMS-MT with defined originating address


	host = '127.0.0.1'
	port = 5672
	vhost = '/'
	username = 'guest'
	password = 'guest'
	spec_file = '/etc/jasmin/resource/amqp0-9-1.xml'
	MO_dst = '24247'
	MO_src = campo[1]
	# transformation of the source address! :-)
	MOD_MO_src = '00' + MO_src[2:]
	texto = campo[0]
 
	spec = txamqp.spec.load(spec_file)

	delegate = TwistedDelegate()
 
	# Connect and authenticate
	d = ClientCreator(reactor, AMQClient, delegate=delegate, vhost=vhost,
	spec=spec).connectTCP(host, port)

	d.addCallback(gotConnection, username, password,MOD_MO_src,MO_dst,texto)
	
	def whoops(err):
	    if reactor.running:
		log.err(err)
                reactor.stop()
 
	d.addErrback(whoops)

	def __init__(self,  amqpBroker):      
	    self.amqpBroker = amqpBroker
  

 
class ServerFactory(protocol.Factory):
    def buildProtocol(self, addr):
        return Server()
 
reactor.listenTCP(11007, ServerFactory()) 
reactor.run()
