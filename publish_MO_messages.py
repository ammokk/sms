#!/usr/bin/env python
"" Thanks to Zoufou to help me.  This is a proof of concept to publish messages directly in RabbitMQ, 
   then Jasmin SMS Gateway will route this messages as coming from an upstream source like a SMSClient / SMS-MO 
   following the rules on the morouter: routing to a smppsapi (SMPP-SERVER) or to httpapi (HTTP-API)
""
import pickle
from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator
from twisted.python import log
from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec

from jasmin.vendor.smpp.pdu.operations import  DeliverSMResp,DeliverSM
from jasmin.managers.content import  DeliverSmContent

@inlineCallbacks
def gotConnection(conn, username, password):
    yield conn.authenticate(username, password)
 
    chan = yield conn.channel(1)
    yield chan.channel_open()


    DeliverSmPDU = DeliverSM( 	source_addr = '584141111330', 
				destination_addr = '24247', 
				short_message = 'Prueba 01 !', 
				seqNum = 1,)

    content = DeliverSmContent(DeliverSmPDU, 'SIMULATOR' )
    
    #print DeliverSmPDU
    
    yield chan.basic_publish(exchange='messaging', routing_key='deliver.sm.SIMULATOR', content=content)

    # A clean way to tear down and stop
    yield chan.channel_close()

    chan0 = yield conn.channel(0)
    yield chan0.connection_close()
    
    reactor.stop()
 
if __name__ == "__main__":
 
    host = '127.0.0.1'
    port = 5672
    vhost = '/'
    username = 'guest'
    password = 'guest'
    spec_file = '/etc/jasmin/resource/amqp0-9-1.xml'
 
    spec = txamqp.spec.load(spec_file)

    delegate = TwistedDelegate()
 
    # Connect and authenticate
    d = ClientCreator(reactor, AMQClient, delegate=delegate, vhost=vhost,
        spec=spec).connectTCP(host, port)

    d.addCallback(gotConnection, username, password)

    def whoops(err):
        if reactor.running:
            log.err(err)
            reactor.stop()
 
    d.addErrback(whoops)
 
    reactor.run()

    def __init__(self,  amqpBroker):      
        self.amqpBroker = amqpBroker
