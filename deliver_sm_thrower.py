from twisted.internet import defer, reactor
from jasmin.vendor.smpp.pdu.operations import DeliverSM
from jasmin.queues.configs import AmqpConfig
from jasmin.queues.factory import AmqpFactory
from jasmin.routing.jasminApi import SmppServerSystemIdConnector
from jasmin.routing.content import RoutedDeliverSmContent
from jasmin.queues.test.test_amqp import waitFor

@defer.inlineCallbacks
def connect_and_publish(pdu, dc):
	# AMQP client object
	config = AmqpConfig()
	config.reconnectOnConnectionLoss = False
	amqp = AmqpFactory(config)

	# Connect
	yield amqp.connect()

	# Wait for channel to get ready
	yield amqp.getChannelReadyDeferred()

	# Prepare content to be published
	# scid (Source connector id) and msgid are not needed, just set them to any value
	# dc (destination connector) is all what you need
	content = RoutedDeliverSmContent(deliver_sm = pdu, 
		msgid = '1',
		scid = 'whatever',
		dc = dc,
		)

	# Publish the pdu
	yield amqp.publish(exchange='messaging', routing_key="deliver_sm_thrower.smpps", content=content)

	# Wait 2s and disconnect
	yield waitFor(2)
	yield amqp.disconnect()

	# Exit
	reactor.stop()

if __name__ == '__main__':
	"""
	The below 'pdu' is published to RabbitMQ so the deliverSmThrower can take care of it and
	send it to the destination connector pointed in 'dc'.
	Note:
	  - Jasmin must be already running before executing this script
	  - 'dc' must be connected and bound at least with TRX or RX.
	Troubleshooting:
	  - Check /var/log/jasmin/deliversm-thrower.log for any delivery error
	"""

	dc = SmppServerSystemIdConnector('system_id')
	pdu = DeliverSM(
		source_addr='1234',
		destination_addr='4567',
		short_message='any content',
	)
	
	# Run !
	connect_and_publish(pdu, dc)
reactor.run()
