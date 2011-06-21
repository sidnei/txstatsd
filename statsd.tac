from twisted.application.internet import TCPClient, UDPServer
from twisted.application.service import Application

from txstatsd.processor import MessageProcessor
from txstatsd.protocol import GraphiteClientFactory, StatsDServerProtocol


application = Application("statsd")
processor = MessageProcessor()

factory = GraphiteClientFactory(processor, 10000)
TCPClient("localhost", 2003, factory).setServiceParent(application)
UDPServer(8125, StatsDServerProtocol(processor)).setServiceParent(application)
