from twisted.application.internet import TCPClient, UDPServer
from twisted.application.service import Application

from txstatsd.server import MessageProcessor
from txstatsd.protocol import GraphiteClientFactory, StatsDProtocol


application = Application("statsd")
processor = MessageProcessor()

factory = GraphiteClientFactory(processor, 10000)
TCPClient("localhost", 2003, factory).setServiceParent(application)
UDPServer(8125, StatsDProtocol(processor)).setServiceParent(application)
