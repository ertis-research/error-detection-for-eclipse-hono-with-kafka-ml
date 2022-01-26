#from uamqp.client import ReceiveClient
#from uamqp.authentication import SASLPlain
from kafka import KafkaProducer
from consts import DEFAULT_PORT_RABBITMQ, MONGO_URI
from flask_pymongo import PyMongo
from schemas import CredentialsFields, TenantFields
from proton.handlers import MessagingHandler
from proton.reactor import Container
from proton.utils import BlockingConnection


def connectWithMongoDB(app):
    app.config["MONGO_URI"] = MONGO_URI
    mongodb_client = PyMongo(app)
    db = mongodb_client.db
    col = db.tenants
    print("[Log APP]: Connected to MongoDB")
    return db, col


def connectWithKafka(tenant):
    producer = KafkaProducer(
        bootstrap_servers=tenant[TenantFields.kafka_server.value],
        api_version=(0, 10, 1)
    )
    print("[Log " + tenant[TenantFields.tenant_id.value] + "]: Connected to KafkaML")
    return producer


class Recv(MessagingHandler):
    def __init__(self, server, address, count):
        super(Recv, self).__init__()
        self.server = server
        self.address = address
        self.expected = count
        self.received = 0

    def on_start(self, event):
        conn = event.container.connect(self.server)
        event.container.create_receiver(conn, self.address)
        print("START" + self.server)

    def on_message(self, event):
        print("MENSAJE")
        #if event.message.id and event.message.id < self.received:
            # ignore duplicate message
            #return
        if self.expected == 0 or self.received < self.expected:
            print(event.message.body)
            self.received += 1
            if self.received == self.expected:
                event.receiver.close()
                event.connection.close()


def connectWithHonoByAMQP(tenant):
    auth = ""
    if TenantFields.amqp_credentials.value in tenant:
        credentials = tenant[TenantFields.amqp_credentials.value]
        auth = credentials[CredentialsFields.username.value] + ":" + credentials[CredentialsFields.password.value] + "@"
    server = "amqp://" + auth + tenant[TenantFields.amqp_server.value]
    address = "telemetry/" + tenant[TenantFields.tenant_id.value]

    conn = BlockingConnection(server)
    receiver = conn.create_receiver(address)

    return conn, receiver

    #return Container(Recv(server, address, 0))



    #auth=None
    #if(TenantFields.amqp_credentials.value in tenant):
    #    auth = SASLPlain(
    #        hostname=hostname,
    #        username=tenant[TenantFields.amqp_credentials.value][CredentialsFields.username.value],
    #        password=tenant[TenantFields.amqp_credentials.value][CredentialsFields.password.value]
    #    )

    #receive_client = ReceiveClient(uri, auth=auth)
    #print("[Log " + tenant[TenantFields.tenantId.value] + "]: Connected to HONO")
    #return receive_client




