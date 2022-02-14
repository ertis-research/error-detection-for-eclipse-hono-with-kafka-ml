from enum import Enum
from marshmallow import Schema, fields as f, validate

class StateOptions(Enum):
    inactive = "inactive"
    active = "active"

class RequiredValueFields(Enum):
    nameValue = "name"
    format = "format"
    _lastValue = "last_value"

class CredentialsFields(Enum):
    username = "username"
    password = "password"

class DeviceFields(Enum):
    device_id = "device_id"
    kafka_topic = "kafka_topic"
    required_values = "required_values"
    state = "state"

class TenantFields(Enum):
    tenant_id = "tenant_id"
    kafka_server = "kafka_server"
    devices = "devices"
    amqp_server = "amqp_server"
    amqp_credentials = "amqp_credentials"
    state = "state"


RequireValueDict = {}
RequireValueDict[RequiredValueFields.format.value] = f.String(required=True)
RequireValueDict[RequiredValueFields.nameValue.value] = f.String(required=True)
RequireValueDict[RequiredValueFields._lastValue.value] = f.String(required=False)

RequireValueSchema = Schema.from_dict(RequireValueDict)

CredentialsDict = {}
CredentialsDict[CredentialsFields.username.value] = f.String(required=True)
CredentialsDict[CredentialsFields.password.value] = f.String(required=True)

CredentialsSchema = Schema.from_dict(CredentialsDict)

DeviceDict = {}
DeviceDict[DeviceFields.device_id.value] = f.String(required=True)
DeviceDict[DeviceFields.kafka_topic.value] = f.String(required=True)
DeviceDict[DeviceFields.required_values.value] = f.List(f.Nested(RequireValueSchema()), required=True)
DeviceDict[DeviceFields.state.value] = f.String(required=True, validate=validate.OneOf([StateOptions.inactive.value, StateOptions.active.value]))

DeviceSchema = Schema.from_dict(DeviceDict)

TenantDict = {}
TenantDict[TenantFields.tenant_id.value] = f.String(required=True)
TenantDict[TenantFields.kafka_server.value] = f.String(required=True)
TenantDict[TenantFields.devices.value] = f.List(f.Nested(DeviceSchema()))
TenantDict[TenantFields.amqp_server.value] = f.String(required=True)
TenantDict[TenantFields.amqp_credentials.value] = f.Nested(CredentialsSchema())
TenantDict[TenantFields.state.value] = f.String(required=True, validate=validate.OneOf([StateOptions.inactive.value, StateOptions.active.value]))


TenantSchema = Schema.from_dict(TenantDict)

def checkTenantSchema(data):
    TenantSchema().load(data)

def checkDeviceSchema(data):
    DeviceSchema().load(data)