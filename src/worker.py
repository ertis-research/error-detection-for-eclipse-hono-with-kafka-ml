from datetime import datetime
import json
import time
from sqlite3 import Time
from threading import Thread, Event

from proton import ConnectionException
from connections import connectWithHonoByAMQP, connectWithKafka
from database import findOneDevice, updateLastValue
from perpetualTimer import PerpetualTimer
from schemas import DeviceFields, RequiredValueFields, StateOptions, TenantFields
from proton._exceptions import Timeout
import numpy as np

calculatedValues = [ "$year", "$month", "$day", "$hour", "$minute", "$second", "$microsecond" ]

def format_kafka(number, format):
    return eval("np." + format)(round(float(number), 2))

#Modificar esto, permitiendo elegir el tipo de los elementos, que elementos quiere actuales del tiempo y cuales quiere almacenar
def send_last_value_to_kafkaml(producer, device):
    ts = datetime.now()
    items = []
    sendData = True
    if (device != None and DeviceFields.required_values.value in device): 
        for value in device[DeviceFields.required_values.value]:
            v = 0
            name = value[RequiredValueFields.nameValue.value]
            if(name in calculatedValues):
                v = eval("ts." + name.replace("$", ""))
            else:
                if RequiredValueFields._lastValue.value in value and value[RequiredValueFields._lastValue.value] != None:
                    v = value[RequiredValueFields._lastValue.value]
                else:
                    print("lack of information")
                    sendData = False
                    return
            items.append(format_kafka(v, value[RequiredValueFields.format.value]))
    print(items)
    #items = np.array([format_kafka(ts.year), format_kafka(ts.month), format_kafka(ts.day), format_kafka(ts.hour), format_kafka(ts.minute), format_kafka(temp), format_kafka(hum)], dtype=np.float64)
    if(sendData == True):
        print("aa")
        items = np.array(items)
        print("bb")
        producer.send(device[DeviceFields.kafka_topic.value], items.tobytes())
        print("cc")
        producer.flush()
        print("Enviando last value a kafkaml. DeviceId: " + device[DeviceFields.device_id.value] + ", Last value: ")

def getTimer(producer, device):
    return PerpetualTimer(send_last_value_to_kafkaml, [producer, device])

# https://codereview.stackexchange.com/a/201760
def find_by_key(data, target):
    for key, value in data.items():
        if key == target:
            return value
        elif isinstance(value, dict):
            res = find_by_key(value, target)
            if res != None: return res
    return None


def updateLastValueFromMsg(msg, tenantId, deviceId):
    device = findOneDevice(tenantId, deviceId)
    jsonBody = json.loads(msg)
    msg_path = None
    msg_value = None
    if "path" in jsonBody: msg_path = jsonBody["path"] 
    if "value" in jsonBody: msg_value = jsonBody["value"]

    if (device != None and DeviceFields.required_values.value in device and msg_path != None and msg_value != None):
        # COMPROBAR SI EL PATH CONTIENE EL NOMBRE, SINO ENTONCES COGER DIRECTAMENTE BODY.VALUE Y BUSCAR EL NOMBRE
        # COMO KEY. TENIENDO YA UNO DE LOS DOS BUSCAR HASTA ENCONTRAR UNA KEY VALUE.
        for value in device[DeviceFields.required_values.value]:
            newValue = msg_value
            nameValue = value[RequiredValueFields.nameValue.value]
            
            if not msg_path.endswith(nameValue) and "/" + nameValue + "/" not in msg_path: #No esta en el path
                newValue = find_by_key(msg_value, nameValue)
            
            if isinstance(newValue, dict):
                newValue = find_by_key(newValue, "value")
                if isinstance(newValue, dict): return
            
            if newValue != None: updateLastValue(newValue, nameValue, tenantId, deviceId)

            

#def getDeviceFromTenant(tenant, deviceId):
    #res = [device for device in tenant[TenantFields.devices.value] if device[DeviceFields.device_id.value]==deviceId]
    #return res[0] if (len(res) > 0) else None

def handle_message(msg, devices_timers, tMsg, tenantId):
    if(msg and msg.body and msg.annotations):
        deviceId = msg.annotations.get(DeviceFields.device_id.value)
        updateLastValueFromMsg(msg.body, tenantId, deviceId)
        print(deviceId + " - " + str(tMsg))
        if deviceId != None and deviceId in devices_timers:
            print("SEND")
            devices_timers[deviceId].reset(tMsg)


class Worker(Thread):

    def __init__(self, tenant):
        super().__init__()
        self.tenant = tenant
        self._stop_event = Event()
        self.producer = connectWithKafka(self.tenant)
        self.devices_timers = {}

    def stop(self):
        self._stop_event.set()
        print("[Log " + self.tenant[TenantFields.tenant_id.value] + "]: Stopped")

    def stopped(self):
        return self._stop_event.is_set()

    def addDevice(self, device):
        if device[DeviceFields.state.value] == StateOptions.active.value:
            #interval = device[DeviceFields._interval.value] if DeviceFields._interval.value in device else None
            t = getTimer(self.producer, device)
            self.devices_timers[device[DeviceFields.device_id.value]] = t
            t.start()
    
    def removeDevice(self, deviceId, pop):
        if deviceId != None and deviceId in self.devices_timers:
            #updateDeviceInterval(self.tenant[TenantFields.tenant_id.value], deviceId, self.devices_timers[deviceId].interval)
            self.devices_timers[deviceId].cancel()
            if pop == True: self.devices_timers.pop(deviceId)
            

    def run(self):
        try:
            conn, receiver = connectWithHonoByAMQP(self.tenant)
        except ConnectionException:
            print("ERROR CONEXION")
            conn, receiver = connectWithHonoByAMQP(self.tenant)

        for device in self.tenant[TenantFields.devices.value]:
            self.addDevice(device)

        print("comienza")
        tMsg = time.time()
        while not self.stopped():
            try:
                msg = receiver.receive(timeout=5)
                ini = tMsg
                tMsg = time.time() 
                print("tiempo 1: " + str(tMsg - ini) + "")
                handle_message(msg, self.devices_timers, tMsg, self.tenant[TenantFields.tenant_id.value])
            except Timeout:
                print("timeout")
            except ConnectionException:
                print("ERROR CONEXION")
                conn, receiver = connectWithHonoByAMQP(self.tenant)

        print("close")

        for deviceId in self.devices_timers.keys():
            self.removeDevice(deviceId, False)

        receiver.close()
        conn.close()
        self.producer.close()
        

