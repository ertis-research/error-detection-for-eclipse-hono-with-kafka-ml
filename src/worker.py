from datetime import datetime
import time
from socket import timeout
from sqlite3 import Time
from threading import Thread, Event, Timer
from connections import connectWithHonoByAMQP, connectWithKafka
from database import updateDeviceInterval
from perpetualTimer import PerpetualTimer
from schemas import DeviceFields, StateOptions, TenantFields
from proton._exceptions import Timeout
import numpy as np



def format_kafka(value, number):
    return eval("np." + format)(round(float(number), 2))

#Modificar esto, permitiendo elegir el tipo de los elementos, que elementos quiere actuales del tiempo y cuales quiere almacenar
def send_last_value_to_kafkaml(producer, device):
    ts = datetime.now()
    print(format_kafka(ts.year, "float64"))
    #items = np.array([format_kafka(ts.year), format_kafka(ts.month), format_kafka(ts.day), format_kafka(ts.hour), format_kafka(ts.minute), format_kafka(temp), format_kafka(hum)], dtype=np.float64)
    #producer.send(device[DeviceFields.kafka_topic], items.tobytes())
    #producer.flush()
    print("Enviando last value a kafkaml. DeviceId: " + device[DeviceFields.device_id.value] + ", Last value: " + device[DeviceFields.required_values.value])

def getTimer(producer, device, interval):
    return PerpetualTimer(send_last_value_to_kafkaml, [producer, device], interval)


    
#def getDeviceFromTenant(tenant, deviceId):
    #res = [device for device in tenant[TenantFields.devices.value] if device[DeviceFields.device_id.value]==deviceId]
    #return res[0] if (len(res) > 0) else None

def handle_message(msg, devices_timers, tMsg):
    if(msg and msg.body and msg.annotations):
        deviceId = msg.annotations.get(DeviceFields.device_id.value)
        if deviceId != None and deviceId in devices_timers:
            #actualizar ultimo valor device
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
            interval = device[DeviceFields._interval.value] if DeviceFields._interval.value in device else None
            t = getTimer(self.producer, device, interval)
            self.devices_timers[device[DeviceFields.device_id.value]] = t
            t.start()
    
    def removeDevice(self, deviceId, pop):
        if deviceId != None and deviceId in self.devices_timers:
            updateDeviceInterval(self.tenant[TenantFields.tenant_id.value], deviceId, self.devices_timers[deviceId].interval)
            self.devices_timers[deviceId].cancel()
            if pop == True: self.devices_timers.pop(deviceId)
            

    def run(self):
        
        for device in self.tenant[TenantFields.devices.value]:
            self.addDevice(device)

        conn, receiver = connectWithHonoByAMQP(self.tenant)

        print("comienza")
        tMsg = time.time()
        while not self.stopped():
            try:
                msg = receiver.receive(timeout=5)
                ini = tMsg
                tMsg = time.time() 
                print("tiempo 1: " + str(tMsg - ini) + "")
                handle_message(msg, self.devices_timers, tMsg)
            except Timeout:
                print("timeout")

        print("close")

        for deviceId in self.devices_timers.keys():
            self.removeDevice(deviceId, False)

        receiver.close()
        conn.close()
        self.producer.close()
        

