from database import getAllTenants
from schemas import DeviceFields, TenantFields, StateOptions
from worker import Worker

threads = {}


def createThread(tenant):
    tenant.pop('_id')
    t = Worker(tenant)
    t.daemon = True
    print(TenantFields.tenant_id.value)
    threads[tenant[TenantFields.tenant_id.value]] = t
    t.start()


def destroyThread(id):
    if id in threads:
        t = threads[id]
        t.stop()
        threads.pop(id)


def stopDeviceInThread(tenantid, device):
    if tenantid in threads:
        threads[tenantid].removeDevice(device[DeviceFields.device_id.value], True)


def startDeviceInThread(tenantid, device):
    if tenantid in threads:
        threads[tenantid].addDevice(device)


def initThreads():
    global threads
    threads = {}
    tenants = getAllTenants()
    for tenant in tenants:
        if(TenantFields.state.value in tenant and tenant[TenantFields.state.value] == StateOptions.active.value):
            createThread(tenant)


def close_running_threads():
    listThreads = list(threads.values())
    for t in listThreads:
        t.stop()
        t.join()
    print("Exiting correctly")