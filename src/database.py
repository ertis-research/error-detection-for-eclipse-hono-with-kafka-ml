from connections import connectWithMongoDB
from schemas import DeviceFields, TenantFields

# MongoDB connection
db = None
col = None

def initDatabase(app):
    global db, col
    db, col = connectWithMongoDB(app)

# --------------------------------------------------
# Query functions
# --------------------------------------------------

def getAllTenants():
    return checkNone(col.find(), col)

def insertOneTenant(data):
    return checkNone(col.insert_one(data), col)

def findOneTenant(tenantId):
    return checkNone(col.find_one(query_tenant(tenantId)), col)

def deleteOneTenant(tenantId):
    return checkNone(col.delete_one(query_tenant(tenantId)), col)

def updateTenantState(tenantId, state):
    checkNone(
        col.update_one(
            query_tenant(tenantId),
            {
                "$set": {
                    TenantFields.state.value: state
                }
            }
        ), 
        col
    )

def addDeviceToTenant(tenantId, deviceData):
    checkNone(
        col.update_one(
            query_tenant(tenantId),
            {
                "$push" : {
                    TenantFields.devices.value : deviceData
                }
            }
        ),
        col
    )

def findOneDevice(tenantId, deviceId):
    res = checkNone(col.find_one(query_get_device(tenantId, deviceId), {TenantFields.devices.value + ".$" : 1, "_id":0}), col)
    if(res != None and TenantFields.devices.value in res and len(res[TenantFields.devices.value]) > 0):
        return res[TenantFields.devices.value][0]
    else:
        return None

def deleteOneDevice(tenantId, deviceId):
    return checkNone(
        col.update_one(
            query_tenant(tenantId), 
            {
                "$pull" : query_device(deviceId)
            }
        ), 
        col
    )

def updateDeviceAttribute(tenantId, deviceId, attribute, value):
    checkNone(
        col.update_one(
            query_get_device(tenantId, deviceId),
            {
                "$set" : {
                    TenantFields.devices.value + ".$." + attribute : value
                }
            }
        ),
        col
    )

def updateDeviceState(tenantId, deviceId, state):
    updateDeviceAttribute(tenantId, deviceId, DeviceFields.state.value, state)

def updateDeviceInterval(tenantId, deviceId, interval):
    updateDeviceAttribute(tenantId, deviceId, DeviceFields._interval.value, interval)


# --------------------------------------------------
# Auxiliary functions
# --------------------------------------------------

def query_tenant(tenantid):
    """Auxiliary function to abstract the query that identifies the tenant"""
    return { TenantFields.tenant_id.value : tenantid}

def query_device(deviceid):
    return { TenantFields.devices.value : { DeviceFields.device_id.value : deviceid}}

def query_get_device(tenantid, deviceid):
    return { TenantFields.tenant_id.value : tenantid, TenantFields.devices.value + "." + DeviceFields.device_id.value : deviceid}

def checkNone(object, check):
    if (check != None):
        return object
    else:
        raise Exception("Uninitialized database")

