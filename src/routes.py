from marshmallow.exceptions import ValidationError
from flask import request, jsonify, Response
from database import addDeviceToTenant, deleteOneDevice, deleteOneTenant, findOneDevice, findOneTenant, getAllTenants, insertOneTenant, updateDeviceState, updateTenantState
from schemas import DeviceFields, StateOptions, TenantFields, checkDeviceSchema, checkTenantSchema
from bson.json_util import dumps
from threads import createThread, destroyThread, startDeviceInThread, stopDeviceInThread
from app import app # Get flask app and mongodb collection


# --------------------------------------------------
# Auxiliary functions
# --------------------------------------------------

def removeAttribute(object, attribute):
    if (attribute in object): object.pop(attribute)
    return object

def removeAttributeInDevices(tenant, attribute):
    tenant[TenantFields.devices.value] = [removeAttribute(device, attribute) for device in tenant[TenantFields.devices.value]]
    return tenant

def removeId(object):
    return removeAttribute(object, "id")

# --------------------------------------------------
# Route functions
# --------------------------------------------------

@app.route("/")
def root():
    return "<p>Api docs</p>"


@app.route("/tenants", methods=['POST', 'GET'])
def tenants():
    if request.method == 'POST':
        # Get the data
        data = request.json

        try:
            # Check that it complies with the scheme
            checkTenantSchema(data)
        except ValidationError as err:
            return jsonify(err.messages), 400

        # Save the data to the database
        insertOneTenant(data)

        msg = ""
        if(data[TenantFields.state.value] == StateOptions.active.value):
            # If it is active we start the thread
            createThread(data)
            msg = "Thread started."

        return jsonify(message=("Tenant created successfully." + msg))

    else:
        # Get all tenants
        tenants = getAllTenants()

        # Return a list with all tenants
        return Response(
            dumps([removeId(tenant) for tenant in tenants]),
            mimetype='application/json'
        )


@app.route('/tenants/<tenantid>', methods=['GET', 'DELETE'])
def tenants_id(tenantid):
    if request.method == 'DELETE':
        # Look for the tenant with given tenantid
        tenant = findOneTenant(tenantid)

        # Close and remove the thread for that tenant
        msg = ""
        if(tenant != None and tenant[TenantFields.state.value] == 'active'):
            destroyThread(tenantid)
            msg = "Thread closed."

        # Try to remove the tenant with given tenantid
        result = deleteOneTenant(tenantid)

        # Returns the result and delete his thread if it was active
        if (result.deleted_count > 0):
            return jsonify(message=("Tenant with tenantid " + tenantid + " removed successfully" + msg))
        else:
            return jsonify(message=("There is no tenant with tenantid " + tenantid))

    else:
        # Look for the tenant with given tenantid
        tenant = findOneTenant(tenantid)

        # Returns the result
        if (tenant != None):
            return Response(
                dumps(removeId(tenant)),
                mimetype='application/json'
            )
        else:
            return jsonify(message=("There is no tenant with tenantid " + tenantid))


@app.route('/tenants/<tenantid>/start', methods=['PUT'])
def start_tenant(tenantid):
    if request.method == 'PUT':
        # Look for the tenant with given tenantid
        tenant = findOneTenant(tenantid)

        # If the tenant exists and is not active, we start it
        if tenant == None:
            return jsonify(message=("There is no tenant with tenantid " + tenantid))

        elif (tenant[TenantFields.state.value] == StateOptions.inactive.value):
            updateTenantState(tenantid, StateOptions.active.value)
            createThread(tenant)
            return jsonify(message="Thread for tenant with tenantid " + tenantid + " started successfully")

        else:
            return jsonify(message="Thread for the tenant with tenantid " + tenantid + " was already started") 


@app.route('/tenants/<tenantid>/stop', methods=['PUT'])
def stop_tenant(tenantid):
    if request.method == 'PUT':
        # Look for the tenant with given tenantid
        tenant = findOneTenant(tenantid)

        # If the tenant exists and is active, we stopped it
        if tenant == None:
            return jsonify(message=("There is no tenant with tenantid " + tenantid))

        if(tenant[TenantFields.state.value] == StateOptions.active.value):
            updateTenantState(tenantid, StateOptions.inactive.value)
            destroyThread(tenantid)
            return jsonify(message="Thread for tenant with tenantid " + tenantid + " stopped successfully")
        else: 
            return jsonify(message="Thread for the tenant with tenantid " + tenantid + " was already stopped")


@app.route("/tenants/<tenantid>/devices", methods=['POST', 'GET'])
def devices(tenantid):
    if request.method == 'POST':
        # Get the data
        data = request.json

        try:
            # Check that it complies with the scheme
            checkDeviceSchema(data)
        except ValidationError as err:
            return jsonify(err.messages), 400

        # Save the data to the database
        addDeviceToTenant(tenantid, data)

        msg = ""
        if(data[DeviceFields.state.value] == StateOptions.active.value):
            # If it is active we start the thread
            #createThread(data)
            msg = "Thread started."

        return jsonify(message=("Device created successfully." + msg))

    else:
        # Get all devices
        devices = {}
        tenant = findOneTenant(tenantid)
        if(tenant != None and TenantFields.devices.value in tenant):
            devices = tenant[TenantFields.devices.value]

        # Return a list with all devices
        return Response(
            dumps([device for device in devices]),
            mimetype='application/json'
        )


@app.route('/tenants/<tenantid>/devices/<deviceid>', methods=['GET', 'DELETE'])
def devices_id(tenantid, deviceid):
    if request.method == 'DELETE':
        # Look for the device with given deviceid
        device = findOneDevice(tenantid, deviceid)

        # Close and remove the thread for that device
        msg = ""
        if(device != None and device[DeviceFields.state.value] == StateOptions.active.value):
            stopDeviceInThread(tenantid, deviceid)
            msg = ". Thread closed."

        # Try to remove the device with given deviceid
        result = deleteOneDevice(tenantid, deviceid)

        # Returns the result and delete his thread if it was active
        if (result.modified_count > 0):
            return jsonify(message=("Device with deviceid " + deviceid + " removed successfully" + msg))
        else:
            return jsonify(message=("There is no device with deviceid " + deviceid + " in tenant with tenantid " + tenantid))

    else:
        # Look for the device with given deviceid
        device = findOneDevice(tenantid, deviceid)

        # Returns the result
        if (device != None):
            return Response(
                dumps(device),
                mimetype='application/json'
            )
        else:
            return jsonify(message=("There is no device with deviceid " + deviceid + " in tenant with tenantid " + tenantid))


@app.route('/tenants/<tenantid>/devices/<deviceid>/start', methods=['PUT'])
def start_devices(tenantid, deviceid):
    if request.method == 'PUT':
        # Look for the device with given deviceid in tenant with given tenantid
        device = findOneDevice(tenantid, deviceid)

        # If the tenant exists and is active, we stopped it
        if device == None:
            return jsonify(message=("There is no device with deviceid " + deviceid + " in tenant with tenantid " + tenantid))

        if(device[DeviceFields.state.value] == StateOptions.inactive.value):
            updateDeviceState(tenantid, deviceid, StateOptions.active.value)
            device[DeviceFields.state.value] = StateOptions.active.value
            startDeviceInThread(tenantid, device)
            return jsonify(message="Thread for device " + deviceid + " in tenant with tenantid " + tenantid + " started successfully")
        else: 
            return jsonify(message="Thread for device " + deviceid + " in tenant with tenantid " + tenantid + " was already started")


@app.route('/tenants/<tenantid>/devices/<deviceid>/stop', methods=['PUT'])
def stop_devices(tenantid, deviceid):
    if request.method == 'PUT':
        # Look for the device with given deviceid in tenant with given tenantid
        device = findOneDevice(tenantid, deviceid)

        # If the tenant exists and is active, we stopped it
        if device == None:
            return jsonify(message=("There is no device with deviceid " + deviceid + " in tenant with tenantid " + tenantid))

        if(device[DeviceFields.state.value] == StateOptions.active.value):
            updateDeviceState(tenantid, deviceid, StateOptions.inactive.value)
            device[DeviceFields.state.value] = StateOptions.inactive.value
            stopDeviceInThread(tenantid, device)
            return jsonify(message="Thread for device " + deviceid + " in tenant with tenantid " + tenantid + " stopped successfully")
        else: 
            return jsonify(message="Thread for device " + deviceid + " in tenant with tenantid " + tenantid + " was already stopped")