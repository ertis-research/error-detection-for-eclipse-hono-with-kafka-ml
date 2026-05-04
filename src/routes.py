from marshmallow.exceptions import ValidationError
from flask import Blueprint, request, jsonify, Response
from flask_restful import Api, Resource
from database import addDeviceToTenant, deleteOneDevice, deleteOneTenant, findOneDevice, findOneTenant, getAllTenants, insertOneTenant, updateDeviceState, updateTenantState
from schemas import DeviceFields, StateOptions, TenantFields, checkDeviceSchema, checkTenantSchema
from bson.json_util import dumps
from threads import createThread, destroyThread, startDeviceInThread, stopDeviceInThread
from __main__ import app

api_blueprint = Blueprint('api', __name__)
api = Api(api_blueprint)

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
# Resource classes
# --------------------------------------------------

class TenantsResource(Resource):
    def post(self):
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
        if data[TenantFields.state.value] == StateOptions.active.value:
            # If it is active we start the thread
            createThread(data)
            msg = "Thread started."

        return jsonify(message=("Tenant created successfully." + msg))

    def get(self):
        # Get all tenants
        tenants = getAllTenants()

        # Return a list with all tenants
        return Response(
            dumps([removeId(tenant) for tenant in tenants]),
            mimetype='application/json'
        )


class TenantResource(Resource):
    def get(self, tenantid):
        # Look for the tenant with given tenantid
        tenant = findOneTenant(tenantid)

        # Returns the result
        if tenant is not None:
            return Response(
                dumps(removeId(tenant)),
                mimetype='application/json'
            )
        else:
            return jsonify(message=("There is no tenant with tenantid " + tenantid)), 404

    def delete(self, tenantid):
        # Look for the tenant with given tenantid
        tenant = findOneTenant(tenantid)

        # Close and remove the thread for that tenant
        msg = ""
        if tenant is not None and tenant[TenantFields.state.value] == 'active':
            destroyThread(tenantid)
            msg = "Thread closed."

        # Try to remove the tenant with given tenantid
        result = deleteOneTenant(tenantid)

        if result.deleted_count > 0:
            return jsonify(message=("Tenant with tenantid " + tenantid + " removed successfully" + msg))
        else:
            return jsonify(message=("There is no tenant with tenantid " + tenantid)), 404

class StartTenantResource(Resource):
    def put(self, tenantid):
        # Look for the tenant with given tenantid
        tenant = findOneTenant(tenantid)

        # If the tenant exists and is not active, we start it
        if tenant is None:
            return jsonify(message=("There is no tenant with tenantid " + tenantid)), 404

        elif tenant[TenantFields.state.value] == StateOptions.inactive.value:
            updateTenantState(tenantid, StateOptions.active.value)
            createThread(tenant)
            return jsonify(message="Thread for tenant with tenantid " + tenantid + " started successfully")

        else:
            return jsonify(message="Thread for the tenant with tenantid " + tenantid + " was already started")

class StopTenantResource(Resource):
    def put(self, tenantid):
        # Look for the tenant with given tenantid
        tenant = findOneTenant(tenantid)

        # If the tenant exists and is active, we stop it
        if tenant is None:
            return jsonify(message=("There is no tenant with tenantid " + tenantid)), 404

        if tenant[TenantFields.state.value] == StateOptions.active.value:
            updateTenantState(tenantid, StateOptions.inactive.value)
            destroyThread(tenantid)
            return jsonify(message="Thread for tenant with tenantid " + tenantid + " stopped successfully")
        else:
            return jsonify(message="Thread for the tenant with tenantid " + tenantid + " was already stopped")

class DevicesResource(Resource):
    def post(self, tenantid):
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
        if data[DeviceFields.state.value] == StateOptions.active.value:
            msg = "Thread started."

        return jsonify(message=("Device created successfully." + msg))

    def get(self, tenantid):
        # Get all devices
        devices = {}
        tenant = findOneTenant(tenantid)
        if tenant is not None and TenantFields.devices.value in tenant:
            devices = tenant[TenantFields.devices.value]

        # Return a list with all devices
        return Response(
            dumps([device for device in devices]),
            mimetype='application/json'
        )

class DeviceResource(Resource):
    def get(self, tenantid, deviceid):
        # Look for the device with given deviceid
        device = findOneDevice(tenantid, deviceid)

        # Returns the result
        if device is not None:
            return Response(
                dumps(device),
                mimetype='application/json'
            )
        else:
            return jsonify(message=("There is no device with deviceid " + deviceid + " in tenant with tenantid " + tenantid)), 404

    def delete(self, tenantid, deviceid):
        # Look for the device with given deviceid
        device = findOneDevice(tenantid, deviceid)

        # Close and remove the thread for that device
        msg = ""
        if device is not None and device[DeviceFields.state.value] == StateOptions.active.value:
            stopDeviceInThread(tenantid, deviceid)
            msg = ". Thread closed."

        # Try to remove the device with given deviceid
        result = deleteOneDevice(tenantid, deviceid)

        if result.modified_count > 0:
            return jsonify(message=("Device with deviceid " + deviceid + " removed successfully" + msg))
        else:
            return jsonify(message=("There is no device with deviceid " + deviceid + " in tenant with tenantid " + tenantid)), 404

class StartDeviceResource(Resource):
    def put(self, tenantid, deviceid):
        # Look for the device with given deviceid in tenant with given tenantid
        device = findOneDevice(tenantid, deviceid)

        if device is None:
            return jsonify(message=("There is no device with deviceid " + deviceid + " in tenant with tenantid " + tenantid)), 404

        if device[DeviceFields.state.value] == StateOptions.inactive.value:
            updateDeviceState(tenantid, deviceid, StateOptions.active.value)
            device[DeviceFields.state.value] = StateOptions.active.value
            startDeviceInThread(tenantid, device)
            return jsonify(message="Thread for device " + deviceid + " in tenant with tenantid " + tenantid + " started successfully")
        else:
            return jsonify(message="Thread for device " + deviceid + " in tenant with tenantid " + tenantid + " was already started")

class StopDeviceResource(Resource):
    def put(self, tenantid, deviceid):
        # Look for the device with given deviceid in tenant with given tenantid
        device = findOneDevice(tenantid, deviceid)

        if device is None:
            return jsonify(message=("There is no device with deviceid " + deviceid + " in tenant with tenantid " + tenantid)), 404

        if device[DeviceFields.state.value] == StateOptions.active.value:
            updateDeviceState(tenantid, deviceid, StateOptions.inactive.value)
            device[DeviceFields.state.value] = StateOptions.inactive.value
            stopDeviceInThread(tenantid, device)
            return jsonify(message="Thread for device " + deviceid + " in tenant with tenantid " + tenantid + " stopped successfully")
        else:
            return jsonify(message="Thread for device " + deviceid + " in tenant with tenantid " + tenantid + " was already stopped")

# --------------------------------------------------
# Register resources
# --------------------------------------------------

api.add_resource(TenantsResource, '/tenants')
api.add_resource(TenantResource, '/tenants/<string:tenantid>')
api.add_resource(StartTenantResource, '/tenants/<string:tenantid>/start')
api.add_resource(StopTenantResource, '/tenants/<string:tenantid>/stop')
api.add_resource(DevicesResource, '/tenants/<string:tenantid>/devices')
api.add_resource(DeviceResource, '/tenants/<string:tenantid>/devices/<string:deviceid>')
api.add_resource(StartDeviceResource, '/tenants/<string:tenantid>/devices/<string:deviceid>/start')
api.add_resource(StopDeviceResource, '/tenants/<string:tenantid>/devices/<string:deviceid>/stop')