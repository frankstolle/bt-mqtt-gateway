from workers.base import BaseWorker
from mqtt import MqttMessage
from interruptingcow import timeout
from bluepy.btle import Peripheral
import logging
import threading
import time

REQUIREMENTS = ['bluepy']

_LOGGER = logging.getLogger('bt-mqtt-gw.cometblue')

class CometBlue():
    def __init__(self, mac, pin):
        self.mac = mac
        self.pin = pin
        self.connection = None
        self.lock = threading.RLock()

    def disconnect(self):
        with self.lock:
            if self.connection == None:
                return
            _LOGGER.debug("disconnect from "+self.mac)
            self.connection.disconnect()
            self.connection = None

    def get_connection(self):
        with self.lock:
            if self.connection == None:
                _LOGGER.debug("connect to "+self.mac)
                self.connection = Peripheral(self.mac, "public")
                try:
                    _LOGGER.debug("send pin to "+self.mac)
                    self.connection.writeCharacteristic(0x0047, self.pin.to_bytes(4, byteorder='little'), withResponse=True)
                except:
                    self.disconnect()
                    raise
            return self.connection

    def read_temperature(self):
        with self.lock:
            ret = dict()
            connection = self.get_connection()
            _LOGGER.debug("read temperatures from "+self.mac)
            data = connection.readCharacteristic(0x003f)
            ret['internal_current_temperature'] = int.from_bytes(bytearray([data[0]]), byteorder='little', signed=True)/2
            ret['offset_temperature'] = int.from_bytes(bytearray([data[4]]), byteorder='little', signed=True)/2
            ret["current_temperature"] = ret['internal_current_temperature'] + ret['offset_temperature']
            ret["target_temperature"] = int.from_bytes(bytearray([data[1]]), byteorder='little', signed=True)/2
            ret["target_low"] = int.from_bytes(bytearray([data[2]]), byteorder='little', signed=True)/2
            ret["target_high"] = int.from_bytes(bytearray([data[3]]), byteorder='little', signed=True)/2    
            ret["window_open_detection"] = data[5]
            ret["window_open_minutes"] = data[6]
            return ret

    def set_target_temperature(self, temperature):
        with self.lock:
            connection = self.get_connection()
            _LOGGER.debug("write temperatures to "+self.mac)
            temperature = max(8, min(28, temperature))
            temp = round(temperature * 2)
            data = bytes([0x80, temp, 0x80, 0x80, 0x80, 0x80, 0x80])
            connection.writeCharacteristic(0x003f, data, withResponse=True)

    def read_battery(self):
        with self.lock:
            connection = self.get_connection()
            _LOGGER.debug("read battery "+self.mac)
            data = connection.readCharacteristic(0x0041)
            return data[0]

class CometBlueController:
    def __init__(self, mac, pin, updateinterval):
        self.lock = threading.RLock()
        self.updateinterval = updateinterval
        self.device = CometBlue(mac, pin)
        self.state = dict()
        self.state['state'] = 'offline'

    def start(self):
        threading.Thread(target=self._update, daemon=True).start()
        
    def _update(self):
        while True:
            try:
                self._read_state()
            except Exception as e:
                self._handle_connecterror(e)
            time.sleep(self.updateinterval)

    def _read_state(self):
        temperature = self.device.read_temperature()
        battery = self.device.read_battery()
        with self.lock:
            self.state['state'] = 'online'
            #FIXME: Frank: hier je nach temperatur entscheiden ob ein oder aus
            self.state['mode'] = 'ON'
            self.state['current_temperature'] = temperature['current_temperature']
            self.state['offset_temperature'] = temperature['offset_temperature']
            self.state['target_temperature'] = temperature['target_temperature']
            self.state['battery'] = battery
            #FIXME: Frank: zeit der aktualisierung mit speichern als unix timestamp oder auch formartiert für den mqtt?

    def _handle_connecterror(self, e):
        with self.lock:
            self.state['state'] = 'offline'
        print(e)
        self.device.disconnect()


    def set_target_temperature(self, temperature):
        try:
            self.device.set_target_temperature(temperature)
            self._read_state()
        except Exception as e:
            self._handle_connecterror(e)

    def get_state(self):
        with self.lock:
            return self.state.copy()

class CometblueWorker(BaseWorker):
    def _setup(self):
        self.dev = dict()
        for name, data in self.devices.items():
            if not 'mac' in data:
                raise Exception("mac missing for "+name)
            if not 'pin' in data:
                raise Exception("pin missing for "+name)
            if 'update_interval' in data:
                updateinterval = data['update_interval']
            else:
                updateinterval = 300
            self.dev[name] = CometBlueController(data['mac'], data['pin'], updateinterval)
            self.dev[name].start()

    def status_update(self):
        ret = []
        for name in self.devices:
            ret += self._get_mqtt_state_messages(name)
        return ret

    def _get_mqtt_state_messages(self, name):
        """ gets mqtt state messages of an device state """
        ret = []
        state = self.dev[name].get_state()
        for key, value in state.items():
            ret.append(MqttMessage(topic=self.format_topic(name, key), payload=value, retain=True))
        return ret

    def on_command(self, topic, value):
        """ handles commands received via mqtt and returns new piblished topics after update """
        _, device_name, method, _ = topic.split('/')
        valueAsString = value.decode("UTF-8")
        ret = []
        if not device_name in self.dev:
            _LOGGER.warn("ignore unknown device "+device_name)
            return []
        if method=="target_temperature":
            self.dev[device_name].set_target_temperature(float(valueAsString))
            pass
        else:
            _LOGGER.warn("unknown method "+method)
            return []
        return self._get_mqtt_state_messages(device_name)
