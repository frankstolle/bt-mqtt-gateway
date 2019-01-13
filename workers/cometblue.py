from workers.base import BaseWorker
from mqtt import MqttMessage
from interruptingcow import timeout
from bluepy.btle import Peripheral
import logging
import threading
import time
import datetime

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

    def set_target_temperature(self, temperature=None, temperature_high=None):
        with self.lock:
            connection = self.get_connection()
            _LOGGER.debug("write temperatures to "+self.mac)
            target_temp = 0x80
            target_high = 0x80
            if temperature is not None:
                temperature = max(7.5, min(28.5, temperature))
                target_temp = round(temperature * 2)
            if temperature_high is not None:
                temperature_high = max(8, min(28, temperature_high))
                target_high = round(temperature_high * 2)
            data = bytes([0x80, target_temp, 0x80, target_high, 0x80, 0x80, 0x80])
            connection.writeCharacteristic(0x003f, data, withResponse=True)

    def read_battery(self):
        with self.lock:
            connection = self.get_connection()
            _LOGGER.debug("read battery "+self.mac)
            data = connection.readCharacteristic(0x0041)
            return data[0]

    def set_pin(self, pin):
        with self.lock:
            connection = self.get_connection()
            _LOGGER.debug("set pin "+str(pin))
            self.connection.writeCharacteristic(0x0047, pin.to_bytes(4, byteorder='little'), withResponse=True)
            self.pin = pin
            

class CometBlueController:
    def __init__(self, mac, pin, updateinterval, storetarget):
        self.lock = threading.RLock()
        self.updateinterval = updateinterval
        #storetarget = saves targetvalue in high target to handle off and heat mode correctly
        self.storetarget = storetarget
        self.device = CometBlue(mac, pin)
        self.lastupdated = 0
        self.state = dict()
        self.state['state'] = 'offline'

    def start(self):
        threading.Thread(target=self._update, daemon=True).start()
        
    def _update(self):
        while True:
            timetosleep = self._get_time_to_sleep_till_update()
            while timetosleep > 0:
                time.sleep(timetosleep)
                timetosleep = self._get_time_to_sleep_till_update()
            try:
                self._read_state()
            except Exception as e:
                self._handle_connecterror(e)

    def _get_time_to_sleep_till_update(self):
        return self.lastupdated + self.updateinterval - time.time()

    def _read_state(self):
        temperature = self.device.read_temperature()
        battery = self.device.read_battery()
        with self.lock:
            self.lastupdated = time.time()
            self.state['state'] = 'online'
            self.state['current_temperature'] = temperature['current_temperature']
            self.state['offset_temperature'] = temperature['offset_temperature']
            if temperature['target_temperature'] < 8:
                self.state['mode'] = 'OFF'
                self.state['target_temperature'] = temperature['target_high'] if self.storetarget else temperature['target_temperature']
            elif temperature['target_temperature'] > 28:
                self.state['mode'] = 'HEAT'
                self.state['target_temperature'] = temperature['target_high'] if self.storetarget else temperature['target_temperature']
            else:
                self.state['mode'] = 'ON'
                self.state['target_temperature'] = temperature['target_temperature']
            self.state['battery'] = battery
            self.state['timestamp'] = datetime.datetime.fromtimestamp(self.lastupdated).strftime("%Y-%m-%d %H:%M:%S")

    def _handle_connecterror(self, e):
        with self.lock:
            self.lastupdated = time.time()
            self.state['state'] = 'offline'
            self.state['timestamp'] = datetime.datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")
        print(e)
        self.device.disconnect()


    def set_target_temperature(self, temperature):
        temperature = max(8, min(28, temperature))
        target_temperatur = None
        target_high = None
        with self.lock:
            if self.state['mode'] == "ON":
                target_temperatur = temperature
            if self.storetarget:
                target_high = temperature
        try:
            if target_temperatur is not None or target_high is not None:
                self.device.set_target_temperature(temperature=target_temperatur, temperature_high=target_high)
            self._read_state()
        except Exception as e:
            self._handle_connecterror(e)

    def set_mode(self, mode):
        target_temperatur = None
        with self.lock:
            if mode.lower() == self.state['mode'].lower():
                return
            if mode.lower() == "off":
                target_temperatur = 7.5
            elif mode.lower() == "heat":
                target_temperatur = 28.5
            else:
                if self.storetarget:
                    target_temperatur = self.state['target_temperature']
                else:
                    target_temperatur = 21
        try:
            if target_temperatur is not None:
                self.device.set_target_temperature(temperature=target_temperatur)
            self._read_state()
        except Exception as e:
            self._handle_connecterror(e)
        
    def get_state(self):
        with self.lock:
            return self.state.copy()

    def set_pin(self, pin):
        self.device.set_pin(int(pin))

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
            if 'storetarget' in data:
                storetarget = data['storetarget']
            else:
                storetarget = false
            self.dev[name] = CometBlueController(data['mac'], data['pin'], updateinterval, storetarget)
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
        if method == "target_temperature":
            self.dev[device_name].set_target_temperature(float(valueAsString))
        elif method == "mode":
            self.dev[device_name].set_mode(valueAsString)
        elif method == "pin" and valueAsString.startswith("PIN:"):
            self.dev[device_name].set_pin(valueAsString[4:])
            return [MqttMessage(topic=self.format_topic(device_name, 'updatedpin'), payload=valueAsString[4:], retain=False)]
        else:
            _LOGGER.warn("unknown method "+method)
            return []
        return self._get_mqtt_state_messages(device_name)
