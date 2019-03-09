from workers.base import BaseWorker
from mqtt import MqttMessage
from interruptingcow import timeout
from bluepy.btle import Peripheral
import logging
import threading
import time
import datetime
import sys

REQUIREMENTS = ['bluepy']

_LOGGER = logging.getLogger('bt-mqtt-gw.cometblue')

# maximum two parallel bluepy connections. will be set in setup
pool_cometblue = None

class CometBlue():
    def __init__(self, mac, pin):
        self.mac = mac
        self.pin = pin
        self.connection = None
        self.lock = threading.RLock()

    def disconnect(self):
        global pool_cometblue
        with self.lock:
            if self.connection == None:
                return
            try:
                _LOGGER.debug("disconnect from "+self.mac)
                self.connection.disconnect()
            finally:
                _LOGGER.debug("release free slot "+self.mac)
                pool_cometblue.release()
                self.connection = None

    def get_connection(self):
        global pool_cometblue
        with self.lock:
            if self.connection == None:
                _LOGGER.debug("wait for free slot "+self.mac)
                pool_cometblue.acquire()
                _LOGGER.debug("acquired slot "+self.mac)
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

    def set_offset_temperature(self, temperature):
        with self.lock:
            connection = self.get_connection()
            _LOGGER.debug("write offset temperatures to "+self.mac)
            temperature = max(-5, min(5, temperature))
            offset_temp = round(temperature * 2)
            if offset_temp < 0:
                offset_temp += 256
            data = bytes([0x80, 0x80, 0x80, 0x80, offset_temp, 0x80, 0x80])
            connection.writeCharacteristic(0x003f, data, withResponse=True)

    def set_target_temperature(self, temperature=None, temperature_high=None, temperature_low=None):
        with self.lock:
            connection = self.get_connection()
            _LOGGER.debug("write temperatures to "+self.mac)
            target_temp = 0x80
            target_high = 0x80
            target_low = 0x80
            if temperature is not None:
                temperature = max(7.5, min(28.5, temperature))
                target_temp = round(temperature * 2)
            if temperature_high is not None:
                temperature_high = max(8, min(28, temperature_high))
                target_high = round(temperature_high * 2)
            if temperature_low is not None:
                temperature_low = max(8, min(28, temperature_low))
                target_low = round(temperature_low * 2)
            data = bytes([0x80, target_temp, target_low, target_high, 0x80, 0x80, 0x80])
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
            _LOGGER.debug("set pin "+str(pin)+" to "+self.mac)
            self.connection.writeCharacteristic(0x0047, pin.to_bytes(4, byteorder='little'), withResponse=True)
            self.pin = pin
            
    def clear_automatic(self):
        """ clears every automatic switch rule in thermostate """
        with self.lock:
            connection = self.get_connection()
            _LOGGER.debug("clear rules "+self.mac)
            #Monday
            self.connection.writeCharacteristic(0x001f, bytes([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), withResponse=True)
            #Thuesday
            self.connection.writeCharacteristic(0x0021, bytes([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), withResponse=True)
            #Wednesday
            self.connection.writeCharacteristic(0x0023, bytes([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), withResponse=True)
            #Thursday
            self.connection.writeCharacteristic(0x0025, bytes([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), withResponse=True)
            #Friday
            self.connection.writeCharacteristic(0x0027, bytes([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), withResponse=True)
            #Saturday
            self.connection.writeCharacteristic(0x0029, bytes([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), withResponse=True)
            #Sunday
            self.connection.writeCharacteristic(0x002b, bytes([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), withResponse=True)
            #holiday 1
            self.connection.writeCharacteristic(0x002d, bytes([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]), withResponse=True)
            #holiday 2
            self.connection.writeCharacteristic(0x002f, bytes([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]), withResponse=True)
            #holiday 3
            self.connection.writeCharacteristic(0x0031, bytes([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]), withResponse=True)
            #holiday 4
            self.connection.writeCharacteristic(0x0033, bytes([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]), withResponse=True)
            #holiday 5
            self.connection.writeCharacteristic(0x0035, bytes([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]), withResponse=True)
            #holiday 6
            self.connection.writeCharacteristic(0x0037, bytes([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]), withResponse=True)
            #holiday 7
            self.connection.writeCharacteristic(0x0039, bytes([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]), withResponse=True)
            #holiday 8
            self.connection.writeCharacteristic(0x003b, bytes([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]), withResponse=True)

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
            failureCount = 0
            timetosleep = self._get_time_to_sleep_till_update()
            while timetosleep > 0:
                time.sleep(timetosleep)
                timetosleep = self._get_time_to_sleep_till_update()
            try:
                self._read_state()
                failureCount = 0
            except:
                failureCount += 1
                self._handle_connecterror(sys.exc_info()[0], failureCount >= 6)
                self._correct_last_update_after_error()

    def _correct_last_update_after_error(self):
        """ correct last update to check again in 10 seconds """
        with self.lock:
            self.lastupdated = time.time() + 10 - self.updateinterval

    def _get_time_to_sleep_till_update(self):
        with self.lock:
            return self.lastupdated + self.updateinterval - time.time()

    def _read_state(self):
        try:
            temperature = self.device.read_temperature()
            battery = self.device.read_battery()
        finally:
            self.device.disconnect()
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
                self.state['mode'] = 'AUTO' if temperature['target_high'] == temperature['target_temperature'] else 'MANUAL'
                self.state['target_temperature'] = temperature['target_temperature']
            self.state['battery'] = battery
            self.state['timestamp'] = datetime.datetime.fromtimestamp(self.lastupdated).strftime("%Y-%m-%d %H:%M:%S")

    def _handle_connecterror(self, e, markAsOffline):
        if markAsOffline:
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
            if self.state['mode'] == "AUTO" or self.state['mode'] == "MANUAL":
                target_temperatur = temperature
            if self.storetarget:
                target_high = temperature
        c=0
        while c < 10:
            try:
                if target_temperatur is not None or target_high is not None:
                    # set high and low target temperature to same value to prevent automatic switch by the thermostate
                    self.device.set_target_temperature(temperature=target_temperatur, temperature_high=target_high, temperature_low=target_high)
                self._read_state()
                return
            except:
                self._handle_connecterror(sys.exc_info()[0], False)
            c += 1

    def set_offset_temperature(self, temperature):
        temperature = max(-5, min(5, temperature))
        c=0
        while c < 10:
            try:
                self.device.set_offset_temperature(temperature)
                self._read_state()
                return
            except:
                self._handle_connecterror(sys.exc_info()[0], False)
            c += 1

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
        except:
            self._handle_connecterror(sys.exc_info()[0], False)

    def get_state(self):
        with self.lock:
            return self.state.copy()

    def set_pin(self, pin):
        self.device.set_pin(int(pin))
        self.device.disconnect()

    def clear_automatic(self):
        self.device.clear_automatic()
        self.device.disconnect()

class CometblueCommand:

    def __init__(self, method, value, device, device_name):
        self.method = method
        self.value = value
        self.device = device
        self.device_name = device_name
        self.thread = threading.Thread(target=self._perform, daemon=True)
        self.thread.start()

    def _perform(self):
        _LOGGER.debug("start command %s:%s", self.method, self.value)
        self.result = None
        if self.method == "target_temperature":
            self.device.set_target_temperature(float(self.value))
        elif self.method == "mode":
            self.device.set_mode(self.value)
        elif self.method == "pin" and self.value.startswith("PIN:"):
            self.device.set_pin(self.value[4:])
        elif self.method == "reset" and self.value == "reset":
            self.device.clear_automatic()
        elif self.method == "offset_temperature":
            self.device.set_offset_temperature(float(self.value))
        else:
            _LOGGER.warn("unknown method %s", self.method)
            self.result = []
        _LOGGER.debug("finished command %s:%s", self.method, self.value)

    def get_result(self):
        if not self.thread.is_alive():
            return self.result
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            return None
        return self.result


class CometblueWorker(BaseWorker):
    def _setup(self):
        global pool_cometblue
        self.dev = dict()
        _LOGGER.debug("configure maximum of "+str(self.maxbluetooth) +
                      " concurrent bluetooth connections")
        pool_cometblue = threading.Semaphore(self.maxbluetooth)
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
                storetarget = False
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
            ret.append(MqttMessage(topic=self.format_topic(
                name, key), payload=value, retain=True))
        return ret

    def on_command(self, topic, value):
        """ handles commands received via mqtt and returns new piblished topics after update """
        _, device_name, method, _ = topic.split('/')
        valueAsString = value.decode("UTF-8")
        ret = []
        if not device_name in self.dev:
            _LOGGER.warn("ignore unknown device "+device_name)
            return []
        _LOGGER.debug("received command %s:%s@%s", method, valueAsString, device_name)
        command = CometblueCommand(method, valueAsString, self.dev[device_name], device_name)
        result = command.get_result()
        _LOGGER.debug("send on_command result %s:%s=%s", topic, valueAsString, result)
        if result != None:
            return result
        return self._get_mqtt_state_messages(device_name)
