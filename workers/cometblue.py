from workers.base import BaseWorker
from mqtt import MqttMessage
from interruptingcow import timeout
from bluepy.btle import Peripheral, BTLEDisconnectError
import logging
import threading
import time
import datetime
import sys
import traceback
import functools
import random

REQUIREMENTS = ['bluepy']

_LOGGER = logging.getLogger('bt-mqtt-gw.cometblue')

# maximum two parallel bluepy connections. will be set in setup
pool_cometblue = None

class CometBlue():
    def __init__(self, mac, pin, interfaces):
        self.mac = mac
        self.pin = pin
        self.connection = None
        self.interfaces = {}
        for interface in interfaces:
            self.interfaces[interface] = 0
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
            #FIXME: Frank: hier ggf. eine Wartezeit einbauen, da sich der chip nicht erholt oder das programm ggf. bis zum neuen versuch noch was macht?

    def get_connection(self):
        global pool_cometblue
        with self.lock:
            if self.connection == None:
                _LOGGER.debug("wait for free slot "+self.mac)
                pool_cometblue.acquire()
                interface = None
                try:
                    _LOGGER.debug("acquired slot "+self.mac)
                    interface = self._get_interface_to_connect()
                    _LOGGER.debug(f"connect to {self.mac}@hci{interface}")
                    self.connection = Peripheral(self.mac, "public", iface=interface)
                except:
                    _LOGGER.debug("release free slot "+self.mac)
                    pool_cometblue.release()
                    self._handle_connect_failure(interface)
                    raise
                try:
                    _LOGGER.debug("send pin to "+self.mac)
                    self.connection.writeCharacteristic(0x0047, self.pin.to_bytes(4, byteorder='little'), withResponse=True)
                except:
                    try:
                        _LOGGER.debug("send default pin to "+self.mac)
                        defaultpin = 0
                        self.connection.writeCharacteristic(0x0047, defaultpin.to_bytes(4, byteorder='little'), withResponse=True)
                        _LOGGER.debug("update pin for "+self.mac)
                        self.connection.writeCharacteristic(0x0047, self.pin.to_bytes(4, byteorder='little'), withResponse=True)
                    except:
                        self.disconnect()
                        self._handle_connect_failure(interface)
                        raise
                self._handle_connect_success(interface)
            return self.connection
    
    def _handle_connect_failure(self, failed_interface):
        if self.interfaces[failed_interface] > 0:
            self.interfaces[failed_interface] = 0
        self.interfaces[failed_interface] -= 1

    def _handle_connect_success(self, success_interface):
        for interface in self.interfaces:
            if self.interfaces[interface] < 0:
                self.interfaces[interface] = 0
        self.interfaces[success_interface] += 1

    def _get_interface_to_connect(self):
        best_count = functools.reduce(lambda result, value: max(result, value), map(lambda key:self.interfaces[key], self.interfaces))
        possible_interfaces = list(map(lambda key: key, filter(lambda key: self.interfaces[key]>=best_count-5, self.interfaces)))
        return possible_interfaces[random.randint(0, len(possible_interfaces)-1)]

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
            _LOGGER.debug("clear rules "+self.mac)
            #monday to sunday
            for id in range(0x001f, 0x002c, 2):
                try:
                    connection = self.get_connection()
                    _LOGGER.debug(f"clear day {id} for {self.mac}")
                    connection.writeCharacteristic(id, bytes([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), withResponse=True)
                except:
                    self.disconnect()
            #holiday 1 till 7
            for id in range(0x002d, 0x003c, 2):
                try:
                    connection = self.get_connection()
                    _LOGGER.debug(f"clear holiday {id} for {self.mac}")
                    connection.writeCharacteristic(id, bytes([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]), withResponse=True)
                except:
                    self.disconnect()

class CometBlueController:
    def __init__(self, mac, pin, updateinterval, storetarget, interfaces):
        self.lock = threading.RLock()
        self.updateinterval = updateinterval
        #storetarget = saves targetvalue in high target to handle off and heat mode correctly
        self.storetarget = storetarget
        self.device = CometBlue(mac, pin, interfaces)
        self.lastupdated = 0
        self.state = dict()
        #no updates to mqtt until known (online or offline)
        self.state['state'] = 'unknown'

    def start(self):
        threading.Thread(target=self._update, daemon=True).start()
        
    def _update(self):
        failureCount = 0
        while True:
            timetosleep = self._get_time_to_sleep_till_update()
            while timetosleep > 0:
                time.sleep(timetosleep)
                timetosleep = self._get_time_to_sleep_till_update()
            try:
                self._read_state()
                failureCount = 0
            except:
                failureCount += 1
                _LOGGER.debug("handle error for "+self.device.mac+" "+str(failureCount))
                # 30 = 30*10 sekunden = 5 Minuten
                self._handle_connecterror(sys.exc_info(), failureCount > 30)
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
        error = e[0]
        if isinstance(e[0], BTLEDisconnectError):
            _LOGGER.warn("got error on connection: BTLEDisconnectError")
        else:
            _LOGGER.warn(f"got error on connection: {e[0]}")
            traceback.print_exception(*e)
        self.device.disconnect()
        del e

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
                self._handle_connecterror(sys.exc_info(), False)
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
                self._handle_connecterror(sys.exc_info(), False)
            c += 1

    def set_real_temperature(self, temperature):
        if not "offset_temperature" in self.state:
            return
        if not "current_temperature" in self.state:
            return
        real_temp = round(temperature*2)/2
        current_temp = self.state["current_temperature"]
        current_offset = self.state["offset_temperature"]
        if real_temp == current_temp:
            _LOGGER.debug(f"skip update of offset temperature, because real temp is current temp: {real_temp}@{self.device.mac}")
            return
        new_offset = max(-5, min(5, real_temp - current_temp + current_offset))
        if new_offset == current_offset:
            _LOGGER.debug(f"skip update of offset temperature, because new offset is old offset: {new_offset} for {real_temp}@{self.device.mac}")
            return
        self.set_offset_temperature(new_offset)


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
            self._handle_connecterror(sys.exc_info(), False)

    def get_state(self):
        with self.lock:
            return self.state.copy()

    def set_pin(self, pin):
        self.device.set_pin(int(pin))
        self.device.disconnect()

    def clear_automatic(self):
        try:
            self.device.clear_automatic()
            self.device.disconnect()
        except:
            self._handle_connecterror(sys.exc_info(), False)

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
        elif self.method == "real_temperature":
            self.device.set_real_temperature(float(self.value))
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
        _LOGGER.debug("configure maximum of "+str(self.maxbluetooth) + " concurrent bluetooth connections")
        if not hasattr(self, "interfaces"):
            self.interfaces = [0]
        _LOGGER.debug(f"using interfaces {self.interfaces}")
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
            self.dev[name] = CometBlueController(data['mac'], data['pin'], updateinterval, storetarget, self.interfaces)
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
        if state["state"] != "unknown":
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
        _LOGGER.debug("received command %s:%s@%s", method, valueAsString, device_name)
        command = CometblueCommand(method, valueAsString, self.dev[device_name], device_name)
        result = command.get_result()
        _LOGGER.debug("send on_command result %s:%s=%s", topic, valueAsString, result)
        if result != None:
            return result
        return self._get_mqtt_state_messages(device_name)
