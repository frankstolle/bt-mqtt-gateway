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
            _LOGGER.debug("read temperatures from "+self.mac)
            ret = dict()
            data = self.get_connection().readCharacteristic(0x003f)
            ret['internal_current_temperature'] = int.from_bytes(bytearray([data[0]]), byteorder='little', signed=True)/2
            ret['offset_temperature'] = int.from_bytes(bytearray([data[4]]), byteorder='little', signed=True)/2
            ret["current_temperature"] = ret['internal_current_temperature'] + ret['offset_temperature']
            ret["target_temperature"] = int.from_bytes(bytearray([data[1]]), byteorder='little', signed=True)/2
            ret["target_low"] = int.from_bytes(bytearray([data[2]]), byteorder='little', signed=True)/2
            ret["target_high"] = int.from_bytes(bytearray([data[3]]), byteorder='little', signed=True)/2    
            ret["window_open_detection"] = data[5]
            ret["window_open_minutes"] = data[6]
            return ret

    def read_battery(self):
        with self.lock:
            _LOGGER.debug("read battery "+self.mac)
            data = self.get_connection().readCharacteristic(0x0041)
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
                temperature = self.device.read_temperature()
                battery = self.device.read_battery()
                with self.lock:
                    self.state['state'] = 'online'
                    self.state['current_temperature'] = temperature['current_temperature']
                    self.state['offset_temperature'] = temperature['offset_temperature']
                    self.state['target_temperature'] = temperature['target_temperature']
                    self.state['battery'] = battery
            except Exception as e:
                with self.lock:
                    self.state['state'] = 'offline'
                print(e)
                self.device.disconnect()
            time.sleep(self.updateinterval)

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
        for name, data in self.devices.items():
            state = self.dev[name].get_state()
            for key, value in state.items():
                ret.append(MqttMessage(topic=self.format_topic(name, key), payload=value))
        return ret
