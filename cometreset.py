#!/usr/bin/env python3

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
import argparse

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
                try:
                    self.connection = Peripheral(self.mac, "public")
                except:
                    _LOGGER.debug("release free slot "+self.mac)
                    pool_cometblue.release()
                    raise
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
            _LOGGER.debug("clear rules "+self.mac)
            #monday to sunday
            for id in range(0x001f, 0x002c, 2):
                try:
                    connection = self.get_connection()
                    _LOGGER.debug(f"clear day {id} for {self.mac}")
                    connection.writeCharacteristic(id, bytes([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]), withResponse=True)
                except BTLEDisconnectError:
                    self.disconnect()

            #holiday 1 till 7
            for id in range(0x002d, 0x003c, 2):
                try:
                    connection = self.get_connection()
                    _LOGGER.debug(f"clear holiday {id} for "+self.mac)
                    connection.writeCharacteristic(id, bytes([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80]), withResponse=True)
                except BTLEDisconnectError:
                    self.disconnect()


def reset(mac, pin):
    cometblue = CometBlue(mac, pin)
    cometblue.clear_automatic()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mac")
    parser.add_argument("pin")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)
    pool_cometblue = threading.Semaphore(1) 
    reset(args.mac, int(args.pin))
