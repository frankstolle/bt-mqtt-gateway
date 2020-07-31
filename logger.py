import logging

import sys

_LOGGER = logging.getLogger('bt-mqtt-gw')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(threadName)s] %(name)s %(message)s"))
_LOGGER.addHandler(handler)
