import logging

format = '%(funcName)s %(message)s'
levels = [logging.WARNING, logging.INFO, logging.DEBUG]

logging.basicConfig(level=levels, format=format)