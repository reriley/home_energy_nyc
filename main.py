from logging.handlers import RotatingFileHandler
import logging
from home_energy_nyc.connections import ConEdReader, NYISOReader, InfluxDBWriter
import configparser


LOG_FILEPATH = r'home_energy_nyc.log'
CONFIG_FILEPATH = r'home_energy_nyc.conf'


def main():
    conf = configparser.ConfigParser()
    conf.read(CONFIG_FILEPATH)

    readers = (
        NYISOReader.from_config(conf),
        ConEdReader.from_config(conf)
    )
    writer = InfluxDBWriter.from_config(conf)
    for r in readers:
        r.query()
        for msr in r.measurements():
            writer.write_measurement(msr)


if __name__ == '__main__':
    _logger = logging.getLogger()
    _logger.setLevel(logging.INFO)
    _handler = RotatingFileHandler(LOG_FILEPATH, maxBytes=10 * 1024)
    _handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)-8s %(message)s'))
    _logger.addHandler(_handler)
    _logger.info('Script started.')
    main()
