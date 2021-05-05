from home_energy_nyc.co2_utils import co2_calc
import pandas as pd
import logging
from coned import Meter
import asyncio
import json
from influxdb import DataFrameClient


_logger = logging.getLogger(__name__)


def catch_external_errors(func):
    def wrapper(*args, **kwargs):
        try:
            val = func(*args, **kwargs)
            return val
        except BaseException as E:
            _logger.error('Error in ' + func.__name__ + '() function: ' + str(E))
            return None
    return wrapper


class Measurement(object):

    def __init__(self, name: str, unit: str, tags: dict = None, database: str = None, time_precision: str = 's'):
        super(Measurement, self).__init__()
        self.name = name
        self.tags = {} if tags is None else tags
        self.tags['unit'] = unit
        self.df = pd.DataFrame
        self.database = database
        self.time_precision = time_precision


class Connection(object):

    def __init__(self, **kwargs):
        pass

    @classmethod
    def from_config(cls, conf):
        kwargs = {s: v for s, v in conf[cls.__name__].items()}
        return cls(**kwargs)


class InfluxDBWriter(Connection):

    def __init__(self, host: str = None, port: int = None, username: str = None, password: str = None):
        """
        A class to write measurements to an InfluxDB time-series database.
        """
        super(InfluxDBWriter, self).__init__()
        self.client = DataFrameClient(
            host=host,
            port=port,
            username=username,
            password=password)

    @catch_external_errors
    def write_measurement(self, msr: Measurement):
        self.client.write_points(dataframe=msr.df,
                                 measurement=msr.name,
                                 tags=msr.tags,
                                 time_precision=msr.time_precision,
                                 database=msr.database)
        _logger.info('{measurement} successfully written to database.'.format(measurement=msr.name))


class Reader(Connection):

    def __init__(self):
        super(Reader, self).__init__()
        self._measurements = dict()

    def __getitem__(self, key):
        return self._measurements.__getitem__(key)

    def measurements(self):
        return self._measurements.values()

    def values(self):
        return self._measurements.values()

    def items(self):
        return self._measurements.items()

    def keys(self):
        return self._measurements.keys()

    def query(self):
        pass


class NYISOReader(Reader):

    URL = 'http://mis.nyiso.com/public/csv/rtfuelmix/{date}rtfuelmix.csv'
    UNIT_FMIX = 'MW'
    UNIT_EMIS = 'kg/MWh CO2 equivalent'
    KEY_FMIX = 'fuel_mix'
    KEY_EMIS = 'emissions'

    def __init__(self,
                 measure_name_fmix: str,
                 measure_name_emis: str,
                 database: str):
        """
        A class to download and hold fuel mix data from the NYISO website and calculated grid carbon intensity.
        :param measure_name_fmix: A name for the fuel mix measurement when written to InfluxDB
        :param measure_name_emis: A name for the emissions measurement when written to InfluxDB
        :param database:     Name of the InfluxDB database to which measurements should be written.
        """
        super().__init__()
        self._measurements[self.KEY_FMIX] = Measurement(name=measure_name_fmix,
                                                        unit=self.UNIT_FMIX,
                                                        database=database)
        self._measurements[self.KEY_EMIS] = Measurement(name=measure_name_emis,
                                                        unit=self.UNIT_EMIS,
                                                        database=database)

    def query(self):
        """Query the NYISO website for data since midnight on the day specified."""
        self._measurements[self.KEY_FMIX].df = self.fetch_data_fmix()
        self._measurements[self.KEY_EMIS].df = self.co2_calc(self._measurements[self.KEY_FMIX].df)

    @catch_external_errors
    def fetch_data_fmix(self):
        today = pd.Timestamp('today', tz='US/Eastern')
        yesterday = today - pd.offsets.Day(1)
        df = pd.concat(
            [
                pd.read_csv(self.URL.format(date=yesterday.strftime('%Y%m%d'))),
                pd.read_csv(self.URL.format(date=today.strftime('%Y%m%d')))
            ]
        )
        df['Gen MW'] = df['Gen MW'].astype(int)
        df['Time Zone'] = df['Time Zone'].map(
            {
                'EDT': '-0400',
                'EST': '-0500'
            }, na_action=None)
        df['Time Stamp'] = df['Time Stamp'] + df['Time Zone']
        df = df.pivot(index='Time Stamp', columns='Fuel Category', values='Gen MW')
        df.index = pd.to_datetime(df.index)  # Why is this an unexpected type?

        df['Natural Gas'] = df['Dual Fuel'] + df['Natural Gas']  # Dual fuel plant almost exclusively burns gas
        df = df.drop(['Dual Fuel'], axis=1)

        last_timestamp = df.index.max()
        first_timestamp = last_timestamp - pd.offsets.Day(1)  # To returns that past 24 hours of data
        return df[first_timestamp:last_timestamp]

    @staticmethod
    def co2_calc(df: pd.DataFrame):
        return co2_calc(df)


class ConEdReader(Reader):

    UNIT = 'kWh'
    KEY_USAGE = 'usage'

    def __init__(self,
                 email: str,
                 password: str,
                 mfa_secret: str,
                 account_uuid: str,
                 meter_number: str,
                 measure_name: str,
                 database: str,
                 browser_path: str = None  # In some systems the packaged browser will not function.
                 ):
        """
        A class to download and hold personal electricity usage meter readings from the ConEd
        website.
        :param email:       The email address of the user's ConEd account
        :param password:    The password of the user's ConEd account
        :param mfa_secret:  To set up a MFA secret, go to coned.com, log in and navigate to profile.
                            Disable MFA and  re-enable. Choose Google Authenticator or okta and choose a device type.
                            When presented with the QR code, click on "Can't scan?".
                            It should provide you with the MFA secret.
        :param account_uuid: The account uuid. To find the account uuid, log into coned.com then use the browser
                             developer tools to search for `uuid` in the network tab.
        :param meter_number: The user's meter number appearing on their bill
        :param measure_name: A name for the measurement when written to InfluxDB
        :param database:     Name of the InfluxDB database to which ConEd measurements should be written.
        """

        super(ConEdReader, self).__init__()
        self.email = email
        self.password = password
        self.mfa_secret = mfa_secret
        self.account_uuid = account_uuid
        self.meter_number = meter_number
        self.browser_path = browser_path

        self._measurements[self.KEY_USAGE] = Measurement(name=measure_name,
                                                         unit=self.UNIT,
                                                         database=database)

    def query(self):
        """Query the ConEd website for the past 24 hours' data."""
        self._measurements[self.KEY_USAGE].df = self.fetch_data_usage()

    @catch_external_errors
    def fetch_data_usage(self):
        loop = asyncio.get_event_loop()
        meter = Meter(
            email=self.email,
            password=self.password,
            mfa_type='TOTP',
            mfa_secret=self.mfa_secret,
            account_uuid=self.account_uuid,
            meter_number=self.meter_number,
            loop=loop,
            browser_path=self.browser_path
        )
        loop.run_until_complete(meter.browse())
        loop.close()

        data = json.loads(meter.raw_data)
        if not data:
            _logger.error('Parse error in ConEdReader: No data received')
            return None
        elif 'reads' not in data:
            _logger.error('ConEdReader: Incorrect data received: {data}'.format(data=data))
        else:
            rows = dict()
            for dct in data['reads']:
                if dct['value'] is not None:
                    rows[dct['endTime']] = {'electrical_energy': dct['value']}
            df = pd.DataFrame.from_dict(rows, orient='index')
            df.index = pd.to_datetime(df.index)  # Why does this throw a warning?
            return df
