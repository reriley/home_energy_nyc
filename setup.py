import pathlib
from setuptools import setup

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

setup(
    name='home_energy_nyc',
    version='0.1.2',
    description='A package for ConEd customers to take smart meter readings and upload to an instance of the InfluxDB'
                'time-series database. The module also includes classes for querying NYISO for coincident fuel mix data'
                'and estimating real-time grid carbon intensity.',
    long_description=README,
    long_description_content_type="text/markdown",
    url='https://github.com/reriley/home_energy_nyc',
    author='Richard Riley',
    author_email='rr.com@pm.me',
    license='MIT',
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
    ],
    packages=['home_energy_nyc'],
    package_data={'home_energy_nyc': ['database/*']},
    include_package_data=True,
    install_requires=['asyncio',
                      'numpy',
                      'influxdb',
                      'pandas',
                      'pyppeteer',
                      'pyotp',
                      'coned @ git+https://github.com/bvlaicu/coned.git#egg=coned',
                      'nypower @ git+https://github.com/reriley/nypower.git#nypower',
                      'nyisotoolkit @ git+https://github.com/m4rz910/NYISOToolkit.git#egg=nyisotoolkit',
                      ],
)
