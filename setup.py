import pathlib
from setuptools import setup

HERE = pathlib.Path(__file__).parent

README = (HERE / "README.md").read_text()

setup(
    name='home_energy_nyc',
    version='0.1.3',
    description='Tools for NYC residents to download their electricity consumption data and estimate their carbon'
                'footprint from NYISO and EPA data. ',
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
                      'nyisotoolkit @ git+https://github.com/m4rz910/NYISOToolkit.git#egg=nyisotoolkit',
                      ],
)