from distutils.core import setup

setup(name='aioinflux3',
      version='0.0.2',
      description='influxdb python client working with asyncio',
      author='Alexander.Li',
      author_email='superpowerlee@gmail.com',
      url='https://github.com/ipconfiger/aio-influx',
      install_requires=[
          "httpx",
          "numpy"
      ],
      packages=['aioinflux3', ],
)
