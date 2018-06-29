# python-amqp1.0-consumer
Simple Python amqp(s) 1.0 consumer for connectivity testing.

Supported Python: 3.5+

### Installation
This project uses Proton lib for amqp connectivity. Unfortunately proton package from pypi is packaged with 
missing/broken dependencies and it will not work. To fix it please install Proton from apt-get (working with Python3.5)

```bash
$ pip3.5 install -r requirements.txt
$ sudo apt-get install python-qpid-proton
```

For Python3.6 Proton can be compiled and installed from sources.


## Example use

Dump messages to stdout

```bash
$ python3.5 amqp_client.py -p processors.file.FileProcessor -Dindent=2 amqp(s)://<user>:<password>@<host>:<port>/<vhost,queue>
```

Store messages in a file

```bash
$ python3.5 amqp_client.py -p processors.file.FileProcessor -Dfile=log.txt -Dindent=2 amqp(s)://<user>:<password>@<host>:<port>/<vhost,queue>
```

Save messages in hourly directories:
```bash
$ python3.5 amqp_client.py -p processors.file.HourlyMultiFileProcessor -dir=log -Dindent=2 amqp(s)://<user>:<password>@<host>:<port>/<vhost,queue>
```