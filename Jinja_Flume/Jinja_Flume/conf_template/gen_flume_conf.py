from configparser import RawConfigParser
from jinja2 import Template
from sys import stdout,argv

text = '''
{{ agent }}.sources = {% for name in dirdict %}{{ name }}_src {% endfor %}
{{ agent }}.sinks = {% for name in dirdict %}{{ name }}_sink {% endfor %}
{{ agent }}.channels = {% for name in dirdict %}{{ name }}_ch {% endfor %}
{% for name, info in dirdict.items() %}
{{ agent }}.sources.{{ name }}_src.channels = {{ name }}_ch
{{ agent }}.sinks.{{ name }}_sink.channel = {{ name }}_ch

###############################################################
{{ agent }}.sources.{{ name }}_src.type = spooldir
{{ agent }}.sources.{{ name }}_src.spoolDir = {{ info['spoolDir']}}
{{ agent }}.sources.{{ name }}_src.basenameHeader = true
{% if 'maxLineLength' in info.keys() %}{{ agent }}.sources.{{ name }}_src.deserializer.maxLineLength = {{ info['maxLineLength']}}
{% elif 'inputCharset' in info.keys() %}{{ agent }}.sources.{{ name }}_src.inputCharset = {{ info['inputCharset']}}
{% else %}{% endif %}
{{ agent }}.sinks.{{ name }}_sink.type = hdfs
{{ agent }}.sinks.{{ name }}_sink.hdfs.path = {{ info['HDFSDir'] }}
{{ agent }}.sinks.{{ name }}_sink.hdfs.fileType = DataStream
{{ agent }}.sinks.{{ name }}_sink.hdfs.writeFormat = Text
{{ agent }}.sinks.{{ name }}_sink.hdfs.minBlockReplicas = 1
{{ agent }}.sinks.{{ name }}_sink.hdfs.rollInterval = 0
{{ agent }}.sinks.{{ name }}_sink.hdfs.rollSize = 0 
{{ agent }}.sinks.{{ name }}_sink.hdfs.rollCount = 0
{{ agent }}.sinks.{{ name }}_sink.hdfs.idleTimeout = 5
{{ agent }}.sinks.{{ name }}_sink.hdfs.filePrefix = %{basename}

{{ agent }}.channels.{{ name }}_ch.type = memory
###############################################################
###############################################################
{% endfor %}
'''
if len(argv)>1:
    config = argv[1]
else:
    print('#Set Config File to flume_source_sink.ini')
    config = 'conf_template/flume_source_sink.ini'

config_info = RawConfigParser()
config_info.optionxform = str

config_info.read(config)
dirdict = {name: dict(info) for name, info in dict(config_info).items() if name!='DEFAULT'}
agent = config_info['DEFAULT']['agent'] 
flume_template = Template(text)
rt = flume_template.render(agent=agent, dirdict=dirdict)
stdout.write(rt)
