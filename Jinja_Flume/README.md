# Flume and Jinja2

### Fluming multiple sources (spoolDir) and sinks (HDFS) & generating flume config using Jinja 

This is a demo on using Jinja to generate flume config and flume data from local directory to HDFS.

# Introduction

Flume configs usually look like this:
```
# Define agent name, source name, sink name and channel name
agent.sources = src1
agent.sinks = sink1
agent.channels = ch1

# linking src1 and sink1 with ch1
agent.sources.src1 = ch1
agent.sinks.sink1 = ch1

##### configs for source, sink and channel ####
agent.sources.src1.... = ....
.....

agent.sinks.sink1..... = ....
.....

agent.channels.ch1...  = .....
.....
```

When adding more sources, sinks and channel to the same agent, the flume config become very long
```
# Define agent name, source name, sink name and channel name (multiple names are seperated with space bar)
agent.sources = src1 src2 src3
agent.sinks = sink1 sink2 sink3
agent.channels = ch1 ch2 ch3

# linking src1 and sink1 with ch1
agent.sources.src1 = ch1
agent.sinks.sink1 = ch1

##### configs for src1, sink1 and ch1 ####
agent.sources.src1.... = ....
..... = ....
agent.sinks.sink1..... = ....
..... = ....
agent.channels.ch1...  = .....
..... = ....

# linking src2 and sink2 with ch2
agent.sources.src2 = ch2
agent.sinks.sink2 = ch2

##### configs for src2, sink2 and ch2 ####
agent.sources.src1.... = ....
agent.sinks.sink1..... = ....
agent.channels.ch1...  = .....
..... = ....

##### configs for src3, sink3 and ch3 ####
.........
.........
```

Which is kind of repetitive to write it manually. Jinja has come to the rescued in generating repetitive templates

## Jinja Templating:

Documentation of Jinja can be found [here](http://jinja.pocoo.org/docs/2.10/).

Example of using Jinja templating syntax:

Template:
```
{%for name in source_names %}
{{ name }}
{% endfor %}
```

which source_names is a list
```
source_names = ['a','b','c']
```

Rendering the Template will give:
```
a
b
c
```

Hence Jinja can be used in generating the repetitive flume config. [SEE THE EXAMPLE CODE HERE!](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/Jinja_Flume/conf_template/gen_flume_conf.py#L47)

---

# Demo with an example

# Objective

We have a folder **files_to_flume** which contains 4 folders that we would like to flume to HDFS. 
- customer
- shop
- long_file
- file_not_utf8

#### Before Flume
   ![before flume](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/fluming_pic/before_flume.png)

#### After Flume
   ![after flume](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/fluming_pic/after_flume.PNG)

# Folder Structure

```
Jinja_Flume
+-- conf_template
|   +-- flume_source_sink.ini
|   +-- flume.conf
|   +-- gen_flume_conf.py
+-- gen_flume_conf.sh
+-- run_flume.sh
+-- flume_pid.txt
+-- kill_flume.sh
```
# General Flow

1. Add required information about the sources and sinks in `conf_template/flume_source_sink.ini`
2. Run `gen_flume_conf.sh` to generate the flume config 
3. Run `run_flume.sh` to start fluming, flume process ID can be checked in `flume_pid.txt`
4. To kill flume, run `kill_flume.sh`

# Explanation


##### *gen_flume_conf.sh*
To run `gen_flume_conf.py` and write the result to `flume.conf`

##### *gen_flume_conf.py* 
py file containing Jinja template in generating flume config. Read source and sink paths from `flume_source_sink.ini` to fill in the template.

##### *run_flume.sh*
To run the flume and redirect the process ID to `flume_pid.txt`. folder, config file name and flume agent name as a variable.

##### *kill_flume.sh*
To read the pid from `flume_pid.txt` and kill the flume process.


# Some Tips 

1. When adding a new source -> sink, the flume process don't have to be stopped, just regenerate the `flume.conf` and flume will auto reload it.

   - Before regenerating the flume.conf, running with 3 sources and 3 sinks:
   ![before updating flume.conf](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/fluming_pic/run_flume_1.PNG)
   - After updating the flume.conf
   ![after adding new source and sink in flume.conf](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/fluming_pic/reload_flume.PNG)

2. If the file is not utf-8 encoded, the value of `inputCharset` need to be set in `flume.conf`. 
   If it is **NOT** SET, flume will show *java.nio.charset.MalformedInputException* ERROR
   ![Encoding ERROR](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/fluming_pic/encoding_error.PNG)
   Can use library such as `chardet` from python to detect the type of encoding of the file(which is windows-1252):
   ![chardet](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/fluming_pic/chardet.PNG)

3. If the length of the lines in the file is too long, exceeding 2048 number of characters, flume will only show WARN log and truncate the line to next line(s). 
   ![WARN message](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/fluming_pic/line_too_long.PNG)
  Hence it is very important to check the max number of character of the line in the file and set it in the flume config's `deserializer.maxLineLength`  before starting a flume.  (Use awk`{print length} to check)
    ![Checking line length using linux command](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/fluming_pic/max_length_line.PNG)


