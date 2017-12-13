# Flume and Jinja2
---
### Fluming multiple sources and sinks & generating flume config using Jinja 

This is a demo on using Jinja to generate flume config and flume data from local directory to HDFS.
We have a folder **files_to_flume** which contains 4 folders that we would like to flume to HDFS. 
- customer
- shop
- long_file
- file_not_utf8

- Before Flume
   ![before flume](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/fluming_pic/before_flume.png)

- After Flume
   ![after flume](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/fluming_pic/after_flume.PNG)

# Folder Structure
---
Jinja_Fllume

|--conf_template

|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--flume_source_sink.ini

|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--flume.conf

|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|--gen_flume_conf.py

|--gen_flume_conf.sh

|--run_flume.sh

|--flume_pid.txt

|--kill_flume.sh

# General Flow
---
1. Add required information about the sources and sinks in `conf_template/flume_source_sink.ini`
2. Run `gen_flume_conf.sh` to generate the flume config 
3. Run `run_flume.sh` to start fluming, flume process ID can be checked in `flume_pid.txt`
4. To kill flume, run `kill_flume.sh`

# Explanation
---


##### *gen_flume_conf.sh*
To run `gen_flume_conf.py` and write the result to `flume.conf`

##### *gen_flume_conf.py* 
py file containing Jinja template in generating flume config. Read source and sink paths from `flume_source_sink.ini` to fill in the template.

##### *run_flume.sh*
To run the flume and redirect the process ID to `flume_pid.txt`. folder, config file name and flume agent name as a variable.

##### *kill_flume.sh*
To read the pid from `flume_pid.txt` and kill the flume process.


# Some Tips 
---
When adding a new source -> sink, the flume process don't have to be stopped, just regenerate the `flume.conf` and flume will auto reload it.

e.g.
1. Before regenerating the flume.conf, running with 3 sources and 3 sinks:
   ![before updating flume.conf](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/fluming_pic/run_flume_1.PNG)
2. After updating the flume.conf
   ![after adding new source and sink in flume.conf](https://github.com/neurotichl/Big-Data-ETL/blob/master/Jinja_Flume/fluming_pic/reload_flume.PNG)

