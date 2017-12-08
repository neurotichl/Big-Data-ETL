folder=conf_template/
conf_file=conf_template/flume.conf
agent_name=secret_agent

flume-ng agent \
-c $folder \
-f $conf_file \
-n $agent_name \
-Dflume.root.logger=INFO,console \
& echo $! $(date '+%F %X')> flume_pid.txt
