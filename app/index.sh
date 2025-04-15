#!/bin/bash
echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"

echo "Input file is :"
echo $1

hdfs dfs -rm -r /tmp/index
hdfs dfs -rm -r /index

mapred streaming \
  -files cql.py,tok.py,mapreduce/mapper1.py#mapper.py,mapreduce/reducer1.py#reducer.py \
  -archives .venv.tar.gz#.venv \
  -mapper ".venv/bin/python mapper.py" \
  -reducer ".venv/bin/python reducer.py" \
  -input "$1" \
  -output /tmp/index

mapred streaming \
  -files cql.py,tok.py,mapreduce/mapper2.py#mapper.py,mapreduce/reducer2.py#reducer.py \
  -archives .venv.tar.gz#.venv \
  -mapper ".venv/bin/python mapper.py" \
  -reducer ".venv/bin/python reducer.py" \
  -input /tmp/index \
  -output /index

hdfs dfs -ls /
