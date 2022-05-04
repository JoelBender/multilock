#!/bin/bash

LOGNAME=pytest-`date +"%Y-%m-%d-%H-%M-%S"`.log
pytest -v -o log_file_level=DEBUG -o log_file=$LOGNAME $@
