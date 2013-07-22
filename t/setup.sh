#!/bin/bash
# set PGDATA environment
# Only in tests
PGDATA=`ps -ax  | grep -oP "(postgres|postmaster) -D (.*)" | awk '{ print $3}' | head -1`"/"
export PGDATA
