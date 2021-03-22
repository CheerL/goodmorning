#! /bin/sh
if [ $1 = 1 ]
then
    python3 watcher.py master
elif [ $1 = 2 ]
then
    python3 watcher.py sub
else
    python3 dealer.py
fi