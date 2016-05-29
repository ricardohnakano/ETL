#!/bin/sh

/usr/bin/python /home/voliveira/tripda-data-warehouse/bin/extract-transform.py
/usr/bin/python /home/voliveira/tripda-data-warehouse/bin/load-users.py