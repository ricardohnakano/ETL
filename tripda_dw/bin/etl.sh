#!/bin/sh

/usr/bin/python /home/voliveira/tripda-data-warehouse/bin/users-extract-transform.py
/usr/bin/python /home/voliveira/tripda-data-warehouse/bin/users-load.py
/usr/bin/python /home/voliveira/tripda-data-warehouse/bin/bookings-extract-transform.py
/usr/bin/python /home/voliveira/tripda-data-warehouse/bin/bookings-load.py
/usr/bin/python /home/voliveira/tripda-data-warehouse/bin/trips-extract-transform.py
/usr/bin/python /home/voliveira/tripda-data-warehouse/bin/trips-load.py
/usr/bin/python /home/voliveira/tripda-data-warehouse/bin/cube-extract-transform.py
/usr/bin/python /home/voliveira/tripda-data-warehouse/bin/cube-load.py
/bin/bash /home/voliveira/UploadTool/bin/UploadFromDB.sh vitor.oliveira@tripda.com.br -A 8caebe0b55b6b68a1c091f3c113eca3f
