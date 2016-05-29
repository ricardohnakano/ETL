import subprocess
import os.path
from datetime import datetime
from datetime import timedelta

DATA_DIR = "/home/voliveira/tripda-data-warehouse/data/"
#DATA_DIR = "/home/rnakano/DW/currency/"

i = datetime.now()
i = i -timedelta(days = 11)

lista = []
for counter in range(0,30):
    i = i -timedelta(days = 1)
    ReferenceDate = i.strftime('%Y-%m-%d')
    lista.append(DATA_DIR + "currency/currency_" + ReferenceDate + ".csv")

for files in lista:
    if os.path.isfile(files) == True:
        subprocess.call("rm " + files, shell=True)