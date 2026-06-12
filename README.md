# NSHM Database Generation

This repository contains a script and schema to generate a database storing fault geometry and rupture information parsing GeoJSON information from the GNS National Seismic Hazard Model.

## Requirments 

- Python 3.x
- numpy
- qcore
- sqlite3

## Obtain the database
You likely don't need to obtain your own database, as they are published on Dropbox at `/QuakeCoRE/Public/NSHM` with every version release. Simply download that file and use it with the package:

``` python
from nshmdb.nshmdb import NSHMDB

db = NSHMDB('nshmdb_v2026.06.1.db') # or whatever the latest version is.
```

## Generate your own database
After installing this package you simply run
```bash
nshmdb 1.0.4 nshmdb.db --api-key API_KEY_HERE
```
