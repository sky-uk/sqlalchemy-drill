# Apache Drill dialect for SQLAlchemy
The primary purpose of this is to have a working dialect for Apache Drill that can be used with Superset 

https://github.com/airbnb/superset

Obviously, a working, robust dialect for Drill serves other purposes as well, but most of the iterative planning for this REPO will be based on working with Superset. Other changes will gladly be incorporated, as long as it doesn't hurt Superset integration. 

## Current Status/Development Approach
Currently we can connect to drill, and issue queries for basic visualizations and get results. We also ennumerate table columns for some times of tables. Here are things that are working as some larger issues to work out. (Individual issues are tracked under issues)

* Connection to Drill via the databases tab in Superset succeeeds
  * You need to specify schema in your connection string because caravel lists tables and will error if you don't have a default schema identified. Tracked in issue: https://github.com/JohnOmernik/sqlalchemy-drill/issues/4
  * Table lists in the connection for some reason only show the first two qualifiers of drill tables.  So if in the schema dfs.root you have two tables, table1 and table2, instead of showing table1 and table2 or dfs.root.table1 and dfs.root.table2, it will just show dfs.root and dfs.root.  This is tracked in issue: https://github.com/JohnOmernik/sqlalchemy-drill/issues/3
* You can add tables under the tables menu
  * Columns are listed properly from the table - However the their types are not all are returned as VARCHAR Tracked here: https://github.com/JohnOmernik/sqlalchemy-drill/issues/10
* You can do basic queries for certain types of viz/tables
  * More advanced queries/joins appear to have issues. As you learn about new ones, please track in issues

So, we are "limping along" and working as is, but contribution and just testing/use to identify issues would be very welcome! 

## Unit tests
The unit tests include two types:

- Base: tests the functions of the main standalone class.  
- Alchemy: tests the package via SQLAlchemy module.  

At first, you must install the package and SQLAlchemy:
```shell
python setup.py install
pip install SQLAlchemy
```
At last, to start the unit tests, type the following command:
```shell
HOST=<drillhost> PORT=<drillport> DB=<workspace> FILE=<workspace_file> SSL=<if ssl> USERNAME=<drill username> PASSWORD=<drill password> python -m unittest -v -f test.base test.alchemy
```
Example:
```shell
HOST=localhost \
PORT=8047 \
DB=dfs.drilldbs \
FILE=region.parquet
SSL=False \
USERNAME=your_username \
PASSWORD=your_password \
python -m unittest -v -f test.base test.alchemy
```

## Getting started
### SQLAlchemy example configuration
To use Apache Drill dialect for SQLAlchemy:
```python
from sqlalchemy import create_engine

# Define the apache drill connection string parameters
drillurl = 'drill+sadrill://<username>:<password>@<drillhost>:<drillport>/dfs.yourdb?use_ssl=True'

# Create SQLAlchemy drill engine
engine = create_engine(drillurl, echo=False)
conn = engine.connect()

rs = self.conn.execute("SELECT * FROM dfs.yourdb LIMIT 10")
for row in rs:
    print(row)
```

### Superset setup
Much of the this readme is wrong right now. I need to do updates
Right now all you need to do is python3 setup.py install this on the box you will be runnign super set on then create a URL that looks like this:

To make it works with superset you must add the following string inside the field SQLAlchemy URI on the database section (`/databaseview/add`):
```
drill+sadrill://<username>:<password>@<drillhost>:<drillport>/dfs.yourdb?use_ssl=True
```

### Many thanks
Many thanks to drillpy and pydrill for code used increating the drilldbapi.py code for connecting!
