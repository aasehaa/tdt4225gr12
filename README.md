# tdt4225gr12
## Group project in course TDT4225

### Very large distributed datavolumes

NOTE: You must be connected to NTNU VPN to run these files. 

#### Applying and removing data from database
To apply data you need to create tables by running ``instance.create_tables()`` and ``instance.apply_data()`` 
in the ````main()```` function. 

If you want to drop tables in the database you need to run only the ``instance-drop_tables()``, and remember to 
comment out ``apply_data()`` and ``create_tables()``. 

When you have marked what method to run, type
````
python3 insertData.py
````


#### Running sql queries
Running the command below will send all sql queries to database. 
````
python3 queryData.py
````
