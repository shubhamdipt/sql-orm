# sql-orm
A simple python ORM (Object relational mapping) which can be used similar to Django ORM and can be easily integrated to any python project.

This ORM can be adapted easily to existing database as well (without even using any migrations). 
It can also be integrated with both MySQL and PostgreSQL in the same project.

### Dependencies

* Python3

### Installation

    $pip install sql-orm

### Set up

Create a directory named db_models and a file named models.py.

Create models in models.py similar to the properties of the columns used in your database.  

Create a config file for connecting to the database.

    [POSTGRESQL]
    DB_HOST = localhost
    DB_PORT = 5432
    DB_NAME = dbname
    DB_USER = user
    DB_PASSWORD = password
    DEBUG = True


Set DEBUG = True only if you wish to see the SQL queries.

If you want to do create the tables as well, create a migrate.py file using: https://github.com/shubhamdipt/sql-orm/blob/master/migrate.py

Sample models can be found in the GitHub repository.

### New features

* Support for schema (can be added as an attribute in the models. "public" is the default schema.)


    _schema = "public"  


* Query: get_or_none (similar to get_or_create)
* Negative indexing support for slicing queryset.
* For setting any ForeignKey, either assign Model object or just the primary key (both works).

#### Differences

* The primary key for every model needs to supplied explicitly.
* All the models should be in one file called models.py in db_models directory.
* verbose_name is optional for every field currently.

#### Missing features / Work in progress

* JSONField for PostgreSQL.
* ManyToMany relationship in models.
* Support for MySQL.
* Support for makemigrations.


Please feel free to contribute.
