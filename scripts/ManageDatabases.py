# -*- coding: utf-8 -*-
"""
Created on Mon Jun 27 16:42:34 2016

MANAGE HYDROCLIMAT DATABASE:
    + DATABASE FUNCTIONALITY
        - Connect database                              (complete)
        - Get database structure                        (complete)
        - Get database tables list                      (complete)
        - Create database                               (complete)
        - Drop database                                 (complete)
        - Copy database                                 (under construction)
    + TABLE FUNCTIONALITY
        - Get table structure                           (complete)
        - Create table                                  (under construction)
        - Drop table                                    (under construction)
        - Alter table                                   (under construction)
        - Update table                                  (under construction)
        - Delete table                                  (under construction)
        - Insert into table                             (under construction)

SUPPORTED DATABASES:
    + PostgreSQL 10.1
    + SQLITE DB (on work)
    + MONGO DB (future module)
    + Spark (future module)

REQUIREMENTS:
    + PostgreSQL 9.6
    + psycopg2 [python module]
    + SQL Alchemy [python module]

@author:    Andrés Felipe Duque Pérez
email:      andresfduque@gmail.com
"""

# Main imports
import sqlalchemy
import psycopg2
from sqlalchemy.exc import OperationalError, NoSuchModuleError, ProgrammingError


# Connect to server
def psql_server(db_driver, user, password, host='localhost', port='5432'):
    """Returns a connection and a metadata object:
            - Supported drivers: postgreSQL
    """

    engine = None
    e = None

    # Set PostgreSQL URL
    url = '{}://{}:{}@{}:{}/'
    url = url.format(db_driver, user, password, host, port)

    # Connect with the help of the PostgreSQL URL
    try:
        engine = sqlalchemy.create_engine(url, client_encoding='utf8')
        meta = sqlalchemy.MetaData(bind=engine)
        meta.reflect(engine)
    except OperationalError as e:
        return engine, str(e)
    except NoSuchModuleError as e:
        return engine, str(e)

    return engine, e


# Get databases in postgres server
def psql_dbs(user, password, host='localhost', port='5432'):
    """Returns a list of databases in a cluster
    """
    url = ("dbname='postgres' " + "user='" + user + "' host='" + host + "' password='" + password + "' port='" +
           port + "'")

    try:
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        cur.execute("""SELECT * from pg_database""")
        dbs = [i[0] for i in cur.fetchall() if i[0][:-1] !=
               'template' and i[0] != 'postgres']
        return dbs
    except OperationalError as e:
        return str(e)


# %% Connect to postgres database
def psql_conn(user, password, db_name, host='localhost', port='5432'):
    """Returns a database connection and metadata
    """
    # required modules
    import sqlalchemy

    engine = None
    meta = None
    e = None

    # Set PostgreSQL URL
    url = '{}://{}:{}@{}:{}/{}'
    url = url.format('postgres', user, password, host, port, db_name)

    # Connect with the help of the PostgreSQL URL
    try:
        engine = sqlalchemy.create_engine(url, client_encoding='utf8')
        meta = sqlalchemy.MetaData(bind=engine)
        meta.reflect(engine)

    except OperationalError as e:
        return engine, meta, str(e)

    except NoSuchModuleError as e:
        return engine, meta, str(e)

    return engine, meta, e


# %% Get postgres database structure
def psql_structure(user, password, db_name, host='localhost', port='5432'):
    """Get postgres database structure [tables and columns]
    """

    # Connect to database
    engine, meta, e = psql_conn(user, password, db_name, host, port)

    # Print database structure
    for table in meta.sorted_tables:
        print(table.name.upper())
        for column in table.c:
            print('    ', column.name, '-->', column.type)


# %% Get postgres tables list
def psql_table_list(user, password, db_name, host, port):
    """Get database tables
    """
    # Connect to database
    con, meta = psql_conn(user, password, db_name, host, port)

    # Print database structure
    for table in meta.sorted_tables:
        print(table.name.upper())


# %% Create database
def psql_create_db(user, password, new_db_name, host='localhost', port='5432'):
    """
        Create new Postgres database [if it doesn't exist]
    """
    # required modules
    import sqlalchemy

    # Set PostgreSQL URL
    url = '{}://{}:{}@{}:{}/{}'
    url = url.format('postgres', user, password, host, port, 'postgres')

    # error
    engine = None
    e = None

    try:
        # Connect to the default database
        engine = sqlalchemy.create_engine(url, client_encoding='utf8')
        meta = sqlalchemy.MetaData(bind=engine)
        meta.reflect(engine)

        # Create new database via command
        cnx = engine.connect()
        cnx.execute('commit')
        create_db = 'create database ' + new_db_name
        cnx.execute(create_db)
        cnx.close()

    except OperationalError as e:
        return engine, str(e)
    except NoSuchModuleError as e:
        return engine, str(e)
    except ProgrammingError as e:
        return engine, str(e)

    return engine, e


# %% Create database
def sqlite_create_db(db_path, db_name):
    """
        Create new Sqlite3 database
    """
    # required modules
    import sqlalchemy
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    if db_path:
        url = 'sqlite:///' + db_path + db_name
    else:
        url = 'sqlite:///' + db_name

    try:
        # Create new database via command
        engine = sqlalchemy.create_engine(url, echo=False)
        Base.metadata.create_all(engine)
        print('!!! New SQLITE3 database "', db_name, '" created successfully!!!')
    except OperationalError as e:
        print(e)

    except NoSuchModuleError as e:
        print(e)

    except ProgrammingError as e:
        print(e)


# %% Drop database
def psql_drop_db(user, password, del_db_name, host='localhost', port='5432'):
    """
        Create new database [if don't exists]
            - Divers: postgres......(on progress)
    """
    # required modules
    import sqlalchemy

    # Set PostgreSQL URL
    url = '{}://{}:{}@{}:{}/{}'
    url = url.format('postgres', user, password, host, port, 'postgres')

    try:
        # Connect to the default database
        con = sqlalchemy.create_engine(url, client_encoding='utf8')
        meta = sqlalchemy.MetaData(bind=con)
        meta.reflect(con)

        # Create new database via command
        cnx = con.connect()
        cnx.execute('commit')
        create_db = 'drop database ' + del_db_name
        cnx.execute(create_db)
        cnx.close()

    except OperationalError as e:
        return str(e)

    except NoSuchModuleError as e:
        return str(e)

    except ProgrammingError as e:
        return str(e)


# %% Get table structure
def psql_table_structure(user, password, db_name, tb_name, host='localhost', port='5432'):
    """
        Get database structure [tables and columns]
            - Divers: postgres......(on progress)
    """

    # Connect to database
    con, meta = psql_conn(user, password, port, db_name, host)

    if con:
        try:
            # Print database structure
            table = meta.tables[tb_name]
            print(tb_name.upper())
            for column in table.c.items():
                print(column)
        except KeyError:
            print('(KeyError) FATAL: table "', tb_name, '" does not exist')
