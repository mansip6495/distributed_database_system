import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    with openconnection.cursor() as c:
        create_table= ("CREATE TABLE %s(userid integer,movieid integer,rating real);"%(ratingstablename))
        c.execute(create_table)

    with openconnection.cursor() as c:
        f = open(ratingsfilepath, "r")
        for line in f:
            input=line.split("::")
            insert=("INSERT INTO %s VALUES(%s,%s,%s)"%(ratingstablename,input[0],input[1],input[2]))
            c.execute(insert)


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    partition=5/numberofpartitions
    # print(partition)
    range_start=partition
    range_end=range_start+partition
    with openconnection.cursor() as c:
        for i in range(numberofpartitions):
            create_table = ("CREATE TABLE range_part%s (LIKE %s);" % (i,ratingstablename))
            c.execute(create_table)
            if i==0:
                insert=("INSERT INTO range_part%s SELECT * FROM ratings WHERE rating>=0 and rating<=%s;"%(i,range_start  ))
                c.execute(insert)
            else:

                insert = ("INSERT INTO range_part%s SELECT * FROM ratings WHERE rating>%s and rating<=%s;" % (i, range_start,range_end))
                c.execute(insert)
                range_start=range_end
                range_end=range_start+partition


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    with openconnection.cursor() as c:
        for i in range(numberofpartitions):
            create_table = ("CREATE TABLE rrobin_part%s (LIKE %s);" % (i, ratingstablename))
            c.execute(create_table)
        c.execute("SELECT * FROM %s"%(ratingstablename))
        entries = c.fetchall()
        row=0
        for entry in entries:
            id = row%numberofpartitions
            insert = ("INSERT INTO rrobin_part%s VALUES(%s,%s,%s)" % (id,entry[0],entry[1],entry[2]))
            c.execute(insert)
            row+=1


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    with openconnection.cursor() as c:
        c.execute("SELECT COUNT(*) FROM ratings")
        count = int(c.fetchone()[0])
        c.execute(
            "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format(
                "rrobin_part"))
        partition_count = int(c.fetchone()[0])
        table_id = count%partition_count
        insert_ratings = ("INSERT INTO %s VALUES(%s,%s,%s)" % (ratingstablename, userid, itemid, rating))
        c.execute(insert_ratings)
        insert_rating_part = ("INSERT INTO rrobin_part%s VALUES(%s,%s,%s)" % (table_id, userid, itemid, rating))
        c.execute(insert_rating_part)


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):

    with openconnection.cursor() as c:
        insert_ratings = ("INSERT INTO %s VALUES(%s,%s,%s)" % (ratingstablename, userid, itemid, rating))
        c.execute(insert_ratings)
        c.execute(
            "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format(
                "range_part"))
        count = int(c.fetchone()[0])
        partition=5/count
        range_start = 0
        range_end = range_start + partition
        for i in range(count):
            if i==0:
                if rating >= range_start and  rating<=range_end:
                    table=i
                    break
                else:
                    range_start = range_end
                    range_end = range_start + partition
            else:
                if rating > range_start and  rating<=range_end:
                    table=i
                    break
                else:
                    range_start = range_end
                    range_end = range_start + partition

        insert_rating_part = ("INSERT INTO range_part%s VALUES(%s,%s,%s)" % (table, userid, itemid, rating))
        c.execute(insert_rating_part)


def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
