'''
@author-name: Rishab Katta

Python program for loading IMDB database from Official IMDB's Datasets and perform querying on them.
'''

import psycopg2
import gzip
import shutil
import time


class DatabaseConnection:

    def __init__(self, h, db, username, pwd):
        '''
        Constructor is used to connect to the database
        :param h: hostname
        :param db: database name
        :param username: Username
        :param pwd: password
        '''
        try:
            self.connection = psycopg2.connect(host=str(h), database=str(db), user=str(username), password=str(pwd))
            self.connection.autocommit=True
            self.cursor=self.connection.cursor()
        except Exception as e:
            print(getattr(e, 'message', repr(e)))
            print(getattr(e, 'message', str(e)))

    def create_tables(self):
        '''
        Create tables for the IMDB database
        :return: None
        '''


        self.cursor.execute("CREATE TABLE Movie(id VARCHAR(20), type VARCHAR(100), title VARCHAR(500), "
                            "originalTitle VARCHAR(500), "
                            "startYear VARCHAR(10),endYear VARCHAR(10), runtime VARCHAR, "
                            "avgRating float, numVotes INT, PRIMARY KEY(id))")

        self.cursor.execute("CREATE temporary TABLE t1(id VARCHAR(20), type VARCHAR(20), "
                            "title VARCHAR(500), originalTitle VARCHAR(500), isAdult VARCHAR(20), "
                            "startYear VARCHAR(10), endYear VARCHAR(10), runtime VARCHAR , genres VARCHAR(500))")

        self.cursor.execute("CREATE temporary TABLE r1(id VARCHAR(20), avgRating float, "
                            "numVotes INT, PRIMARY KEY(id))")

        self.cursor.execute("CREATE TABLE Genre(id SERIAL, genre VARCHAR(500), PRIMARY KEY(id))")


        self.cursor.execute("CREATE TABLE Movie_Genre(genre INT, movie VARCHAR(20), "
                            "FOREIGN KEY(genre) REFERENCES Genre(id), FOREIGN KEY(movie) REFERENCES Movie(id))")

        self.cursor.execute("CREATE temporary TABLE t(id VARCHAR(20),name VARCHAR(500) NOT NULL,"
                            "birthYear VARCHAR(10), deathYear VARCHAR(10), primaryProfession VARCHAR(100), "
                            "knownForTitles VARCHAR(100), PRIMARY KEY (id))")

        self.cursor.execute("CREATE TABLE Member(id VARCHAR(20), name VARCHAR(500), birthYear VARCHAR(10), "
                            "deathYear VARCHAR(10), PRIMARY KEY(id))")

        self.cursor.execute("CREATE temporary TABLE t2(tconst VARCHAR(20), ordering VARCHAR(10),"
                            "nconst VARCHAR(20),category VARCHAR(100), job VARCHAR(500), charactersPlayed VARCHAR(500))")

        self.cursor.execute("CREATE TABLE Movie_Actor(actor VARCHAR(20), movie VARCHAR(20), UNIQUE(actor, movie), "
                            "FOREIGN KEY(actor) REFERENCES Member(id), FOREIGN KEY(movie) REFERENCES Movie(id))")

        self.cursor.execute("CREATE TABLE Movie_Writer(writer VARCHAR(20), movie VARCHAR(20), "
                            "FOREIGN KEY(writer) REFERENCES Member(id), FOREIGN KEY(movie) REFERENCES Movie(id))")

        self.cursor.execute("CREATE TABLE Movie_Director(director VARCHAR(20), movie VARCHAR(20), "
                            "FOREIGN KEY(director) REFERENCES Member(id), FOREIGN KEY(movie) REFERENCES Movie(id))")

        self.cursor.execute("CREATE TABLE Movie_Producer(producer VARCHAR(20), movie VARCHAR(20), "
                            "FOREIGN KEY(producer) REFERENCES Member(id), FOREIGN KEY(movie) REFERENCES Movie(id))")

        self.cursor.execute("CREATE TABLE Role(id SERIAL, role VARCHAR(500), PRIMARY KEY(id))")

        self.cursor.execute("CREATE TABLE Actor_Movie_Role(actor VARCHAR(20), movie VARCHAR(20), role INT)")


    def insert_tables(self):
        '''
        Insert data from imdb datasets into the database
        :return: None
        '''


        with gzip.open(str(path) + 'title.basics.tsv.gz', 'rb') as f_in:
            with open(str(path) + 'title.basics.tsv', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        file = str(path) + 'title.basics.tsv'
        self.cursor.execute("COPY t1(id, type, title, originalTitle,"
                            " isAdult, startYear, endYear, runtime, genres) FROM %s DELIMITER E'\t'", (file,))

        with gzip.open(str(path) + 'title.ratings.tsv.gz', 'rb') as f_in:
            next(f_in)
            with open(str(path) + 'title.ratings.tsv', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        file = str(path) + 'title.ratings.tsv'
        self.cursor.execute("COPY r1(id, avgRating, numVotes) FROM %s DELIMITER E'\t'",(file,))

        self.cursor.execute("CREATE temporary TABLE tmp1 AS SELECT M.id, M.type, M.title, M.originalTitle, "
                            "M.startYear, M.endYear, M.runtime, R.avgRating, R.numVotes"
                            " FROM r1 R INNER JOIN t1 M ON M.id= R.id")

        self.cursor.execute("CREATE temporary TABLE tmp2 AS SELECT M.id, M.genres FROM r1 R INNER JOIN t1 M ON M.id= R.id")


        self.cursor.execute("INSERT INTO Movie(id, type, title, originalTitle, startYear ,"
                            "endYear , runtime , avgRating , numVotes)"
                            "SELECT * from tmp1")

        self.cursor.execute("INSERT INTO Genre(genre)"
            "SELECT DISTINCT genres FROM t1")

        self.cursor.execute("DROP TABLE r1")
        self.cursor.execute("DROP TABLE tmp1")
        self.cursor.execute("DROP TABLE tmp2")

        self.cursor.execute("INSERT INTO Movie_Genre(genre, movie)"
                            " SELECT  G.id, M.id FROM t1 T INNER JOIN Genre G on "
                            "G.genre= T.genres INNER JOIN Movie M on T.id=M.id")

        self.cursor.execute("DROP TABLE t1")

        with gzip.open(str(path) + 'name.basics.tsv.gz', 'rb') as f_in:
            next(f_in)
            with open(str(path) + 'name.basics.tsv', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        file=str(path) + 'name.basics.tsv'
        self.cursor.execute("COPY t(id, name, birthYear, deathYear, primaryProfession,"
                            " knownForTitles) FROM %s DELIMITER E'\t'", (file,))
        self.cursor.execute("INSERT INTO Member(id,  name, birthYear, deathYear) SELECT id,"
                            " name, birthYear, deathYear from t WHERE id LIKE 'n%'")
        self.cursor.execute("DROP TABLE t")


        with gzip.open(str(path) + 'title.principals.tsv.gz', 'rb') as f_in:
            with open(str(path) + 'title.principals.tsv', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        file = str(path) + 'title.principals.tsv'
        self.cursor.execute("COPY t2(tconst, ordering, nconst, category, job,"
                            " charactersPlayed) FROM %s DELIMITER E'\t'", (file,))





        self.cursor.execute("CREATE temporary TABLE tmp_a AS SELECT DISTINCT P.nconst as actor, P.tconst as movie FROM t2 P "
                            "INNER JOIN Movie M ON M.id= P.tconst inner join Member R ON P.nconst=R.id WHERE lower(P.category) LIKE '%actor%'")

        self.cursor.execute("CREATE temporary TABLE tmp_w AS SELECT P.nconst as writer, P.tconst as movie FROM t2 P "
                            "INNER JOIN Movie M ON M.id= P.tconst inner join Member R ON P.nconst=R.id WHERE lower(P.category) LIKE '%writer%'")

        self.cursor.execute("CREATE temporary TABLE tmp_d AS SELECT P.nconst as director, P.tconst as movie FROM t2 P "
                            "INNER JOIN Movie M ON M.id= P.tconst inner join Member R ON P.nconst=R.id WHERE lower(P.category) LIKE '%director%'")

        self.cursor.execute("CREATE temporary TABLE tmp_p AS SELECT P.nconst as producer,P.tconst as movie FROM t2 P "
                            "INNER JOIN Movie M ON M.id= P.tconst inner join Member R ON P.nconst=R.id WHERE lower(P.category) LIKE '%producer%'")

        self.cursor.execute("INSERT INTO Movie_Actor SELECT * FROM tmp_a")

        self.cursor.execute("INSERT INTO Movie_Writer SELECT * FROM tmp_w")

        self.cursor.execute("INSERT INTO Movie_Director SELECT * FROM tmp_d")

        self.cursor.execute("INSERT INTO Movie_Producer SELECT * FROM tmp_p")



        self.cursor.execute("DROP TABLE tmp_a ")
        self.cursor.execute("DROP TABLE tmp_w ")
        self.cursor.execute("DROP TABLE tmp_d ")
        self.cursor.execute("DROP TABLE tmp_p ")

        self.cursor.execute("INSERT INTO Role(role) SELECT DISTINCT charactersPlayed FROM t2")



        self.cursor.execute("INSERT INTO Actor_Movie_Role SELECT ma.actor, ma.movie, R.id FROM t2 T RIGHT OUTER JOIN Role R "
                            "on T.charactersPlayed=R.role RIGHT OUTER JOIN Movie_Actor ma ON ma.movie = T.tconst")

        self.cursor.execute("DROP TABLE t2")



        self.cursor.execute("ALTER TABLE Actor_Movie_Role ADD CONSTRAINT fk_somename FOREIGN KEY(actor, movie) "
                            "REFERENCES Movie_Actor(actor, movie) ")

        self.cursor.execute("ALTER TABLE Movie ALTER COLUMN runtime TYPE INT USING runtime::integer")

        self.cursor.execute("ALTER TABLE member ALTER COLUMN deathyear TYPE INT USING deathyear::integer")

        self.cursor.execute("ALTER TABLE member ALTER COLUMN birthyear TYPE INT USING birthyear::integer")

        self.cursor.execute("ALTER TABLE Movie ALTER COLUMN startyear TYPE INT USING startyear::integer")

        self.cursor.execute("ALTER TABLE Movie ALTER COLUMN endyear TYPE INT USING endyear::integer")


    def sql_query(self):
        '''
        Perform querying on the IMDB database
        :return: None
        '''

        start_time=time.time()
        self.cursor.execute("select count(actor) as no_of_invalid_roles from actor_movie_role am full outer join role r "
                            "on am.role=r.id where r.role IS NULL")
        print("--- %s seconds for 2.1 ---" % (time.time() - start_time))
        rows = self.cursor.fetchall()

        count=0
        print("printing only first five rows if available for 2.1")
        for row in rows:
            print(row)
            count+=1
            if count>5:
                break



        start_time = time.time()
        self.cursor.execute("select a.id from member a join movie_actor ma on ma.actor=a.id join movie m on m.id=ma.movie "
                            "where lower(a.name) like 'phi%' and a.deathYear is null and m.startYear != 2014 ")
        print("--- %s seconds for 2.2 ---" % (time.time() - start_time))

        rows = self.cursor.fetchall()

        print("printing only first five rows if available for 2.2")
        count = 0
        for row in rows:
            print(row)
            count += 1
            if count > 5:
                break

        start_time = time.time()
        self.cursor.execute("select mp.producer from movie_producer mp join movie_genre mg on mp.movie=mg.movie join genre "
                            "g on mg.genre=g.id join movie m on mp.movie=m.id "
                            "join member mb on mp.producer=mb.id where lower(g.genre) like '%talk-show%' and "
                            "m.startyear=2017 and lower(mb.name) like '%gill%' "
                            " group by mp.producer having count(mp.movie)> 50 ")
        print("--- %s seconds for 2.3 ---" % (time.time() - start_time))

        rows = self.cursor.fetchall()

        print("printing only first five rows if available for 2.3")
        count = 0
        for row in rows:
            print(row)
            count += 1
            if count > 5:
                break

        start_time = time.time()
        self.cursor.execute("select avg(m.runtime) from movie_writer mw join movie m on mw.movie=m.id join member mb on mb.id=mw.writer "
                            "where mb.deathyear is null and lower(m.originaltitle) like '%bhardwaj%' group by mw.writer")
        print("--- %s seconds  for 2.4---" % (time.time() - start_time))

        rows = self.cursor.fetchall()

        print("printing only first five rows if available for 2.4")
        count = 0
        for row in rows:
            print(row)
            count += 1
            if count > 5:
                break

        start_time = time.time()
        self.cursor.execute("select mp.producer from movie_producer mp "
                            "left outer join movie m on mp.movie=m.id left outer join member mb on mb.id= mp.producer "
                            "where mb.deathyear is null and m.runtime>120 group by mp.producer having "
                            "count(mp.movie)=(select max(c) from (select count(mp.movie) as c from movie_producer mp "
                            "left outer join movie m on mp.movie=m.id left outer join member mb on mb.id= mp.producer "
                            "where mb.deathyear is null and m.runtime>120 group by mp.producer) as nestedsubquery)")
        print("--- %s seconds  for 2.5 ---" % (time.time() - start_time))

        rows = self.cursor.fetchall()

        print("printing only first five rows if available for 2.5")
        count = 0
        for row in rows:
            print(row)
            count += 1
            if count > 5:
                break

        start_time = time.time()
        self.cursor.execute("select amr.actor from actor_movie_role amr inner join member mb on mb.id=amr.actor "
                            "inner join role r on r.id=amr.role where mb.deathyear is null and "
                            "(lower(r.role) like '__jesus__' or lower(r.role) like '__christ__' "
                            " or lower(r.role) like '__jesus christ__')")
        print("--- %s seconds for 2.6 ---" % (time.time() - start_time))

        rows = self.cursor.fetchall()

        print("printing only first five rows if available for 2.6")
        count = 0
        for row in rows:
            print(row)
            count += 1
            if count > 5:
                break

    def indexing(self):
        '''
        Create indexes on the specific columns in the tables in the database for faster query processing
        :return: None
        '''

        self.cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        self.cursor.execute("CREATE INDEX role_role ON Role USING GIN (role gin_trgm_ops)")

        self.cursor.execute("CREATE INDEX amr_index on actor_movie_role(role)")
#--------------------------------------------------------------------------------------------------

        self.cursor.execute("CREATE INDEX ma_index ON movie_actor(actor)")

        self.cursor.execute("CREATE INDEX ma_index1 ON movie_actor(movie)")

        self.cursor.execute("CREATE INDEX mem_index ON member(name)")

        self.cursor.execute("CREATE INDEX mem_index1 ON member(deathyear)")

        self.cursor.execute("CREATE INDEX mov_index ON movie(startyear)")

#--------------------------------------------------------------------------------------------------

        self.cursor.execute("CREATE INDEX some_index_name ON movie_producer(producer)")

        self.cursor.execute("CREATE INDEX mg_index ON movie_genre(movie)")

        self.cursor.execute("CREATE INDEX mg_index1 ON movie_genre(genre)")

        self.cursor.execute("CREATE INDEX g_index ON genre(genre)")

        self.cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        self.cursor.execute("CREATE INDEX some_name ON Genre USING GIN (genre gin_trgm_ops)")

#--------------------------------------------------------------------------------------------------


        self.cursor.execute("CREATE INDEX movie_index ON Movie USING GIN (originaltitle gin_trgm_ops)")

        self.cursor.execute("CREATE INDEX mw_writer ON Movie_writer(writer)")

#--------------------------------------------------------------------------------------------------------


        self.cursor.execute("CREATE INDEX mp_index1 ON movie_producer(movie)")

        self.cursor.execute("CREATE INDEX movie_runtime ON Movie(runtime)")





if __name__ == '__main__':
    h = str(input("Enter host name"))
    db = str(input("Enter Database Name"))
    username = str(input("Enter username"))
    pwd = str(input("Enter password"))
    path = str(input("Enter Path except the file name - example- C:/users/files/"))

    database_connection = DatabaseConnection(h, db, username, pwd)
    database_connection.create_tables()
    database_connection.insert_tables()
    # database_connection.sql_query()
    # database_connection.indexing()
    # print("After indexing")
    # database_connection.sql_query()