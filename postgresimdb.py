import psycopg2
import gzip
import shutil


class DatabaseConnection:

    def __init__(self, h,db,username, pwd):
        try:
            self.connection = psycopg2.connect(host=str(h), database=str(db), user=str(username), password=str(pwd))
            self.connection.autocommit=True
            self.cursor=self.connection.cursor()
        except Exception as e:
            print(getattr(e, 'message', repr(e)))
            print(getattr(e, 'message', str(e)))

    def create_tables(self):


        self.cursor.execute("CREATE temporary TABLE t(nconst VARCHAR(20),primaryName VARCHAR(500) NOT NULL,"
                            "birthYear VARCHAR(10), deathYear VARCHAR(10), primaryProfession VARCHAR(100), "
                            "knownForTitles VARCHAR(100), PRIMARY KEY (nconst))")

        self.cursor.execute("CREATE TABLE Persons (nconst VARCHAR(20), primaryName VARCHAR(500) NOT NULL, "
                            "PRIMARY KEY(nconst))")

        self.cursor.execute("CREATE temporary TABLE t1(tconst VARCHAR(20), titleType VARCHAR(20), "
                            "primaryTitle VARCHAR(500), originalTitle VARCHAR(500), isAdult VARCHAR(20), "
                            "startYear VARCHAR(10), endYear VARCHAR(10), runTime VARCHAR(1000), genres VARCHAR(500))")

        self.cursor.execute("CREATE TABLE Movies(tconst VARCHAR(20), originalTitle VARCHAR(500), "
                            "genres VARCHAR(500), PRIMARY KEY(tconst))")

        self.cursor.execute("CREATE temporary TABLE t2(tconst VARCHAR(20), ordering VARCHAR(10),"
                            "nconst VARCHAR(20),category VARCHAR(100), job VARCHAR(500), charactersPlayed VARCHAR(500))")

        self.cursor.execute("CREATE TABLE Principals(tconst VARCHAR(20), nconst VARCHAR(20), "
                            "ordering VARCHAR(10), category VARCHAR(100), PRIMARY KEY(tconst, nconst,ordering))")

        self.cursor.execute("CREATE TABLE Ratings(tconst VARCHAR(20), averageRating float, "
                            "numVotes INT, PRIMARY KEY(tconst))")

    def insert_tables(self,path):

        with gzip.open(str(path) + 'name.basics.tsv.gz', 'rb') as f_in:
            next(f_in)
            with open(str(path) + 'name.basics.tsv', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        file=str(path) + 'name.basics.tsv'
        self.cursor.execute("COPY t(nconst, primaryName, birthYear, deathYear, primaryProfession,"
                            " knownForTitles) FROM %s DELIMITER E'\t'", (file,))
        self.cursor.execute("INSERT INTO Persons(nconst, primaryName) SELECT nconst,"
                            " primaryName from t WHERE nconst LIKE 'n%'")
        self.cursor.execute("DROP TABLE t")

        ##------------------------------------------------------------------------------------------------------#


        with gzip.open(str(path) + 'title.basics.tsv.gz', 'rb') as f_in:
            with open(str(path) + 'title.basics.tsv', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        file = str(path) + 'title.basics.tsv'
        self.cursor.execute("COPY t1(tconst, titleType, primaryTitle, originalTitle,"
                            " isAdult, startYear, endYear, runTime, genres) FROM %s DELIMITER E'\t'",(file,))
        self.cursor.execute("INSERT INTO Movies(tconst, originalTitle, genres) "
                            "SELECT tconst, originalTitle, genres from t1 WHERE (isAdult='0')")
        self.cursor.execute("DROP TABLE t1")

        #------------------------------------------------------------------------------------------------------#

        with gzip.open(str(path) + 'title.principals.tsv.gz', 'rb') as f_in:
            with open(str(path) + 'title.principals.tsv', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        file = str(path) + 'title.principals.tsv'
        self.cursor.execute("COPY t2(tconst, ordering, nconst, category, job,"
                            " charactersPlayed) FROM %s DELIMITER E'\t'", (file,))
        self.cursor.execute("INSERT INTO Principals(tconst, nconst, ordering, "
                            "category) SELECT tconst, nconst, ordering,"
                            " category FROM t2 "
                            "WHERE (category= 'director' OR category='writer' "
                            "OR category='producer' OR category='actor')")
        self.cursor.execute("DROP TABLE t2 ")

        self.cursor.execute(
            "CREATE temporary TABLE tmp AS SELECT P.tconst, P.nconst, P.ordering, "
            "P.category FROM Principals P "
            "INNER JOIN Movies M ON M.tconst= P.tconst inner join Persons R ON P.nconst=R.nconst")
        self.cursor.execute("TRUNCATE Principals")
        self.cursor.execute("INSERT INTO Principals SELECT * FROM tmp")

        #------------------------------------------------------------------------------------------------------#

        with gzip.open(str(path) + 'title.ratings.tsv.gz', 'rb') as f_in:
            next(f_in)
            with open(str(path) + 'title.ratings.tsv', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        file = str(path) + 'title.ratings.tsv'
        self.cursor.execute("COPY Ratings(tconst, averageRating, numVotes) FROM %s DELIMITER E'\t'",(file,))

        self.cursor.execute("CREATE temporary TABLE tmp1 AS SELECT R.tconst, R.averageRating, R.numVotes"
                            " FROM Ratings R INNER JOIN Movies M ON M.tconst= R.tconst")
        self.cursor.execute("TRUNCATE Ratings")
        self.cursor.execute("INSERT INTO Ratings SELECT * FROM tmp1")

        #------------------------------------------------------------------------------------------------------#

        #Adding the Foreign Key constraints
        self.cursor.execute(
            "ALTER TABLE Principals ADD CONSTRAINT fk_somename FOREIGN KEY(tconst) REFERENCES Movies(tconst)")
        self.cursor.execute(
            "ALTER TABLE Principals ADD CONSTRAINT fk_someothername FOREIGN KEY(nconst) REFERENCES Persons(nconst)")
        self.cursor.execute(
            "ALTER TABLE Ratings ADD CONSTRAINT fk_someothername FOREIGN KEY(tconst) REFERENCES Movies(tconst)")


if __name__ == '__main__':
    h= str(input("Enter host name"))
    db=str(input("Enter Database Name"))
    username=str(input("Enter username"))
    pwd=str(input("Enter password"))
    path=str(input("Enter Path except the file name - example- C:/users/files/"))

    database_connection = DatabaseConnection(h,db,username,pwd)
    database_connection.create_tables()
    database_connection.insert_tables(path)
