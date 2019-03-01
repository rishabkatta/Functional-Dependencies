'''
@author-name: Rishab Katta

Python3 program for determining Functional Dependencies of the previously loaded IMDB database.
'''

import psycopg2

import time
from itertools import combinations
from  builtins import any as b_any



class DatabaseConnection:

    def __init__(self,h,db,username,pwd):
        '''
        Constructor is used to connect to the database
        :param h: hostname
        :param db: database name
        :param username: Username
        :param pwd: password
        '''
        try:
            self.connection = psycopg2.connect(host=str(h), database=str(db), user=str(username), password=str(pwd))
            # self.connection = psycopg2.connect(host='localhost', database='IMDB', user='user000', password='abcde')
            self.connection.autocommit=True
            self.cursor=self.connection.cursor()
        except Exception as e:
            print(getattr(e, 'message', repr(e)))
            print(getattr(e, 'message', str(e)))

    def create_table(self):
        self.cursor.execute("create table normalization(nid SERIAL, movieId VARCHAR, "
                            "type VARCHAR, startYear INT, runtime INT, avgRating float, genreId INT, genre VARCHAR, "
                            "   memberId VARCHAR, birthYear INT, role VARCHAR, CONSTRAINT unique_const UNIQUE(movieid, memberid,genreid), PRIMARY KEY(movieid, memberid, genreid))")

    def insert_table(self):
        self.cursor.execute("insert into normalization(movieId, type, startyear, runtime, avgrating, genreid, genre, memberid, birthyear,role) "
                            "select m.id, m.type, m.startyear, m.runtime, m.avgrating, g.id, mg.genre, mb.id, mb.birthyear, amr.role from movie m inner join movie_genre mg on m.id=mg.movie "
                            "inner join genre g on g.id=mg.genre inner join movie_actor ma on m.id=ma.movie inner join member mb on mb.id = ma.actor inner join actor_movie_role amr on amr.actor = mb.id "
                            "inner join role r on amr.role=r.id where runtime>= 90 and mb.id in (select actor from role r inner join actor_movie_role amr on amr.role = r.id group by actor having count(amr.role)=1)")

    def func_depd_naive(self):
        '''
        function for determining functional dependencies using the naive approach.
        :return: None
        '''

        input = ['movieid', 'type', 'startyear', 'runtime', 'avgrating', 'genreid', 'genre', 'memberid', 'birthyear', 'role']

        output = sum([list(map(list, combinations(input, i))) for i in range(len(input) + 1)], [])

        output.pop(0) #deleting the empty set

        output=[', '.join(sub_list) for sub_list in output]

        func_dep=[]

        for left_column in output:
            for right_column in input:
                query="SELECT count(DISTINCT " + str(right_column) + " ) from normalization GROUP BY " + str(left_column) +" HAVING COUNT(DISTINCT " + str(right_column) +" ) > 1"
                self.cursor.execute(query)
                row = self.cursor.fetchone()
                if row is None:
                    fd=str(left_column) + "-->" + str(right_column)
                    func_dep.append(fd)

        print(func_dep)



    def func_depd_pruning(self):
        '''
                function for determining functional dependencies using the naive approach.
                :return: None
                '''
        input = ['movieid', 'type', 'startyear', 'runtime', 'avgrating', 'genreid', 'genre', 'memberid', 'birthyear',
                 'role']

        output = sum([list(map(list, combinations(input, i))) for i in range(3)], [])

        output.pop(0)  # deleting the empty set

        concat_list = [', '.join(sub_list) for sub_list in output]


        table_list=[]
        table_dict={}

        for column in concat_list:
            column_list = []
            query = "SELECT array_agg(nid) FROM normalization GROUP BY "+ column +" order by " + column
            self.cursor.execute(query)
            arrays = self.cursor.fetchall()
            for array in arrays:
                array=str(array)
                array = array.strip('()[],')
                column_list.append(array.translate('()[]').split(', '))
            table_list.append(column_list)


        table_dict.clear()


        for i in range(0,len(concat_list)):
            table_dict[concat_list[i]] = table_list[i]

        func_depd=[]

        for left_col in table_dict.keys():
            for right_col in input:
                count=0
                lolleft=table_dict[left_col]
                lolright=table_dict[right_col]
                for left_list in lolleft:
                    for right_list in lolright:
                        if set(left_list) <= set(right_list):
                            count+=1
                            break
                        else:
                            continue
                if count == len(lolleft):
                    leftcollist=left_col.split(", ")
                    someflag=True
                    for col in leftcollist:
                        if col.strip(" ") != right_col.strip(" "):
                            continue
                        else:
                            someflag=False
                    if someflag:
                        word = "-->" + str(right_col)
                        if not b_any(word in x for x in func_depd):
                            func_depd.append(left_col + "-->"+ right_col)


        print(func_depd)






if __name__ == '__main__':
    h = str(input("Enter host name"))
    db = str(input("Enter Database Name"))
    username = str(input("Enter username"))
    pwd = str(input("Enter password"))

    database_connection = DatabaseConnection(h,db,username,pwd)
    # database_connection.create_table()
    # database_connection.insert_table()
    print("insertion complete.. now determining functional dependencies..")
    start_time=time.time()
    database_connection.func_depd_naive()
    print("--- %s seconds  for naive ---" % (time.time() - start_time))

    st=time.time()
    database_connection.func_depd_pruning()
    print("--- %s seconds  for pruning---" % (time.time() - st))


