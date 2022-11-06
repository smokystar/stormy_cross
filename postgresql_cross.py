import psycopg2
con = psycopg2.connect(user="postgres",
                                password="5466",
                                host="localhost",
                                port="5432",
                                database="cross_exch")
cur = con.cursor()