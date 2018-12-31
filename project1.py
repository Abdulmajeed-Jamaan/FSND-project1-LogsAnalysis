# coding: utf-8
import psycopg2

DBNAME = "news"


def question_1():
    print("\n1. What are the most popular three articles of all time?\n")
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()

    c.execute("""select articles.title , count(*) as views from log , articles
    where articles.slug = substring(log.path from 10) group by articles.title
    order by views desc limit 3;""")

    posts = c.fetchall()
    db.close()
    for title, views in posts:
        print("\""+str(title)+"\""+" — "+str(views)+" views")
    print("\n")


def question_2():
    print("\n2. Who are the most popular article authors of all time?\n")
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()

    c.execute("""select authors.name , count(*) as views from authors,articles,log
    where authors.id = articles.author
    and articles.slug = substring(log.path from 10)
    group by authors.name order by views desc;""")

    posts = c.fetchall()
    db.close()
    for name, views in posts:
        print(str(name)+" — "+str(views)+" views")
    print("\n")


def question_3():
    print("\n3. On which days did more than 1% of requests lead to errors?\n")
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    subq_for_total = """(select to_char(time, 'DD Mon YYYY') as day
    , count(*) as num from log group by to_char(time, 'DD Mon YYYY')
    order by to_char(time, 'DD Mon YYYY')) as total"""

    subq_for_errors = """(select to_char(time, 'DD Mon YYYY') as day
    , count(*) as num from log where substring(status from 1 for 3) = '404'
    group by to_char(time, 'DD Mon YYYY')
    order by to_char(time, 'DD Mon YYYY')) as error"""

    c.execute("""select total.day
    , trunc((cast(error.num as decimal)/total.num)*100,2) as error
    from """+subq_for_total+""" , """+subq_for_errors+""" where total.day = error.day
    and trunc((cast(error.num as decimal)/total.num)*100,2) > 1 """)

    posts = c.fetchall()
    db.close()
    for day, error in posts:
        print(str(day)+" — "+str(error)+"% errors")
    print("\n")

question_1()
question_2()
question_3()
