import psycopg2





# ConnectDB
def connect(db="news"):
    """Connect to the PostgreSQL DB and returns a database connection."""
    try:
        db = psycopg2.connect("db={}".format(db))   # ** Connetion by DB*
        c = db.cursor()
        return db, c    # ** When DB connected *
    except:
        print("can't connect by DB")   # ** If DB Not connect *



# Execute query:

def excuite(q):
    db, c = connection()
    c = db.cursor()
    c.execute(q)
    return c.fetchall()
    db.close()


# Create file to send output

file_output = open('Print_output.txt', 'w')


#______________ 1 __________________

pp_article = """create view pep_articles as
                      select articles.t, count(articles.t)
                      as num from articles
                      join log on log.path like concat('%', articles.slug, '%')
                      group by articles.t
                      order by num DESC limit 8;
                      select * from pep_articles;
          """



#________________ 2 ___________________

pp_author = """ create view pep_authors as
                     select authors.n, count(*)
                     as num from articles
                     join authors on articles.author = authors.id
                     join log on log.path like concat('%', articles.slug, '%')
                     group by authors.n
                     order by num DESC;
                     select * from pep_authors;
          """


# On which days did more than 1% of requests lead to errors?
qq_errors = """create view que_error as
                  select main.date,(100.0*main.error/main.num)
                  from (select date_trunc('day', time) as date,
                  count(id) as num,
                  sum(case when status='404 NOT FOUND' then 1 else 0 end)
                  as error from log group by date) as main
                  where (100.0*main.error/main.num) >1;
                  select * from que_error;
          """




#First:

def pp_articles(query):

    results = excuite(query)  #Most 8 articles of all tim

    file_output.write("\n First : \n \n")

    for t, views in results:    #result
        file_output.write("\t" + t + "--" + str(views) + " views \n")


#Second:

def pp_authors(query):

    results = excuite(query)
    file_output.write("\n Second : \n \n")
    for author, views in results:    #result
        file_output.write("\t" + author + "--"+str(views) + " views \n")


#Third :
def qq_error(query):

    results = excuite(query)
    file_output.write("\n Third :\n \n")
    for date, error in results:     #result
        file_output.write("\t {0:%B %d, %Y} -- {1:.2f} % errors".format(date, error))


if __name__ == '__main__':
    pp_articles(pp_article)
    pp_authors(pp_author)
    qq_error(qq_errors)

file_output.close()   #CLOSE FILe










