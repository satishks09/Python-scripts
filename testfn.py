import os
import csv,codecs
import sys
import ftplib
import pysftp
import importlib
import psycopg2
import configparser

def file_count():
    sum_count = 0
    mylist = {}
    for fn in os.listdir('.'):
        if os.path.isfile(fn):
            ext = os.path.splitext(fn)[-1].lower()
            if ext == '.csv' and fn.find('TaskExtractDaily-Comments-2017') != -1:
                print(fn)
                with open(fn, "r", encoding="utf8") as f:
                    reader = csv.reader(f, delimiter=",", dialect='excel')

                    data = list(reader)
                    row_count = len(data)
                    if row_count != 0:
                        row_count = row_count - 1;
                    mylist[fn] = row_count

                    print(row_count)
                    print(mylist[fn])
                    sum_count = sum_count + row_count
                    print('Sum', sum_count)

    #for fn, cnt in mylist.items():
    #    print((fn, mylist[fn]))
    return mylist

def gp_count():
    sec = "SectionTwo"
    config = configparser.ConfigParser()
    config.sections()
    config.read('config.ini')
    os.putenv('PGPASSWORD', config[sec]['PASS'])

    options = config.options("SectionTwo")
    # Name = config.get("Section1")
    print(options)
    db = config[sec]['db']
    user = config[sec]['user']
    host = config[sec]['host']
    port = config[sec]['port']
    print(db, user, host, port)

    try:
        with (psycopg2.connect("dbname='%s' user='%s' host='%s' port='%s'" % (db, user, host, port))) as conn:
            cur = conn.cursor()
            # cur.execute("""select count(*) from sor_sis.sor_cbcard""")
            cur.execute("""select 'TaskExtractDaily-Comments-20170222_0306.csv',count(*) from ctx.mv_cc_ci_activity
    union all
    select 'TaskExtractDaily-Comments-20170412_0223.csv',count(*) from ctx.mv_cc_ci_address""")
            # cur.execute("""select 'cc_ci_address',count(*) from ctx.mv_cc_ci_address""")
            rows = cur.fetchall()
            print("\nShow me the databases:\n")
            env = os.getenv('PGPASSWORD', 'xyz')
            print(env)
            #for row in rows:
                #print("   ", row[0], " ", row[1])
            conn.commit()
    except:
        print("Unable to connect to db")
    return rows

counts=file_count()
for fn, cnt in counts.items():
    print((fn, counts[fn]))

counts_gp=gp_count()
for row in counts_gp:
      print("   ", row[0], " ", row[1])


with open("outfile.csv",'w') as fout:
    wr=csv.writer(fout)
    flag = 0
    for fn, cnt in counts.items():
        for row in counts_gp:
            if fn==row[0]:
                diff=counts[fn]-row[1]
                print(fn,diff)
                OutputRow=[fn,cnt,row[1],diff]
                flag = 1

                print(OutputRow)

                #wr.writerow(OutputRow)
        if flag==0:
            OutputRow=[fn,cnt,'NULL','NULL']
        else:
            flag=0
        wr.writerow(OutputRow)


