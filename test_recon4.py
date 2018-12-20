import psycopg2
import configparser
import os, sys
import codecs, csv
import cx_Oracle
import datetime
import ibm_db

#Final counts and sum recon output to tables. No sched and threading added.

def connect_pg():

    try:
        conn = psycopg2.connect(database="adics", user="postgres", password="")
        return conn
    except:
        print("Cant connect to Postgres database")

    return


def get_conn_det(conn):
    conn_det = {'host': conn[3], 'port': conn[4], 'service_name': conn[7], 'source': conn[8],'user': conn[1], 'password': conn[2], 'database': conn[6], 'source_system': conn[0]}
    return conn_det


def get_connection(conn_det):

    try:
        if conn_det.get('source') == 'GP':
            print("dbname='%s' user='%s' host='%s' port='%s'" % (conn_det.get('database'), conn_det.get('user'), conn_det.get('host'), conn_det.get('port')))

            conn = psycopg2.connect("dbname='%s' user='%s' password='%s' host='%s' port='%s'" % (conn_det.get('database'), conn_det.get('user'), conn_det.get('password'), conn_det.get('host'), conn_det.get('port')))

        elif conn_det.get('source') == 'Oracle':

            dsn = cx_Oracle.makedsn(conn_det.get('host'), conn_det.get('port'), service_name=conn_det.get('service_name'))
            conn = cx_Oracle.connect(user=conn_det.get('user'), password=conn_det.get('password'), dsn=dsn)

        elif conn_det.get('source') == 'DB2':

            conn = ibm_db.connect("DATABASE=;HOSTNAME=;PORT=;PROTOCOL=TCPIP;UID=;PWD=;", "", "")

        return conn

    except:
        print("Cant connect to %s database" % conn_det.get('source'))

    return


def dexec(cur,schema,table_name,field_name,sql_fld,test_type):

    if sql_fld is None:
        if test_type == 'COUNT':
            cur.execute("""select count(*) from """ + schema + """.""" + table_name)
        elif test_type == 'SUM':
            cur.execute("""select sum("""+field_name+""") from """ + schema + """.""" + table_name)
            #cur.execute(table_name)
    else:
        cur.execute(sql_fld)
    cnt = cur.fetchone()

    return cnt


def dexec_db2(conn,schema,table_name,field_name,sql_fld,test_type):

    if sql_fld is None:
        if test_type == 'COUNT':
            sql = """select count(*) from """ + schema + """.""" + table_name + """ with ur"""
            stmt = ibm_db.exec_immediate(conn, sql)
        elif test_type == 'SUM':
            sql = """select sum("""+field_name+""") from """ + schema + """.""" + table_name + """ with ur"""
            stmt = ibm_db.exec_immediate(conn, sql)

    else:
        stmt = ibm_db.exec_immediate(conn, sql_fld)

    cnt = []
    while ibm_db.fetch_row(stmt) != False:
        cnt.append(ibm_db.result(stmt, 0))

    return cnt

server=''
database=''
username=''
password=''
conn = pyodbc.connect(init_string="driver={SQLOLEDB}; server=+ServerName+; database=+MSQLDatabase+; trusted_connection=true")
conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

/usr/lib/libmyodbc5.so


def get_count(conn,tests,test_type,src_sys):

    conn_det = get_conn_det(conn)

    #print('conn_det',conn_det)

    connect = get_connection(conn_det)

    if conn_det.get('source') != 'DB2':
        cur = connect.cursor()

    count = []

    for row in tests:
        if src_sys == 'EDH':
            cnt = dexec(cur, row[4], row[5],None,row[7],'COUNT')
        elif conn_det.get('source') != 'DB2':
            cnt = dexec(cur, row[2], row[3],None,row[6],'COUNT')
        else:
            cnt = dexec_db2(connect,row[2], row[3], None, row[6], 'COUNT')

        count.append([row[2],row[3],row[4],row[5],int(cnt[0])])
        print(row[3])

    if conn_det.get('source') != 'DB2':
        cur.close()
        connect.close()
    else:
        ibm_db.close(connect)

    return count


def get_sum(conn, tests, test_type, src_sys):
    conn_det = get_conn_det(conn)

    connect = get_connection(conn_det)

    if conn_det.get('source') != 'DB2':
        cur = connect.cursor()

    sum_val = []

    for row in tests:
        if src_sys == 'EDH':
            cnt = dexec(cur, row[4], row[5],row[8],row[7],'SUM')
        elif conn_det.get('source') != 'DB2':
            cnt = dexec(cur, row[2], row[3],row[8].upper(),row[6],'SUM')
        else:
            cnt = dexec_db2(connect,row[2], row[3],row[8].upper(),row[6],'SUM')

        if cnt is not None:
            sum_val.append([row[2], row[3],row[4],row[5],row[8],cnt[0]])
        print(sum_val)

    if conn_det.get('source') != 'DB2':
        cur.close()
        connect.close()

    return sum_val

def write_output(count_gp, count_src, src_sys,cur ):

    i=0

    now = datetime.datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    for row in count_gp:
        OutputRow = [src_sys, row[0], row[1],row[2], row[3],int(count_src[i][4]),int(row[4])]

        diff = int(count_src[i][4]) - int(row[4])

        print('out',OutputRow)

        #sql = """INSERT into counts_recon(source_system, source_schema, source_table, target_schema,target_table) VALUES( % s, % s, % s, % s, % s)"""
        sql = "INSERT into counts_recon(source_system, source_schema, source_table, target_schema,target_table, source_count,target_count, difference,run_time) VALUES('"+str(src_sys)+"','"+row[0]+"','"+row[1]+"','"+row[2]+"','"+row[3]+"',"+str(count_src[i][4])+","+str(row[4])+","+str(diff)+",'"+now_str+"')"
        print(sql)
        cur.execute(sql)
        i = i + 1

    return

def write_sum_output(sum_gp, sum_src, src_sys,cur ):

    i=0

    now = datetime.datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    for row in sum_gp:
        #OutputRow = [src_sys, row[0], row[1],row[2], row[3],int(count_src[i][4]),int(row[4])]

        diff = float(sum_src[i][5]) - float(row[5])

        #print('out',OutputRow)

        #sql = """INSERT into counts_recon(source_system, source_schema, source_table, target_schema,target_table) VALUES( % s, % s, % s, % s, % s)"""
        sql = "INSERT into sum_recon(source_system, source_schema, source_table, target_schema,target_table,sum_field, source_sum,target_sum, difference,run_time) VALUES('"+str(src_sys)+"','"+row[0]+"','"+row[1]+"','"+row[2]+"','"+row[3]+"','"+row[4]+"',"+str(sum_src[i][5])+","+str(row[5])+","+str(diff)+",'"+now_str+"')"
        print(sql)
        cur.execute(sql)
        i = i + 1

    return

if __name__ == '__main__':

    # sec = "SectionNine"
    # config = configparser.ConfigParser()
    # config.sections()
    # config.read('config.ini')
    #
    # src_sys = config[sec]['src_sys']
    # test_group_id = config[sec]['test_group_id']

    for arg in sys.argv:
        if 'src_sys' in arg:
            pos = arg.find('=') + 1
            src_sys = arg[pos:]
        if 'test_group_id' in arg:
            pos = arg.find('=') + 1
            test_group_id = arg[pos:]

    pg_conn = connect_pg()
    pg_cur = pg_conn.cursor()

    pg_cur.execute("""select * from connections where source_system = '"""+src_sys+"""'""")
    src_conn = pg_cur.fetchone()
    #print('src_conn',src_conn)

    pg_cur.execute("""select * from connections where source_system = 'EDH'""")
    gp_conn = pg_cur.fetchone()
    #print('gp_conn',gp_conn)

    pg_cur.execute("""select * from test_conditions where test_group_id = """+test_group_id)
    tests = pg_cur.fetchall()
    #print('tests',tests)

    pg_cur.execute("""select * from test_group where test_group_id ="""+test_group_id)
    test_type = pg_cur.fetchone()
    #print('test_type',test_type)



    if test_type[2] == 'COUNT':
        cnt_gp = get_count(gp_conn,tests,test_type,'EDH')
        cnt_src = get_count(src_conn,tests,test_type,src_sys)
        #print (cnt_gp[0][0],cnt_src)
        write_output(cnt_gp,cnt_src,src_sys,pg_cur)
    elif test_type[2] == 'SUM':
        sum_gp = get_sum(gp_conn,tests,test_type,'EDH')
        sum_src = get_sum(src_conn,tests,test_type,src_sys)
        #print('gp',sum_gp)
        write_sum_output(sum_gp, sum_src, src_sys, pg_cur)

    pg_cur.close()
    pg_conn.commit()
    pg_conn.close()
