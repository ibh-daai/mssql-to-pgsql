import sys, pyodbc, psycopg2, psycopg2.extras

def insert_row(cursor, table, data):
    sql = 'insert into "%s" ("%s") values ' % (table, '","'.join(data.keys())) + "(%s);" % (','.join(['%s'] * len(data)))
    cursor.execute(sql, list(data.values()))

if __name__ == '__main__':
    # Connect to PG SQL
    connect = "host='pghost' dbname='pgdb' user='pguser' password='pgpass' client_encoding='UTF8'"
    pgconn = psycopg2.connect(connect)
    pgconn.autocommit = True
    pgsql = pgconn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Connect to MS SQL
    msconn = pyodbc.connect('DRIVER={SQL Server};SERVER=sqlserver;DATABASE=sqldb;UID=sqluser;PWD=sqlpassword')
    mssql = msconn.cursor()
    mssql.execute("SELECT * from sometable where somecriteria IS TRUE")
    row = mssql.fetchone()
    i = 0
    while row:
        #print(row)
        #exit()
        i += 1
        if( (i % 1000) == 0 ):
            print("%.2f percent complete" % (i * 100/239228)) # update 239228 to reflect the actual number of rows

        data = {}
        # Change the follow to work for your own source vs. destination table schemas
        data['mrn'] = row[0];
        data['issuer'] = row[1];
        data['name'] = row[2];
        data['dob'] = row[3];
        data['age'] = row[4];
        data['gender'] = row[5];

        data['accession'] = row[10];
        data['study_ts'] = row[11];
        data['uid'] = row[12];
        # 13 intentionally skipped - study status is useless to us
        data['modality'] = row[14];
        data['body_part'] = row[15];
        data['procedure_code'] = row[16];
        data['study_description'] = row[17];
        data['referring_unit'] = row[18];
        data['case_history'] = row[19];
        data['referring_physician'] = row[20];
        data['institution'] = row[21];
        data['device'] = row[22];
        # 23 intentionally skipped - file path has no use to us
        data['report_ts'] = row[24];
        data['reporting_rad'] = row[25];
        data['signing_rad'] = row[26];
        data['report_text'] = row[27].replace('\0', ' '); # remove NUL characters

        #print(data);
        #exit()
        insert_row(pgsql, 'original_study', data)
        row = mssql.fetchone()
        #break

