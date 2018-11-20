import psycopg2


def master_search(hosts, port, db, user, password):
    for host in hosts:
        conn = psycopg2.connect(dbname=db, user=user, password=password, host=host, port=port)
        cursor = conn.cursor()
        cursor.execute('select pg_is_in_recovery()')
        rows = cursor.fetchall()
        is_slave = rows[0][0]
        cursor.close()
        conn.close()
        if not is_slave:
            return host
    raise Exception('master host not found')
