import psycopg2
import json
from urllib import request, parse

try:
    connection = psycopg2.connect(user="ags",password="",host="127.0.0.1",
                                   port="5432",database="ags")

    cursor = connection.cursor()
    cursor.execute("select * from click")

    columns = ('txid',)

    for click in cursor.fetchall():

        message = json.dumps(dict(zip(columns, click)))

        clen = len(message)
        req = request.Request("https://xev0lh9vb0.execute-api.us-west-2.amazonaws.com/asaushkin/click",
                             message.encode(),
                             {'Content-Type': 'application/json', 'Content-Length': clen})
        f = request.urlopen(req)
        print(f.read())

    cursor.close()
    connection.close()

except (Exception, psycopg2.Error) as error:
    print ("Error while fetching data from PostgreSQL", error)

