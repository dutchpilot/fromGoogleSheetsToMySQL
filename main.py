import sys
import requests
import csv
import io
from mysql.connector import connect, Error
from datetime import datetime

import config

try:
    connection = connect(user=config.USER, password=config.PASSWORD, host=config.HOST, port=config.PORT, database=config.DATABASE)
    cursor = connection.cursor()
except Error as e:
    print('ERROR: ' + str(e))
    sys.exit(0)

show_table_query = "DESCRIBE clients"
cursor.execute(show_table_query)
result = cursor.fetchall()
database_fields = ""
formatted_string_for_update_tmp = ""
i = 1
for row in result:
    database_fields = database_fields + row[0] + ","
    i += 1
    formatted_string_for_update_tmp = formatted_string_for_update_tmp + row[0] + "='{" + str(i-3) + "}',"
database_fields = database_fields[0:len(database_fields)-1]
formatted_string_for_update_tmp = formatted_string_for_update_tmp[10:len(formatted_string_for_update_tmp)-1]

#print(result)

url = "https://docs.google.com/spreadsheets/d/{0}/export?format=csv".format(config.FILE_ID)
request_to_google_sheet = requests.get(url)
sio = io.StringIO(request_to_google_sheet.content.decode('utf-8'), newline=None)
reader = csv.reader(sio, dialect=csv.excel)

row_count = 0

for row in reader:
    row_count += 1
    if row_count > 1: #Skip the first row

        cursor.execute("SELECT COUNT(*) FROM clients WHERE id=" + row[0])
        rowcount = cursor.fetchone()[0]

        if rowcount == 0: #Unique record (SQL INSERT)
            data = "'"\
            + row[0] + "','"\
            + row[1] + "','"\
            + row[2] + "','"\
            + str(datetime.strptime(row[3],"%Y-%m-%d %H:%M:%S")) + "','"\
            + str(datetime.strptime(row[4],"%Y-%m-%d %H:%M:%S"))+ "','"\
            + row[5] + "','"\
            + row[6] + "','"\
            + row[7] + "','"\
            + row[8] + "','"\
            + row[9] + "','" \
            + row[10] + "','" \
            + row[11] + "','"\
            + row[13] + "','"\
            + row[14] + "','"\
            + row[15] + "','"\
            + row[16] + "','"\
            + row[17] + "','"\
            + row[19] + "','"\
            + str({'Да': 1}.get(row[20], 0)) + "','"\
            + row[36] + "','"\
            + row[37] + "','"\
            + row[38] + "','"\
            + row[39] + "','"\
            + row[40] + "','"\
            + row[41]+ "'"

            insert_clients_query = """
            INSERT INTO clients ({0})
            VALUES ({1})
            """.format(database_fields, data)

            try:
                cursor.execute(insert_clients_query)
                connection.commit()
            except Error as e:
                print('ERROR: ' + str(e))

        elif rowcount == 1: #Non-unique record (SQL UPDATE)
            formatted_string_for_update = formatted_string_for_update_tmp.format(
                row[1],
                row[2],
                str(datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S")),
                str(datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S")),
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
                row[11],
                row[13],
                row[14],
                row[15],
                row[16],
                row[17],
                row[19],
                str({'Да': 1}.get(row[20], 0)),
                row[36],
                row[37],
                row[38],
                row[39],
                row[40],
                row[41]
            )

            update_clients_query = """
            UPDATE clients
            SET {0}
            WHERE id = {1}
            """.format(formatted_string_for_update, row[0])

            try:
                cursor.execute(update_clients_query)
                connection.commit()
            except Error as e:
                print('ERROR: ' + str(e))

    first_row = False

connection.close()
