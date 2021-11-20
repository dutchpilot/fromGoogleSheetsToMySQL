import sys
import requests
import csv
import io
from mysql.connector import connect, Error
from datetime import datetime
import config


# Connect to MySQL server
try:
    db_connection = connect(user=config.USER, password=config.PASSWORD, host=config.HOST, port=config.PORT, database=config.DATABASE)
    cursor = db_connection.cursor()
except Error as e:
    print('ERROR: ' + str(e))
    sys.exit(0)


# Get the DB fields
show_table_query = "DESCRIBE clients"
cursor.execute(show_table_query)
result = cursor.fetchall()
db_fields = ""
formatted_string_for_update_template = ""
i = 1
for row in result:
    db_fields = db_fields + row[0] + ","
    i += 1
    formatted_string_for_update_template = formatted_string_for_update_template + row[0] + "='{" + str(i-3) + "}',"

db_fields = db_fields[0:len(db_fields)-1]
formatted_string_for_update_template = formatted_string_for_update_template[10:len(formatted_string_for_update_template)-1]


# Get the GoogleSheet
url = "https://docs.google.com/spreadsheets/d/{0}/export?format=csv".format(config.FILE_ID)
sio = io.StringIO(requests.get(url).content.decode('utf-8'), newline=None)
googlesheet_csv = csv.reader(sio, dialect=csv.excel)

rows_in_googlesheet = 0


# Read the GoogleSheet line by line
for row in googlesheet_csv:
    rows_in_googlesheet += 1
    if rows_in_googlesheet > 1: #Skip the first row

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
            """.format(db_fields, data)

            try:
                cursor.execute(insert_clients_query)
                db_connection.commit()
            except Error as e:
                print('ERROR: ' + str(e))

        elif rowcount == 1: #Non-unique record (SQL UPDATE)
            formatted_string_for_update = formatted_string_for_update_template.format(
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
                db_connection.commit()
            except Error as e:
                print('ERROR: ' + str(e))

db_connection.close()
