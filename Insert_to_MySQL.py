#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import glob
import mysql.connector
from mysql.connector import Error


# In[ ]:


def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData

def insertBLOB(id_photo, photo):
    print("Inserting BLOB into smavi photo_raw database")
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='smavi',
                                             user='root',
                                             password='isi_password')

        cursor = connection.cursor()
        sql_insert_blob_query = "INSERT INTO photo_raw (id, photo) VALUES (%s,%s)"

        empPicture = convertToBinaryData(photo)

        # Convert data into tuple format
        insert_blob_tuple = (id_photo, empPicture)
        result = cursor.execute(sql_insert_blob_query, insert_blob_tuple)
        connection.commit()
        print("Image inserted successfully as a BLOB into smavi photo_raw database", result)

    except mysql.connector.Error as error:
        print("Failed inserting BLOB data into MySQL table {}".format(error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
            


# In[ ]:


file = glob.glob('E:/to_mysql/Foto/*.jpeg')
i = 1

for img in file:
    insertBLOB(i, img)
    print(i, img)
    i+=1

