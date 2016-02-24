import glob
import pymysql
import csv
import time
import datetime
# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='dev_user',
                             password='dev_user',
                             db='johan_db',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

abs_path = "/home/prakhar/Projects/order_947089/allstocks_20150*/*.csv"
file_names = []

# read each file
for filename in sorted(glob.iglob(abs_path)):
    # create the table name from the file name
    file = filename[55:]
    newfile = file[6:-4]
    table = newfile.upper() + '_full'

    # insert script for each row of the table
    sql = "insert into `__table__name__` (`stock_date`,`stock_time`,`open_price`,`high_price`,`low_price`,`closing_price`,`volume`,`splits`,`earnings`,`dividends`) values (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s)"

    # was a preliminary condition for testing, can be removed later
    # if 1:
        # replace the table name in the script
    sql = sql.replace("__table__name__",table)

    # open the file
    with open(filename, 'rt') as csvfile:
        # create a file reader
        rowreader = csv.reader(csvfile, delimiter=',')
        # set the default previous time to 0000
        previous = time.mktime(time.strptime("0100","%H%M"))
        # also set previous timestamp to 0000
        previous_t_stamp = "0000"
        # default the old_t = previous to process the first row
        old_t = previous
        count = 0

        # read each row in the csv file
        for row in rowreader:

            # read the data only if hte entry was made between 0930 - 1600
            if int(row[1])>=930 and int(row[1])<=1600:

                # change the time to 4 digit
                if len(row[1]) == 3:
                    t = "0"+row[1]
                else:
                    t= row[1]

                # get the current time and time difference
                current_time = time.mktime(time.strptime(t, "%H%M"))
                difference = int(current_time-previous)/60

                # this check is for first record in file
                if old_t == previous:
                    # if the first row is after the 930, then make sure the reading is populated from 0930
                    if float(difference) > 510.0:
                        final = float(difference) - 510.0
                        for temp in range(0,int(final),5):
                            count = count+1
                            temp_row = row
                            temp_time = str((datetime.datetime.strptime("0925", '%H%M')+datetime.timedelta(minutes=temp)).time())
                            temp_row[1] = temp_time[0:5].replace(":","")
                            with connection.cursor() as cursor:
                                cursor.execute(sql, (temp_row[0],str(temp_row[1]+"00"),temp_row[2],temp_row[3],temp_row[4],temp_row[5],temp_row[6],temp_row[7],temp_row[8],temp_row[9]))
                                cursor.close()
                    # if not then, write the first row to database
                    else :
                        with connection.cursor() as cursor:
                            cursor.execute(sql, (row[0],str(row[1]+"00"),row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
                            cursor.close()
                    count = count+1
                # checck the time difference and perform logic accordingly
                elif difference == 0.0:
                    count = count+1
                    with connection.cursor() as cursor:
                        cursor.execute(sql, (row[0],str(row[1]+"00"),row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
                        cursor.close()
                elif difference == 5.0:
                    with connection.cursor() as cursor:
                        cursor.execute(sql, (row[0],str(row[1]+"00"),row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
                        cursor.close()
                    count = count+1
                elif difference > 5.0 :
                    for temp in range(5,int(difference+1),5):
                        count = count+1
                        temp_row = row
                        temp_time = str((datetime.datetime.strptime(previous_t_stamp, '%H%M')+datetime.timedelta(minutes=temp)).time())
                        temp_row[1] = temp_time[0:5].replace(":","")
                        with connection.cursor() as cursor:
                            cursor.execute(sql, (temp_row[0],str(temp_row[1]+"00"),temp_row[2],temp_row[3],temp_row[4],temp_row[5],temp_row[6],temp_row[7],temp_row[8],temp_row[9]))
                            cursor.close()
                previous = current_time
                previous_t_stamp = row[1]
                last_row = row
    temp_count = count
    if count<=79:
        for temp in range(1,80-count):
            temp_count = temp_count + 1
            temp_time = str((datetime.datetime.strptime(last_row[1], '%H%M')+datetime.timedelta(minutes=5)).time())
            last_row[1] = temp_time[0:5].replace(":","")
            if last_row[1] == "1605":
                break
            else:
                with connection.cursor() as cursor:
                    cursor.execute(sql, (last_row[0],str(last_row[1]+"00"),last_row[2],last_row[3],last_row[4],last_row[5],last_row[6],last_row[7],last_row[8],last_row[9]))
                    cursor.close()
    print(filename,temp_count)
connection.commit()
connection.close()