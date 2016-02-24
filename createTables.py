import pymysql
# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='dev_user',
                             password='dev_user',
                             db='johan_db',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

for line in open("tablenames.txt") :
    sql = "CREATE TABLE IF NOT EXISTS `__table__name__` (`id` int(11) NOT NULL,`stock_date` date NOT NULL,`stock_time` time NOT NULL,`open_price` double(8,4) NOT NULL DEFAULT '0.0000',`high_price` double(8,4) NOT NULL DEFAULT '0.0000',`low_price` double(8,4) NOT NULL DEFAULT '0.0000',`closing_price` double(8,4) NOT NULL DEFAULT '0.0000',`volume` double(8,2) NOT NULL DEFAULT '0.00',`splits` int(11) NOT NULL,`earnings` int(11) NOT NULL,`dividends` double(10,9) DEFAULT '0.000000000') ENGINE=InnoDB DEFAULT CHARSET=latin1;ALTER TABLE `__table__name__` ADD PRIMARY KEY (`id`);ALTER TABLE `__table__name__`MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;"
    sql = sql.replace("__table__name__",line.rstrip())
    print(sql)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
    finally:
        # connection.close()
        print(sql)


