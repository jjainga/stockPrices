from confluent_kafka import Consumer, KafkaError
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import os

load_dotenv()

# Configure MySQL connection
cnx = mysql.connector.connect(
    user=os.getenv('USER'),
    password=os.getenv('PASSWORD'),
    host=os.getenv('HOST'),
    port=os.getenv('PORT'),
    database=os.getenv('database'))


cursor = cnx.cursor()

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['stock_prices'])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print(f'End of partition reached {msg.topic()}/{msg.partition()}')
        else:
            print(f'Error while polling message: {msg.error()}')
    else:
        msg_value = msg.value().decode()
        msg_value_list = msg_value.split(',')
        symbol = msg_value_list[0]
        price = float(msg_value_list[1])
        datetime = msg_value_list[2]
        print(f'Received message: {symbol}, {price}, {datetime}')

        # Insert data into MySQL table
        try:
            add_stock_price = ("INSERT INTO stock_prices "
                               "(symbol, price, datetime) "
                               "VALUES (%s, %s, %s)")
            data_stock_price = (symbol, price, datetime)
            cursor.execute(add_stock_price, data_stock_price)
            cnx.commit()
            print(f'Successfully added {symbol} stock price to MySQL database.')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
cursor.close()
cnx.close()

