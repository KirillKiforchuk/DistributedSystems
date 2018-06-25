import psycopg2


def fly_book():
    print("Fly Booking")
    data = {'client_name': input("Client name: "), 'fly_number': input("Fly number: "),
            'city_from': input("From: "), 'city_to': input("To: "), 'date': input("Date: ")
            }

    return data


def hotel_book():
    print("Hotel Booking")
    data = {'client_name': input("Client name: "), 'hotel_name': input("Hotel name: "),
            'arrival': input("Arrival: "), 'departure': input("Departure: ")
            }

    return data


def insert_fly(cursor, data):
    sql = """INSERT INTO "FlyBooking"."FlyBookingData"("ClientName", "FlyNumber", "CityFrom", "CityTo", "Date")
             VALUES(%(client_name)s, %(fly_number)s, %(city_from)s, %(city_to)s, %(date)s);"""
    cursor.execute(sql, data)


def insert_hotel(cursor, data):
    sql = """INSERT INTO "HotelBooking"."HotelBookingData"("ClientName", "HotelName", "Arrival", "Departure")
             VALUES (%(client_name)s, %(hotel_name)s, %(arrival)s, %(departure)s);"""
    cursor.execute(sql, data)


def take_amount(cur):
    sql = """UPDATE public."Account" SET "amount" = "amount" - 1;"""
    cur.execute(sql)


def update_fly(cur, client_name, date):
    sql = """UPDATE "FlyBooking"."FlyBookingData"
             SET "Date" = %s WHERE "ClientName" = %s;"""
    cur.execute(sql, (date, client_name))


def update_hotel(cur, client_name, date):
    sql = """UPDATE "HotelBooking"."HotelBookingData"
             SET "Arrival" = %s WHERE "ClientName" = %s;"""
    cur.execute(sql, (date, client_name))


def delete_fly(cur, client_name):
    sql = """DELETE FROM "FlyBooking"."FlyBookingData" WHERE "ClientName" = %s;"""
    cur.execute(sql, (client_name,))


def delete_hotel(cur, client_name, ):
    sql = """DELETE FROM "HotelBooking"."HotelBookingData" WHERE "ClientName" = %s;"""
    cur.execute(sql, (client_name,))


def insert_transaction(conn):
    xid = conn.xid(1, 'insert_transaction', 'insert')
    conn.tpc_begin(xid)
    cur = conn.cursor()
    fly_data = fly_book()
    insert_fly(cur, fly_data)
    hotel_data = hotel_book()
    insert_hotel(cur, hotel_data)
    try:
        take_amount(cur)
        conn.tpc_prepare()
        if fly_data['client_name'] != hotel_data['client_name'] or fly_data['date'] != hotel_data['arrival']:
            print("Transaction rollback")
            conn.tpc_rollback()
        else:
            print("Transaction commit")
            conn.tpc_commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        conn.tpc_rollback()



    '''print("FlyBookingData")
    cur.execute("""SELECT * FROM "FlyBooking"."FlyBookingData";""")
    rows = cur.fetchall()
    for row in rows:
        print(row)

    print("HotelBookingData")
    cur.execute("""SELECT * FROM "HotelBooking"."HotelBookingData";""")
    rows = cur.fetchall()
    for row in rows:
        print(row)

    print("amount")
    cur.execute("""SELECT * FROM public."Account";""")
    rows = cur.fetchall()
    for row in rows:
        print(row)'''

    cur.close()


def update_transaction(conn, client_name, date):
    xid = conn.xid(2, 'update_transaction', 'update')
    conn.tpc_begin(xid)
    cur = conn.cursor()
    update_fly(cur, client_name, date)
    update_hotel(cur, client_name, date)
    conn.tpc_prepare()
    conn.tpc_commit()

    print("FlyBookingData")
    cur.execute("""SELECT * FROM "FlyBooking"."FlyBookingData";""")
    rows = cur.fetchall()
    for row in rows:
        print(row)

    print("HotelBookingData")
    cur.execute("""SELECT * FROM "HotelBooking"."HotelBookingData";""")
    rows = cur.fetchall()
    for row in rows:
        print(row)

    cur.close()


def delete_transaction(conn, client_name,   ):
    xid = conn.xid(3, 'delete_transaction', 'delete')
    conn.tpc_begin(xid)
    cur = conn.cursor()
    delete_fly(cur, client_name)
    count1 = cur.rowcount
    delete_hotel(cur, client_name)
    count2 = cur.rowcount
    conn.tpc_prepare()
    if count1 != count2:
        print("Transaction rollback")
        conn.tpc_rollback()
    else:
        print("Transaction commit")
        conn.tpc_commit()

    print("FlyBookingData")
    cur.execute("""SELECT * FROM "FlyBooking"."FlyBookingData";""")
    rows = cur.fetchall()
    for row in rows:
        print(row)

    print("HotelBookingData")
    cur.execute("""SELECT * FROM "HotelBooking"."HotelBookingData";""")
    rows = cur.fetchall()
    for row in rows:
        print(row)

    cur.close()


def main():
    conn = psycopg2.connect(database='Booking', user='postgres', password='23nfbh05')

    insert_transaction(conn)
    #update_transaction(conn, input("Input name to update: "), input("Input new date: "))
    #delete_transaction(conn, input("Input name to delete: "))

    conn.close()

    return 0


main()
