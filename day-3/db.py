import sqlite3
import time 

def init_db():
    con = sqlite3.connect("shop.db")
    con.execute("CREATE TABLE IF NOT EXISTS products "
                "(id INTEGER PRIMARY KEY, name TEXT, price REAL, stock INTEGER)")
    con.execute("INSERT OR IGNORE INTO products VALUES "
                "(1,'Keyboard',49.5,10),(2,'Mouse',19.99,25),(3,'Monitor',199.0,5),"
                "(4,'Webcam',59.0,12),(5,'Headset',89.9,8)")
    con.commit()
    con.close()

def db_get_product(pid):
    time.sleep(0.3)
    con = sqlite3.connect("shop.db")
    con.row_factory = sqlite3.Row
    row = con.execute("SELECT * FROM products WHERE id = ?", (pid,)).fetchone()
    con.close()
    return dict(row) if row else None 

