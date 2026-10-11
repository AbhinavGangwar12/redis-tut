import json 
import redis 
import sqlite3
from db import db_get_product 

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def get_product(pid):
    key = f"cache:product:{pid}"
    cached = r.get(key)
    if cached is not None:
        r.incr("stats:cache:hits")
        return json.loads(cached)
    r.incr("stats:cache:misses")
    product = db_get_product(pid)
    r.set(key, json.dumps(product), ex=60)
    return product

# Delete on write 
def update_price(pid, new_price):
    con = sqlite3.connect("shop.db")
    con.execute("UPDATE products SET price = ? WHERE id = ?", (new_price, pid))
    con.commit()
    con.close()
    r.delete(f"cache:product:{pid}")

if __name__ == '__main__':
    print("--"*20)
    print(f"Cache hits : {r.get("stats:cache:hits")}" )
    print(f"Cache misses : {r.get("stats:cache:hits")}" )

    print(get_product(1))
    print("--"*20)
    print("--"*20)

    print(f"Cache hits : {r.get("stats:cache:hits")}" )
    print(f"Cache misses : {r.get("stats:cache:hits")}" )
    print("--"*20)
    print("--"*20)
    
    print(get_product(1))
    print("--"*20)
    print("--"*20)
    
    print(f"Cache hits : {r.get("stats:cache:hits")}" )
    print(f"Cache misses : {r.get("stats:cache:hits")}" )

    print("--"*20)
    print("--"*20)