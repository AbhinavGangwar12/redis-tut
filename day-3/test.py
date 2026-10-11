from db import db_get_product, init_db 

init_db()
print(db_get_product(1))
print(db_get_product(99))