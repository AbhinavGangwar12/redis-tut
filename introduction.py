import redis 
r = redis.Redis(host='localhost', port=6379, db=0)
r.set('name', 'John')
print(r.get('name').decode('utf-8'))
r.set('name', 'Doe')
print(r.get('name').decode('utf-8'))
r.delete('name')
print(r.get('name'))
