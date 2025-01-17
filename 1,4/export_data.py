from sqlalchemy import create_engine
import pandas 
engine = create_engine('postgresql://postgres:postgres@localhost/postgres')
df = pandas.read_sql_query('SELECT * FROM dm.dm_f101_round_f', engine)
engine.dispose()
df.to_csv('dm.dm_f101_round_f.csv', index=False)
