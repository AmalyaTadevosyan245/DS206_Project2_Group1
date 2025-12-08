from utils import load_db_config, create_db_connection, read_sql_file

print("\n----- TEST 1: Loading config file -----")
cfg = load_db_config("sql_server_config.cfg")
print(cfg)

print("\n----- TEST 2: Connecting to SQL Server -----")
conn = create_db_connection(cfg)
print("Connection successful!")

print("\n----- TEST 3: Reading an SQL file -----")
sql_text = read_sql_file("infrastructure_initiation/dimensional_db_creation.sql")
print(sql_text[:200])  # print first 200 chars

conn.close()
print("\nALL TESTS PASSED.\n")
