# from utils import load_db_config, create_db_connection

# cfg = load_db_config()
# conn = create_db_connection(cfg)

# cursor = conn.cursor()
# cursor.execute("SELECT TOP 5 name FROM sys.tables;")
# print(cursor.fetchall())

# conn.close()

# from pipeline_dimensional_data.tasks import (
#     task_update_dim_categories,
#     task_update_dim_customers,
#     task_update_dim_employees,
#     task_update_dim_products,
#     task_update_dim_shippers,
#     task_update_dim_suppliers,
#     task_update_dim_territories,
# )

# print(task_update_dim_categories({"success": True}))
# print(task_update_dim_customers({"success": True}))
# print(task_update_dim_employees({"success": True}))
# print(task_update_dim_products({"success": True}))
# print(task_update_dim_shippers({"success": True}))
# print(task_update_dim_suppliers({"success": True}))
# print(task_update_dim_territories({"success": True}))


from pipeline_dimensional_data.tasks import (
    task_update_factorders
)

result = task_update_factorders(
    prereq={"success": True},
    start_date="1995-01-01",
    end_date="1997-12-31"
)

print(result)