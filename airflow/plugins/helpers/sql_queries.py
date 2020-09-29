
class SQLQueries:
    create_redshift_table = """
    CREATE TABLE IF NOT EXISTS public.log_functions_analysis (
    vehicle_id VARCHAR(30),
    function_name VARCHAR(40),
    start_time TIMESTAMP,
    function_duration DECIMAL(18, 5)
) compound sortkey(function_name, start_time);"""