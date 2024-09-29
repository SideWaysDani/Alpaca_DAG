import psycopg2
from psycopg2 import sql
from  war_clone_test import GenericDBHelper
import psycopg2
from psycopg2.extras import RealDictCursor  # Recommended for easier data handling


# Define connection parameters
db_params = {
    'host': 'sthub.c3uguk04fjqb.ap-southeast-2.rds.amazonaws.com',
    'database': 'postgres',
    'user': 'stpostgres',
    'password': 'stocktrader'
}

# Define the value to be added/subtracted
strength_change = 8000  # You can modify this value as needed

# Establish the connection (using RealDictCursor for convenience)
try:
    conn = psycopg2.connect(**db_params, cursor_factory=RealDictCursor)
    cur = conn.cursor()
except Exception as e:
    print(f"Connection error: {e}")
    exit(1)

# Get the current remaining strength using parameterized query for security
q_get_strength = """SELECT remaining_strength FROM war_iter_4_2.account WHERE account_id = %s"""
cur.execute(q_get_strength, (1,))  # Pass account_id as a tuple parameter

try:
    current_strength = cur.fetchone()["remaining_strength"]  # Assuming a single row
except TypeError:
    print("No account found with ID 1 or invalid data type returned")
except KeyError:
    print("Missing 'remaining_strength' column in the result")
    conn.close()
    exit(1)

# Check if the update will result in a positive remaining strength
if current_strength + strength_change > 0:
    # Construct the update query with parameterized query for security
    q_update = """UPDATE war_iter_4_2.account
                   SET active_strength = active_strength,
                       user_id = 1,
                       total_strength = total_strength + %s,
                       remaining_strength = remaining_strength + %s
                   WHERE account_id = 1"""
    cur.execute(q_update, (strength_change, strength_change))
    conn.commit()
    print("Account strength updated successfully")
else:
    print("Update skipped: Insufficient remaining strength")

# Close the connection
conn.close()