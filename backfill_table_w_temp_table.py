import psycopg2
import os
import string
import random
from datetime import datetime, timedelta

os.environ['REDSHIFT_USERNAME'] = 'username'
os.environ['REDSHIFT_PASSWORD'] = 'password'

# Function to generate a random string of characters
def random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

# set up connection parameters
s = ("dbname=redshiftdb host=live-idl-prod-redshift-component-redshiftcluster-1q6vyltqf8lth.ctm1v7db0ubd.eu-west-1.redshift.amazonaws.com "
     f"port=5439 user={os.environ['REDSHIFT_USERNAME']} password={os.environ['REDSHIFT_PASSWORD']}")

# connect to Redshift
conn = psycopg2.connect(s)
cur = conn.cursor()

# Create a cursor object
cur = conn.cursor()

# Set the start and end dates
start_date = datetime.strptime('??/??/????', '%d/%m/%Y')
end_date = datetime.strptime('??/??/????', '%d/%m/%Y')

# Loop over each day between the start and end dates
while start_date < end_date:
    
    # Create a unique temp table name
    table_name = 'table_name_{}_{}'.format(
        start_date.strftime('%Y%m%d'),
        random_string(8)
    )

    # Execute the query with the unique table name
    query = """
        
    """.format(# e.g table_name,                        # SQL Parameters in Order
               start_date.strftime('%Y%m%d'), 
               end_date.strftime('%Y%m%d'), 

    cur.execute(query)

    # Increment the start date by ? days / weeks / months
    start_date += timedelta(days="?")
    
    # Print completion date for the current day
    print(f"Query for {start_date - timedelta(days="?")} completed.")

# Close the cursor and connection
cur.close()
conn.close()

# finish
print("Script execution completed.")
