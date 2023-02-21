# Note that this code is written to be as easy to understand as possible.
# There are many ways to write this in a more concise manner, but this way
# will still work just fine.

# Importing the required packages for all your data framing needs.
import pandas as pd

# The Snowflake Connector library.
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

import os, sys
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from utils import snowpark_utils
session = snowpark_utils.get_snowpark_session()

# (FYI, if you don't want to hard code your password into a script, there are 
# some other options for security.)

# Create a cursor object.


# Execute a statement that will delete the data from the current folder.
# If you would prefer not to do this, then just comment it out.
# In fact, we'll leave it commented out, just in case the data file you 
# are importing will be appended to the existing table, and not replacing it.
# sql = "truncate table if exists YOUR_TABLE_NAME"
# cur.execute(sql)

# Close the cursor.


## Phase II: Upload from the Exported Data File.
# Let's import a new dataframe so that we can test this.
original = r"/Users/sodonnell/Documents/NBC_SandySpring_reviews.csv" # <- Replace with your path.
delimiter = "," # Replace if you're using a different delimiter.

# Get it as a pandas dataframe.
total = pd.read_csv(original, sep = delimiter)

# Drop any columns you may not need (optional).
# total.drop(columns = ['A_ColumnName',
#                       'B_ColumnName'],
#                        inplace = True)

# Rename the columns in the dataframe if they don't match your existing table.
# This is optional, but ESSENTIAL if you already have created the table format
# in Snowflake.
# total.rename(columns={"A_ColumnName": "A_COLUMN", 
#                       "B_ColumnName": "B_COLUMN"},
#                        inplace=True)

# Actually write to the table in snowflake.
write_pandas(session, total, "YOUR_TABLE_NAME")

# (Optionally, you can check to see if what you loaded is identical
# to what you have in your pandas dataframe. Perhaps... a topic for a future 
# blog post.)

## Phase III: Turn off the warehouse.
# Create a cursor object.

# Execute a statement that will turn the warehouse off.
sql = "ALTER WAREHOUSE WAREHOUSE SUSPEND"
cur.execute(sql)

# Close your cursor and your connection.
cur.close()
conn.close()

# And that's it. Much easier than using the load data utility, but maybe
# not as user friendly.