import pyodbc
from decouple import config

# Define the database connection parameters
server = None
database = None

# Specify the path to the abc.txt file
# file_path = 'pers.txt'
#
# # Open and read the file
# try:
#     with open(file_path, 'r') as file:
#         server = file.readline().strip()
#         database = file.readline().strip()
# except FileNotFoundError:
#     print(f"The file '{file_path}' was not found.")
# except Exception as e:
#     print(f"An error occurred: {str(e)}")
server = config('SVR')
database = config('DBS')

# Define the SQL INSERT query
insert_query = "INSERT INTO Employees (Name, Surname, Salary) VALUES (?, ?, ?)"

# Create a list of parameter values for the INSERT query
parameter_values = ["Jeff", "Davids", "1250,50"]


try:
    # Establish a connection to the database
    connection = pyodbc.connect(
        f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};'
    )

    # Create a cursor object to interact with the database
    cursor = connection.cursor()

    # Execute the INSERT query with parameter values
    cursor.execute(insert_query, parameter_values)
    connection.commit()

    # Check the number of rows affected by the INSERT operation
    rows_affected = cursor.rowcount

    if rows_affected > 0:
        print("Insertion successful.")
    else:
        print("No rows were inserted.")

    # Close the cursor and database connection
    cursor.close()
    connection.close()

except Exception as ex:
    print(f"Error: {str(ex)}")

