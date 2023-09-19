import pyodbc

# Define the database connection parameters
server = 'LAPTOP-JFG8PKR1'
database = 'TestDB'

# Define the SQL INSERT query
insert_query = "INSERT INTO Employees (Name, Surname, Salary) VALUES (?, ?, ?)"

# Create a list of parameter values for the INSERT query
parameter_values = ["Sam", "Jackson", "600"]

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

