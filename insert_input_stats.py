import csv
from database import DatabaseHandler

# Create an instance of DatabaseHandler
db_handler = DatabaseHandler()

try:
    # Connect to the database
    db_handler.connect()
    
    # Initialize a list to store all data tuples
    data_list = []
    
    # Read CSV file
    with open('input_stats.csv', 'r') as file:
        csv_reader = csv.reader(file)
        
        # Skip the first two header rows
        next(csv_reader)
        next(csv_reader)

        # Process each row
        for row in csv_reader:
            try:
                # Extract data
                industry_group = row[0]
                count = int(row[1].replace(',', '')) if row[1] else None
                
                # Extract other columns, converting to float where possible
                data = [industry_group, count]
                for value in row[2:]:
                    try:
                        data.append(float(value.strip('%')) if '%' in value else float(value))
                    except ValueError:
                        data.append(None)

                # Convert all elements in the data list to strings
                data = [str(item) if item is not None else None for item in data]

                # Convert the list to a tuple and add to the data_list
                data_list.append(tuple(data))
            
            except Exception as e:
                print(f"An error occurred while processing row: {row}")
                print(f"Error: {e}")
    
    # Insert all data into the database in one shot
    if data_list:
        sql_query = '''
        INSERT INTO input_stats VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
        '''
        
        try:
            db_handler.execute_query_many(sql_query, data_list)
            print(f"Inserted {len(data_list)} rows successfully.")
        except Exception as e:
            print(f"An error occurred while inserting data: {e}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the connection
    db_handler.close()
    print("Database connection closed.")
