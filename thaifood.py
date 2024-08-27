import sqlite3
import pandas as pd
import sys 


# Connect to SQLite database
conn = sqlite3.connect('thailand_foods.db')
cursor = conn.cursor()

# Drop the table if it exists
cursor.execute("DROP TABLE IF EXISTS ingredients")
cursor.execute("DROP TABLE IF EXISTS thailand_foods")
# Create the table
create_table_query = """
CREATE TABLE thailand_foods (
    dish_id INTEGER PRIMARY KEY AUTOINCREMENT,
    en_name VARCHAR(255),
    th_name VARCHAR(255),
    ingredients TEXT,
    course VARCHAR(100),
    province VARCHAR(100),
    region VARCHAR(100)
);
"""
cursor.execute(create_table_query)

create_table_query2= """
CREATE TABLE ingredients (
    dish_id INTEGER,
    ingredient VARCHAR(255),
    FOREIGN KEY (dish_id) REFERENCES thailand_foods(dish_id)
)
"""
cursor.execute(create_table_query2)
# Load CSV and insert data
df = pd.read_csv('thailand_foods.csv')
insert_query = """
INSERT INTO thailand_foods (en_name, th_name, ingredients, course, province, region)
VALUES (?, ?, ?, ?, ?, ?)
"""

for index, row in df.iterrows():
    cursor.execute(insert_query, tuple(row))
cursor.execute("SELECT dish_id, ingredients FROM thailand_foods")
rows = cursor.fetchall()

for row in rows:
    dish_id = row[0]
    ingredients = row[1].split('+')  # Assuming ingredients are separated by '+'
    for ingredient in ingredients:
        cursor.execute("INSERT INTO ingredients (dish_id, ingredient) VALUES (?, ?)", (dish_id, ingredient.strip()))


conn.commit()

if sys.argv[1] == "1":
# Query 1 Find the top 3 provinces with the most number of dishes listed in the thailand_foods table. 
    query1="""
    SELECT province, count(*) as dish_counts
    FROM thailand_foods
    GROUP BY province
    ORDER BY dish_counts DESC
    LIMIT 3
    """
    cursor.execute(query1)
    rows = cursor.fetchall()

    for place, number in rows:
        print(f"{place} {number}")

# Query 2 Find the most popular ingredients 
elif sys.argv[1] == "2":
    query2="""
    SELECT ingredient, COUNT(ingredient) as count
    FROM ingredients
    GROUP BY ingredient
    ORDER BY count DESC
    """

    cursor.execute(query2)
    popular_ingredients  = cursor.fetchall()
    # print(popular_ingredients)

    for ingredients, amount in popular_ingredients:
        # skipping unkown ingredients
        if ingredients != "Unknown":
            print(f"{ingredients} {amount}")
            break



# Query 3
# Find the most common course type and list the top 2 course types along with
# the number of dishes associated with each.

elif sys.argv[1] == "3":
    query3="""
    SELECT course, COUNT(course) as number
    FROM thailand_foods
    GROUP BY course
    ORDER BY number DESC
    LIMIT 2
"""

cursor.execute(query3)
popular_course = cursor.fetchall()

for courses, amount in popular_course:
    print(f"{courses} {amount}")

# Close connection
cursor.close()
conn.close()
