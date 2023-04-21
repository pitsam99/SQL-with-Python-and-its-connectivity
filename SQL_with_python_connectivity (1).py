#!/usr/bin/env python
# coding: utf-8

# In[105]:


import mysql.connector
from mysql.connector import Error
import pandas as pd


# In[106]:


mydb =mysql.connector.connect (
            host="localhost",
            user="root",
            passwd="****")
print(mydb)


# In[107]:


mycursor=mydb.cursor()
mycursor.execute("SHOW DATABASES")

# output in list
# databases = [db[0] for db in mycursor]  
# databases

# output in tupple
for db in mycursor:
    print(db)


# In[76]:


mycursor=mydb.cursor()
mycursor.execute(" USE sql1")
mycursor.execute(" SHOW TABLES")

for tb in mycursor:
    print(tb)
    
# tabl=[tb[0] for tb in mycursor]
# tabl


# In[77]:


mycursor=mydb.cursor()
mycursor.execute(" USE sql1")
mycursor.execute(" SELECT * FROM sql1.category")


for data in mycursor:
    print(data)


# #  1st Method
# ## The pd.read_sql() method reads data from the specified SQL query and returns a pandas dataframe with the retrieved data. 

# In[92]:


mydbs=mysql.connector.connect(host="localhost",user="root",passwd="****",database="sql1")
df=pd.read_sql("SELECT * from category",con=mydbs)


# In[93]:


df


# # 2nd Method
# ## Connect to the database with sqlalchemy 
# 
# ## The pd.read_sql_table method reads data from the specified SQL query and returns a pandas dataframe. 

# In[94]:


from sqlalchemy import create_engine

# Connect to the database
engine = create_engine('mysql+mysqlconnector://root:*****@localhost:3306/sql1')

# Read a table into a DataFrame
df1 = pd.read_sql_table('category', con=engine)


# In[100]:


df1


# In[ ]:





# In[82]:


df1


# In[101]:


df_amended=df.rename(columns={"category": "new_category"})


# In[104]:


df_amended


# # Export a Pandas DataFrame to MySQL database
# 

# In[99]:


# Connect to the MySQL database using sqlalchemy
engine = create_engine('mysql+mysqlconnector://root:****@localhost:3306/sql1')

# Insert the data from the dataframe into the MySQL database
df_amended.to_sql('ameded_category', con=engine, if_exists='append', index=False)

# Close the database connection
engine.dispose()


# In[109]:


mycursor=mydb.cursor()

mycursor.execute("USE sql1")
mycursor.execute("SHOW TABLES")

for tb in mycursor:
    print(tb)


# # Creating new database and table

# In[111]:


mydb = mysql.connector.connect(host="localhost",user="root",passwd="****")

# Create database and table
mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE python_example;USE python_example;CREATE TABLE customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), address VARCHAR(255))")


# In[112]:


# mydb = mysql.connector.connect(host="localhost",user="root",passwd="*****")

# # Create database and table
# mycursor = mydb.cursor()
# mycursor.execute("CREATE DATABASE python_example2")
# mycursor.execute("USE python_example2")
# mycursor.execute("CREATE TABLE customers2 (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), address VARCHAR(255))")


# # Inserting values into the table

# In[114]:



mydb = mysql.connector.connect(host="localhost",user="root",passwd="****",database="python_example")
mycursor = mydb.cursor()
sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
val = [
  ("rakesh", "kolata"),
  ("dhiman", "kolkata"),
  ("chadu", "ahmedabad"),
  ("ankit", "bangalore")
]
mycursor.executemany(sql, val)
mydb.commit()
print(mycursor.rowcount, "records inserted.")


# In[124]:


pd.read_sql("SELECT * from customers",con=mydb)


# # Updating values in the table

# In[125]:


mydb = mysql.connector.connect(host="localhost",user="root",passwd="****",database="python_example")
mycursor = mydb.cursor()
sql = "UPDATE customers SET name = %s WHERE name = %s"
val = ("chadu ghosh", "chadu")

mycursor.execute(sql, val)
mydb.commit()


# In[126]:


pd.read_sql("SELECT * from customers",con=mydb)


# In[ ]:




