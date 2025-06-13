# STO's library for handling data
This project is still very early in the development phase. The idea is for it to serve as a library for handling database connections and common operations made in Python. Said operations include:

+ Loading tables into pandas DataFrames.
+ Saving pandas DataFrames into tables of the database, with proper datatypes, and primary key and foreign key annotations.
+ Comfortably normalizing dataframes by replacing sets of columns with foreign keys.
