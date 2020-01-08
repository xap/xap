# Script for creating demo schema and data
CREATE TABLE Person(Id INTEGER PRIMARY KEY,FirstName VARCHAR(20),LastName VARCHAR(30),Age INTEGER)
INSERT INTO Person VALUES (1, 'John', 'Doe', 21)
INSERT INTO Person VALUES (2, 'Jane', 'Smith', 21)