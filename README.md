# Fundamentals of Data Science and Engineering - Databases
# Project Wildfire

This project is designed to store Portugal fire registers available in Instituto da Conservação das Naturezas e Florestas. It was applied all the knowledge from Databases discipline, such as UML Modeling, Relational Models and Python.
This repository includes UML Model, Relational Model, scripts to setup the database and also a Python application to load and manage the data in the database.

## Group 7
Andre de Oliveira - up202403079

Daniel Gil Martinez - up202400081

Project deliverables were developed with contributions of all team members. In order to highlight the main contributions of each participant in our group, database design and scripts were concentraded with Andre de Oliveira and Python tasks were mostly developed by Daniel Gil Martinez. 

## Overview

Key included deliverables:
- Database Design
  - UML diagram: uml.png
  - Relational model: relational.doc
  - SQL script that creates the database: fires.sql
- Python application: Wildfires folder. Start the application running main.py
  - Load Data from Excel: Select menu `[5] Load data from excel`
  - User Interaction: Different options are available in main.py

## Features in Python Application

- **Manipulate data in the database**: The application allows to insert, update and delete data from the database.
- **Load data from excel file**: It load the database with the data from wildfires.xlsx. The application deletes all the existing data before start loadign the file records.
- **Run Queries**: You can run many pre defined queries im the database.
- **Check Graphics**: You can see different graphics implemented.

## Technologies Used

- **SQL**: [PostgreSQL]
- **Python**: For data manipulation and automation.

## Project Structure

```plaintext
db_project/
├── wildfires/          # Python application to manage the database and make queries.
│   └── main.py         # Starts the application with all possible actions described in the menu.
│   └── wildfires.xlsx  # Fire dataset available to be loaded by the application.
├── uml.png             # UML Diagram
├── fires.sql           # DDL Script to create all database tables and constraints.
├── relational.doc      # Relational Model
├── requirements.txt    # List of dependencies (if using Python scripts)
└── LICENSE             # License file
