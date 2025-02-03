import tkinter as tk
import customtkinter as ctk
import mysql.connector
import psycopg2
import sqlite3
import pandas as pd
import os
from tkinter import messagebox, filedialog
from tkinter import ttk
from mysql.connector import pooling
import traceback

class MZDBApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("MZ-DB Application")
        self.geometry("800x700")
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")

        self.create_widgets()

        # Initialize connection pool (MySQL & PostgreSQL)
        self.mysql_pool = None
        self.pg_pool = None
        self.db_connection = None

    def create_widgets(self):
        # Labels and Input Fields
        self.db_type_label = ctk.CTkLabel(self, text="Database Type:")
        self.db_type_label.grid(row=0, column=0, padx=10, pady=10, sticky="e")
        
        self.db_type_var = tk.StringVar(value="MySQL")
        self.db_type_menu = ctk.CTkOptionMenu(self, variable=self.db_type_var, values=["MySQL", "PostgreSQL", "SQLite"])
        self.db_type_menu.grid(row=0, column=1, padx=10, pady=10, sticky="w")

        self.host_label = ctk.CTkLabel(self, text="Host:")
        self.host_label.grid(row=1, column=0, padx=10, pady=10, sticky="e")
        self.host_entry = ctk.CTkEntry(self)
        self.host_entry.grid(row=1, column=1, padx=10, pady=10, sticky="w")

        self.user_label = ctk.CTkLabel(self, text="User:")
        self.user_label.grid(row=2, column=0, padx=10, pady=10, sticky="e")
        self.user_entry = ctk.CTkEntry(self)
        self.user_entry.grid(row=2, column=1, padx=10, pady=10, sticky="w")

        self.password_label = ctk.CTkLabel(self, text="Password:")
        self.password_label.grid(row=3, column=0, padx=10, pady=10, sticky="e")
        self.password_entry = ctk.CTkEntry(self, show="*")
        self.password_entry.grid(row=3, column=1, padx=10, pady=10, sticky="w")

        self.database_label = ctk.CTkLabel(self, text="Database:")
        self.database_label.grid(row=4, column=0, padx=10, pady=10, sticky="e")
        self.database_entry = ctk.CTkEntry(self)
        self.database_entry.grid(row=4, column=1, padx=10, pady=10, sticky="w")

        # Buttons
        self.connect_button = ctk.CTkButton(self, text="Connect", command=self.connect_db)
        self.connect_button.grid(row=5, column=0, columnspan=2, pady=10, padx=10, sticky="ew")

        self.query_label = ctk.CTkLabel(self, text="Query:")
        self.query_label.grid(row=6, column=0, padx=10, pady=10, sticky="ne")
        self.query_text = ctk.CTkTextbox(self, height=5)
        self.query_text.grid(row=6, column=1, padx=10, pady=10, sticky="ew")

        self.execute_button = ctk.CTkButton(self, text="Execute Query", command=self.execute_query)
        self.execute_button.grid(row=7, column=0, columnspan=2, pady=10, padx=10, sticky="ew")

        self.export_button = ctk.CTkButton(self, text="Export Results", command=self.export_results)
        self.export_button.grid(row=8, column=0, columnspan=2, pady=10, padx=10, sticky="ew")

        self.export_sql_button = ctk.CTkButton(self, text="Export Schema to SQL", command=self.export_schema_to_sql)
        self.export_sql_button.grid(row=9, column=0, columnspan=2, pady=10, padx=10, sticky="ew")

        self.output_text = ctk.CTkTextbox(self, height=10)
        self.output_text.grid(row=10, column=0, columnspan=2, padx=10, pady=10, sticky="ew")

        self.status_label = ctk.CTkLabel(self, text="Status: Ready")
        self.status_label.grid(row=11, column=0, columnspan=2, padx=10, pady=10)

    def connect_db(self):
        try:
            db_type = self.db_type_var.get()
            host = self.host_entry.get()
            user = self.user_entry.get()
            password = self.password_entry.get()
            database = self.database_entry.get()

            if db_type == "MySQL":
                self.db_connection = mysql.connector.connect(
                    host=host, user=user, password=password, database=database)
                self.update_status("Connected to MySQL", "green")
                
                # Connection Pooling for MySQL
                self.mysql_pool = mysql.connector.pooling.MySQLConnectionPool(
                    pool_name="mzdb_pool",
                    pool_size=5,
                    host=host,
                    user=user,
                    password=password,
                    database=database
                )
                
            elif db_type == "PostgreSQL":
                self.db_connection = psycopg2.connect(
                    host=host, user=user, password=password, dbname=database)
                self.update_status("Connected to PostgreSQL", "green")
                
                # Connection Pooling for PostgreSQL
                self.pg_pool = psycopg2.pool.SimpleConnectionPool(
                    1, 5, host=host, user=user, password=password, dbname=database
                )
                
            elif db_type == "SQLite":
                self.db_connection = sqlite3.connect(database)
                self.update_status("Connected to SQLite", "green")

        except Exception as e:
            self.update_status(f"Connection failed: {str(e)}", "red")
            print(f"Error connecting to {db_type}: {str(e)}")

    def execute_query(self):
        query = self.query_text.get("1.0", tk.END).strip()
        if query:
            self.update_status("Executing query...", "yellow")
            try:
                cursor = self.db_connection.cursor()
                cursor.execute(query)
                results = cursor.fetchall()

                # Display results
                self.output_text.delete(1.0, tk.END)
                for row in results:
                    self.output_text.insert(tk.END, str(row) + "\n")
                self.update_status("Query executed successfully", "green")
            except Exception as e:
                self.update_status(f"Error executing query: {str(e)}", "red")
                print(f"Error executing query: {str(e)}")

    def export_results(self):
        # Fetch results to export
        query = self.query_text.get("1.0", tk.END).strip()
        if query:
            try:
                cursor = self.db_connection.cursor()
                cursor.execute(query)
                results = cursor.fetchall()

                # Ask for export type (CSV, JSON, Excel)
                export_type = messagebox.askquestion("Export", "Select export type: CSV, JSON or Excel")

                # Export to CSV
                if export_type == "CSV":
                    file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
                    if file_path:
                        df = pd.DataFrame(results)
                        df.to_csv(file_path, index=False)
                        self.update_status("Exported to CSV successfully", "green")

                # Export to JSON
                elif export_type == "JSON":
                    file_path = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON files", "*.json")])
                    if file_path:
                        df = pd.DataFrame(results)
                        df.to_json(file_path, orient="records")
                        self.update_status("Exported to JSON successfully", "green")

                # Export to Excel
                elif export_type == "Excel":
                    file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
                    if file_path:
                        df = pd.DataFrame(results)
                        df.to_excel(file_path, index=False)
                        self.update_status("Exported to Excel successfully", "green")

            except Exception as e:
                self.update_status(f"Error exporting results: {str(e)}", "red")
                print(f"Error exporting results: {str(e)}")

    def export_schema_to_sql(self):
        try:
            db_type = self.db_type_var.get()
            if not self.db_connection:
                self.update_status("Please connect to the database first.", "red")
                return
            
            folder_path = os.path.dirname(os.path.realpath(__file__))  # Current script directory
            sql_file = os.path.join(folder_path, "database_schema.sql")
            
            cursor = self.db_connection.cursor()
            
            if db_type == "MySQL":
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                with open(sql_file, "w") as f:
                    for table in tables:
                        table_name = table[0]
                        f.write(f"DROP TABLE IF EXISTS `{table_name}`;\n")
                        cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
                        create_table_sql = cursor.fetchone()[1]
                        f.write(create_table_sql + ";\n\n")
            elif db_type == "PostgreSQL":
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                tables = cursor.fetchall()
                with open(sql_file, "w") as f:
                    for table in tables:
                        table_name = table[0]
                        f.write(f"DROP TABLE IF EXISTS {table_name} CASCADE;\n")
                        cursor.execute(f"SELECT pg_get_tabledef('{table_name}')")
                        create_table_sql = cursor.fetchone()[0]
                        f.write(create_table_sql + ";\n\n")
            elif db_type == "SQLite":
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                with open(sql_file, "w") as f:
                    for table in tables:
                        table_name = table[0]
                        f.write(f"DROP TABLE IF EXISTS {table_name};\n")
                        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                        create_table_sql = cursor.fetchone()[0]
                        f.write(create_table_sql + ";\n\n")

            self.update_status(f"Schema exported to {sql_file} successfully.", "green")

        except Exception as e:
            self.update_status(f"Error exporting schema: {str(e)}", "red")
            print(f"Error exporting schema: {str(e)}")

    def update_status(self, text, color):
        self.status_label.configure(text=text, text_color=color)

if __name__ == "__main__":
    try:
        app = MZDBApp()
        app.mainloop()
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
