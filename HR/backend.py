from flask import Flask, render_template
import sqlite3

app = Flask(__name__)

# 創建數據庫連接
def create_connection():
    conn = sqlite3.connect('database.db')
    return conn

# 後台數據頁面
@app.route('/hr_dashboard')
def hr_dashboard():
    conn = create_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM employees")
    employees = cursor.fetchall()
    conn.close()

    return render_template('hr_dashboard.html', employees=employees)

if __name__ == '__main__':
    app.run()
