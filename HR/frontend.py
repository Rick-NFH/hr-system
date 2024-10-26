from flask import Flask, render_template, request, redirect, url_for
import sqlite3
import uuid

app = Flask(__name__)

# 創建數據庫連接
def create_connection():
    conn = sqlite3.connect('database.db')
    return conn

# 創建員工表單頁面
@app.route('/new_employee/<link_id>', methods=['GET', 'POST'])
def new_employee(link_id):
    if request.method == 'POST':
        name = request.form['name']
        salary = request.form['salary']

        conn = create_connection()
        cursor = conn.cursor()

        cursor.execute("INSERT INTO employees (name, salary) VALUES (?, ?)", (name, salary))
        conn.commit()
        conn.close()

        return "恭喜入職！"

    return render_template('employee_form.html')

# 生成唯一連結
@app.route('/generate_link')
def generate_link():
    link_id = str(uuid.uuid4())
    link = f"{request.host_url}new_employee/{link_id}"
    return f"給新人提供這個連結: {link}"

if __name__ == '__main__':
    app.run()
