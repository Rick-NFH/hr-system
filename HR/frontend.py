from flask import Flask, render_template, request, redirect, url_for
import sqlite3

app = Flask(__name__)

# 資料庫連接
DATABASE = '/home/rickneex/employee_data.db'

def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

# 顯示生成連結的頁面
@app.route('/')
def index():
    return redirect(url_for('generate_link'))

# 新員工填寫表單並提交
@app.route('/submit', methods=['POST'])
def submit():
    name = request.form['name']
    salary = request.form['salary']
    unique_link = url_for('employee_form', employee_id=name, _external=True)
    conn = get_db_connection()
    conn.execute('INSERT INTO employees (name, salary, unique_link) VALUES (?, ?, ?)', (name, salary, unique_link))
    conn.commit()
    conn.close()
    return "提交成功！"

if __name__ == '__main__':
    app.run(debug=True)
