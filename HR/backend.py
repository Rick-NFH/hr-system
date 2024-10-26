import sqlite3
from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)

# 資料庫路徑
DATABASE = '/home/rickneex/employee_data.db'

# 創建資料庫連接
def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

# 初始化資料庫
def init_db():
    conn = get_db_connection()
    with conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS employees (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL,
                            salary TEXT NOT NULL,
                            unique_link TEXT NOT NULL
                        );''')
    conn.close()

# 初始化資料庫
init_db()

# HR 查看所有提交的資料
@app.route('/hr')
def hr_dashboard():
    conn = get_db_connection()
    employees = conn.execute('SELECT * FROM employees').fetchall()
    conn.close()
    return render_template('hr_dashboard.html', employees=employees)

# 用於生成新員工的表單
@app.route('/generate-link', methods=['GET', 'POST'])
def generate_link():
    if request.method == 'POST':
        name = request.form['name']
        salary = request.form['salary']
        # 生成唯一連結
        unique_link = url_for('employee_form', employee_id=name, _external=True)
        conn = get_db_connection()
        conn.execute('INSERT INTO employees (name, salary, unique_link) VALUES (?, ?, ?)', (name, salary, unique_link))
        conn.commit()
        conn.close()
        return redirect(url_for('hr_dashboard'))
    return render_template('generate_link.html')

# 員工表單
@app.route('/employee/<employee_id>', methods=['GET', 'POST'])
def employee_form(employee_id):
    if request.method == 'POST':
        # 員工提交後顯示感謝頁面
        return "感謝您，提交已完成，恭喜入職！"
    return render_template('employee_form.html', employee_id=employee_id)

if __name__ == '__main__':
    app.run(debug=True)
