from flask import Flask, render_template
import csv

app = Flask(__name__)

# 文件存儲路徑
FILE_PATH = '/home/rickneex/hr-system/employee_data.csv'

# HR後台查看所有員工的資料
@app.route('/hr_dashboard')
def hr_dashboard():
    employees = []
    with open(FILE_PATH, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # 跳過標題行
        for row in reader:
            employees.append({'name': row[0], 'salary': row[1]})
    return render_template('hr_dashboard.html', employees=employees)

if __name__ == '__main__':
    app.run(debug=True)
