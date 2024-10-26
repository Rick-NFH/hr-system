from flask import Flask, render_template, request, redirect, url_for
import csv
import os

app = Flask(__name__)

# CSV 檔案路徑
FILE_PATH = '/home/rickneex/hr-system/employee_data.csv'

# 檢查 CSV 檔案是否存在，若不存在則創建並添加標題
if not os.path.exists(FILE_PATH):
    with open(FILE_PATH, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['姓名', '薪資'])  # CSV 標題


# 顯示生成連結頁面
@app.route('/')
def index():
    return render_template('generate_link.html')  # 渲染首頁模板，顯示連結生成頁面


# 處理提交表單的路由
@app.route('/submit', methods=['POST'])
def submit():
    employee_name = request.form['name']
    employee_salary = request.form['salary']

    # 將表單數據追加到 CSV 檔案中
    with open(FILE_PATH, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([employee_name, employee_salary])

    # 提交成功後顯示成功頁面
    return render_template('success.html')


# 顯示 HR 後台資料的頁面
@app.route('/hr_dashboard')
def hr_dashboard():
    employees = []

    # 讀取 CSV 檔案中的員工資料
    with open(FILE_PATH, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # 跳過標題行
        for row in reader:
            employees.append({'name': row[0], 'salary': row[1]})

    return render_template('hr_dashboard.html', employees=employees)


if __name__ == '__main__':
    app.run(debug=True)
