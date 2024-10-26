from flask import Flask, render_template, request, redirect, url_for
import csv
import os

app = Flask(__name__)

# CSV 檔案路徑
FILE_PATH = '/home/rickneex/hr-system/employee_data.csv'


# 渲染生成連結頁面
@app.route('/')
def index():
    return render_template('generate_link.html')


# 處理提交的路由
@app.route('/submit', methods=['POST'])
def submit():
    employee_name = request.form['name']
    employee_salary = request.form['salary']

    # 將表單提交的數據保存到 CSV 檔案
    with open(FILE_PATH, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([employee_name, employee_salary])

    return render_template('success.html')


if __name__ == '__main__':
    app.run(debug=True)
