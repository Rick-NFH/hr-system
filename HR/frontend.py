from flask import Flask, render_template, request, redirect, url_for
import uuid

app = Flask(__name__)

# 保存生成的連結和員工提交的資料
employee_data = {}


@app.route('/')
def index():
    return render_template('generate_link.html')


@app.route('/generate_link', methods=['POST'])
def generate_link():
    # 生成一個唯一的 UUID 作為鏈接
    unique_id = str(uuid.uuid4())
    form_link = url_for('employee_form', unique_id=unique_id, _external=True)

    # 將新生成的鏈接存入 employee_data 字典，初始化為空數據
    employee_data[unique_id] = {'link': form_link, 'name': None, 'salary': None}

    return render_template('generate_link.html', form_link=form_link)


@app.route('/employee_form/<unique_id>', methods=['GET', 'POST'])
def employee_form(unique_id):
    if request.method == 'POST':
        name = request.form['name']
        salary = request.form['salary']
        # 保存提交的數據到 employee_data 字典
        if unique_id in employee_data:
            employee_data[unique_id]['name'] = name
            employee_data[unique_id]['salary'] = salary
        return f"恭喜 {name} 入職，工資為 {salary}！"

    return render_template('employee_form.html', unique_id=unique_id)


@app.route('/hr_dashboard')
def hr_dashboard():
    # 顯示所有生成的表單鏈接和員工提交的資料
    return render_template('hr_dashboard.html', employee_data=employee_data)


if __name__ == '__main__':
    app.run()
