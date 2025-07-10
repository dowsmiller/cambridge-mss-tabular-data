from flask import Flask, render_template, request, redirect, make_response
import pandas as pd
import os
from werkzeug.utils import secure_filename
from app_helpers import load_and_merge
import json

template_dir = os.path.join(os.path.dirname(__file__), 'app', 'templates')
upload_folder = os.path.join(os.path.dirname(__file__), 'app', 'uploads')
static_folder = os.path.join(os.path.dirname(__file__), 'app', 'static')

app = Flask(__name__, template_folder=template_dir, static_folder=static_folder)
app.config['UPLOAD_FOLDER'] = upload_folder

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        files = request.files.getlist('files')
        file_paths = []
        for file in files:
            filename = secure_filename(file.filename)
            path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(path)
            file_paths.append(path)
        return redirect('/select?files=' + ','.join(file_paths))
    return render_template('index.html')

@app.route('/select')
def select():
    file_paths_param = request.args.get('files')
    if not file_paths_param:
        return redirect('/')  # Redirect to the upload page if no files are provided

    # Read column names from uploaded files
    file_paths = file_paths_param.split(',')
    all_columns = {fp: pd.read_csv(fp, nrows=5).columns.tolist() for fp in file_paths}
    return render_template('select_columns.html', files=all_columns)

@app.route('/require_values', methods=['GET', 'POST'])
def require_values():
    if request.method == 'POST':
        # Retrieve selected columns from the previous step
        form_data = request.form.to_dict(flat=False)
        files_with_columns = {file: columns for file, columns in form_data.items() if file != 'files'}

        return render_template('require_values.html', files=files_with_columns)

    # Handle GET request to allow navigation back to this page
    # You may need to pass default or cached data here
    return redirect('/select')

merged_csv_path = os.path.join(app.config['UPLOAD_FOLDER'], 'merged_output.csv')

@app.route('/merge', methods=['POST'])
def merge():
    global merged_df
    form_data = request.form.to_dict(flat=False)

    exclude_empty = form_data.pop('exclude_empty_rows', ['false'])[0] == 'true'
    required_columns = form_data.pop('required_columns', [])

    files_with_columns = {file: columns for file, columns in form_data.items() if file != 'required_columns' and file != 'exclude_empty_rows'}

    # Parse the files JSON string back into a dictionary
    files_with_columns = json.loads(form_data['files'][0])

    merged_df = load_and_merge(files_with_columns)

    if exclude_empty or required_columns:
        part_id_col = 'metadata: part ID'
        cols_to_check = [col for col in merged_df.columns if col in required_columns]
        if exclude_empty:
            cols_to_check += [col for col in merged_df.columns if col != part_id_col]
        # Ensure rows are excluded if any required column is empty
        merged_df = merged_df.dropna(subset=cols_to_check, how='any')

    merged_df.to_csv(merged_csv_path, index=False, encoding='utf-8-sig')

    html_table = merged_df.fillna('').to_html(classes='data-table', index=False)
    return render_template('result.html', table=html_table)

@app.route("/download_csv")
def download():
    global merged_df
    response = make_response(merged_df.fillna('').to_csv(index=False))
    response.headers["Content-Disposition"] = "attachment; filename=merged.csv"
    response.headers["Content-Type"] = "text/csv"
    return response

@app.route("/download_json")
def download_json():
    global merged_df
    response = make_response(merged_df.fillna('').to_json(orient='records', force_ascii=False))
    response.headers["Content-Disposition"] = "attachment; filename=merged.json"
    response.headers["Content-Type"] = "application/json"
    return response

if __name__ == '__main__':
    app.run(debug=True)
