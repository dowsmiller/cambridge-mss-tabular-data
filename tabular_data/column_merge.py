import os
import argparse
import json
import traceback
from io import StringIO
import pandas as pd
from flask import Flask, render_template, request, redirect, make_response
from waitress import serve
from werkzeug.utils import secure_filename
from column_merge_helpers import load_and_merge

template_dir = os.path.join(os.path.dirname(__file__), 'column_merge', 'templates')
static_folder = os.path.join(os.path.dirname(__file__), 'column_merge', 'static')

app = Flask(__name__, template_folder=template_dir, static_folder=static_folder)

uploaded_files_data = {}

merged_df_cache = {}

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        uploaded_files_data.clear()
        files = request.files.getlist('files')
        for file in files:
            filename = secure_filename(file.filename) if file.filename else ""
            content = file.read()
            content_str = content.decode('utf-8-sig').lstrip('\r\n')
            uploaded_files_data[filename] = content_str

        return redirect('/select?files=' + ','.join(uploaded_files_data.keys()))

    instructions = (
        "Step 1: Upload one or more CSV files. "
        "Click 'Select files' to choose files, then 'Continue' to proceed."
    )
    return render_template('index.html', instructions=instructions)

@app.route('/select')
def select():
    file_names_param = request.args.get('files')
    if not file_names_param:
        return redirect('/')

    file_names = file_names_param.split(',')
    all_columns = {}
    for filename in file_names:
        content_str = uploaded_files_data.get(filename)
        if not content_str:
            continue
        df = pd.read_csv(StringIO(content_str), nrows=5, encoding='utf-8-sig')
        df.columns = df.columns.str.strip()
        all_columns[filename] = df.columns.tolist()

    safe_keys = {
        filename: filename.replace('.', '_').replace(' ', '_').replace(':', '_').replace('/', '_')
        for filename in all_columns.keys()
    }

    instructions = (
        "Step 2: Select the columns to include from each file. "
        "Choose a merge key (the column used to combine files) for each file. "
        "The default is the 'Part ID' column, if found."
    )

    return render_template(
        'select_columns.html',
        files=all_columns,
        safe_keys=safe_keys,
        instructions=instructions
    )

@app.route('/require_values', methods=['GET', 'POST'])
def require_values():
    if request.method == 'POST':
        form = request.form.to_dict(flat=False)

        merge_keys = {}
        files_with_columns = {}
        for key, values in form.items():
            if key == 'files':
                continue
            if key.endswith('_merge_key'):
                file = os.path.basename(key.rsplit('_merge_key', 1)[0])
                merge_keys[file] = values[0]
            else:
                files_with_columns[key] = values

        for file, merge_key in merge_keys.items():
            if file not in files_with_columns:
                files_with_columns[file] = []
            if merge_key not in files_with_columns[file]:
                files_with_columns[file].append(merge_key)

        instructions = (
            "Step 3: Choose which columns must contain data, and optionally exclude empty rows. "
        )

        return render_template('require_values.html', files=files_with_columns,
                               files_json=json.dumps(files_with_columns),
                               merge_keys=merge_keys,
                               instructions=instructions)
    return redirect('/select')

@app.route('/merge', methods=['POST'])
def merge():
    form = request.form.to_dict(flat=False)

    exclude_empty = form.pop('exclude_empty_rows', ['false'])[0] == 'true'
    exclude_duplicates = form.pop('exclude_duplicates', ['false'])[0] == 'true'
    required_columns = form.pop('required_columns', [])
    files_with_columns = json.loads(form['files'][0])

    merge_keys = {key.rsplit('_merge_key', 1)[0]: val[0] for key, val in form.items() if key.endswith('_merge_key')}

    csv_contents = {filename: uploaded_files_data[filename] for filename in files_with_columns.keys()}

    merged_df = load_and_merge(files_with_columns, merge_keys, csv_contents)

    # Rename merge key column
    merged_df.rename(columns={'metadata: part ID': 'merge_key'}, inplace=True)

    if exclude_empty or required_columns:
        if required_columns:
            prefixed_required_cols = []
            for col in required_columns:
                for filename, cols in files_with_columns.items():
                    prefix = os.path.splitext(filename)[0]
                    if col in cols:
                        prefixed_required_cols.append(f"{prefix}: {col}")
            cols_to_check = prefixed_required_cols
        else:
            cols_to_check = [col for col in merged_df.columns if col != 'merge_key']

        merged_df = merged_df.dropna(subset=cols_to_check, how='any')

    if exclude_duplicates:
        merged_df = merged_df.drop_duplicates()

    merged_df_cache['data'] = merged_df

    html_table = merged_df.fillna('').to_html(classes='data-table', index=False)

    return render_template('result.html', table=html_table)

@app.route("/download_csv")
def download_csv():
    if 'data' not in merged_df_cache:
        return render_template('error.html', error_message="No merged data available to download."), 404

    output = StringIO()
    merged_df_cache['data'].to_csv(output, index=False, encoding='utf-8-sig')
    csv_data = output.getvalue()

    response = make_response(csv_data)
    response.headers["Content-Disposition"] = "attachment; filename=merged.csv"
    response.headers["Content-Type"] = "text/csv"
    return response

@app.route("/download_json")
def download_json():
    if 'data' not in merged_df_cache:
        return render_template('error.html', error_message="No merged data available to download."), 404

    json_data = merged_df_cache['data'].fillna('').to_json(orient='records', force_ascii=False)

    response = make_response(json_data)
    response.headers["Content-Disposition"] = "attachment; filename=merged.json"
    response.headers["Content-Type"] = "application/json"
    return response

@app.errorhandler(Exception)
def handle_all_exceptions(e):
    tb = traceback.format_exc()
    app.logger.error(f"Unhandled Exception: {tb}")

    error_msg = (
        "An unexpected error occurred. "
        "Please try again."
    )

    if app.debug:
        error_msg += f"<pre>{tb}</pre>"

    return render_template('error.html', error_message=error_msg), 500

@app.errorhandler(404)
def page_not_found(e):
    return render_template('error.html', error_message="404 Not Found: The requested page does not exist."), 404

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run the column merge web app.")
    parser.add_argument('--debug', action='store_true', help='Enable debug mode.')

    args = parser.parse_args()

    app.debug = args.debug

    host = '127.0.0.1'
    port = 5000

    print(f"Starting server at http://{host}:{port}/")
    print("Press CTRL+C to quit.")

    if args.debug:
        app.run(debug=True, use_reloader=False, host=host, port=port)
    else:
        serve(app, host=host, port=port)