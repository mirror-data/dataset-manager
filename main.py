import os
import sqlite3
from uuid import uuid4

from flask import Flask, request, jsonify

from csv2db import csv2db

myenv = os.environ.copy()

app = Flask(__name__)
UPLOAD_FOLDER = '/tmp/flask'
ALLOWED_EXTENSIONS = {'csv'}

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/')
def index():
    return """
    <!doctype html>
    <title>Mirror dataset manager</title>
    <h1>Mirror Dataset Manager</h1>
    <h3> <code>POST /upload</code></h3>
    <p>to upload a csv file and convert it to a sqlite database. </p>
    <h3><code>GET /list</code>   </h3>
    <p>to list all the databases. </p>
    <h3><code>POST /{UUID}/exec</code>   </h3>
    <p>to execute a query on a database. request json body: <code>sql</code>  </p>
    <h3><code>GET /{UUID}/schema</code>   </h3>
    <p>to get the schema of a database. </p>
    """


@app.route('/upload', methods=['GET', 'POST'])
def upload_dataset():
    if request.method == 'POST':
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'})

        file = request.files['file']
        if not file or file.filename == '':
            return jsonify({'error': 'No selected file'})
        if not allowed_file(file.filename):
            return jsonify({'error': 'File type not allowed. only csv files are allowed'})

        uuid = str(uuid4())
        dirname = os.path.join(app.config['UPLOAD_FOLDER'], uuid)
        csv_file = os.path.join(dirname, "data.csv")
        db_file = os.path.join(dirname, "data.db")
        name_file = os.path.join(dirname, "name")
        os.makedirs(dirname)
        with open(name_file, 'w') as f:
            f.write(file.filename)
        file.save(csv_file)

        error = csv2db(csv_file, db_file)
        if error:
            return jsonify({'error': 'Error while converting csv to sqlite, msg: ' + error.decode('utf-8')})

        if not os.path.exists(db_file):
            return jsonify({'error': 'Error while converting csv to sqlite, file not found'})

        return jsonify({'success': 'File uploaded successfully', 'uuid': uuid})
    return '''
    <!doctype html>
    <title>Upload Your CSV</title>
    <h1>Upload your csv</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''


@app.route('/list', methods=['GET'])
def list_datasets():
    ids = os.listdir(app.config['UPLOAD_FOLDER'])
    results = []
    for uuid in ids:
        with open(os.path.join(app.config['UPLOAD_FOLDER'], uuid, 'name'), 'r') as f:
            name = f.read()
        results.append({'uuid': uuid, 'name': name})
    return jsonify(results)


@app.route('/<uuid>/exec', methods=['POST'])
def get_dataset(uuid):
    sql = request.json['sql']
    if not sql:
        return jsonify({'error': 'sql query is required'})
    db_file = os.path.join(app.config['UPLOAD_FOLDER'], uuid, 'data.db')
    if not os.path.exists(db_file):
        return jsonify({'error': 'dataset not found'})
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    return jsonify(rows)


@app.route('/<uuid>/schema', methods=['GET'])
def get_dataset_schema(uuid):
    db_file = os.path.join(app.config['UPLOAD_FOLDER'], uuid, 'data.db')
    if not os.path.exists(db_file):
        return jsonify({'error': 'dataset not found'})
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info('dataset')")
    rows = cur.fetchall()
    return jsonify(rows)


if __name__ == "__main__":
    app.run(debug=True)
