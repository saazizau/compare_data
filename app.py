from flask import Flask, request, jsonify, send_file
from flask_wtf import FlaskForm
from wtforms import FileField, SubmitField
from wtforms.validators import InputRequired
from werkzeug.utils import secure_filename
import os
from flask_cors import CORS
import pandas as pd
import json
from array import array
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from io import StringIO, BytesIO
import numpy as np


def allowed_file(filename):
    ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsx'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_columns(pathfile1, pathfile2):
    file1 = pd.read_csv(pathfile1)
    file2 = pd.read_csv(pathfile2)
    
    hasil = {
        'file1': {'indeks':list(range(0,len(file1.columns.values))),'nama':file1.columns.values.tolist()},
        'file2': {'indeks':list(range(0,len(file2.columns.values))),'nama':file2.columns.values.tolist()}
    }
    return hasil

def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

def write_csv_file(df, file_path):
    try:
        df.to_csv(file_path, index=False)
        print(f"CSV file written successfully: {file_path}")
    except Exception as e:
        print(f"Error writing CSV file: {e}")

def process_data(file1, deleted1, file2, deleted2, kunci_file1, kunci_file2, persentase):
    if (file1 is None) or (file2 is None):
        return pd.DataFrame()

    hasil = pd.concat([file1,file2], axis=1)
    hasil = hasil.drop(hasil.index)

    # Mengambil nama kolom kunci
    nama_kunci_file1 = file1.columns[kunci_file1]
    nama_kunci_file2 = file2.columns[kunci_file2]
    file1 = file1[~file1[nama_kunci_file1].isna()]
    file2 = file2[~file2[nama_kunci_file2].isna()]

    # Mengambil data pada kolom kunci
    kunci_file1 = file1.loc[file1[nama_kunci_file1].notna(),nama_kunci_file1].values
    kunci_file2 = file2.loc[file2[nama_kunci_file2].notna(),nama_kunci_file2].values

    # Definisikan fungsi untuk mencari klinik yang cocok dari satu sumber data ke sumber data lainnya
    def find_matching_data(source_names, target_names):

        matches = {}
        for source_name in source_names:
            match = process.extractOne(source_name, target_names, scorer=fuzz.token_set_ratio)
            if match[1] > persentase:  # Ambil hanya jika kesamaannya lebih dari 80%
                matches[source_name] = match[0]
        return matches
    
    data_hasil_matching = pd.DataFrame(columns=['File 1', 'File 2'])
    matches_file2_file1 = find_matching_data(kunci_file1, kunci_file2)

    for source_name, match in matches_file2_file1.items():
        data_hasil_matching = data_hasil_matching._append({'File 1':source_name, 'File 2':match}, ignore_index=True)
    
    for _ in range(max(file1.shape[0],file2.shape[0])+1):
        baris_kosong = pd.DataFrame({}, index=[0])
        hasil = hasil._append(baris_kosong, ignore_index=True)
        
    for i, value in enumerate(file1[nama_kunci_file1].values):
        i+=1
        hasil.iloc[i-1:i, :file1.shape[1]] = file1.iloc[i-1:i,:]
        if value in data_hasil_matching['File 1'].values:
            nama_file_2 = data_hasil_matching.loc[data_hasil_matching['File 1'] == value, 'File 2'].values[0]
            hasil.iloc[i-1:i, file1.shape[1]:] = file2.loc[file2[nama_kunci_file2] == nama_file_2, :].iloc[0]

    # Menghapus Kolom
    deleted2 = np.array(deleted2) + file1.shape[1]
    hasil = hasil.drop(hasil.columns[deleted1 + deleted2.tolist()], axis=1)

    return hasil


app = Flask(__name__)
app.config['SECRET_KEY'] = 'supersecretkey'
app.config['UPLOAD_FOLDER'] = 'static/files'
CORS(app)

@app.route('/upload', methods=['POST'])
def upload():
    if request.method == 'POST':
        file1 = request.files['file1']   
        file2 = request.files['file2'] 

        if file1 and file2 and allowed_file(file1.filename) and allowed_file(file2.filename):
            file1.save(os.path.join(app.config['UPLOAD_FOLDER'], "file1.csv"))
            file2.save(os.path.join(app.config['UPLOAD_FOLDER'], "file2.csv"))

            return jsonify(get_columns('static/files/file1.csv','static/files/file2.csv')), 200
        else:
            return jsonify({"error": "Invalid file format"}), 400

@app.route('/proccess', methods=['POST'])
def receive_json():
    try:
        # Get the JSON data from the request
        data = request.get_json()
        print(data)
        
        with open("static/files/data.json", 'w') as json_file:
            json.dump(data, json_file)

        file1 = read_csv_file("static/files/file1.csv")
        file2 = read_csv_file("static/files/file2.csv")

        kolom_kunci_1 = -1
        kolom_kunci_2 = -1
        persentase = 99

        for i, value in enumerate(data["same_column"][3]):
            if data['same_column'][3][i]:
                kolom_kunci_1 = data['same_column'][0][i]
                kolom_kunci_2 = data['same_column'][1][i]    
                persentase = data['same_column'][4][i]  
        
        deleted_indeks1 = data['deleted_column'][0]
        deleted_indeks2 = data['deleted_column'][1]
        

        hasil = process_data(file1, deleted_indeks1, file2, deleted_indeks2, kolom_kunci_1, kolom_kunci_2, persentase);

        # Convert DataFrame to CSV format in memory
        csv_data = hasil.to_csv(index=False)

        # Create an in-memory file-like object in binary mode
        csv_io = BytesIO()
        csv_io.write(csv_data.encode('utf-8'))
        csv_io.seek(0)
        # Send the CSV file as a response with appropriate headers
        return send_file(
            csv_io,
            mimetype='text/csv',
            as_attachment=True,
            download_name='output.csv'
        )


    except Exception as e:
        # Handle any errors that may occur
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

# { 'same_colom' = [[1,2],[4,3],[4,5]], 'deleted_column' = [[2,3,5],[7,8,9]] }