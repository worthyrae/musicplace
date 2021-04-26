from flask import Flask, render_template, request, redirect, url_for
import datetime
import mysql.connector
import string
import random
import boto3
import io
import pandas as pd

app = Flask(__name__)

@app.route('/', methods=['POST', 'GET'])
def home():
    total_cash = get_total_available_cash()
    return render_template('index.htm', total_cash=total_cash)

@app.route('/add', methods=['POST', 'GET'])
def add():
    album_name = request.args.get('album_name')
    artist_name = request.args.get('artist_name')
    release_date = request.args.get('release_date')
    duration = int(request.args.get('duration'))
    equity = float(request.args.get('equity'))
    total_shares = int(request.args.get('total_shares'))
    revenue_per_stream = float(request.args.get('revenue_per_stream'))
    add_offering(album_name, artist_name, release_date, duration, equity, total_shares, revenue_per_stream)
    total_cash = get_total_available_cash()
    return render_template('index.htm', total_cash=total_cash)

@app.route('/edit', methods=['POST', 'GET'])
def edit():
    search_album_name = request.args.get('search_album_name')
    search_artist_name = request.args.get('search_artist_name')
    album_name, artist_name, release_date, duration, equity, total_shares, revenue_per_stream = get_offering_info(search_artist_name, search_album_name)
    return render_template('edit.htm', album_name=album_name, artist_name=artist_name, release_date=release_date, duration=duration, equity=equity, total_shares=total_shares, revenue_per_stream=revenue_per_stream)

@app.route('/submit', methods=['POST', 'GET'])
def submit():
    album_name = request.args.get('album_name')
    artist_name = request.args.get('artist_name')
    release_date = request.args.get('release_date')
    duration = int(request.args.get('duration'))
    equity = float(request.args.get('equity'))
    total_shares = int(request.args.get('total_shares'))
    revenue_per_stream = float(request.args.get('revenue_per_stream'))
    release_date = datetime.datetime.strptime(release_date, "%Y, %m, %d, %H, %M, %S")
    expiration_date = release_date + datetime.timedelta(days=duration)
    mysql_modify(release_date, expiration_date, total_shares, equity, revenue_per_stream, album_name)
    total_cash = get_total_available_cash()
    return render_template('index.htm', total_cash=total_cash)

def mysql_connect():
    conn = mysql.connector.connect(
        host="104.236.51.8",
        port="3306",
        user="worthy",
        password="FM9r?S9wMRk-r+UE"
    )
    return conn

def get_offering_info(artist_name, album_name):
    conn = mysql_connect()
    cursor = conn.cursor()
    cursor.execute("use musicplace")
    cursor.execute("SELECT * FROM Offering")
    release_date = ""
    duration = ""
    total_shares = ""
    equity = ""
    revenue_per_stream = ""
    for row in cursor:
        if artist_name.lower().strip() == row[1].lower().strip() and album_name.lower().strip() == row[2].lower().strip():
            release_date = row[4]
            expiration_date = row[5]
            release_date_obj = release_date.strftime("%Y, %m, %d, %H, %M, %S")
            duration = (expiration_date - release_date).days
            total_shares = row[6]
            equity = row[7]
            revenue_per_stream = row[8]
            break
    conn.close()
    return album_name, artist_name, release_date_obj, duration, equity, total_shares, revenue_per_stream

def generate_album_id():
    chars=string.ascii_lowercase + string.digits
    album_id1 = ''.join(random.choice(chars) for _ in range(8))
    album_id2 = ''.join(random.choice(chars) for _ in range(4))
    album_id3 = ''.join(random.choice(chars) for _ in range(4))
    album_id4 = ''.join(random.choice(chars) for _ in range(4))
    album_id5 = ''.join(random.choice(chars) for _ in range(12))
    album_id = album_id1 + '-' + album_id2 + '-' + album_id3 + '-' + album_id4 + '-' + album_id5
    conn = mysql_connect()
    cursor = conn.cursor()
    cursor.execute("use musicplace")
    cursor.execute("SELECT id FROM Offering")
    all_rows = cursor.fetchall()
    for row in all_rows:
        if row[0] == album_id:
            cursor.close()
            conn.close()
            generate_album_id()
        else:
            pass
    cursor.close()
    conn.close()
    return album_id

def get_data_file():
    s3Connection = boto3.client('s3',
                                region_name="us-east-1",
                                aws_access_key_id="AKIAVRRC3CHW6B5YRZPB",
                                aws_secret_access_key="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")
    offeringFile = s3Connection.get_object(Bucket="musicplace-data", Key="offering_data.csv")
    offeringFileDf = pd.read_csv(io.BytesIO(offeringFile['Body'].read()), encoding="utf-8")
    return offeringFileDf

def upload_data_file():
    s3Connection = boto3.client('s3',
                                region_name="us-east-1",
                                aws_access_key_id="AKIAVRRC3CHW6B5YRZPB",
                                aws_secret_access_key="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")
    s3Connection.upload_file('offering_data.csv', 'musicplace-data', 'offering_data.csv')

def mysql_upload(album_id, artist_name, album_name, release_date, expiration_date, total_shares, equity, revenue_per_stream, status):
    sql = "INSERT INTO Offering (id, artist, albumName, coverUrl, releaseDate, expirationDate, totalShares, equity, streamRevenue, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (album_id, artist_name, album_name, 'NULL', release_date, expiration_date, total_shares, equity, revenue_per_stream, status)
    conn = mysql_connect()
    cursor = conn.cursor()
    cursor.execute("USE musicplace")
    cursor.execute(sql, val)
    conn.commit()
    cursor.close()
    conn.close()

def mysql_modify(release_date, expiration_date, total_shares, equity, stream_revenue, album_name):
    sql = "UPDATE Offering SET releaseDate=%s, expirationDate=%s, totalShares=%s, equity=%s, streamRevenue=%s WHERE albumName=%s"
    val = (release_date, expiration_date, total_shares, equity, stream_revenue, album_name)
    conn = mysql_connect()
    cursor = conn.cursor()
    cursor.execute("USE musicplace")
    cursor.execute(sql, val)
    conn.commit()
    cursor.close()
    conn.close()

def get_total_available_cash():
    conn = mysql_connect()
    cursor = conn.cursor()
    cursor.execute("use musicplace")
    cursor.execute("SELECT * FROM User")
    total_cash = 0
    for row in cursor:
        total_cash += row[7]
    cursor.close()
    conn.close()
    return total_cash

def add_offering(album_name, artist_name, release_date, duration, equity, total_shares, revenue_per_stream):
    album_id = generate_album_id()
    release_date = datetime.datetime.strptime(release_date, "%Y, %m, %d, %H, %M, %S")
    expiration_date = release_date + datetime.timedelta(days=duration)
    status = 'Upcoming'
    mysql_upload(album_id, artist_name, album_name, release_date, expiration_date, total_shares, equity, revenue_per_stream, status)
    data_df = get_data_file()
    data_df = data_df.append({'album_id': album_id,
                              'artist_name': artist_name,
                              'album_name': album_name,
                              'cover_url': 'NULL',
                              'release_date': release_date,
                              'expiration_date': expiration_date,
                              'total_shares': total_shares,
                              'equity': equity,
                              'revenue_per_stream': revenue_per_stream,
                              'status': status}, ignore_index=True)
    data_df.to_csv(r'offering_data.csv', index=False)
    upload_data_file()

if __name__ == "__main__":
    app.run(debug=True)
