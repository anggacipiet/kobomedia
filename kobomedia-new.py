import streamlit as st
import json
import os
import pathlib
import re
import requests
import time
import zipfile
from io import BytesIO

# Konfigurasi
KOBO_CONF = 'kobo.json'
REWRITE_DOWNLOAD_URL = True

# Fungsi-fungsi dari kode awal
def download_all_media(data_url, stats, *args, **kwargs):
    data_res = requests.get(
        data_url, headers=kwargs['headers'], params=kwargs['params']
    )
    if data_res.status_code != 200:
        return stats

    data = data_res.json()
    next_url = data['next']
    results = data['results']

    if not results:
        return stats

    for sub in results:
        attachments = sub.get('_attachments', [])
        media_filenames = [
            sub.get(name) for name in kwargs['question_names'].split(',')
        ]
        media_filenames = [
            get_valid_filename(name)
            for name in media_filenames
            if name is not None
        ]

        if not attachments:
            continue

        sub_dir = os.path.join(kwargs['asset_uid'], sub['_uuid'])
        if not os.path.isdir(sub_dir):
            os.makedirs(sub_dir)

        for attachment in attachments:
            download_url = attachment['filename']
            if REWRITE_DOWNLOAD_URL:
                download_url = rewrite_download_url(
                    download_url, kwargs['kc_url']
                )
            filename = get_filename(attachment['filename'])

            if kwargs['question_names'] and filename not in media_filenames:
                continue

            file_path = os.path.join(sub_dir, filename)
            if os.path.exists(file_path):
                if kwargs['verbosity'] == 3:
                    st.write(f'File already exists, skipping: {file_path}')
                stats['skipped'] += 1
                continue
            download_media_file(
                url=download_url, path=file_path, stats=stats, *args, **kwargs
            )

    if next_url is not None:
        download_all_media(data_url=next_url, stats=stats, *args, **kwargs)

    return stats


def download_media_file(url, path, stats, headers, chunk_size, *args, **kwargs):
    stream_res = requests.get(url, stream=True, headers=headers)
    if stream_res.status_code != 200:
        if kwargs['verbosity'] == 3:
            st.write(f'Fail: {path}')
        stats['failed'] += 1
        return stats

    with open(path, 'wb') as f:
        for chunk in stream_res.iter_content(chunk_size):
            f.write(chunk)

    if kwargs['verbosity'] == 3:
        st.write(f'Success: {path}')
    stats['successful'] += 1

    time.sleep(kwargs['throttle'])

    return stats


def get_clean_stats():
    return {'successful': 0, 'failed': 0, 'skipped': 0}


def get_config():
    conf_path = os.path.join(pathlib.Path(__file__).resolve().parent, KOBO_CONF)
    with open(conf_path, 'r') as f:
        settings = json.loads(f.read())
    return settings


def get_data_url(asset_uid, kf_url):
    return f'{kf_url}/api/v2/assets/{asset_uid}/data'


def get_filename(path):
    return path.split('/')[-1]


def get_params(limit=100, query='', *args, **kwargs):
    params = {'format': 'json', 'limit': limit}
    if query:
        params['query'] = query
    return params


def get_valid_filename(name):
    s = str(name).strip().replace(' ', '_')
    s = re.sub(r'(?u)[^-\w.]', '', s)
    return s


def rewrite_download_url(filename, kc_url):
    return f'{kc_url}/media/original?media_file={filename}'


def zip_folder(folder_path, output_zip_path):
    """Mengompres folder ke dalam file ZIP."""
    with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)


def main(asset_uid, verbosity=3, *args, **kwargs):
    settings = get_config()
    options = {
        'asset_uid': asset_uid,
        'params': get_params(*args, **kwargs),
        'headers': {'Authorization': f'Token {settings["token"]}'},
        'verbosity': verbosity,
    }
    data_url = get_data_url(asset_uid, settings['kf_url'])
    stats = download_all_media(
        data_url,
        stats=get_clean_stats(),
        *args,
        **kwargs,
        **options,
        **settings,
    )
    
    # Kompres folder ke dalam file ZIP
    folder_path = os.path.join(os.getcwd(), asset_uid)
    zip_path = os.path.join("/tmp", f"{asset_uid}.zip")
    zip_folder(folder_path, zip_path)
    return stats, zip_path


# Streamlit App
st.title("Kobo Media Downloader Dashboard")

# Input dari pengguna
asset_uid = st.text_input("Asset UID", value="your-asset-uid")
question_names = st.text_input("Question Names (comma-separated)", value="photo,audio")
limit = st.number_input("Limit", min_value=1, value=100)
query = st.text_input("Custom Query", value="")
chunk_size = st.number_input("Chunk Size", min_value=1, value=1024)
throttle = st.number_input("Throttle (seconds)", min_value=0, value=1)
verbosity = st.selectbox("Verbosity Level", [1, 2, 3], index=2)

# Tombol untuk memulai unduhan
if st.button("Start Download"):
    # Validasi input
    if not asset_uid.strip():
        st.error("Please provide a valid Asset UID.")
    else:
        # Panggil fungsi utama untuk mengunduh media
        stats, zip_path = main(
            asset_uid=asset_uid,
            limit=limit,
            query=query,
            question_names=question_names,
            chunk_size=chunk_size,
            throttle=throttle,
            verbosity=verbosity,
        )
        
        # Tampilkan statistik hasil unduhan
        st.subheader("Download Statistics")
        st.json(stats)
        
        # Tambahkan tombol unduh untuk file ZIP
        if os.path.exists(zip_path):
            with open(zip_path, "rb") as f:
                st.download_button(
                    label="Download ZIP",
                    data=f,
                    file_name=f"{asset_uid}.zip",
                    mime="application/zip"
                )
        else:
            st.error("ZIP file could not be created.")