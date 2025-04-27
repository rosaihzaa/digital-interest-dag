import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from digitalDag.credentials import credentialBq


def extract():
    df_customer = pd.read_csv('digital_customer.csv')
    return df_customer

def transform():
    # New Columns (Tanggal Login, Hari Login, Weekday/day)
    df_customer = extract()
    df_customer['Jam Login'] = pd.to_datetime(df_customer['Jam Login'], errors='coerce')
    df_customer['Tanggal Login'] = df_customer['Jam Login'].dt.date
    df_customer['Bulan Login'] = df_customer['Jam Login'].dt.month_name()
    df_customer['Hari Login'] = df_customer['Jam Login'].dt.day_name()
    def week(day):
        if day == 'Saturday' or day == 'Sunday':
            return 'Weekend'
        else:
            return 'Weekday'
    df_customer['Weekeday/end'] = df_customer['Hari Login'].apply(week)

    df_customer['Jam Login'] = df_customer['Jam Login'].dt.strftime('%H:%M:%S')
    df_customer['Jam Login'] = pd.to_datetime(df_customer['Jam Login'])
    df_customer['Jam Login'] = df_customer['Jam Login'].dt.time

    # New Columns (Status Waktu)
    def peak_hour(hour):
        if hour >= datetime.strptime('06:00:00', '%H:%M:%S').time() and hour <= datetime.strptime('10:00:00', '%H:%M:%S').time():
            return 'Sibuk'
        elif hour >= datetime.strptime('16:00:00', '%H:%M:%S').time() and hour <= datetime.strptime('20:00:00', '%H:%M:%S').time():
            return 'Sibuk'
        else:
            return 'Tidak Sibuk'

    df_customer['Status Waktu'] = df_customer['Jam Login'].apply(peak_hour)
    
    # New Columns (Usia and Generation Customer)
    df_customer['Usia'] = 2025 - df_customer['Tahun Lahir']
    def generation(year):
        if year >=1997: return 'Gen Z'
        elif year >= 1981: return 'Milenial'
        else: return 'Lainnya'
    df_customer['Generasi'] = df_customer['Tahun Lahir'].apply(generation)

    # New Columns (OS HP)
    def os_hp(merek):
        if merek in ['Apple']: return 'IOS'
        else: return 'Android'
    df_customer['OS HP'] = df_customer['Merek HP'].apply(os_hp)

    # New Columns (Skor Minat Digital)
    df_customer['Skor Minat Digital'] = df_customer['Minat Digital'].map({'Low Interested': 1, 
                                                                            'Middle Interested': 2, 
                                                                            'High Interested': 3})
    return df_customer

def load():
    data_digital = transform()
    # Ganti nama kolom dengan karakter yang valid di BigQuery
    data_digital.columns = data_digital.columns.str.replace(r'[^\w]', '_', regex=True)
    project_id = 'yourprojectid'
    dataset_id = 'yourdatasetid'
    table_id = 'yourtableid'
    
    client = bigquery.Client(project=project_id, credentials=credentialBq())
    table_ref = f'{project_id}.{dataset_id}.{table_id}'

    job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition='WRITE_TRUNCATE'
    )
    
    client.load_table_from_dataframe(data_digital, table_ref, job_config=job_config)

def confidence_interval():
    ci = """SELECT 
            AVG(`Skor_Minat_Digital`) - 1.96 * STDDEV_POP(`Skor_Minat_Digital`) / SQRT(COUNT(*)) AS ci_lower,
            AVG(`Skor_Minat_Digital`) + 1.96 * STDDEV_POP(`Skor_Minat_Digital`) / SQRT(COUNT(*)) AS ci_upper
            FROM yourprojectid-datasetid-tableid; 
            """
    return ci


