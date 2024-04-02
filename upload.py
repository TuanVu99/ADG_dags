from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from database import fast_insert, get_data, execute
import pandas as pd
import os
import shutil
from PMH import update_bill_datamart



default_args = {
    'owner': 'ducnn',
    'depends_on_past': False,
    'start_date': '2021-04-02',
    'email_on_failure': False,
    'email_on_retry': False,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'Upload_Master_data',
    default_args=default_args,
    description='Upload_Master_data',
    schedule_interval="0/15 * * * *",
)

def n(df, index1):
    return df.columns[index1]

def check_update_time(file):
    time = max([os.path.getctime(file), os.path.getctime(file)])
    last_time = get_last_update(file)
    if time > last_time:
        return True, time
    else:
        return False, None

def get_last_update(file):
    file_name = file.split('/')[-1]
    return list(get_data(f"""SELECT 0 UNION SELECT value from data_lake.last_update where obj = '{file_name}'"""))[-1][0]

def update_last_update(file, time):
    file_name = file.split('/')[-1]
    execute(f"""UPDATE data_lake.last_update set value = {time} where obj = '{file_name}'""")

def update_chi_tieu(file, code, kenh, dong_sp):
    check, time = check_update_time(file)
    if check:
        # shutil.copyfile(path + file, path + 'done/' + file+str(time))
        df = pd.read_excel(file, sheet_name=None)
        sheet_name = list(df.keys())[-1]
        df = pd.read_excel(file,sheet_name=sheet_name)
        df = pd.read_excel(file,sheet_name=sheet_name, converters={n(df, i): str for i in (0,2,3)})
        data = []
        if file in ['/opt/shared/BU1/KA/BU1_KA_2023.xlsx' , '/opt/shared/BU2/KA/BU2_KA_2023.xlsx']:
            columns =list(range(20))
        else:
            columns = list(range(14))
            print(file)
        for row in df.iterrows():
            row = list(row[1])
            if pd.isnull(row[0]):
                continue
            data.append(dict(zip(columns, [None if pd.isnull(i) else i for i in row])))
        table, schema = 'chi_tieu_doanh_so_quan_ly_khu_vuc', 'reporting'
        tmp_kenh = f""" and "Kênh" = '{kenh}'"""
        tmp_dong_sp = f""" and "Ngành" = '{dong_sp}'"""
        fast_insert(data, table, schema, columns, sql_pre_insert=f"""DELETE FROM {schema}.{table} WHERE "Com.Code" = '{code}' {tmp_dong_sp if dong_sp else ''} {tmp_kenh if kenh else ''} and EXTRACT('Year' FROM "Kỳ") in ('2024','2023') """)
        update_last_update(file, time)

def update_cmd(file, bu, kenh, dong_sp):
    check, time = check_update_time(file)
    if check:
        df = pd.read_excel(file, sheet_name=None)
        sheet_name = list(df.keys())[-1]
        df = pd.read_excel(file,sheet_name=sheet_name)
        df = pd.read_excel(file,sheet_name=sheet_name, converters={n(df, i): str for i in (0,2,3)})
        df.head(3)
        data = []
        columns = ['bu', 'sales_org', 'dong_sp', 'kenh_ban_hang', 'ma_khach_hang', 'ten_khach_hang', 'tinh', 'quan_huyen', 'dia_chi', 'sales_district', 'sales_group', 'sales_office', "GDKD", "ma_P_KD", "TP_KD", 'ma_vung', 'truong_vung', 'khu_vuc', "QLKV", 'nhom_dai_ly', 'ma_nhom', 'long_name']
        file_columns = ['bu', 'sales_org', 'dong_sp', 'kenh_ban_hang', 'ma_khach_hang', 'ten_khach_hang','long_name', 'tinh', 'quan_huyen', 'dia_chi', 'sales_district', 'sales_group', 'sales_office', "GDKD", "ma_P_KD", "TP_KD", 'ma_vung', 'truong_vung', 'khu_vuc', "QLKV", 'nhom_dai_ly', 'ma_nhom']
        for row in df.iterrows():
            row = list(row[1])
            if pd.isnull(row[0]):
                continue
            data.append(dict(zip(file_columns, [None if pd.isnull(i) else i for i in row])))
        table, schema = 'customer', 'master_data'
        tmp_kenh = f""" and kenh_ban_hang = '{kenh}'"""
        tmp_dong_sp = f""" and dong_sp = '{dong_sp}'"""
        fast_insert(data, table, schema, columns, sql_pre_insert=f"""DELETE FROM {schema}.{table} WHERE bu = '{bu}' {tmp_dong_sp if dong_sp else ''} {tmp_kenh if kenh else ''}""")
        update_last_update(file, time)

def update_chi_tieu_BU1_DL_CC():
    file = '/opt/shared/BU1/DL_CC/BU1_CC-ĐL_2023.xlsx'
    update_chi_tieu(file, '1100', '01', '01')

def update_CMD_BU1_DL_CC():
    file = '/opt/shared/BU1/DL_CC/CMD_BU1_ĐLCC.xlsx'
    update_cmd(file, 'BU1', '01', '01')

def update_chi_tieu_BU1_DL_NH():
    file = '/opt/shared/BU1/DL_NH/BU1_NH-ĐL_2023.xlsx'
    update_chi_tieu(file, '1100', '01', '02')

def update_CMD_BU1_DL_NH():
    file = '/opt/shared/BU1/DL_NH/CMD_BU1_ĐLNH.xlsx'
    update_cmd(file, 'BU1', '01', '02')

def update_chi_tieu_BU1_KA():
    file = '/opt/shared/BU1/KA/BU1_KA_2023.xlsx'
    update_chi_tieu(file, '1100', '02', None)

def update_CMD_BU1_KA():
    file = '/opt/shared/BU1/KA/CMD_BU1_KA.xlsx'
    update_cmd(file, 'BU1', '02', None)

def update_chi_tieu_BU2_DL_CC():
    file = '/opt/shared/BU2/DL_CC/BU2_CC-ĐL_2023.xlsx'
    update_chi_tieu(file, '1000', '01', '01')

def update_CMD_BU2_DL_CC():
    file = '/opt/shared/BU2/DL_CC/CMD_BU2_ĐLCC.xlsx'
    update_cmd(file, 'BU2', '01', '01')

def update_chi_tieu_BU2_DL_NH():
    file = '/opt/shared/BU2/DL_NH/BU2_NH-ĐL_2023.xlsx'
    update_chi_tieu(file, '1000', '01', '02')

def update_CMD_BU2_DL_NH():
    file = '/opt/shared/BU2/DL_NH/CMD_BU2_ĐLNH.xlsx'
    update_cmd(file, 'BU2', '01', '02')

def update_chi_tieu_BU2_Eratek():
    file = '/opt/shared/BU2/Eratek/BU2_Eratek-ĐL_2023.xlsx'
    update_chi_tieu(file, '1000', '01', '09')

def update_CMD_BU2_Eratek():
    file = '/opt/shared/BU2/Eratek/CMD_BU2_ĐL Thép nhẹ.xlsx'
    update_cmd(file, 'BU2', '01', '09')

def update_chi_tieu_BU2_KA():
    file = '/opt/shared/BU2/KA/BU2_KA_2023.xlsx'
    update_chi_tieu(file, '1000', '02', None)

def update_CMD_BU2_KA():
    file = '/opt/shared/BU2/KA/CMD_BU2_KA.xlsx'
    update_cmd(file, 'BU2', '02', None)

def update_CMD_BU2_PKK():
    file = '/opt/shared/BU2/PKK/CMD_BU2_ĐL PKK.xlsx'
    update_cmd(file, 'BU2', '01', '06')

def update_chi_tieu_BU2_XK():
    file = '/opt/shared/BU2/XK/BU2_KDQT_2023.xlsx'
    update_chi_tieu(file, '1000', '04', None)

def update_CMD_BU2_XK():
    file = '/opt/shared/BU2/XK/CMD_BU2_KDQT.xlsx'
    update_cmd(file, 'BU2', '04', None)

def update_nhom_tp():
    file = '/opt/shared/Nhom HH/Nhóm TP.xlsx'
    check, time = check_update_time(file)
    if check:
        df = pd.read_excel(file, sheet_name='Nhóm HH')
        df = pd.read_excel(file, sheet_name='Nhóm HH', converters={n(df, 0): str, n(df, 6):str})
        data = []
        columns = list(range(7))
        for row in df.iterrows():
            row = list(row[1])
            if pd.isnull(row[0]):
                continue
            data.append(dict(zip(columns, [None if pd.isnull(i) else i for i in row])))
        table, schema = 'material_upload', 'master_data'
        fast_insert(data, table, schema, columns, sql_pre_insert=f"""TRUNCATE TABLE {schema}.{table}""")
        update_last_update(file, time)

begin = DummyOperator(dag=dag, task_id="begin")
chi_tieu = DummyOperator(dag=dag, task_id="update_chi_tieu")
cmd = DummyOperator(dag=dag, task_id="update_cmd")
chi_tieu_BU1_DL_CC = PythonOperator(task_id = "chi_tieu_BU1_DL_CC", python_callable = update_chi_tieu_BU1_DL_CC, dag = dag)
chi_tieu_BU1_DL_NH = PythonOperator(task_id = "chi_tieu_BU1_DL_NH", python_callable = update_chi_tieu_BU1_DL_NH, dag = dag)
chi_tieu_BU1_KA = PythonOperator(task_id = "chi_tieu_BU1_KA", python_callable = update_chi_tieu_BU1_KA, dag = dag)
chi_tieu_BU2_DL_CC = PythonOperator(task_id = "chi_tieu_BU2_DL_CC", python_callable = update_chi_tieu_BU2_DL_CC, dag = dag)
chi_tieu_BU2_DL_NH = PythonOperator(task_id = "chi_tieu_BU2_DL_NH", python_callable = update_chi_tieu_BU2_DL_NH, dag = dag)
chi_tieu_BU2_DL_KA = PythonOperator(task_id = "chi_tieu_BU2_DL_KA", python_callable = update_chi_tieu_BU2_KA, dag = dag)
chi_tieu_BU2_DL_Eratek = PythonOperator(task_id = "chi_tieu_BU2_DL_Eratek", python_callable = update_chi_tieu_BU2_Eratek, dag = dag)
chi_tieu_BU1_DL_XK = PythonOperator(task_id = "chi_tieu_BU1_DL_XK", python_callable = update_chi_tieu_BU2_XK, dag = dag)

cmd_BU1_DL_CC = PythonOperator(task_id = "cmd_BU1_DL_CC", python_callable = update_CMD_BU1_DL_CC, dag = dag)
cmd_BU1_DL_NH = PythonOperator(task_id = "cmd_BU1_DL_NH", python_callable = update_CMD_BU1_DL_NH, dag = dag)
cmd_BU1_KA = PythonOperator(task_id = "cmd_BU1_KA", python_callable = update_CMD_BU1_KA, dag = dag)
cmd_BU2_DL_CC = PythonOperator(task_id = "cmd_BU2_DL_CC", python_callable = update_CMD_BU2_DL_CC, dag = dag)
cmd_BU2_DL_NH = PythonOperator(task_id = "cmd_BU2_DL_NH", python_callable = update_CMD_BU2_DL_NH, dag = dag)
cmd_BU2_DL_KA = PythonOperator(task_id = "cmd_BU2_DL_KA", python_callable = update_CMD_BU2_KA, dag = dag)
cmd_BU2_DL_Eratek = PythonOperator(task_id = "cmd_BU2_DL_Eratek", python_callable = update_CMD_BU2_Eratek, dag = dag)
cmd_BU1_DL_PKK = PythonOperator(task_id = "cmd_BU1_DL_PKK", python_callable = update_CMD_BU2_PKK, dag = dag)
cmd_BU1_DL_XK = PythonOperator(task_id = "cmd_BU1_DL_XK", python_callable = update_CMD_BU2_XK, dag = dag)

material_nhom_tp = PythonOperator(task_id = "material_nhom_tp", python_callable = update_nhom_tp, dag = dag)


begin >> [chi_tieu, cmd, material_nhom_tp]
chi_tieu >> [chi_tieu_BU1_DL_CC, chi_tieu_BU1_DL_NH, chi_tieu_BU1_KA,chi_tieu_BU2_DL_CC,chi_tieu_BU2_DL_NH,chi_tieu_BU2_DL_KA,chi_tieu_BU2_DL_Eratek,chi_tieu_BU1_DL_XK]
cmd >> [cmd_BU1_DL_CC,cmd_BU1_DL_NH,cmd_BU1_KA,cmd_BU2_DL_CC,cmd_BU2_DL_NH,cmd_BU2_DL_KA,cmd_BU2_DL_Eratek,cmd_BU1_DL_PKK,cmd_BU1_DL_XK]
