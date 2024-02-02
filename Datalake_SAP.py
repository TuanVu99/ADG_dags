from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from database import fast_insert, get_data, insert_into
from datetime import datetime, timedelta
from itertools import product
import requests
import psycopg2 as psg


default_args = {
    'owner': 'ducnn',
    'depends_on_past': False,
    'start_date': '2021-04-02',
    'email_on_failure': False,
    'email_on_retry': False,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'Update_data_SAP',
    default_args=default_args,
    description='Update_data_SAP',
    schedule_interval="0 5/7 * * *",
)

def update_fagll03():
    start_date = (datetime.now() - timedelta(days=60))
    # start_date = datetime(2022,1,1)
    to_date = datetime.now()
    # to_date = datetime(2022,12,21)
    table = 'fagll03'
    schema = 'reporting'
    list_columns_date = ['DOCUMENT_DATE', 'POSTING_DATE', 'ENTERED_ON', 'CLEARING_DATE']
    # columns = ["COMPANY_CODE", "TRADING_PARENT", "TYPE", "DOCUMENT_DATE", "POSTING_DATE", "GL_ACCOUNT", "DOCUMENT_NUMBER", "DEBITORCREDIT", "AMOUNT_IN_LC", "POSTING_KEY", "LOCAL_CURRENCY", "TEXT", "CASH_FLOW_ID", "ENTERED_ON", "ACCOUNT", "REVERSE_CLEARNING", "REVERSE_WITH", "PROFIT_CENTER", "CREDIT_CONTROL_AREA", "USER_NAME", "MATERIAL", "PLANT", "CUSTOMER", "VENDOR", "COST_CENTER", "PERIOD_PER_YEAR", "CLEARING_DATE"]
    columns = ["DOCUMENT_DATE", "POSTING_DATE", "DOCUMENT_NUMBER", "USER_NAME", "None", "MATERIAL", "None", "CUSTOMER", "CREDIT_CONTROL_AREA", "POSTING_KEY", "None", "None", "AMOUNT_IN_LC", "None", "None",  "TEXT", 
               "None", "PROFIT_CENTER", "GL_ACCOUNT", "REVERSE_WITH", "DEBITORCREDIT", "None", "None", "None", "None", "COST_CENTER", "PLANT", "COMPANY_CODE", "PERIOD_PER_YEAR", "CLEARING_DATE", "VENDOR"]
    columns_clear_0 = ["CUSTOMER", 'PROFIT_CENTER']
    headers={
        'Authorization': 'Basic UE1IOkFERzEyMzQ1Ng=='
    }
    while (start_date < to_date):
        print(start_date)
        url = f"http://erp.austdoorgroup.vn:8000/adg_if/api_fagll03?sap-client=900&COMPANY_CODE_F=0&POSTING_DATE_F={start_date.strftime('%Y%m%d')}&POSTING_DATE_T={start_date.strftime('%Y%m%d')}&GL_ACCOUNT_F=0&GL_ACCOUNT_T=91111040000&COMPANY_CODE_T=7000"
        response = requests.request("GET", url, headers=headers).json()['SAP_DATA']['DATA']
        if len(response) > 0:
            fast_insert(response, table, schema, columns, date_columns=list_columns_date, sql_pre_insert=f"DELETE FROM {schema}.{table} WHERE \"Posting_Date\" = '{start_date.strftime('%Y-%m-%d')}'", columns_clear_0=columns_clear_0)
        start_date += timedelta(days=1)

def update_zgl03():
    return True
    start_date = (datetime.now() - timedelta(days=10)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # start_date = datetime(2022,1,1)
    end_date = datetime.now()
    
    while start_date < end_date:
        print(start_date)
        data = []
        table = 'zagl03'
        schema = 'reporting'
        columns = ["Doanh thu bán hàng", "Các khoản giảm trừ doanh thu", "Doanh thu thuần về BH và CCDV", "Giá vốn hàng bán", "Lợi nhuận gộp về BH và CCDV", "Doanh thu hoạt động tài chính", "Chi phí tài chính", "Trong đó: - Lãi vay phải trả", "               - Chi phí tài chính khác", "Chi phí bán hàng", "Chi phí quản lý doanh nghiệp", "Lợi nhuận thuần từ HĐKD", "Thu nhập khác", "Chi phí khác", "Lợi nhuận khác", "Tổng lợi nhuận kế toán trước thuế", "Chi phí thuế TNDN hiện hành", "Chi phí thuế TNDN hoãn lại", "Lợi nhuận sau thuế TNDN", "date", "company"]
        for comp_code in (1000, 1100, 3000, 4000, 6000, 2000, 5000):
            url = f"http://erp.austdoorgroup.vn:8000/adg_if/api_gl03?sap-client=900&COMP_CODE={comp_code}&year={start_date.year}&RAPE_FR={start_date.month}"
            tmp = {'date': start_date, 'company': comp_code}
            for row in requests.request("GET", url).json()['DATA']:
                tmp[row['CHITIEU']] = row['SOCUOIKY']
            data.append(tmp)
        print(data)
        fast_insert(data, table, schema, columns, date_columns=columns, sql_pre_insert=f"DELETE FROM {schema}.{table} WHERE \"date\" = '{start_date.strftime('%Y-%m-%d')}'")
        start_date = (start_date + timedelta(days=32)).replace(day=1)

def update_zgl04():
    data = []
    table = 'zgl04'
    schema = 'reporting'
    columns = ["gl_account", "name", "dau_ky_no", "dau_ky_co", "phat_sinh_no", "phat_sinh_co", "cuoi_ky_no", "cuoi_ky_co", "company", "shiftdate"]
    start_date = (datetime.now() - timedelta(days=20)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_date = datetime(2022,1,1)
    end_date = datetime.now()
    key_func = lambda company, gl_account, date: ";".join([company, gl_account, date.strftime('%Y%m%d')])
    mapping = {row[0]: row[1] for row in get_data("select gl_account, name from master_data.account_hierarchy")}
    data_raw = {key_func(*row[:3]): row[-1] for row in get_data(f"select company, gl_account, shiftdate, cuoi_ky_co - cuoi_ky_no as co from reporting.zgl04 where shiftdate = '{(start_date - timedelta(days=2)).replace(day=1).strftime('%Y-%m-%d')}'")}
    data_now = {key_func(row[3], row[2], datetime(row[0], row[1], 1)): row for row in get_data(f"""
                    select 
                        extract ('Year' from f."Posting_Date") :: int, extract ('Month' from f."Posting_Date") :: int, f."G/L_Account" , f."Company_Code"::varchar,
                        sum(case when f."Amount_in_Local_Currency" > 0 then f."Amount_in_Local_Currency" else 0 end) as "no",
                        sum(case when f."Amount_in_Local_Currency" < 0 then -f."Amount_in_Local_Currency" else 0 end) as co
                    from 
                        reporting.fagll03 f
                    where f."Posting_Date" >= '{start_date.strftime('%Y-%m-%d')}' and f."Reversed_With" is null and f."G/L_Account"  not like '331%' and  f."G/L_Account"  not like '131%'
                    group by extract ('Year' from f."Posting_Date"), extract ('Month' from f."Posting_Date"), f."G/L_Account" , f."Company_Code"::varchar
                    order by extract ('Year' from f."Posting_Date"), extract ('Month' from f."Posting_Date") """)}
    date = start_date
    while date < end_date:
        for company, gl_account in product(['1000', '1100', '1300', '2000', '3000', '4000', '5000', '6000'], mapping.keys()):
            key_pre = key_func(company, gl_account, (date-timedelta(days=1)).replace(day=1))
            key_now = key_func(company, gl_account, date)
            pre_value = data_raw.get(key_pre,0)
            value = data_now.get(key_now, [0]*7)[5] - data_now.get(key_now, [0]*7)[4] + pre_value 
            data.append(dict(zip(
                columns,
                [gl_account, mapping.get(gl_account,''),-pre_value if pre_value<0 else 0,pre_value if pre_value>0 else 0,data_now.get(key_now, [0]*7)[4], data_now.get(key_now, [0]*7)[5],-value if value<0 else 0,value if value>0 else 0, company, date]
            )))
            data_raw[key_now] = value
        date = (date + timedelta(days=32)).replace(day=1)
    fast_insert(data, table, schema, columns, sql_pre_insert=f"DELETE FROM {schema}.{table} WHERE \"shiftdate\" >= '{start_date.strftime('%Y-%m-%d')}'")

def update_zgl04_131():
    table, schema = 'zgl04', 'reporting'
    start_date = (datetime.now() - timedelta(days=20)).replace(day=1)
    insert_into(
        f"""
            insert into 
                {schema}.{table} 
            select 
                f.gl_account, ah."name", sum(dau_ky_no), sum(dau_ky_co), sum(phat_sinh_no), sum(phat_sinh_co), sum(cuoi_ky_no), sum(cuoi_ky_co),f.company, first_day_of_month  
            from 
                staging.fagll03_131 f
            left join 
                master_data.account_hierarchy ah on f.gl_account = ah.gl_account 
            where cuoi_ky_no > 0 and first_day_of_month >= '{start_date.strftime('%Y-%m-%d')}'
            group by first_day_of_month, f.company, f.gl_account , ah."name" """,
        f"""
            DELETE FROM {schema}.{table} WHERE shiftdate >= '{start_date.strftime('%Y-%m-%d')}' and gl_account like '131%'
        """)

def update_staging_fagll03_131():
    table, schema = 'fagll03_131', 'staging'
    columns = ['company', 'customer', 'gl_account', 'dau_ky_no', 'dau_ky_co', 'phat_sinh_no', 'phat_sinh_co', 'cuoi_ky_no', 'cuoi_ky_co', 'first_day_of_month']
    start_date, end_date = (datetime.now() - timedelta(days=20)).replace(day=1, hour=0, minute=0, second=0, microsecond=0), datetime.now()
    # start_date = datetime(2021,12,1)
    key_func = lambda arr: ";".join([str(v) for v in arr])
    data_raw = {start_date.strftime('%Y-%m-%d'):{key_func(row[1:4]): dict(zip(columns, row[1:])) for row in get_data(f"""
        SELECT 
            first_day_of_month, company, customer, gl_account, dau_ky_no, dau_ky_co, phat_sinh_no, phat_sinh_co, cuoi_ky_no, cuoi_ky_co
        FROM 
            staging.fagll03_131 
        where 
            first_day_of_month = '{start_date.strftime('%Y-%m-%d')}' and company != customer
    """)}}
    data_now={}
    for row in get_data(
        f"""select
            make_date(extract('Year' from f."Posting_Date")::int, extract('Month' from f."Posting_Date")::int, 1),
            f."Company_Code"::varchar as company,
            f."Customer" as customer, 
            f."G/L_Account",
            sum(case when f."Amount_in_Local_Currency" > 0 then f."Amount_in_Local_Currency" else 0 end) as phat_sinh_no,
            sum(case when f."Amount_in_Local_Currency" < 0 then -f."Amount_in_Local_Currency" else 0 end) as phat_sinh_co
        from 
            reporting.fagll03 f
        where 
            f."Posting_Date" >= '{start_date.strftime('%Y-%m-%d')}' and  f."G/L_Account" like '131%' and f."Customer" != f."Company_Code"::varchar
        group by 
            extract ('Year' from f."Posting_Date"), extract ('Month' from f."Posting_Date"), 
            f."G/L_Account" , 
            f."Company_Code"::varchar, f."Customer" """):
        key = row[0].strftime('%Y-%m-%d')
        if key not in data_now:
            data_now[key] = {}
        key2 = key_func(row[1:4])
        data_now[key][key2] = row[1:]
    
    while start_date < end_date:
        next_date, key, key_next = (start_date+timedelta(days=32)).replace(day=1), start_date.strftime('%Y-%m-%d'), (start_date+timedelta(days=32)).replace(day=1).strftime('%Y-%m-%d')
        if key_next not in data_now:
            data_now[key_next] = {}
        if key_next not in data_raw:
            data_raw[key_next] = {}
        if key not in data_raw:
            data_raw[key] = {}
            
        # khoi tao data thang tiep theo (next_date, next_key ) = data hien tai (date, key)
        for key2 in data_raw[key]:
            data_raw[key_next][key2] = dict(zip(
                columns,
                [data_raw[key][key2]['company'], data_raw[key][key2]['customer'], data_raw[key][key2]['gl_account'], data_raw[key][key2]['cuoi_ky_no'], data_raw[key][key2]['cuoi_ky_co'], 0, 0, data_raw[key][key2]['cuoi_ky_no'], data_raw[key][key2]['cuoi_ky_co'], next_date]
            ))
        
        # tinh phat sinh dau ky => cuoi ky no, co
        for key2 in data_now[key_next]:    
            #check du lieu thang tiep theo da co chua
            if key2 not in data_raw[key_next]:
                data_raw[key_next][key2] = dict(zip(
                    columns,
                    [*data_now[key_next][key2][1:4],0,0,0,0,0,0, next_date]
                ))
            tmp = data_raw[key_next][key2]
            tmp['phat_sinh_no'],tmp['phat_sinh_co'] = data_now[key_next][key2][-2:]
            value = tmp['dau_ky_no'] - tmp['dau_ky_co'] + (tmp['phat_sinh_no']-tmp['phat_sinh_co'])
            tmp['cuoi_ky_no'], tmp['cuoi_ky_co'] = value if value > 0 else 0, 0 if value > 0 else -value    
        fast_insert([data_raw[next_date.strftime('%Y-%m-%d')][key] for key in data_raw[next_date.strftime('%Y-%m-%d')]], table, schema, columns, sql_pre_insert=f"""DELETE FROM {schema}.{table} WHERE first_day_of_month = '{next_date.strftime('%Y-%m-%d')}'""")
        start_date = next_date


begin = DummyOperator(dag=dag, task_id="begin")
task_update_fagll03 = PythonOperator(task_id = "update_fagll03", python_callable = update_fagll03, dag = dag)
task_update_zgl03 = PythonOperator(task_id = "update_zgl03", python_callable = update_zgl03, dag = dag)
task_update_zgl04 = PythonOperator(task_id = "update_zgl04", python_callable = update_zgl04, dag = dag)
task_update_staging_fagll03_131 = PythonOperator(task_id = "update_staging_fagll03_131", python_callable = update_staging_fagll03_131, dag = dag)
task_update_zgl04_131 = PythonOperator(task_id = "update_zgl04_131", python_callable = update_zgl04_131, dag = dag)

begin >> [task_update_fagll03, task_update_zgl03] 
task_update_fagll03 >> [task_update_zgl04, task_update_staging_fagll03_131] >> task_update_zgl04_131
