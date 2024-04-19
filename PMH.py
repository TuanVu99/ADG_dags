from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from database import fast_insert, get_data, insert_into
from scripts import send_email
from datetime import datetime, timedelta
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
    'Update_data_PMH',
    default_args=default_args,
    description='Update_data_PMH',
    schedule_interval="0 */1 * * *",
)
def update_bill(table = 'bill_sap', schema='data_lake', host = 'http://erp.austdoorgroup.vn:8000', debug = False, headers={
    'Authorization': 'Basic UE1IOkFERzEyMzQ1Ng=='
    }):
    from_date = (datetime.now() - timedelta(days=60))
    to_date = datetime.now()
    
    
    list_columns_date = ['CREATE_ON', 'BILLING_DATE', 'NGAY_HD', 'NET_DUE_DATE', 'SO_DATE', 'DELIVERY_DATE']
    columns = [
        "SALE_ORG", "DIST_CHANNEL", "DIVISION", "CREATE_BY", "CREATE_ON", "BILLING_DATE", "BILLING_DOC", "OUTBOUND_DELIVERY", 
        "FI_DOC", "PROFIT_CENTER", "CUST_GROUP", "SALE_OFF", "SALE_GROUP", "SALE_DISTRICT", "GL_ACCT", "CUSTOMER", "SALE_ORDER", 
        "MATERIAL", "MATERIAL_GROUP", "PROD_HIER", "UOM", "SLOC", "PLANT", "QUANTITY", "BASE_UNIT", "TONGTIEN_TT", "SUM_CK", 
        "VAT_AMOUNT", "TONGTIEN", "DIACHI_GH", "CURRENCY", "HMTD", "MST", "KH_HD", "SO_HD", "NGAY_HD", "PAYMENT_TERM", 
        "NET_DUE_DATE", "BILLING_TYPE", "CANCELLED", "SO_DATE", "DELIVERY_DATE", "TONGTIEN_TCK", "TRONG_LUONG", "SO_TYPE", 
        "TY_GIA", "bu", "bu_detail", "new_company", "new_channel", "new_division"]
    
    if debug:
        from_date = datetime(2023,1,1)
        while from_date < to_date:
            print(from_date)
            url = f"{host}/adg_if/get_bill_infor?sap-client=900&billing_datef={from_date.strftime('%Y%m%d')}&billing_datet={from_date.strftime('%Y%m%d')}"
            response = requests.request("GET", url, headers=headers).json()['DATA']
            if len(response) > 0:
                fast_insert(response, table, schema, columns, date_columns=list_columns_date, sql_pre_insert=f"DELETE FROM data_lake.bill_sap WHERE \"BILLING_DATE\" = '{from_date.strftime('%Y-%m-%d')}'")  
            from_date += timedelta(days=1)
    else:
        url = f"{host}/adg_if/get_bill_infor?sap-client=900&billing_datef={from_date.strftime('%Y%m%d')}&billing_datet={to_date.strftime('%Y%m%d')}"
        response = requests.request("GET", url, headers=headers).json()['DATA']
        if len(response) > 0:
            fast_insert(response, table, schema, columns, date_columns=list_columns_date, sql_pre_insert=f"DELETE FROM data_lake.bill_sap WHERE \"BILLING_DATE\" >= '{from_date.strftime('%Y-%m-%d')}'")

def update_bill_satging():
    table = 'bill'
    schema='staging'
    date_from = (datetime.now()-timedelta(days=60)).strftime('%Y-%m-%d')
    date_from = datetime(2023,1,1).strftime('%Y-%m-%d')
    columns = ["company", "channel", "division", "CREATE_BY", "CREATE_ON", "BILLING_DATE", "BILLING_DOC", "OUTBOUND_DELIVERY", 
           "FI_DOC", "profitcenter", "CUST_GROUP", "sales_off", "SALE_GROUP", "SALE_DISTRICT", "GL_ACCT", "customer", 
           "SALE_ORDER", "MATERIAL", "MATERIAL_GROUP", "PROD_HIER", "UOM", "SLOC", "plant", "QUANTITY", "BASE_UNIT", 
           "TONGTIEN_TT", "SUM_CK", "VAT_AMOUNT", "TONGTIEN", "DIACHI_GH", "CURRENCY", "HMTD", "MST", "KH_HD", "SO_HD", 
           "NGAY_HD", "PAYMENT_TERM", "NET_DUE_DATE", "BILLING_TYPE", "CANCELLED", "SO_DATE", "DELIVERY_DATE", "TONGTIEN_TCK", 
           "TRONG_LUONG", "SO_TYPE", "TY_GIA", 'bu', 'bu_detail', 'new_company', 'new_channel', 'new_division']

    data = [dict(zip(columns, row)) for row in get_data(f"""SELECT * FROM data_lake.bill_sap where "BILLING_DATE" >= '{date_from}'""")]
    kh_mb = {row[0] for row in get_data("""SELECT distinct customer from master_data.customer_region where region = 'MB'""")}
    kh_mn = {row[0] for row in get_data("""SELECT distinct customer from master_data.customer_region where region = 'MN'""")}
    error = []
    for row in data:
        row.update(dict(zip(['bu', 'bu_detail', 'new_company', 'new_channel', 'new_division'], get_bu(row, kh_mb, kh_mn, error))))
    fast_insert(data, table, schema, columns, sql_pre_insert=f"DELETE FROM staging.bill WHERE billing_date >= '{date_from}'")
    if len(error) > 0:
        send_email(['ducnn@austdoor.com', 'quentp@austdoor.com'], "ERROR JOB UPDATE BILL", '\n'.join([str(i) for i in error]))


def update_bill_datamart():
    date_from = (datetime.now().replace(day=1)-timedelta(days=30)).strftime('%Y-%m-%d')
    date_from = datetime(2022,1,1)
    data = get_data(f"""
        select 
            b.new_company, new_channel, new_division, billing_date, billing_doc, profit_center, customer_group,
            sale_office, sale_group, sale_district, gl_account, b.customer, b.material, b.material_group, uom, sloc, plant, 
            case when 
                uom::text = 'Kilogam'::text and new_division = '02'::text then  b.quantity
            when 
                uom::text = 'M'::text and new_division = '02'::text then trong_luong::double precision
            when 
                uom::text = 'm'::text and new_division = '02'::text then trong_luong::double precision
            else 
                b.quantity
            end as so_luong,
            b.unit_price::double precision * COALESCE(b.ty_gia,1) as don_gia,
            case when 
               b.currency ='USD' then  b.tong_tien_chua_ck * COALESCE(b.ty_gia*1000,1)
            else 
                b.tong_tien_chua_ck * COALESCE(b.ty_gia,1)
            end,
            b.tong_chiet_khau,
            b.vat_amount,
            b.tong_tien_thanh_toan,
            b.hmtd,
            b.mst,
            b.payment_term,
            b.bu, 
            c.sales_district AS vung_khu_vuc,
            b.sale_order,
            c.ma_vung AS vung,
            c.khu_vuc AS khu_vuc,
            c. "GDKD",
            c. "TP_KD",
            c.truong_vung as truong_vung,
            c.ma_nhom,
            c. "QLKV",
            b.billing_type,
            b.so_type,
            b.ty_gia,
            m.material_description,
            c.ten_khach_hang
        from 
            staging.bill b
            LEFT JOIN master_data.customer c ON c.bu = b.bu  
                AND c.dong_sp = b.new_division 
                AND c.kenh_ban_hang = b.new_channel 
                AND c.ma_khach_hang = b.customer
            LEFT JOIN master_data.material m ON b.material::text = m.material
            -- left join master_data.customer_name u on u.customer::text = b.customer
            -- left join master_data.truong_vung tv on tv.bu = b.bu and tv.channel = b.new_channel and tv.division = b.new_division and tv.customer = b.customer
        WHERE
            b.cancel IS NULL and so_type not like 'ZRE%'  
            and so_type not like 'ZKO%'  
            and gl_account like '511%'
            and new_company is not NULL
            and billing_date >= '{date_from}'
    """)
    columns = list(range(len(data[0]) + 1))
    data = [dict(zip(columns, [*row, get_kenh_nganh(*row[:3])])) for row in data]
    table = 'billing'
    schema= 'reporting'
    fast_insert(data, table, schema, columns, sql_pre_insert=f"DELETE FROM {schema}.{table} where ngay_hoa_don >= '{date_from}'")
    start = (datetime.now().replace(day=1) + timedelta(days=31)).replace(day=1)
    end = datetime(start.year + 1, 1,1)
    while start < end:
        insert_into(f"""
            insert into reporting.billing   
            select distinct sales_organiztion, kenh_ban_hang, dong_sp , '{start.strftime('%Y-%m-%d')}'::date, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL, NULL,  kenh_nganh from reporting.billing 
        """)
        start = (start + timedelta(days=32)).replace(day=1)

def get_kenh_nganh(company, channel, division):
    if company == '4000':
        return 'ACMB'
    elif company == '1000' and channel == '03':
        return 'ACMN'
    elif channel == '04':
        return 'XK'
    elif channel == '05':
        return 'Noi_bo'
    elif channel == '99' or division == '99':
        return 'Khac'
    elif division == '03':
        return 'Austwin'
    elif division == '90':
        return 'DV'
    elif division in ('07', '09'):
        return 'DL-Thep'
    else:
        return '-'.join([
            {'01': 'DL', '02': 'KA'}.get(channel,''), 
            {'01': 'CC', '02': 'NH', '04': 'go', '06': 'PKK', '11': 'Solar', '12': 'NT'}.get(division, '')
        ])
    
def get_bu(row, kh_mb, kh_mn, error):
    if row['customer'][0] == '7' and row['company'] != '4000':
        return ['', '', '', '', '']
    elif row['profitcenter'] == '0140000002':
        return ['BU2', 'BU2_Austcare MN', '1000', '03', '90']
    elif row['profitcenter'] == '0140000001':
        return ['BU4', 'BU4_Austcare MB', '4000', '03', '90']
    elif row['company'] == '6000':
        # return {'01': ['BU6', 'BU6_Đại lý', '6000', '01', '12'],
        #     '02': ['BU6', 'BU6_Dự án', '6000', '02', '12'],
        #     '04': ['BU6', 'BU6_Xuất khẩu', '6000', '04', '12'],
        #     '05': ['BU6', 'BU6_Nội bộ', '6000', '05', '12'],
        #     '99': ['BU6', 'BU6_Khác', '6000', '99', '99']}.get(row['channel'], ['BU6', 'Loi channel BU6', '6000', row['channel'], row['division']])
        if row['channel'] == '08' and row['division'] == '03':
            return ['BU6', 'BU6', '6000', '01', '03']
        elif row['channel'] == '99':
            return ['BU6', 'BU6_Khác', '6000', '99', '99']
        else:
            return ['BU6', 'BU6', '6000', row['channel'], row['division']]
    elif row['company'] == '5000':
        return ['BU3', 'BU3_ADMN', '5000', '02', row['division']]
    elif row['company'] == '2000':
        return ['SADO', 'SADO', '2000', 'Z3', 'Z2']
    elif row['company'] == '4000':
        return ['BU4', 'BU4_Austcare MB', '4000', '03', '90']
    elif row['company'] in ('1100', '1300','1110', '1120'):
        if row['channel'] in ('06', '07'):
            return ['BU1', 'BU1_Đại lý', row['company'], '01', row['division']]
        elif row['channel'] == '05':
            if row['customer'] == '1000003489':
                return ['BU1', 'BU1', row['company'], '02', row['division']]
            if row['customer'] in ('1000000001', '5000', '1000003489', '6000'):
                return ['BU1', 'BU1_CTTV', row['company'], '05', row['division']]
            elif row['customer'] in ('1000', '2300') and row['material'][0] == 2 :
                return ['BU1', 'BU1_CTTV', row['company'], '05', row['division']]
            else:
                return ['', 'Khong tinh doanh thu', '', '', '']
        elif row['channel'] == '03':
            return ['BU1', 'BU1_Khác', row['company'], '99', '99']
        else:
            return ['BU1', 'BU1', row['company'],row['channel'], row['division']]
    elif row['company'] in ('3000'):
        return ['', '', '', '', '']
    elif row['company'] in ('2300'):
        return ['BU2', 'BU2', '2300', row['channel'], row['division']]
    elif row['company'] in ('1000','1010'):
        if row['division'] == '03':
            return ['BU6', 'BU6_Cửa nhôm AWD', row['company'], '01', '03']
        elif row['division'] == '06':
            return ['BU2', 'BU2_Đại lý_PKK', row['company'], '01', '06']
        elif row['division'] == '09':
            return ['BU2', 'BU2_Đại lý_Thép nhẹ', row['company'], '01', '09']
        elif row['channel'] == '04':
            return ['BU2', 'BU2_Xuất khẩu (KDQT)', row['company'], '04', row['division']]
        elif row['channel'] == '05':
            if row['customer'] in ('1000000001', '5000', '1000003489', '6000'):
                return ['BU2', 'BU2_CTTV', row['company'], '05', row['division']]
            elif row['customer'] in ('1300', '1100') and row['material'][0] == 2 :
                return ['BU2', 'BU2_CTTV', row['company'], '05', row['division']]
            else:
                return ['', 'Khong tinh doanh thu', '', '', '']
        elif row['division'] in ('99', '00'):                
            return ['BU2', 'BU2_Khác', row['company'], '99', '99']
        elif row['division'] == '01' and row['channel'] in ('01','07', '06'):
            return ['BU2', 'BU2_Đại lý_Cửa cuốn', row['company'], '01', '01']
        elif row['channel'] in ('01', '06','07') and row['division'] == '02':
            bu = ('BU2', row['company']) if row['sales_off'] in ('MN', 'MT2') else ('BU1', '1100')
            return [bu[0], f'{bu[0]}_Đại lý_Nhôm hệ', bu[1], '01', '02']
        elif row['channel'] == '02':
            # if row['customer'] not in kh_mb | kh_mn:
            #     error.append({'customer': row['customer'], 'company': row['company'], 'channel': row['channel'], 'division': row['division'],'error': 'Customer KA mới, chưa được khai báo'})
            bu = ('BU1', '1100') if row['customer'] in kh_mb else ('BU2', row['company'])
            return {
                '01': [bu[0], f'{bu[0]}_KA_Cửa cuốn', bu[1], '02', row['division']],
                '02': [bu[0], f'{bu[0]}_KA_Nhôm hệ & Nhôm CN', bu[1], '02', row['division']],
                '11': [bu[0], f'{bu[0]}_KA_Solar', bu[1], '02', row['division']]
            }.get(row['division'],['', 'loi division channel 02 company 1000', '', ''])
        else:
            error.append({'customer': row['customer'], 'company': row['company'], 'channel': row['channel'], 'division': row['division'], 'error': 'Miss case company 1000'})
            return ['', 'error: miss case company 1000', '', '', '']
    elif row['company'] == '9000':
        if row['channel'] == '05':
            return ['', '', '', '']
        elif row['channel'] == '99':
            return ['HO', 'HO', '9000', '99', '99']
        elif row['channel'] == '02':
            return ['BU1', 'BU1_KA', '1100', row['channel'], row['division']]
        else:
            error.append({'customer': row['customer'], 'company': row['company'],'channel': row['channel'], 'division': row['division'], 'error': 'Miss case channel'})
            return ['', 'Miss case channel', '', '', '']
   
    else:
        error.append({'customer': row['customer'], 'company': row['company'],'channel': row['channel'], 'division': row['division'], 'error': 'Company moi'})
        return ['', 'error: company', '', '', '']

def update_bill_prod():
    update_bill(debug=True)
def update_bill_qas():
    return ("Tam thoi dung")
    host = "http://103.21.148.147:8021"
    table = 'bill_qas' 
    schema = 'pmh_test'
    update_bill(table=table, schema=schema, host=host, headers=None)

begin = DummyOperator(dag=dag, task_id="begin")

task_update_bill_prod = PythonOperator(task_id = "update_bill_prod", python_callable = update_bill_prod, dag = dag)
task_update_bill_qas = PythonOperator(task_id = "update_bill_qas", python_callable = update_bill_qas, dag = dag)
task_update_bill_staging = PythonOperator(task_id = "update_bill_satging", python_callable = update_bill_satging, dag = dag)
# task_update_bill_datamart = PythonOperator(task_id = "update_bill_datamart", python_callable = update_bill_datamart, dag = dag)


begin >> [task_update_bill_prod, task_update_bill_qas]
task_update_bill_prod >> task_update_bill_staging 
# >> task_update_bill_datamart

