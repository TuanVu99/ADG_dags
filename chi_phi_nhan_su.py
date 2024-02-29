def chi_phi_nhan_su():
     """insert into reporting.chi_phi_nhan_su
  	select tmp.phap_nhan, tmp.khoi, tmp.costcenter, tmp.phong_ban, tmp.first_day_of_month, tmp.chi_phi_ke_hoach, tmp3.so_luong, tmp2.chi_phi_thuc_hien, case when tmp3.so_luong = 0 or tmp3.so_luong is null then 0 else  COALESCE(tmp2.chi_phi_thuc_hien, 0)/COALESCE(tmp3.so_luong, 1) end as binh_quan_tren_nguoi from (
	   	select 
	   		phap_nhan, khoi, costcenter, phong_ban, first_day_of_month, sum(chi_phi_ke_hoach) as chi_phi_ke_hoach from reporting.chi_phi_nhan_su_old cpnso 
	   	where 
	   		first_day_of_month = '2023-07-01'
	   	group by 
	   		phap_nhan, khoi, costcenter, phong_ban, first_day_of_month ) as tmp
   	full outer join (
		select 
			f."Cost_Center", sum("Amount_in_Local_Currency")/1000000 as chi_phi_thuc_hien  from reporting.fagll03 f 
	    join 
	    	master_data.costcenter_ns cn on cn.company = f."Company_Code" and cn.costcenter = f."Cost_Center" and cn.gl_account = f."G/L_Account" and commitment like '10%'
	    where 
	    	"Posting_Date" >= '2023-07-01' and "Posting_Date" < '2023-08-01' 
	    group by 
	    	f."Cost_Center" ) as tmp2 on tmp.costcenter = tmp2."Cost_Center"
	full outer join (
		select 
			costcenter, sum(so_thuc_hien) as so_luong from reporting.data_so_luong_nhan_su dslns 
	   	where 
	   		first_day_of_month = '2023-07-01'
	   	group by costcenter  ) as tmp3 on tmp.costcenter = tmp3.costcenter"""