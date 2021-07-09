delimiter //
drop procedure if exists tb_with_date//
create procedure tb_with_date() 
begin 
declare num_group int default 0;
declare done int default 0;
declare cred_id , dpd,grp int ;
declare rep_date date;
declare cur cursor for select *, (coalesce(lead(dpd) over() , delays.dpd+1) - delays.dpd) grp  from delays;
declare continue handler for not found set done = 1;
drop table if exists new_tb_with_normal_group;
create table new_tb_with_normal_group (cred_id int, rep_date date , dpd int , grp int);
open cur;
while done = 0 do  
fetch cur into cred_id , rep_date ,dpd , grp;
if not grp is null then 
while grp=1 do
insert into new_tb_with_normal_group values(cred_id , rep_date, dpd ,num_group);
fetch cur into cred_id , rep_date ,dpd , grp;
end while;
set num_group = num_group+1;
end if;
end while;
select T.cred_id , min(T.rep_date) as start_ , max(T.rep_date) as end_ from new_tb_with_normal_group T group by T.cred_id , T.grp;
close cur;
end //
call tb_with_date()//
select * from new_tb_with_normal_group//
#select * from delays;