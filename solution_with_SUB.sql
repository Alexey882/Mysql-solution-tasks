SELECT DISTINCT cred_id
, min(rep_date) over (partition by cred_id, rnk) as delay_begin
, max(rep_date) over (partition by cred_id, rnk)  as delay_end
FROM (SELECT 
DISTINCT  cred_id, 
rep_date,
dpd,
dense_rank () over (partition by dpd ORDER BY cred_id) rnk
FROM (SELECT *,
 (coalesce(lead(dpd) over() , delays.dpd+1) - delays.dpd) grp 
 from delays) T1
WHERE grp = 1
order by 1, 2) T2
order by cred_id;