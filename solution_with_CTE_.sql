WITH ONE as 
(SELECT *,
 (coalesce(lead(dpd) over() , delays.dpd+1) - delays.dpd) grp 
 from delays),
TWO as
(SELECT 
DISTINCT  cred_id, 
rep_date
, dpd
, dense_rank () over (partition by dpd ORDER BY cred_id) rnk
FROM ONE
WHERE grp = 1
order by 1, 2)

SELECT 
DISTINCT cred_id
, min(rep_date) over (partition by cred_id, rnk) as delay_begin
, max(rep_date) over (partition by cred_id, rnk)  as delay_end
FROM TWO
order by cred_id;