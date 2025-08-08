- Example 1 // Hierarchy

create or replace table emps (name string, mgr string);

insert into emps values
('SMITH' , 'FORD' ),
('ALLEN' , 'BLAKE'),
('WARD'  , 'BLAKE'),
('JONES' , 'KING' ),
('MARTIN', 'BLAKE'),
('BLAKE' , 'KING' ),
('CLARK' , 'KING' ),
('SCOTT' , 'JONES'),
('KING'  , null   ),
('TURNER', 'BLAKE'),
('ADAMS' , 'SCOTT'),
('JAMES' , 'BLAKE'),
('FORD'  , 'JONES'),
('MILLER', 'CLARK');




with recursive mgmt (e, m, path) as
(
select name, mgr, array(name)
  from emps
 where mgr is null
 union all
select e.name, e.mgr, array_append(m.path, e.name)
  from mgmt as m
  join emps as e on e.mgr = m.e
)
select e, m, path
  from mgmt;









-- Example 2 // Formula evaluation

declare or replace variable v_formula string default '6*3/2*9-4+7';

with recursive cte_recurs (formula, num, lvl) as
(
select v_formula
     , substring(v_formula from 1 for 1)::double
     , 1y
 union all
select formula
     , case substring(formula from lvl * 2 for 1)
         when '*' then num * substring(formula from lvl * 2 + 1 for 1)::double
         when '/' then num / substring(formula from lvl * 2 + 1 for 1)::double
         when '+' then num + substring(formula from lvl * 2 + 1 for 1)::double
         when '-' then num - substring(formula from lvl * 2 + 1 for 1)::double
       end
     , lvl + 1y
  from cte_recurs
 where substring(formula from lvl * 2 for 1) <> ''
)
  select num, lvl
    from cte_recurs
order by lvl;














-- Example 3 // Sessionization

create or replace table sessionize
( id    bigint    not null
, ts    timestamp not null
);

insert into sessionize (id, ts)
select id + 1
     , timestampadd(minute, id + 1, case when id <= 5 then timestamp '2025-07-11 11:00:00' when id <= 15 then timestamp '2025-07-11 12:00:00' else timestamp '2025-07-11 13:00:00' end)
  from range(20)
;

select id, extract(hour from ts) as hr, ts from sessionize order by 1;


-- Rule 1 -- Session ends after 5 minutes of inactivity (browsing a page)
with cte_chk_new_session (ts, new_session) as
(
  select ts
       , (timestampdiff(minute, lag(ts) over W, ts) >= 5
      or lag(ts) over W is null)::tinyint
    from sessionize
  window W as (order by ts)
)
  select extract(hour from ts) as hr, ts
       , sum(new_session) over W as session_id
    from cte_chk_new_session
  window W as (order by ts)
order by ts asc;


-- Rule 2 -- Session last 5 minutes after they start (validity of a cart)
with recursive cte_session (id, ts, session_ts, session_id) as
(
select id, ts, ts, 1
  from sessionize
 where id = 1
 union all
select s.id, s.ts
     , case when timestampdiff(minute, c.session_ts, s.ts) >= 5 then s.ts else c.session_ts end
     , c.session_id + case when timestampdiff(minute, c.session_ts, s.ts) >= 5 then 1 else 0 end
  from cte_session as c
  join sessionize  as s on s.id = c.id + 1
)
  select id, ts, session_ts, session_id
    from cte_session
order by ts asc;
