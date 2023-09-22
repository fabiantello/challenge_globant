----- Question 1

------ First, install extension in postgresql
CREATE EXTENSION IF NOT EXISTS tablefunc;

--- After that, run this query
SELECT *
 FROM crosstab(
 'SELECT d.department
		,j.job  
		,concat(''Q'' , extract(quarter from cast(datetime as date)) ) as quarter
		,sum(he.id)
	FROM hired_employees he 
	left join departments d
		on he.department_id = d.id
	left join jobs j
		on he.job_id = j.id
	where extract(year from cast(datetime as date)) = 2021
	group by d.department
		,j.job  
		,concat(''Q'' , extract(quarter from cast(datetime as date)) )
	order by 1,2',
 'SELECT DISTINCT concat(''Q'' , extract(quarter from cast(datetime as date)) )
 FROM hired_employees he 
	left join departments d
		on he.department_id = d.id
	left join jobs j
		on he.job_id = j.id
	where extract(year from cast(datetime as date)) = 2021
 ORDER BY 1'
 )AS pivot_table (department TEXT, job TEXT, Q1 INT, Q2 INT, Q3 INT, Q4 INT);


 ----- Question 2
 