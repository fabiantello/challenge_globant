CREATE TABLE IF NOT EXISTS jobs (
  id INT NOT NULL,
  job TEXT,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS error_jobs (
  id INT,
  job TEXT
);

CREATE TEMPORARY TABLE IF NOT EXISTS temp_jobs (
  id INT,
  job TEXT
);

COPY temp_jobs FROM '/path/to/csv/jobs.csv' WITH (FORMAT csv);
--COPY jobs FROM '/path/to/csv/jobs.csv' WITH (FORMAT csv);

INSERT INTO jobs
SELECT id,job
FROM temp_jobs
WHERE id IS NOT NULL
;

INSERT INTO error_jobs
SELECT id,job
FROM temp_jobs
WHERE id IS NULL
;


CREATE TABLE IF NOT EXISTS departments (
  id INT NOT NULL,
  department TEXT,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS error_departments (
  id INT,
  department TEXT
);

CREATE TEMPORARY TABLE IF NOT EXISTS temp_departments (
  id INT,
  department TEXT
);

COPY temp_departments FROM '/path/to/csv/departments.csv' WITH (FORMAT csv);

INSERT INTO departments
SELECT id,department
FROM temp_departments
WHERE id IS NOT NULL
;

INSERT INTO departments
SELECT id,department
FROM temp_departments
WHERE id IS NULL
;


CREATE TABLE IF NOT EXISTS hired_employees (
  id INT NOT NULL,
  "name" text,
  "datetime" VARCHAR(25) NOT NULL,
  department_id INT NOT NULL,
  job_id INT NOT NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_department
      FOREIGN KEY(department_id)
    REFERENCES departments(id),
  CONSTRAINT fk_jobs
      FOREIGN KEY(job_id)
    REFERENCES jobs(id)
);

CREATE TABLE IF NOT EXISTS error_hired_employees (
  id INT,
  "name" TEXT,
  "datetime" VARCHAR(25),
  department_id INT,
  job_id INT
);

CREATE TEMPORARY TABLE IF NOT EXISTS temp_hired_employees (
  id INT,
  "name" TEXT,
  "datetime" VARCHAR(25),
  department_id INT,
  job_id INT
);

COPY temp_hired_employees FROM '/path/to/csv/hired_employees.csv' WITH (FORMAT csv);

INSERT INTO hired_employees --(id,"name","datetime",department_id, job_id)
SELECT id,"name","datetime",department_id, job_id
FROM temp_hired_employees
WHERE coalesce(department_id,-1) IN (select distinct id from departments)
  AND coalesce(job_id,-1) IN (select distinct id from jobs)
  AND "datetime" IS NOT NULL
;
-- WHERE department_id IS NOT NULL
--   AND job_id IS NOT NULL
--   AND "datetime" IS NOT NULL
-- ;

INSERT INTO error_hired_employees --(id,"name","datetime",department_id, job_id)
SELECT id,"name","datetime",department_id, job_id
FROM temp_hired_employees
WHERE id IS NULL
  OR coalesce(department_id,-1) NOT IN (select distinct id from departments)
  OR coalesce(job_id,-1) NOT IN (select distinct id from jobs)
  OR "datetime" IS NULL
;