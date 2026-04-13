## Objective
Demonstrate advanced SQL proficiency by successfully implementing 
Window functions, CTEs, and SCD Type 2 logic within PySpark SQL or a 
connected relational database (AWS RDS/Aurora) in 
at least 2 pipeline tasks by April 19, 2026.

## Expected result

Working SCD Type 2 implementation reviewed by mentor; 
correct use of Window functions and CTEs confirmed in 
code review.

--- 
I took first and second pipelines from the task 1 and added
CTE, Window function and SCD type 2. Most of the code is the same.

The main function which demonstrates SCD 2 is `save_as_scd2()`.
It is used in a separate function `incremental_load_with_scd2()`

In the folder task_4 you can find source .py files for Databricks notebooks.
