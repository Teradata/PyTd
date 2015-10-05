/********************************************************************************
*  Project:  Project_name                                                  
*  Describtion: Details 
*********************************************************s***********************/
.Logon Server/Userid,PWD;

** Initiate remark for start of script
.REMARK "<<<< Processing Initiated >>>>"

SELECT DATE,TIME;
.SET WIDTH 132;

CREATE TABLE Sou_EMP_Tab
( EMP_ID   Integer,
EMP_Name Char(10)
)Primary Index (EMP_ID);

INSERT INTO Sou_EMP_Tab
(1, 'bala');

.export report file=c:\p\hi.txt
.set retlimit 4
select 'test;'
;
select 'test' as "test;"
;
.export reset;

.import vartext ',' file = c:\p\var.txt
.quiet on;
.repeat *;
/*using i_eid(integer),
           i_ename(varchar(30)),
           i_sal(dec(6,2)),
           i_grade(varchar(30)),
           i_dept(varchar(30))
insert into tab_name(eid,ename,sal,grade,dept)
values(:i_eid,:i_ename,:i_sal,:i_grade,:i_dept);*/

INSERT INTO Sou_EMP_Tab
(2, 'nawab')
