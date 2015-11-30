

-- This is a comment; with a semi-colon
/** This comment's open quote should not break multi-statement parsing **/

-- CREATING TABLE
CREATE TABLE ${sampleTable} (a INTEGER,
    /* THis is a comment */
	b VARCHAR(100),
	c TIMESTAMP WITH TIME ZONE,
	e NUMERIC(20,10),
	f NUMBER(18,2) NULL FORMAT '-----------------.99' TITLE 'f'
);

-- THIS IS ALSO A TEST
INSERT INTO ${sampleTable} VALUES (23, 'This -----  is a test;Making sure semi-colons
in statements work.$$', '2015-05-30 12:00:00-GMT', 1.23456, 789);

-- AND THIS
SELECT COUNT(*) AS ";count" FROM ${sampleTable};

;

-- MORE COMMENTS
SELECT * FROM ${sampleTable}



-- EVEN MORE
