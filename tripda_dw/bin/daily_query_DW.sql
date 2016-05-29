SELECT
	*
FROM
	cube
WHERE
	cube.reference_date = DATE(SUBDATE(NOW(),1)) OR
	cube.reference_date = DATE(SUBDATE(NOW(),8)) OR
	cube.reference_date = DATE(SUBDATE(NOW(),15)) OR 
	cube.reference_date = DATE(SUBDATE(NOW(),22))