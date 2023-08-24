.param init
-- set the home directory and prefix to the compiled extensions folder:
.param set $home "'/home/scb'"
.param set $prefix "'code/sqlite/extensions'"

-- load the vector and cdfn extensions:
SELECT load_extension((SELECT $home FROM sqlite_parameters)||'/'||(SELECT $prefix FROM sqlite_parameters)||'/vector');
SELECT load_extension((SELECT $home FROM sqlite_parameters)||'/'||(SELECT $prefix FROM sqlite_parameters)||'/cdf');

.mode list
SELECT '----- '||datetime('now')||' -----';
SELECT printf('');
SELECT printf('Test of the Virtual Table for NASA''s Common Data Format');
SELECT printf('');

-- create a test CDF file
-- it cannot exist before
.system touch ./testzvars2.cdf
.system rm ./testzvars2.cdf
-- .system ls -l /home/scb/data/cdf
SELECT printf('Creating virtual table:');
SELECT printf('CREATE VIRTUAL TABLE t2 USING cdffile(''/home/scb/data/cdf/testzvars2'', ''c'');');
CREATE VIRTUAL TABLE t2 USING cdffile('./testzvars2', 'c');
SELECT printf('');
SELECT printf('Virtual table t2 and related were created. The following tables exist:');
.mode box
SELECT name FROM sqlite_schema WHERE type='table';
.mode list
SELECT printf('');
SELECT printf('Table t2:');
.mode box
SELECT * FROM pragma_table_info('t2');
SELECT * FROM t2;
.mode list
SELECT printf('');

-- insert zvariables:
.mode list
SELECT printf('Table t2_zvars:');
.mode box
SELECT * FROM pragma_table_info('t2_zvars');
.mode list
SELECT printf('inserting/creating zvars ''site'', ''latitude'', ''longitude'', ''height'':');
INSERT INTO t2_zvars(name, dataspec) VALUES('Site', 51);
INSERT INTO t2_zvars(name) VALUES('Latitude');
INSERT INTO t2_zvars(name) VALUES('Longitude');
INSERT INTO t2_zvars(name, dataspec) VALUES('Height WGS84', 'float');
INSERT INTO t2_zvars(name, dataspec, numdims, dimsizes) VALUES('Min/max temperature', 'float', 1, 2); 
.mode box
SELECT * FROM t2_zvars;
SELECT name,padvalue FROM t2_zvars WHERE id='3';
SELECT name,padvalue FROM t2_zvars WHERE name LIKE 'Height WGS84';

-- preallocate records (normally not needed, automatically done by the CDF library)
.mode list
SELECT printf('preallocate ''site'':');
UPDATE t2_zvars SET maxalloc=3 WHERE id=1;
.mode box
SELECT * FROM t2_zvars;
.mode list
SELECT printf('preallocate all zvars:');
UPDATE t2_zvars SET maxalloc=3;
.mode box
SELECT * FROM t2_zvars;

-- insert records
.mode list
SELECT printf('');
SELECT printf('Table t2_zrecs:');
.mode box
SELECT * FROM pragma_table_info('t2_zrecs');
.mode list
SELECT printf('Inserting record ''Uppsala'', 59.838, 17.648, 38:');
INSERT INTO t2_zrecs VALUES(NULL, 'Uppsala', 59.838, 17.648, 38, float32(17.0, 22.0));
SELECT printf('Inserting record ''Kiruna'', 67.87, 20.43, 418:');
INSERT INTO t2_zrecs VALUES(NULL, 'Kiruna', 67.87, 20.43, 418, NULL);
SELECT printf('Inserting record ''Lund C'', 55.71, 13.19, 43:');
INSERT INTO t2_zrecs VALUES(NULL, 'Lund C', 55.71, 13.19, 43, float32(14, 23));
.mode list
SELECT printf('Table t2_zrecs with vector extension:');
.mode box
SELECT Id,Site,Latitude,Longitude,float32("Height WGS84") AS Height,
       float32("Min/max temperature", 1) AS Tmin, float32("Min/max temperature", 2) AS Tmax FROM t2_zrecs;
.mode list
SELECT printf('Table t2_zrecs with CDF REAL4/FLOAT types printed as BLOB');
.mode box
SELECT Id,Site,Latitude,Longitude,hex("Height WGS84") AS Height,
       hex("Min/max temperature") AS "Tmin/Tmax" FROM t2_zrecs;

-- rename a zvar
.mode list
SELECT printf('');
SELECT printf('Site -> Place:');
UPDATE t2_zvars SET name='Place', padvalue='xxx' WHERE id=1;
.mode box
SELECT * FROM t2_zvars;

-- update a pad value

-- delete a zvar
.mode list
SELECT printf('');
SELECT printf('Deleting zvar "Min/Max temperature"');
DELETE FROM t2_zvars WHERE id=5;
SELECT printf('Table t2_zvars:');
.mode box
SELECT * FROM t2_zvars;
.mode list
SELECT printf('Table t2_zrecs:');
.mode box
SELECT Id,Place,Latitude,Longitude,float32("Height WGS84") AS Height FROM t2_zrecs;

-- delete zvar records
.mode list
SELECT printf('');
SELECT printf('Deleting record "Lund C"');
DELETE FROM t2_zrecs WHERE Place='Lund C';
SELECT printf('Table t2_zrecs:');
.mode box
SELECT Id,Place,Latitude,Longitude,float32("Height WGS84") AS Height FROM t2_zrecs;

-- insert attributes
.mode list
SELECT printf('');
SELECT printf('Table t2_attrs:');
.mode box
SELECT * FROM pragma_table_info('t2_attrs');
.mode list
INSERT INTO t2_attrs VALUES(NULL, 'Description', 'global');
INSERT INTO t2_attrs VALUES(NULL, 'Source', 'global');
INSERT INTO t2_attrs VALUES(NULL, 'Epilogue', 'global');
INSERT INTO t2_attrs VALUES(NULL, 'Units', 'variable');
.mode box
SELECT * FROM t2_attrs;

-- insert global attribute entries
.mode list
SELECT printf('');
SELECT printf('Table t2_attrgents:');
.mode box
SELECT * FROM pragma_table_info('t2_attrgents');
INSERT INTO t2_attrgents(attrid, value) VALUES(1, 'Various locations');
INSERT INTO t2_attrgents VALUES(NULL, 'Source', 1, NULL, NULL, 'Google maps');
INSERT INTO t2_attrgents VALUES(NULL, 'Epilogue', 1, NULL, NULL, 9999);
SELECT * FROM t2_attrgents;

-- delete an attribute
.mode list
SELECT printf('');
SELECT printf('Deleting again "Epilogue" which changes the Id of attribute Units');
DELETE FROM t2_attrs WHERE Name='Epilogue';
.mode box
SELECT * FROM t2_attrs;
SELECT * FROM t2_attrgents;

-- insert variable attribute entries
.mode list
SELECT printf('Table t2_attrzents:');
.mode box
SELECT * FROM pragma_table_info('t2_attrzents');
INSERT INTO t2_attrzents VALUES(3, NULL, 'Place', NULL, NULL, 'text');
INSERT INTO t2_attrzents VALUES(3, NULL, 'Latitude', NULL, NULL, 'deg');
INSERT INTO t2_attrzents VALUES(3, NULL, 3, NULL, NULL, 'deg');
SELECT * FROM t2_attrzents;

-- update variable attribute entriy
.mode list
SELECT printf('');
SELECT printf('Updating "Epilogue" entry for "Place"');
UPDATE t2_attrzents SET Value='n/a' WHERE Name='Units' AND zVar='Place';
SELECT printf('Table t2_attrzents:');
.mode box
SELECT * FROM t2_attrzents;
