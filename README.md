# sqlite_cdf_vtab
The virtual table supports listing, creating and deleting data in a CDF file.

Common Data Fromat (CDF) is a self-describing data format for the storage of scalar and
     multidimensional data in a platform- and discipline-independent way.

CDF is used in Space Physics and curated by the NASA GSFC: [https://cdf.gsfc.nasa.gov/](https://cdf.gsfc.nasa.gov/)

File `testcdfn.sql` is a script for the SQLite CLI `sqlite3`, with examples how to create a CDF files with zVariables and to insert records and attributes.


Examples:

SELECT load_extension('/pathtoextensions/cdfzvars');
-- load a CDF file with ESA Swarm L1b LP data for reading:
