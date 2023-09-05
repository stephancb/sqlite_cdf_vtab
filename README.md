# sqlite_cdf_vtab
The virtual table supports reading, creating/inserting and deleting data in a CDF file.

Common Data Fromat (CDF) is a self-describing data format for the storage of scalar and
multidimensional data in a platform- and discipline-independent way. CDF is used in Space
Physics and curated by the NASA GSFC:
[https://cdf.gsfc.nasa.gov/](https://cdf.gsfc.nasa.gov/)

# Installation

The SQLite virtual table for CDF files can be installed by cloning the git repository.
Change to the `src` directory. The following further instructions have so far been tested
only with Linux:

Typing `make` compiles a `cdf.so` shared object, which can be loaded as a [runtime
extension](https://www.sqlite.org/loadext.html):

# Using the CDF virtual table

- in the [C API](https://www.sqlite.org/cintro.html) with the function
  [`sqlite3_load_extension`](https://www.sqlite.org/c3ref/load_extension.html);

- in SQL with the function
  [`load_extension`](https://www.sqlite.org/lang_corefunc.html#load_extension), i.e.
  `SELECT load_extension(...)`;

- in the [CLI](https://www.sqlite.org/cli.html) with the dot command `.load`

Loading extensions in SQLite must have been enabled, see
[here](https://www.sqlite.org/c3ref/enable_load_extension.html). In the CLI loading
extension is enabled by default.

File `testcdfn.sql` is a script for the SQLite CLI `sqlite3`, with examples how to create
a CDF files with zVariables and to insert records and attributes.


Examples:

