/*
** 2022-12-30
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file codes a virtual table system for CDF files, https://cdf.gsfc.nasa.gov/
** Examples:
**
**    CREATE VIRTUAL TABLE l1blp_attrentry USING cdfattrentry('SW_OPER_EFIC_LP_1B_20200502T000000_20200502T235959_0502_MDR_EFI_LP');
**    SELECT * FROM l1blp_attrentry;
*/
#if !defined(SQLITEINT_H)
#include "sqlite3ext.h"
#endif
SQLITE_EXTENSION_INIT1

/* #define PADISNULL */

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include <cdf.h>

typedef struct CdfVTab CdfVTab;
struct CdfVTab {         /* All CDF tables have the CDFid file id and a mode: */
  sqlite3_vtab base;     /* Base class.  Must be first */
  CDFid        id;       /* CDF file identifier. */
  char         mode;     /* 'c'=create, 'd' deletes the file, 'r' read only, 'w' read and write
                             's' read only, connect with CDFid, do not close;
                             't' add zVars, connect with CDFid, do not close*/
  sqlite3*     db;       /* database connection */ 
  char*        name;     /* virtual table name */
};
static CDFid cdf_id(CdfVTab *vp) { return vp->id; }
static char cdf_mode(CdfVTab *vp) { return vp->mode; }

/* A cursor for the CDF attribute entires virtual table */
typedef struct CdfVTabCursor CdfVTabCursor;
struct CdfVTabCursor {
    sqlite3_vtab_cursor basecur;     /* Base class.  Must be first */
    /* CdfVTab            *vtabp;       */
    CDFid               id;          /* CDF file descriptor */
    sqlite_int64        rowid;       /* rowid */
};
static sqlite_int64 cur_rowid(CdfVTabCursor *cp) { return cp->rowid; }

/* Helper functions */

/*
** Dequote the string
** This is a verbatim copy of vsv_dequote at http://www.dessus.com/files/vsv.c
** Retrieved 2022-12-29
*/
static void cdf_dequote(char *z)
{
    int j;
    char cQuote = z[0];
    size_t i, n;

    if (cQuote!='\'' && cQuote!='"')
    {
        return;
    }
    n = strlen(z);
    if (n<2 || z[n-1]!=z[0])
    {
        return;
    }
    for (i=1, j=0; i<n-1; i++)
    {
        if (z[i]==cQuote && z[i+1]==cQuote)
        {
            i++;
        }
        z[j++] = z[i];
    }
    z[j] = 0;
}
/*
** Quote the string
*/
static void cdf_quote(char *z, char *const qz)
{
    qz[0] = '"';
    stpcpy(stpcpy(&qz[1], z), "\"");
}
/*
** Remove the *.cdf extension if present
*/
static void cdf_rmext(char *z)
{
    int n = strlen(z);
    if( strncmp(z+n-4, ".cdf", 4)==0 || strncmp(z+n-4, ".CDF", 4)==0 )
        z[n-4] = '\0';
}

static void cdf_prep_name(const char *argstr, char *name)
{
    strncpy(name, argstr, CDF_PATHNAME_LEN+2);
    name[CDF_PATHNAME_LEN+2] = '\0';
    /* Remove quotation marks around the argument added by sqlite: */
    cdf_dequote(name);
    cdf_rmext(name);
}

static CDFstatus cdf_open(const char *argstr, char *name, CDFid *idp)
{
    cdf_prep_name(argstr, name);
    return CDFopenCDF(name,  idp);
}

static CDFstatus cdf_createfile(const char *argstr, char *name, CDFid *idp)
{
    cdf_prep_name(argstr, name);
    return CDFcreateCDF(name,  idp);
}

/*
 * Return the element size in bytes of the various CDF datatypes 
 */
static int cdf_elsize(long cdftype) {
    switch (cdftype) {
        case CDF_REAL8:
        case CDF_DOUBLE:
        case CDF_EPOCH:
        case CDF_INT8:
        case CDF_TIME_TT2000:
            return 8;
        case CDF_REAL4:
        case CDF_FLOAT:
        case CDF_INT4:
        case CDF_UINT4:
            return 4;
        case CDF_INT2:
        case CDF_UINT2:
            return 2;
        case CDF_INT1:
        case CDF_UINT1:
        case CDF_BYTE:
        case CDF_CHAR:
        case CDF_UCHAR:
            return 1;
        case CDF_EPOCH16:
            return 16;
        default:
            return 0;
    }
}

/*
 * Return the SQLite types to which the various CDF datatypes are mapped, 
 * assuming the vector extension is availables
 */
static int cdf_sqlitetype(long cdftype) {
    switch (cdftype) {
        case CDF_REAL8:
        case CDF_DOUBLE:
        case CDF_EPOCH:
        case CDF_TIME_TT2000:
            return SQLITE_FLOAT;
        case CDF_INT8:
        case CDF_INT4:
        case CDF_UINT4:
        case CDF_INT2:
        case CDF_UINT2:
        case CDF_INT1:
        case CDF_UINT1:
        case CDF_BYTE:
            return SQLITE_INTEGER;
        case CDF_CHAR:
        case CDF_UCHAR:
            return SQLITE_TEXT;
        case CDF_REAL4:
        case CDF_FLOAT:
        case CDF_EPOCH16:
            return SQLITE_BLOB;
        default:
            return 0;
    }
}

const char* typestrs[] = {
    "real8", "double", "epoch", "time_tt2000", "int8", "int4", "uint4",
    "int2", "uint2", "int1", "uint1", "byte", "char", "uchar", "real4", "float", "epoch16", ""
};

const int typeids[] = {
    CDF_REAL8, CDF_DOUBLE, CDF_EPOCH, CDF_TIME_TT2000, CDF_INT8, CDF_INT4, CDF_UINT4,
    CDF_INT2, CDF_UINT2, CDF_INT1, CDF_UINT1, CDF_BYTE, CDF_CHAR, CDF_UCHAR, CDF_REAL4, CDF_FLOAT, CDF_EPOCH16, 0
};
/*
 * Return human readable strings for the various CDF datatypes
 */
static const char* cdf_typestr(long cdftype) {
    long k;

    for( k=0; k<17; k++ )
        if( typeids[k]==cdftype )
            break;
    return typestrs[k];
}
/*
 * Return the CDF type id for a CDF type string, not starting with "CDF_..."
 */
static int cdf_typeid(const char *typestr) {
    long k;

    for( k=0; k<17; k++ )
        if( strncmp(typestrs[k], typestr, 11)==0 )
            break;
    return typeids[k];
}
/*
 * Return the CDF type id for a CDF type string, not starting with "CDF_..."
 */
static int cdf_typesql(const int sqltype) {
    switch( sqltype ) {
        case SQLITE_INTEGER:
            return CDF_INT8;
        case SQLITE_FLOAT:
            return CDF_REAL8;
        case SQLITE_TEXT:
            return CDF_CHAR;
        default:
            return 0;
    }
}

static int cdf_valfuncid(long cdftype) {
    if( cdftype<=CDF_UINT4 || cdftype==CDF_BYTE || cdftype==CDF_TIME_TT2000 )
        return 0;
    else if ( cdftype==CDF_REAL8 || cdftype==CDF_DOUBLE || cdftype==CDF_EPOCH )
        return 1;
    else if( cdftype==CDF_CHAR || cdftype==CDF_UCHAR )
        return 2;
    else if ( cdftype==CDF_REAL4 || cdftype==CDF_FLOAT )
        return 3;
    else if ( cdftype==CDF_EPOCH16 )
        return 4;
    else
        return -1;
};

static char* typetext[] = {"ANY", "INTEGER", "REAL", "TEXT", "BLOB", "NULL"};

/*
** Parameters:
**    file name       CDF filename (without ".cdf" extension)
**    mode            'c', 'd', 'r' or 'w'
**   c    create the CDF file
**   d    delete the CDF file
**   r    read only
**   w    read/write
*/
static int cdfFileConnect(
        sqlite3 *db,
        void *pAux,
        int argc, const char *const*argv,
        sqlite3_vtab **ppVtab,
        char **pzErr)
{
    CDFstatus    status;
    CDFid        id;
    char        *z;
    sqlite3_str *zsql = sqlite3_str_new(db);
    CdfVTab     *filevtabp;
    char         name[CDF_PATHNAME_LEN+4],mstr[4],mode,submode;
    int          rc;

    if( argc<4 ) {
        *pzErr = sqlite3_mprintf( "at least one arg is needed, must be the CDF file name!" );
        return SQLITE_ERROR;
    }

    if( strlen(argv[3])>CDF_PATHNAME_LEN+3 ) {
        *pzErr = sqlite3_mprintf( "CDF file name is too long!" );
        return SQLITE_ERROR;
    }

    if( argc>4 ) {
        if( strlen(argv[4])>3 ) {
            *pzErr = sqlite3_mprintf( "mode argument needs to be exactly one char!" );
            return SQLITE_ERROR;
        }
        strncpy(mstr, argv[4], 4);
        cdf_dequote(mstr);
        mode = mstr[0];
        if( strchr("cdrw", mode)==NULL ) {
            *pzErr = sqlite3_mprintf("mode %c unknown, must be c, d, r or w", mode);
            return SQLITE_ERROR;
        }
    } else
        mode = 'r';

    if( mode=='c' ) {
        status = cdf_createfile(argv[3], name, &id);
        if( status!=CDF_OK ) {
            char statustext[CDF_STATUSTEXT_LEN+1];
            CDFgetStatusText(status, statustext);
            *pzErr = sqlite3_mprintf("Cannot create CDF file '%s'\n%s", name, statustext);
            return SQLITE_ERROR;
        }
    } else {
        status = cdf_open(argv[3], name, &id);
        if( status!=CDF_OK ) {
            char statustext[CDF_STATUSTEXT_LEN+1];
            CDFgetStatusText(status, statustext);
            *pzErr = sqlite3_mprintf("Cannot open CDF file '%s'\n%s", name, statustext);
            return SQLITE_CANTOPEN;

        }
        if( mode=='d' ) {
            status = CDFdelete(id);
            if( status!=CDF_OK ) {
                char statustext[CDF_STATUSTEXT_LEN+1];
                CDFgetStatusText(status, statustext);
                *pzErr = sqlite3_mprintf("Cannot delete CDF file '%s'\n%s", name, statustext);
                return SQLITE_CANTOPEN;
            }
            return SQLITE_OK;
        }
    }

    sqlite3_str_appendf(zsql, "CREATE TABLE cdf_file_ignored (\n");
    sqlite3_str_appendf(zsql, "    cdfid INTEGER PRIMARY KEY,\n");
    sqlite3_str_appendf(zsql, "    name TEXT NOT NULL\n);\n");
    rc = sqlite3_declare_vtab(db, sqlite3_str_value(zsql));
    if( rc!=SQLITE_OK ) {
        *pzErr = sqlite3_mprintf("Bad schema \n%s\nerror code: %d\n", z, rc);
        return SQLITE_ERROR;
    }
    
    filevtabp = sqlite3_malloc( sizeof(*filevtabp) );
    if( filevtabp==0 )
        return SQLITE_NOMEM;
    filevtabp->id   = id;
    filevtabp->mode = mode;
    filevtabp->db   = db;
    filevtabp->name = sqlite3_malloc( strlen(argv[2])+1 );
    stpcpy(filevtabp->name, argv[2]);

    *ppVtab = (sqlite3_vtab*) filevtabp;
    
    if( mode=='r' )
        submode = 's';
    else
        submode = 't';

    sqlite3_str_reset(zsql);
    sqlite3_str_appendf(zsql, "CREATE VIRTUAL TABLE %s_zvars USING cdfzvars('%d','%c')",
            argv[2], (long) id, submode);
    rc = sqlite3_exec(db, sqlite3_str_value(zsql), NULL, NULL, NULL);
    
    sqlite3_str_reset(zsql);
    sqlite3_str_appendf(zsql, "CREATE VIRTUAL TABLE %s_zrecs USING cdfzrecs('%d','%c')",
            argv[2], (long) id, submode);
    rc = sqlite3_exec(db, sqlite3_str_value(zsql), NULL, NULL, NULL);

    sqlite3_str_reset(zsql);
    sqlite3_str_appendf(zsql, "CREATE VIRTUAL TABLE %s_attrs USING cdfattrs('%d','%c')",
            argv[2], (long) id, submode);
    rc = sqlite3_exec(db, sqlite3_str_value(zsql), NULL, NULL, NULL);

    sqlite3_str_reset(zsql);
    sqlite3_str_appendf(zsql, "CREATE VIRTUAL TABLE %s_attrgents USING cdfattrgentries('%d','%c')",
            argv[2], (long) id, submode);
    rc = sqlite3_exec(db, sqlite3_str_value(zsql), NULL, NULL, NULL);

    sqlite3_str_reset(zsql);
    sqlite3_str_appendf(zsql, "CREATE VIRTUAL TABLE %s_attrzents USING cdfattrzentries('%d','%c')",
            argv[2], (long) id, submode);
    rc = sqlite3_exec(db, sqlite3_str_value(zsql), NULL, NULL, NULL);

    sqlite3_free(sqlite3_str_finish(zsql));

    return rc;
}

static int cdf_close(CdfVTab *pv){
    CDFstatus status = CDF_OK;
    if( strchr("st", cdf_mode(pv))==NULL ) {
        status = CDFcloseCDF(pv->id);
        if( status!=CDF_OK) {
            char statustext[CDF_STATUSTEXT_LEN+1];
            CDFgetStatusText(status, statustext);
            char **pzErr = &pv->base.zErrMsg;
            *pzErr = sqlite3_mprintf("Closing CDF file failed:\n%s", statustext);
            return SQLITE_ERROR;
        }
    }

    return SQLITE_OK;
}
/*
** This method is the destructor for CDF virtual tables.
*/
static int cdfVTabDisconnect(sqlite3_vtab *pvtab){
    CdfVTab *pv = (CdfVTab*) pvtab;
    int rc = cdf_close(pv);
    sqlite3_free(pv->name);
    sqlite3_free(pv);

    return rc;
}

/*
** There is only one row, estimatedCost is 1:
*/
static int cdfFileBestIndex(
        sqlite3_vtab *vtabp,
        sqlite3_index_info *idxinfop)
{
    idxinfop->estimatedCost = 1;

    return SQLITE_OK;
}

/*
** Constructor for a new CdfVTabCursor object.
*/
static int cdfVTabOpen(
        sqlite3_vtab* vtabp,
        sqlite3_vtab_cursor** ppcur)
{
    CdfVTabCursor *curp = sqlite3_malloc64(sizeof(CdfVTabCursor));
    if( curp==0 ) return SQLITE_NOMEM;

    curp->id    = ((CdfVTab*) vtabp)->id;
    curp->rowid = 1;

    *ppcur = (sqlite3_vtab_cursor*) curp;
    return SQLITE_OK;
}

/*
** Destructor for a CdfVTabCursor.
*/
static int cdfVTabClose(sqlite3_vtab_cursor *cp){
    sqlite3_free((CdfVTabCursor*) cp);

    return SQLITE_OK;
}

/*
** No index/binary search availble, we start at rowid 1:
*/
static int cdfVTabFilter(
        sqlite3_vtab_cursor *cp, 
        int idxNum, const char *idxStr,
        int argc, sqlite3_value **argv
){
    ((CdfVTabCursor*) cp)->rowid = 1;
    return SQLITE_OK;
}

/*
** Advance a CdfVTabCursor to its next row of input.
*/
static int cdfVTabNext(sqlite3_vtab_cursor *cp) {
    ((CdfVTabCursor*) cp)->rowid++;
    
    return SQLITE_OK;
}
/*
** Return TRUE if the cursor has been moved past, the cdffile virtual table has only one row:
*/
static int cdfFileEof(sqlite3_vtab_cursor *cp) { return ((CdfVTabCursor*) cp)->rowid > 1; }

/*
** Return values of columns for the row at which the CdfVTabCursor is currently pointing.
*/
static int cdfFileColumn(
        sqlite3_vtab_cursor *curp,  /* The cursor */
        sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
        int iCol)
{
    CDFid id = ((CdfVTabCursor*) curp)->id;

    if( iCol==0 )   /* CDF file id */
        sqlite3_result_int64(ctx, (sqlite_int64) id);
    else if( iCol==1 ) {  /* CDF file name */
        char name[CDF_PATHNAME_LEN];
        CDFstatus status = CDFgetName(id, name);
        sqlite3_result_text(ctx, name, strnlen(name, CDF_PATHNAME_LEN), SQLITE_TRANSIENT);
    } else 
        return SQLITE_ERROR;

    return SQLITE_OK;
}

/*
** Return the rowid field in the cursor:
*/
static int cdfVTabRowid(sqlite3_vtab_cursor *cp, sqlite_int64 *prowid) {
    *prowid = cur_rowid((CdfVTabCursor*) cp);
    return SQLITE_OK;
}

/*
** The xConnect and xCreate methods do the same thing, but they must be
** different so that the virtual table is not an eponymous virtual table.
*/
static int cdfFileCreate(
        sqlite3 *db,
        void *pAux,
        int argc, const char *const*argv,
        sqlite3_vtab **ppVtab,
        char **pzErr
        ){
    return cdfFileConnect(db, pAux, argc, argv, ppVtab, pzErr);
}

static sqlite3_module CdfFileModule = {
  0,                       /* iVersion */
  cdfFileCreate,      /* xCreate */
  cdfFileConnect,     /* xConnect */
  cdfFileBestIndex,   /* xBestIndex */
  cdfVTabDisconnect,  /* xDisconnect */
  cdfVTabDisconnect,  /* xDestroy */
  cdfVTabOpen,        /* xOpen - open a cursor */
  cdfVTabClose,       /* xClose - close a cursor */
  cdfVTabFilter,      /* xFilter - configure scan constraints */
  cdfVTabNext,        /* xNext - advance a cursor */
  cdfFileEof,         /* xEof - check for end of scan */
  cdfFileColumn,      /* xColumn - read data */
  cdfVTabRowid,       /* xRowid - read data */
  0,                  /* xUpdate - not updatable */
  0,                       /* xBegin */
  0,                       /* xSync */
  0,                       /* xCommit */
  0,                       /* xRollback */
  0,                       /* xFindMethod */
  0,                       /* xRename */
};

/* A cursor for the CDF zVars virtual table */
typedef struct CdfzVarsCursor CdfzVarsCursor;
struct CdfzVarsCursor {
    sqlite3_vtab_cursor base;       /* Base class.  Must be first */
    CDFid id;                       /* CDF file identifier. */
    sqlite_int64 zvarid;            /* The current rowid. */
    sqlite_int64 lastrow;           /* The last zvar, which the cursor should reach */
};

static int cdf_prep_idmode(
        int argc, const char *const*argv,
        char **pzErr,
        CDFid *idp, char *modep
){
    CDFstatus   status;
    char        name[CDF_PATHNAME_LEN+4],mstr[4];

    if( argc<4 ) {
        *pzErr = sqlite3_mprintf( "at least one arg is needed, must be the CDF file name!" );
        return SQLITE_ERROR;
    }
    if( argc>4 ) {
        if( strlen(argv[4])>3 ) {
            *pzErr = sqlite3_mprintf( "mode argument needs to be exactly one char!" );
            return SQLITE_ERROR;
        }
        strncpy(mstr, argv[4], 4);
        cdf_dequote(mstr);
        *modep = mstr[0];
        if( strchr("rwst", *modep)==NULL ) {
            *pzErr = sqlite3_mprintf("mode %c unknown, must be r, w, s or t", *modep);
            return SQLITE_ERROR;
        }
    } else
        *modep = 'r';

    if( *modep=='r' || *modep=='w' ) {
        if( strlen(argv[3])>CDF_PATHNAME_LEN+2 ) {
            *pzErr = sqlite3_mprintf( "CDF file name is too long!" );
            return SQLITE_ERROR;
        }
        status = cdf_open(argv[3], name, idp);
        if( status!=CDF_OK ) {
            char statustext[CDF_STATUSTEXT_LEN+1];
            CDFgetStatusText(status, statustext);
            *pzErr = sqlite3_mprintf("Cannot open CDF file '%s'\n%s", name, statustext);
            return SQLITE_CANTOPEN;
        }
    } else {
        strncpy(name, argv[3], CDF_PATHNAME_LEN+2);
        name[CDF_PATHNAME_LEN+2] = '\0';
        cdf_dequote(name);
        *idp = (CDFid) atol(name);
    }
    return SQLITE_OK;
}

static int cdf_createvtab(sqlite3 *db, sqlite3_str *zsql, CDFid id, char mode, const char *name,
        char **pzErr, sqlite3_vtab **ppVtab)
{
    char    *z = sqlite3_str_value(zsql);
    int      rc;
    CdfVTab *vtabp = 0;

    rc = sqlite3_declare_vtab(db, z);
    if( rc!=SQLITE_OK ) {
        *pzErr = sqlite3_mprintf("Bad schema \n%s\nerror code: %d\n", z, rc);
        sqlite3_free(sqlite3_str_finish(zsql));
        return SQLITE_ERROR;
    }
    sqlite3_free(sqlite3_str_finish(zsql));

    vtabp = sqlite3_malloc( sizeof(CdfVTab) );
    *ppVtab = (sqlite3_vtab*) vtabp;
    if( vtabp==0 ) return SQLITE_NOMEM;
    memset(vtabp, 0, sizeof(CdfVTab));

    vtabp->id   = id;
    vtabp->mode = mode;
    vtabp->db   = db;
    vtabp->name = sqlite3_malloc( strlen(name)+1 );
    stpcpy(vtabp->name, name);

    return rc;
}

static int cdfzVarsConnect(
        sqlite3 *db,
        void *pAux,
        int argc, const char *const*argv,
        sqlite3_vtab **ppVtab,
        char **pzErr)
{
    CDFid          id;
    char           mode;
    sqlite3_str   *zsql = sqlite3_str_new(db);
    int            rc;

    rc = cdf_prep_idmode(argc, argv, pzErr, &id, &mode);
    if( rc!=SQLITE_OK ) return rc;

    sqlite3_str_appendf(zsql, "CREATE TABLE cdf_zvars_ignored (\n");
    sqlite3_str_appendf(zsql, "    id INTEGER PRIMARY KEY,\n");
    sqlite3_str_appendf(zsql, "    name TEXT,\n");
    sqlite3_str_appendf(zsql, "    dataspec DEFAULT 45,\n");
    sqlite3_str_appendf(zsql, "    numelem INTEGER DEFAULT 1,\n");
    sqlite3_str_appendf(zsql, "    numdims INTEGER DEFAULT 0,\n");
    sqlite3_str_appendf(zsql, "    dimsizes BLOB DEFAULT NULL,\n");
    sqlite3_str_appendf(zsql, "    recvariance INTEGER DEFAULT -1,\n");
    sqlite3_str_appendf(zsql, "    dimvariances BLOB DEFAULT NULL,\n");
    sqlite3_str_appendf(zsql, "    maxwritten INTEGER DEFAULT 0,\n");
    sqlite3_str_appendf(zsql, "    maxalloc INTEGER DEFAULT 0,\n");
    sqlite3_str_appendf(zsql, "    padvalue\n");
    sqlite3_str_appendf(zsql, ");\n");

    return cdf_createvtab(db, zsql, id, mode, argv[2], pzErr, ppVtab);
}


/*
** For a clause like WHERE name=='zVarName' use CDFgetVarNum instead of a full table scan.
** Forward full table scan is supported. xBestIndex becomes mostly a no-op.
*/
static int cdfzVarsBestIndex(
        sqlite3_vtab *vtabp,
        sqlite3_index_info *iip
){
    CdfVTab *vp = (CdfVTab*) vtabp;
    CDFstatus status;
    long nzvars;

    iip->idxNum = 0;
    iip->idxStr = "";

    status = CDFgetNumzVars(vp->id, &nzvars);
    iip->estimatedCost = nzvars;

    for( int k=0; k<iip->nConstraint; k++ )
        if( iip->aConstraint[k].usable && iip->aConstraint[k].iColumn==1 && 
            iip->aConstraint[k].op==SQLITE_INDEX_CONSTRAINT_EQ )
        {
            iip->estimatedRows = 1;
            iip->aConstraintUsage[k].argvIndex = 1;
            iip->idxFlags = SQLITE_INDEX_SCAN_UNIQUE;
            iip->estimatedCost = 2.0;
            iip->idxNum = SQLITE_INDEX_CONSTRAINT_EQ;
            iip->aConstraintUsage[k].argvIndex = 1;
            iip->aConstraintUsage[k].omit = 1;
        }

    /*  printf("iColumn = %d, op = %d, usable = %d\n",
                iip->aConstraint[k].iColumn,
                iip->aConstraint[k].op,
                iip->aConstraint[k].usable);
                */

    return SQLITE_OK;
}

/*
** For a constraint like WHERE name=='zVarName'
** we use CDFgetVarNum instead of a full table scan.
*/
static int cdfzVarsFilter(
        sqlite3_vtab_cursor *curp, 
        int idxNum, const char *idxStr,
        int argc, sqlite3_value **argv)
{
    CdfzVarsCursor *cp = (CdfzVarsCursor*) curp;

    CDFstatus status = CDFgetNumzVars(cp->id, &(cp->lastrow));

    if( idxNum==SQLITE_INDEX_CONSTRAINT_EQ && argc>0 ) {
        const char *varname = sqlite3_value_text(argv[0]);
        long varnum = CDFgetVarNum(cp->id, (char*) varname);
        /* printf("zVarsFilter: varname = %s, varnum = %d\n", varname, varnum); */
        cp->zvarid  = varnum+1;
        cp->lastrow = cp->zvarid;
    } else
        cp->zvarid = 1;

    return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved beyond the nr of zVars
*/
static int cdfzVarsEof(sqlite3_vtab_cursor *curp){
    CdfzVarsCursor *cp = (CdfzVarsCursor*) curp;
    return cp->zvarid > cp->lastrow;
}

static CDFstatus result_zvarid(sqlite3_context *ctx, CDFid id, long kzvar) {
    sqlite3_result_int(ctx, kzvar);
    return CDF_OK;
}
static CDFstatus result_varname(sqlite3_context *ctx, CDFid id, long kzvar) {
    char varName[CDF_VAR_NAME_LEN256];
    /* char * varName = sqlite3_malloc(CDF_VAR_NAME_LEN256); */
    memset(varName, '\0', CDF_VAR_NAME_LEN256);
    CDFstatus status = CDFgetzVarName(id, kzvar-1, varName);
    if( status>=CDF_OK )
        sqlite3_result_text64(ctx, varName, strlen(varName)+1, SQLITE_TRANSIENT, SQLITE_UTF8);
    /* sqlite3_result_text(ctx, varName, -1, sqlite3_free); */

    return status;
}
static CDFstatus result_datatype(sqlite3_context *ctx, CDFid id, long kzvar) {
    long n=0;
    CDFstatus status = CDFgetzVarDataType(id, kzvar-1, &n);
    if( status>=CDF_OK )
        sqlite3_result_int64(ctx, n);
    return status;
}
static CDFstatus result_numelements(sqlite3_context *ctx, CDFid id, long kzvar) {
    long n=0;
    CDFstatus status = CDFgetzVarNumElements(id, kzvar-1, &n);
    if( status==CDF_OK )
        sqlite3_result_int64(ctx, n);
    return status;
}
static CDFstatus result_numdims(sqlite3_context *ctx, CDFid id, long kzvar) {
    long n=0;
    CDFstatus status = CDFgetzVarNumDims(id, kzvar-1, &n);
    if( status>=CDF_OK )
        sqlite3_result_int64(ctx, n);
    return status;
}
static CDFstatus result_dimsizes(sqlite3_context *ctx, CDFid id, long kzvar) {
    long k,n=0;
    long dimsize;
    long *dimsizes;
    char *dimszstr;

    CDFstatus status = CDFgetzVarNumDims(id, kzvar-1, &n);
    if( status>=CDF_OK ) {
        if( n<=0 )
            sqlite3_result_null(ctx);
        else if( n==1 ) {
            status = CDFgetzVarDimSizes(id, kzvar-1, &dimsize);
            sqlite3_result_int64(ctx, dimsize);
        } else {
            dimsizes = sqlite3_malloc(n*sizeof(long));
            status = CDFgetzVarDimSizes(id, kzvar-1, dimsizes);
            dimszstr = sqlite3_malloc(n*12);
            dimszstr[0] = '\0';
            for( k=0; k<n; k++ )
                sprintf(dimszstr+strlen(dimszstr), "%d,", dimsizes[k]);
            dimszstr[strlen(dimszstr)-1] = '\0';
            sqlite3_result_text(ctx, dimszstr, -1, sqlite3_free);
            sqlite3_free(dimsizes);
        }
    }
    return status;
}
static CDFstatus result_recvariance(sqlite3_context *ctx, CDFid id, long kzvar) {
    long n=0;
    CDFstatus status = CDFgetzVarRecVariance(id, kzvar-1, &n);
    if( status>=CDF_OK )
        sqlite3_result_int64(ctx, n);
    return status;
}
static CDFstatus result_dimvariances(sqlite3_context *ctx, CDFid id, long kzvar) {
    long k,n=0;
    long dimvar;
    long *dimvars;
    char *dimvarstr,*dimvarp;
    
    CDFstatus status = CDFgetzVarNumDims(id, kzvar-1, &n);
    if( status>=CDF_OK )
        if( n<=0 )
            sqlite3_result_null(ctx);
        else if( n==1 ) {
            status = CDFgetzVarDimVariances(id, kzvar-1, &dimvar);
            sqlite3_result_int64(ctx, dimvar);
        } else {
            dimvars = sqlite3_malloc(n*sizeof(long));
            status = CDFgetzVarDimVariances(id, kzvar-1, dimvars);
            dimvarstr = sqlite3_malloc(n*4);
            dimvarp   = dimvarstr;
            for( k=0; k<n; k++ )
                dimvarp += sprintf(dimvarp, "%d,", dimvars[k]);
            dimvarp[-1] = '\0';
            sqlite3_result_text(ctx, dimvarstr, -1, sqlite3_free);
            sqlite3_free(dimvars);
        }
 
    return status;
}
static CDFstatus result_maxwrittenrec(sqlite3_context *ctx, CDFid id, long kzvar) {
    long n=0;
    CDFstatus status = CDFgetzVarMaxWrittenRecNum(id, kzvar-1, &n);
    if( status>=CDF_OK )
        sqlite3_result_int64(ctx, n);
    return status;
}
static CDFstatus result_maxallocrec(sqlite3_context *ctx, CDFid id, long kzvar) {
    long n;
    CDFstatus status = CDFgetzVarMaxAllocRecNum(id, kzvar-1, &n);
    if( status>=CDF_OK )
        sqlite3_result_int64(ctx, n);
    return status;
}
static CDFstatus result_padvalue(sqlite3_context *ctx, CDFid id, long kzvar) {
    long datatype,numelem;
    CDFdata value;
    CDFstatus status;

    if( (status=CDFreadzVarPadValue(id, kzvar-1, &datatype, &numelem, &value))<CDF_OK )
        return status;;

    switch (cdf_sqlitetype(datatype)) {
        case SQLITE_FLOAT:
            sqlite3_result_double(ctx, ((double*) value)[0]);
            break;
        case SQLITE_INTEGER:
            if( datatype==CDF_INT8 )
                sqlite3_result_int64(ctx, ((sqlite3_int64*) value)[0]);
            else if( datatype==CDF_INT2 || datatype==CDF_UINT2 )
                sqlite3_result_int(ctx, (int) ((short*) value)[0]);
            else if( datatype==CDF_INT1 || datatype==CDF_UINT1 )
                sqlite3_result_int(ctx, (int) ((char*) value)[0]);
            else
                sqlite3_result_int(ctx, ((int*) value)[0]);
            break;
        case SQLITE_TEXT:
            sqlite3_result_text(ctx, (const char*) value, -1, SQLITE_TRANSIENT);
            break;
        case SQLITE_BLOB:
            if( datatype==CDF_REAL4 || datatype==CDF_FLOAT )
                sqlite3_result_double(ctx, (double) ((float*) value)[0]);
            else
                sqlite3_result_blob(ctx, (const void*) value, 16, SQLITE_TRANSIENT);
            break;
        default:
            status = -1;
    }
    CDFdataFree(value);

    return SQLITE_OK;
}
static CDFstatus result_padset(sqlite3_context *ctx, CDFid id, long kzvar) {
    CDFstatus status;

    printf("result_padset: status = %d\n", CDFconfirmzVarPadValueExistence(id, kzvar-1));
    if( (status=CDFconfirmzVarPadValueExistence(id, kzvar-1))==NO_PADVALUE_SPECIFIED )
        sqlite3_result_int(ctx, 1);
    else
        sqlite3_result_int(ctx, 0);

    return CDF_OK;
}

/*
** Return values of columns for the row at which the CsvCursor
** is currently pointing.
*/
static int cdfzVarsColumn(
        sqlite3_vtab_cursor *curp,  /* The cursor */
        sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
        int iCol                    /* Which column to return */
){
    CdfzVarsCursor *cp = (CdfzVarsCursor*) curp;
    CDFstatus status;
    static CDFstatus (*res[12])(sqlite3_context*, CDFid, long) = {result_zvarid,
        result_varname, result_datatype, result_numelements, result_numdims, result_dimsizes,
        result_recvariance, result_dimvariances, result_maxwrittenrec, result_maxallocrec,
        result_padvalue};

    if( sqlite3_vtab_nochange(ctx) ) return SQLITE_OK;

    status = (*res[iCol])(ctx, cp->id, cp->zvarid);
    if( status!=CDF_OK )
        return SQLITE_ERROR;
    else
        return SQLITE_OK;
}
/*
** Return the rowid field in the cursor:
*/
static int cdfzVarsRowid(sqlite3_vtab_cursor *cp, sqlite_int64 *prowid) {
    *prowid = ((CdfzVarsCursor*) cp)->zvarid;
    return SQLITE_OK;
}

/* Update the zRec virtual table: */
static int cdf_recreate_zrecs(CdfVTab *vp) {
    char *zrecnm;
    sqlite3_str *zsql = sqlite3_str_new(vp->db);
    long n;
    int rc;

    n      = strlen(vp->name);
    zrecnm = sqlite3_malloc( n+1 );
    stpcpy(stpncpy(zrecnm, vp->name, n-5), "zrecs");
    sqlite3_str_appendf(zsql, "DROP TABLE \"%s\";", zrecnm);
    if( (rc = sqlite3_exec(vp->db, sqlite3_str_value(zsql), NULL,NULL,NULL))!=SQLITE_OK )
        goto cleanup;

    sqlite3_str_reset(zsql);
    sqlite3_str_appendf(zsql, "CREATE VIRTUAL TABLE \"%s\" USING cdfzrecs('%d','%c')",
            zrecnm, vp->id, vp->mode);
    rc = sqlite3_exec(vp->db, sqlite3_str_value(zsql), NULL,NULL,NULL);

cleanup:
    sqlite3_free(zrecnm);
    sqlite3_free(sqlite3_str_finish(zsql));

    return rc;
}


static int zvars_upd_padval(sqlite3_value **argv, CDFid id, long varnum, char **pzErr) {
    CDFstatus status = CDF_OK;
    long datatype;

    if( sqlite3_value_type(argv[12])!=SQLITE_NULL ) { /* pad value*/
        status = CDFgetzVarDataType(id, varnum, &datatype);
        switch (datatype) {
            case CDF_REAL8:
            case CDF_DOUBLE:
            case CDF_EPOCH:
                double d = sqlite3_value_double(argv[12]);
                CDFsetzVarPadValue(id, varnum, &d);
                break;
            case CDF_INT8:
                long l = sqlite3_value_int64(argv[12]);
                CDFsetzVarPadValue(id, varnum, &l);
                break;
            case CDF_INT4:
            case CDF_UINT4:
            case CDF_INT2:
            case CDF_UINT2:
            case CDF_INT1:
            case CDF_UINT1:
            case CDF_BYTE:
                int i = sqlite3_value_int(argv[12]);
                status = CDFsetzVarPadValue(id, varnum, &i);
                break;
            case CDF_CHAR:
            case CDF_UCHAR:
                status = CDFsetzVarPadValue(id, varnum, (char*) sqlite3_value_text(argv[12]));
                break;
            case CDF_REAL4:
            case CDF_FLOAT:
                float f = (float) sqlite3_value_double(argv[12]);
                status = CDFsetzVarPadValue(id, varnum, &f);
                break;
            case CDF_EPOCH16:
                if( sqlite3_value_bytes(argv[12])!=16 ) {
                    *pzErr = sqlite3_mprintf("Pad value for EPOCH16 must be 16 bytes");
                    return SQLITE_ERROR;
                }
                status = CDFsetzVarPadValue(id, varnum, (void*) sqlite3_value_blob(argv[12]));
                break;
            default:
                *pzErr = sqlite3_mprintf("Illegal CDF datatype %d for pad value %d", datatype, varnum);
                return SQLITE_ERROR;
        }
        if( status!=CDF_OK ) {
            char statustext[CDF_STATUSTEXT_LEN+1];
            CDFgetStatusText(status, statustext);
            *pzErr = sqlite3_mprintf("Cannot set pad value:\n%s", statustext);
            return SQLITE_ERROR;
        }
    }

    return SQLITE_OK;
}

static int cdfzVarsUpdate(
        sqlite3_vtab *vtabp,
        int argc,
        sqlite3_value **argv,
        sqlite_int64 *pRowid
) {
    CdfVTab *vp = (CdfVTab*) vtabp;
    CDFstatus status;
    const char *varName;
    char *zrecnm;
    long datatype;
    long numelem,numdims,*dimsizes=NULL,recvariance,*dimvars=NULL,varnum,nzvars;
    char *dimparstr,*dimpp;
    long dimsize,dimvar,k,n;
    int update=0,rc;

    if( vp->mode=='r' || vp->mode=='s' ) {
        vtabp->zErrMsg = sqlite3_mprintf("Read only, zVars are not added!");
        return SQLITE_READONLY;
    }

    switch (argc) {
        case 1:  /* delete a zVar */
            if( sqlite3_value_type(argv[0])!=SQLITE_NULL ) {
                status = CDFdeletezVar(vp->id, sqlite3_value_int64(argv[0])-1);
                if( status!=CDF_OK ) {
                    char statustext[CDF_STATUSTEXT_LEN+1];
                    CDFgetStatusText(status, statustext);
                    vtabp->zErrMsg = sqlite3_mprintf("Deleting zvarid %d failed:\n%s",
                            varnum+1, statustext);
                    return SQLITE_ERROR;
                }
            }
            /* update the zrec virtual table: */
            if( (rc = cdf_recreate_zrecs(vp))!=SQLITE_OK ) {
                printf("zVarsUpdate: recreate_zrecs returned %d\n", rc); 
                return rc;
            }
            break;
        default:  /* create or update a zVar */
            switch( sqlite3_value_type(argv[0]) ) {
                case SQLITE_NULL: /* SQLite INSERT creates a zVar: */
                    varName = sqlite3_value_text(argv[3]);

                    if( sqlite3_value_type(argv[4])==SQLITE_INTEGER || sqlite3_value_type(argv[4])==SQLITE_FLOAT ) {
                        datatype = sqlite3_value_int64(argv[4]);
                        if( datatype>CDF_UCHAR || datatype<CDF_INT1 || datatype==3 || datatype==13 ||
                                (datatype>4  && datatype<8)  || (datatype>8 && datatype<11) ||
                                (datatype>14 && datatype<21) || (datatype>22 && datatype<31) ||
                                (datatype>33 && datatype<41) || (datatype>41 && datatype<44) ||
                                (datatype>45 && datatype<51) ) {
                            vtabp->zErrMsg = sqlite3_mprintf("Invalid datatype %d for zVar %s",
                                    datatype, varName);
                            return SQLITE_ERROR;
                        }
                    }
                    else if( sqlite3_value_type(argv[4])==SQLITE_TEXT ) { /* CDF datatype */
                        datatype = cdf_typeid(sqlite3_value_text(argv[4]));
                        if( datatype==0 ) {
                            vtabp->zErrMsg = sqlite3_mprintf("Unknown typestring %s for zVar %s",
                                    sqlite3_value_text(argv[4]), varName);
                            return SQLITE_ERROR;
                        }
                    } else if( sqlite3_value_type(argv[4])==SQLITE_NULL )
                        datatype = 45;
                    else {
                        vtabp->zErrMsg = sqlite3_mprintf("Illegal BLOB for dataspec zVar %s", varName);
                        return SQLITE_ERROR;
                    }

                    if( datatype==51 || datatype==52 )
                        if( sqlite3_value_type(argv[5])==SQLITE_NULL )
                            numelem = 64;
                        else
                            numelem = sqlite3_value_int64(argv[5]);
                    else
                        numelem = 1;

                    numdims = sqlite3_value_int64(argv[6]);
                    if( numdims==0 ) {
                        dimsize  = 0;
                        dimsizes = &dimsize;
                    } else if( numdims==1 ) {   /* vector */
                        dimsize  = sqlite3_value_int64(argv[7]);
                        if( dimsize<=0 ) {
                            vtabp->zErrMsg = sqlite3_mprintf("Invalid dimsize %d!", dimsize);
                            return SQLITE_ERROR;
                        }
                        dimsizes = &dimsize;
                    } else if( numdims>1 )  { /* matrix or higher, numdims must be string */
                        dimsizes = sqlite3_malloc(numdims*sizeof(long));
                        if( sqlite3_value_type(argv[7])==SQLITE_TEXT ) {
                            dimparstr = sqlite3_malloc(sqlite3_value_bytes(argv[7]));
                            strcpy(dimparstr, sqlite3_value_text(argv[7]));
                            dimsizes[0] = strtol(dimparstr, &dimpp, 0);
                            for( k=1; k<numdims; k++ )
                                dimsizes[k] = strtol(dimpp+1, &dimpp, 0);
                            sqlite3_free(dimparstr);
                        } else {
                            vtabp->zErrMsg = sqlite3_mprintf("Invalid dimsizes of type %d!",
                                    sqlite3_value_type(argv[7]));
                            return SQLITE_ERROR;
                        }
                    } else if( numdims<0 ) {
                        vtabp->zErrMsg = sqlite3_mprintf("Invalid numdims %d!", numdims);
                        return SQLITE_ERROR;
                    }

                    if( sqlite3_value_type(argv[8])!=SQLITE_NULL ) {
                        recvariance = sqlite3_value_int64(argv[8]);
                        if( recvariance!=VARY && recvariance!=NOVARY ) {
                            vtabp->zErrMsg = sqlite3_mprintf("Invalid recvariance %d!", recvariance);
                            return SQLITE_ERROR;
                        }
                    } else recvariance = VARY;

                    if( numdims==0 ) {
                        dimvar  = VARY;
                        dimvars = &dimvar;
                    } else if( numdims==1 ) {   /* vector, default dim variance is VARY  */
                        dimvar  = sqlite3_value_type(argv[9])==SQLITE_NULL ? VARY : sqlite3_value_int64(argv[9]);
                        dimvars = &dimvar;
                    } else if( numdims>1 )  { /* matrix or higher, recvars must be string */
                        dimvars = sqlite3_malloc(numdims*sizeof(long));
                        if( sqlite3_value_type(argv[9])==SQLITE_TEXT ) {
                            dimparstr = sqlite3_malloc(sqlite3_value_bytes(argv[9]));
                            strcpy(dimparstr, sqlite3_value_text(argv[9]));
                            dimvars[0] = strtol(dimparstr, &dimpp, 0);
                            for( k=1; k<numdims; k++ )
                                dimvars[k] = strtol(dimpp+1, &dimpp, 0);
                            sqlite3_free(dimparstr);
                        } else if ( sqlite3_value_type(argv[9])==SQLITE_NULL )
                            for( k=0; k<numdims; k++ )
                                dimvars[k] = VARY;
                    } else {
                        vtabp->zErrMsg = sqlite3_mprintf("Invalid varsizes of type %d!",
                                sqlite3_value_type(argv[9]));
                        return SQLITE_ERROR;
                    }
                    if( numdims>0 )
                        for( k=0; k<numdims; k++ )
                            if( dimvars[k]!=VARY && dimvars[k]!=NOVARY ) {
                                vtabp->zErrMsg = sqlite3_mprintf("Invalid dimvariance %d!", dimvars[k]);
                                return SQLITE_ERROR;
                            }

                    if( sqlite3_value_type(argv[10])!=SQLITE_NULL ) {
                        vtabp->zErrMsg = sqlite3_mprintf("Column maxwritten is read-only!");
                        return SQLITE_ERROR;
                    }

                    status = CDFgetNumzVars(vp->id, &nzvars);
                    status = CDFcreatezVar(vp->id, varName, datatype, numelem, numdims, dimsizes,
                            recvariance, dimvars, &varnum);
                    if( status<CDF_OK ) {
                        char statustext[CDF_STATUSTEXT_LEN+1];
                        CDFgetStatusText(status, statustext);
                        vtabp->zErrMsg = sqlite3_mprintf("Creating zvar %s failed:\n%s", varName, statustext);
                        return SQLITE_ERROR;
                    } /* else
                         printf("zVarsUpdate: zvar %s created\n", varName); */
                    if( nzvars!=varnum ) {
                        vtabp->zErrMsg = sqlite3_mprintf("Creating zvar %s failed:\n var number %d should be %d\n",
                                varName, varnum, nzvars);
                        return SQLITE_ERROR;
                    }

                    if( sqlite3_value_type(argv[11])!=SQLITE_NULL ) { /* maxalloc */
                        long nalloc = sqlite3_value_int64(argv[11]);
                        status = CDFsetzVarAllocRecords(vp->id, varnum, nalloc);
                        if( status<CDF_OK ) {
                            char statustext[CDF_STATUSTEXT_LEN+1];
                            CDFgetStatusText(status, statustext);
                            vtabp->zErrMsg = sqlite3_mprintf("Allocating records for %s failed: %s",
                                    varName, statustext);
                            return SQLITE_ERROR;
                        }
                    }

                    /* pad value: */ 
                    if( zvars_upd_padval(argv, vp->id, varnum, &vtabp->zErrMsg)!=SQLITE_OK )
                        return SQLITE_ERROR;

                    if( numdims>1 ) {
                        sqlite3_free(dimsizes);
                        sqlite3_free(dimvars);
                    }
                    /* update the zrec virtual table: */
                    if( (rc = cdf_recreate_zrecs(vp))!=SQLITE_OK )
                        return rc;
                    break;
                default:  /* SQLite UPDATE can rename a zVariable, change the max allocated record or the pad value: */
                    long change = 0;
                    for( k=4; k<11; k++ ) 
                        change += 1-sqlite3_value_nochange(argv[k]);

                    if( change ) {
                        vtabp->zErrMsg = sqlite3_mprintf("zVar can be only be renamed or max allocated records or pad value can be updated");
                        return SQLITE_ERROR;
                    }
                    varnum = sqlite3_value_int64(argv[0])-1;
                    if( !sqlite3_value_nochange(argv[3]) ) {
                        varName = sqlite3_value_text(argv[3]);
                        status = CDFrenamezVar(vp->id, varnum, varName);
                        if( status<CDF_OK ) {
                            char statustext[CDF_STATUSTEXT_LEN+1];
                            CDFgetStatusText(status, statustext);
                            vtabp->zErrMsg = sqlite3_mprintf("Renaming zvarid %d failed:\n%s",
                                    varnum+1, statustext);
                            return SQLITE_ERROR;
                        }
                        /* update the zrec virtual table: */
                        if( (rc = cdf_recreate_zrecs(vp))!=SQLITE_OK )
                            return rc;
                    }
                    if( !sqlite3_value_nochange(argv[11]) ) {
                        long nalloc = sqlite3_value_int64(argv[11]);
                        status = CDFsetzVarAllocRecords(vp->id, varnum, nalloc);
                        if( status<CDF_OK ) {
                            char statustext[CDF_STATUSTEXT_LEN+1];
                            CDFgetStatusText(status, statustext);
                            vtabp->zErrMsg = sqlite3_mprintf("Allocating records for zvarid %d failed:\n%s",
                                    varnum+1, statustext);
                            return SQLITE_ERROR;
                        }
                    }

                    /* pad value: */ 
                    if( !sqlite3_value_nochange(argv[12])
                            && zvars_upd_padval(argv, vp->id, varnum, &vtabp->zErrMsg)!=SQLITE_OK )
                        return SQLITE_ERROR;
            }
    }
    return SQLITE_OK;
}

/*
** The xConnect and xCreate methods do the same thing, but they must be
** different so that the virtual table is not an eponymous virtual table.
*/
static int cdfzVarsCreate(
        sqlite3 *db,
        void *pAux,
        int argc, const char *const*argv,
        sqlite3_vtab **ppVtab,
        char **pzErr
        ){
    return cdfzVarsConnect(db, pAux, argc, argv, ppVtab, pzErr);
}

static sqlite3_module CdfzVarsModule = {
  0,                      /* iVersion */
  cdfzVarsCreate,         /* xCreate */
  cdfzVarsConnect,        /* xConnect */
  cdfzVarsBestIndex,      /* xBestIndex */
  cdfVTabDisconnect,      /* xDisconnect */
  cdfVTabDisconnect,      /* xDestroy */
  cdfVTabOpen,            /* xOpen - open a cursor */
  cdfVTabClose,           /* xClose - close a cursor */
  cdfzVarsFilter,         /* xFilter - configure scan constraints */
  cdfVTabNext,            /* xNext - advance a cursor */
  cdfzVarsEof,            /* xEof - check for end of scan */
  cdfzVarsColumn,         /* xColumn - read data */
  cdfzVarsRowid,          /* xRowid - read data */
  cdfzVarsUpdate,         /* xUpdate */
  0,                      /* xBegin */
  0,                      /* xSync */
  0,                      /* xCommit */
  0,                      /* xRollback */
  0,                      /* xFindMethod */
  0,                      /* xRename */
};

/* Module CdfRecsModule */

typedef struct CdfzVarsRecords CdfzVarsRecords;
/* typedef struct CdfVTab CdfzVarsRecords; */
struct CdfzVarsRecords {
    CdfVTab      cdfvtp;            /* Parent class.  Must be first */

    long         nzvars;            /* Nr of zVars. */
    long*        nbytes;            /* Nr of bytes (buffer size) needed to read the CDF zVar. */
    int*         sqltypes;          /* SQL type to which the CDF zVar is converted. */
    int*         valtypes;          /* Function id to convert SQLite value to CDF variable */
};

/* A cursor for the CDF zVars virtual table */
/* typedef struct CdfVTabCursor CdfzVarsRecCursor; */
typedef struct CdfzVarsRecCursor CdfzVarsRecCursor;
struct CdfzVarsRecCursor {
    sqlite3_vtab_cursor basecur;     /* Base class.  Must be first */
    CDFid               id;          /* CDF file identifier. */
    sqlite_int64        recid;       /* rowid */
    long* maxwritten;                /* Max written record number for each zvar. */
};

static int cdfzRecsConnect(
        sqlite3 *db,
        void *pAux,
        int argc, const char *const*argv,
        sqlite3_vtab **ppVtab,
        char **pzErr)
{
    CDFid            id;
    CDFstatus        status;
    char             mode,varName[CDF_VAR_NAME_LEN256+4],*z;
    sqlite3_str     *zsql = sqlite3_str_new(db);
    CdfzVarsRecords *vtabp = 0;
    long             k,kzvar,nzvars;
    long             cdftype,sqlitetype,numdims,kdim,nelem;
    long             dimsizes[CDF_MAX_DIMS],*nbytes;
    int             *sqltypes,*valtypes;
    int              rc;

    rc = cdf_prep_idmode(argc, argv, pzErr, &id, &mode);
    if( rc!=SQLITE_OK ) return rc;

    sqlite3_str_appendf(zsql, "CREATE TABLE cdf_recs_ignored (\n");
    sqlite3_str_appendf(zsql, "    Id INTEGER PRIMARY KEY NOT NULL");
    
    status = CDFgetNumzVars(id, &nzvars);
    if( status!=CDF_OK ) {
        char statustext[CDF_STATUSTEXT_LEN+1];
        CDFgetStatusText(status, statustext);
        *pzErr = sqlite3_mprintf("CDFgetNumzVars failed,\n%s", statustext);
        return SQLITE_ERROR;
    }
    nbytes   = sqlite3_malloc64(nzvars*sizeof(long));
    sqltypes = sqlite3_malloc64(nzvars*sizeof(int));
    valtypes = sqlite3_malloc64(nzvars*sizeof(int));

    for( kzvar=0; kzvar<nzvars; kzvar++ ) {
        for( k=0; k<CDF_VAR_NAME_LEN256+4; k++ )
            varName[k] = '\0';

        status = CDFgetzVarName(id, kzvar, varName);
        status = CDFgetzVarDataType(id, kzvar, &cdftype);
        status = CDFgetzVarNumDims(id, kzvar, &numdims);

        sqlite3_str_appendf(zsql, ",\n");
        if( numdims==0 ) {
            sqlitetype = cdf_sqlitetype(cdftype);
            sqltypes[kzvar] = sqlitetype;
            sqlite3_str_appendf(zsql, "    \"%s\" %s", varName, typetext[sqlitetype]);
            nbytes[kzvar] = cdf_elsize(cdftype);
        } else {
            status = CDFgetzVarDimSizes (id, kzvar, dimsizes);
            nelem = 1;
            for( kdim=0; kdim<numdims; kdim++ )
                nelem *= dimsizes[kdim];

            sqlite3_str_appendf(zsql, "    \"%s\" BLOB", varName);
            sqltypes[kzvar] = SQLITE_BLOB;
            nbytes[kzvar]   = cdf_elsize(cdftype)*nelem;
        }
        valtypes[kzvar] = cdf_valfuncid(cdftype);
    }
    sqlite3_str_appendf(zsql, "\n);");

    z = sqlite3_str_value(zsql);
    rc = sqlite3_declare_vtab(db, z);
    if( rc!=SQLITE_OK ) {
        *pzErr = sqlite3_mprintf("Bad schema \n%s\nerror code: %d\n", z, rc);
        return SQLITE_ERROR;
    }
    sqlite3_free(sqlite3_str_finish(zsql));

    vtabp = sqlite3_malloc( sizeof(*vtabp) );
    if( vtabp==0 ) return SQLITE_NOMEM;
    *ppVtab = (sqlite3_vtab*) vtabp;
    memset(vtabp, 0, sizeof(*vtabp));

    vtabp->cdfvtp.id   = id;
    vtabp->cdfvtp.mode = mode;
    vtabp->cdfvtp.db   = db;
    vtabp->cdfvtp.name = sqlite3_malloc( strlen(argv[2])+1 );
    stpcpy(vtabp->cdfvtp.name, argv[2]);
    vtabp->nzvars = nzvars;
    vtabp->sqltypes = sqltypes;
    vtabp->valtypes = valtypes;
    vtabp->nbytes   = nbytes;

    return rc;
}
/*
** This method is the destructor of a CdfzVarsRecords object.
*/
static int cdfzRecsDisconnect(sqlite3_vtab *pvtab){
    CdfzVarsRecords* p = (CdfzVarsRecords*) pvtab;
    sqlite3_free(p->valtypes);
    sqlite3_free(p->sqltypes);
    sqlite3_free(p->nbytes);

    return cdfVTabDisconnect(pvtab);
}

/*
** Only a forward full table scan is supported.
** A binary search could be done if the MONOTON attribute is set, to be implemented. 
*/
static int cdfzRecsBestIndex(
        sqlite3_vtab *vtabp,
        sqlite3_index_info *idxinfop
){
    CdfzVarsRecords *vp = (CdfzVarsRecords*) vtabp;
    CDFstatus status;
    long kzvar;
    long maxrec;

    if( idxinfop->nConstraint>0 )
        kzvar = idxinfop->aConstraint[0].iColumn;
    else
        kzvar = 0;
    status = CDFgetzVarMaxWrittenRecNum(vp->cdfvtp.id, kzvar, &maxrec);
    idxinfop->idxNum = 1;
    idxinfop->idxStr = "";
    idxinfop->estimatedCost = maxrec;
    return SQLITE_OK;
}

/*
** Constructor for a new CdfzVarsRecords cursor object.
*/
static int cdfzRecsOpen(
        sqlite3_vtab* vtabp,
        sqlite3_vtab_cursor** ppcur
){
    long nzvars;
    CDFstatus status;
    CdfzVarsRecords   *vp = (CdfzVarsRecords*) vtabp;
    CdfzVarsRecCursor *cp = sqlite3_malloc64(sizeof(CdfzVarsRecCursor));
    if( cp==0 ) return SQLITE_NOMEM;

    cp->id    = vp->cdfvtp.id;
    cp->recid = 1;

    status = CDFgetNumzVars(vp->cdfvtp.id, &nzvars);
    cp->maxwritten = sqlite3_malloc(nzvars*sizeof(long));
    for( int k=0; k<nzvars; k++ )
        status = CDFgetzVarMaxWrittenRecNum(vp->cdfvtp.id, k, &(cp->maxwritten[k]));

    *ppcur = (sqlite3_vtab_cursor*) cp;
    return SQLITE_OK;
}

/*
** Destructor for a CdfzVarCursor.
*/
static int cdfzRecsClose(sqlite3_vtab_cursor *curp)
{
    CdfzVarsRecCursor *cp = (CdfzVarsRecCursor*) curp; 
    sqlite3_free(cp->maxwritten);
    sqlite3_free(cp);

    return SQLITE_OK;
}

/*
** xFilter simply rewinds to the beginning.
** A binary search could be done if the MONOTON attribute is set, still needs to be implemented. 
*/
static int cdfzRecsFilter(
        sqlite3_vtab_cursor *cp, 
        int idxNum, const char *idxStr,
        int argc, sqlite3_value **argv
){
    ((CdfzVarsRecCursor*) cp)->recid = 1;
    return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved beyond the maximum written record nr across all zVars
*/
static int cdfzRecsEof(sqlite3_vtab_cursor *curp){
    CdfzVarsRecCursor *cp = (CdfzVarsRecCursor*) curp;
    CDFstatus status;
    long zvarsmaxw;

    status = CDFgetzVarsMaxWrittenRecNum(cp->id, &zvarsmaxw);

    return cp->recid > zvarsmaxw+1;
}

static CDFstatus result_cdfint(sqlite3_context *ctx, CDFid id, long lCol, long recid, long) {
    sqlite_int64 ibuf;
    CDFstatus status = CDFgetzVarRecordData(id, lCol-1, recid-1, &ibuf);
    sqlite3_result_int64(ctx, ibuf);
    
    return status;
}

static CDFstatus result_cdfdouble(sqlite3_context *ctx, CDFid id, long lCol, long recid, long) {
    double dbuf;
    CDFstatus status = CDFgetzVarRecordData(id, lCol-1, recid-1, &dbuf);
    sqlite3_result_double(ctx, dbuf);

    return status;
}

static CDFstatus result_cdftext(sqlite3_context *ctx, CDFid id, long lCol, long recid, long) {
    char *buf;
    long  n=0;
    CDFstatus status = CDFgetzVarNumElements(id, lCol-1, &n);
    buf    = sqlite3_malloc64(n+1);
    memset(buf, 0, n+1);
    status = CDFgetzVarRecordData(id, lCol-1, recid-1, buf);
    /* sqlite3_result_text64(ctx, buf, n, sqlite3_free, SQLITE_UTF8); This does not seem to work with CDF */
    sqlite3_result_text(ctx, buf, -1, sqlite3_free);

    return status;
}

static CDFstatus result_cdfblob(sqlite3_context *ctx, CDFid id, long lCol, long recid, long nbytes) {
    char buf8[8] = "";
    char *buf;
    CDFstatus status;

    if( nbytes<=8 ) {
        status = CDFgetzVarRecordData(id, lCol-1, recid-1, buf8);
        sqlite3_result_blob64(ctx, buf8, nbytes, SQLITE_TRANSIENT);
    } else {
        buf    = sqlite3_malloc64(nbytes);
        status = CDFgetzVarRecordData(id, lCol-1, recid-1, buf);
        sqlite3_result_blob64(ctx, buf, nbytes, sqlite3_free);
    }

    return status;
}

/*
** Return values of columns for the row at which the CdfRecordsCursor
** is currently pointing.
*/
static int cdfzRecsColumn(
        sqlite3_vtab_cursor *curp,  /* The cursor */
        sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
        int iCol
){
    CdfzVarsRecCursor *cp = (CdfzVarsRecCursor*) curp;
    CdfzVarsRecords   *vp = (CdfzVarsRecords*) curp->pVtab;
    CDFstatus status = CDF_OK;
    CDFid id;
    int sqltype;

    sqlite_int64 n;
    char* buf;

    static CDFstatus (*res[4])(sqlite3_context*, CDFid, long, long, long) = {
        result_cdfint, result_cdfdouble, result_cdftext, result_cdfblob};

    char **pzErr = &cp->basecur.pVtab->zErrMsg;

    /* return immediately if called by cdfUpdate and the column is unchanged*/
    if( sqlite3_vtab_nochange(ctx) )
        return SQLITE_OK;

    if( iCol==0 )
        sqlite3_result_int64(ctx, cp->recid);
    else if( iCol>0 && iCol<=vp->nzvars) { 
        id      = cp->id;
        sqltype = vp->sqltypes[iCol-1];
        status  = (*res[sqltype-1])(ctx, id, (long) iCol, cp->recid, vp->nbytes[iCol-1]);
        if( status<CDF_OK ) {
            char statustext[CDF_STATUSTEXT_LEN+1];
            CDFgetStatusText(status, statustext);
            *pzErr = sqlite3_mprintf("When retrieving zVar %d: %s", iCol, statustext);
            return SQLITE_ERROR;
        }
    } else {
        *pzErr = sqlite3_mprintf("iCol %d not a valid column number", iCol);
        return SQLITE_ERROR;
    }

    return SQLITE_OK;
}

static CDFstatus value_int64_2cdf(sqlite3_value *val, CDFid id, long kz, long kr, char**)
{
    long icolval = sqlite3_value_int64(val);
    return CDFputzVarRecordData(id, kz, kr, &icolval);
}

static CDFstatus value_double_2cdf(sqlite3_value *val, CDFid id, long kz, long kr, char**)
{
    double dcolval = sqlite3_value_double(val);
    return CDFputzVarRecordData(id, kz, kr, &dcolval);
}

static CDFstatus value_text_2cdf(sqlite3_value *val, CDFid id, long kz, long kr, char**)
{
    return CDFputzVarRecordData(id, kz, kr, (char*) sqlite3_value_text(val));
}

static CDFstatus value_float_2cdf(sqlite3_value *val, CDFid id, long kz, long kr, char **pzErr)
{
    int sqltype = sqlite3_value_type(val);
    if( sqltype==SQLITE_BLOB ) {
        if( sqlite3_value_bytes(val)!=4 ) {
            *pzErr = sqlite3_mprintf("insert of binary FLOAT needs a 4 octets long BLOB");
            return SQLITE_ERROR;
        }
        return CDFputzVarRecordData(id, kz, kr, (void*) sqlite3_value_blob(val));
    } else {
        float fcolval = (float) sqlite3_value_double(val);
        return CDFputzVarRecordData(id, kz, kr, &fcolval);
    }
}

static CDFstatus value_epoch_2cdf(sqlite3_value *val, CDFid id, long kz, long kr, char **pzErr)
{
    if( sqlite3_value_type(val)!=SQLITE_BLOB || sqlite3_value_bytes(val)!=16 ) {
            *pzErr = sqlite3_mprintf("insert of CDF_EPOCH16 needs a 16 octets long BLOB");
            return SQLITE_ERROR;
    }

    return CDFputzVarRecordData(id, kz, kr, (void*) sqlite3_value_blob(val));
}

static CDFstatus (*valfunc[5])(sqlite3_value*, CDFid, long, long, char**) = {
    value_int64_2cdf, value_double_2cdf, value_text_2cdf, value_float_2cdf, value_epoch_2cdf
};

static int cdfzRecsUpdate(sqlite3_vtab *vtabp, int argc, sqlite3_value **argv, sqlite_int64 *rowid ) {
    CdfzVarsRecords *vp = (CdfzVarsRecords*) vtabp;
    char **pzErr = &vp->cdfvtp.base.zErrMsg;
    CDFid id = vp->cdfvtp.id;
    CDFstatus status = CDF_OK;
    int   sqltype,valtype;
    long  nzvars,cdftype,numdims,kdim,nelem,kcdfrec,maxcdfrec;
    long  dimsizes[CDF_MAX_DIMS];
    sqlite_int64 icolval;
    void *bp;

    if( strchr("rs", vp->cdfvtp.mode)!=NULL ) {
        *pzErr = sqlite3_mprintf("Read only, records are not added/updated/deleted!");
        return SQLITE_READONLY;
    }

    status = CDFgetNumzVars(id, &nzvars);
    if( status!=CDF_OK ) {
        char statustext[CDF_STATUSTEXT_LEN+1];
        CDFgetStatusText(status, statustext);
        *pzErr = sqlite3_mprintf("Getting num of zVars failed:\n%s", statustext);
        return SQLITE_ERROR;
    }

    switch (argc) {
        case 1:  /* delete a record */
            kcdfrec = sqlite3_value_int64(argv[0])-1;
            for( long kzvar=0; kzvar<nzvars; kzvar++ ) {
                status = CDFdeletezVarRecords(id, kzvar, kcdfrec, kcdfrec);
                if( status!=CDF_OK ) {
                    char statustext[CDF_STATUSTEXT_LEN+1];
                    CDFgetStatusText(status, statustext);
                    *pzErr = sqlite3_mprintf("Deleting zVar failed:\n%s", statustext);
                    return SQLITE_ERROR;
                }
            }
            break;
        default:  /* insert or replace or update */
            /* if( sqlite3_value_type(argv[1])!=SQLITE_NULL ) { /* shouldn't because of WITHOUT ROWID */ /*
             *    *pzErr= sqlite3_mprintf("WITHOUT ROWID, argv[1] is supposed to be NULL");
             *    return SQLITE_ERROR;
             *}
             */

            /* status = CDFgetNumzVars(vp->id, &nzvars);
             * if( status!=CDF_OK ) {
             *     *pzErr = sqlite3_mprintf("'%d' was returned when gettin nr of CDF zVars!", status);
             *     return SQLITE_ERROR;
             * }
             */
            /* if( argc-2!=nzvars+1 ) { */
            if( argc-2!=nzvars+1 ) {
                *pzErr = sqlite3_mprintf("Nr of columns %d is not equal nr of zVars %d+1!",
                                         argc-2, nzvars+1);
                return SQLITE_ERROR;
            }

            if( sqlite3_value_type(argv[0])!=SQLITE_NULL ) { /* UPDATE .. SET .. WHERE rec ... ; */
                kcdfrec = sqlite3_value_int64(argv[0])-1;
            } else { /* insert a new record */
                status = CDFgetzVarsMaxWrittenRecNum(id, &maxcdfrec);
                if( status!=CDF_OK ) {
                    char statustext[CDF_STATUSTEXT_LEN+1];
                    CDFgetStatusText(status, statustext);
                    *pzErr = sqlite3_mprintf("CDFgetzVarsMaxWrittenRecNum failed:\n%s", statustext);
                    return SQLITE_ERROR;
                }
                if( sqlite3_value_type(argv[2])!=SQLITE_NULL ) /* INSERT at specific record number*/
                    kcdfrec = sqlite3_value_int64(argv[2]);
                else 
                    kcdfrec = maxcdfrec+1;
                *rowid = kcdfrec;
            }
            /*
             *pzErr = sqlite3_mprintf("cdfUpdate: maxcdfrec,kcdfrec='%d','%d'", maxcdfrec, kcdfrec);
             return SQLITE_ERROR;
             */

            for( long kzvar=0; kzvar<nzvars; kzvar++ ) {
                char varName[CDF_VAR_NAME_LEN256+16];
                status = CDFgetzVarName(id, kzvar, varName);
                status = CDFgetzVarDataType(id, kzvar, &cdftype);

                /* SQLITE_NULL is not inserted into CDF */
                if( sqlite3_value_nochange(argv[kzvar+3])
                        || sqlite3_value_type(argv[kzvar+3])==SQLITE_NULL )
                    continue;

                status = CDFgetzVarDataType(id, kzvar, &cdftype);
                /* printf("kzvar = %d, cdftype = %d, double = %f\n", 
                   kzvar, cdftype, sqlite3_value_double(argv[kzvar+3]));
                   */
                status = CDFgetzVarNumDims(id, kzvar, &numdims);

                if( numdims==0 ) {
                    /* valtype = cdf_valfuncid(cdftype); */
                    valtype = vp->valtypes[kzvar];
                    if( valtype<0 ) {
                        *pzErr = sqlite3_mprintf("unknown CDF type '%d' !", cdftype);
                        return SQLITE_ERROR;
                    }
                    status = valfunc[valtype](argv[kzvar+3], id, kzvar, kcdfrec, pzErr);
                    if( status!=CDF_OK ) {
                        if( status!=SQLITE_ERROR ) {
                            char statustext[CDF_STATUSTEXT_LEN+1];
                            CDFgetStatusText(status, statustext);
                            *pzErr = sqlite3_mprintf("CDFputzVarRecordData failed:\n%s", statustext);
                        }
                        return SQLITE_ERROR;
                    }
                } else { /* insert a blob as multi-dimensional variable */
                    status = CDFgetzVarNumDims(id, kzvar, &numdims);
                    status = CDFgetzVarDimSizes(id, kzvar, dimsizes);
                    nelem = 1;
                    for( kdim=0; kdim<numdims; kdim++ )
                        nelem *= dimsizes[kdim];

                    if( sqlite3_value_type(argv[kzvar+3])==SQLITE_NULL ) { /* Insert a zero byte vector */ 
                        bp = sqlite3_malloc(nelem);
                        if( !bp )
                            return SQLITE_NOMEM;
                        memset(bp, 0, nelem);
                    } else {
                        if( cdf_elsize(cdftype)*nelem!=sqlite3_value_bytes(argv[kzvar+3]) ) {
                            *pzErr = sqlite3_mprintf("BLOB size '%d' does not match CDF dims");
                            return SQLITE_ERROR;
                        }
                        bp = (void*) sqlite3_value_blob(argv[kzvar+3]);
                    }
                    status = CDFputzVarRecordData(id, kzvar, kcdfrec, bp);
                    if( sqlite3_value_type(argv[kzvar+3])==SQLITE_NULL )
                        sqlite3_free(bp);
                    if( status!=CDF_OK ) {
                        char statustext[CDF_STATUSTEXT_LEN+1];
                        CDFgetStatusText(status, statustext);
                        *pzErr = sqlite3_mprintf("CDFputzVarRecordData failed:\n%s", statustext);
                        return SQLITE_ERROR;
                    }
                }
            }
    } /* end switch (argc) */

    return SQLITE_OK;
}

/*
** The xConnect and xCreate methods do the same thing, but they must be
** different so that the virtual table is not an eponymous virtual table.
*/
static int cdfzRecsCreate(
        sqlite3 *db,
        void *pAux,
        int argc, const char *const*argv,
        sqlite3_vtab **ppVtab,
        char **pzErr)
{
    return cdfzRecsConnect(db, pAux, argc, argv, ppVtab, pzErr);
}

static sqlite3_module CdfRecsModule = {
  0,                      /* iVersion */
  cdfzRecsCreate,         /* xCreate */
  cdfzRecsConnect,        /* xConnect */
  cdfzRecsBestIndex,      /* xBestIndex */
  cdfzRecsDisconnect,     /* xDisconnect */
  cdfzRecsDisconnect,     /* xDestroy */
  cdfzRecsOpen,           /* xOpen - open a cursor */
  cdfzRecsClose,          /* xClose - close a cursor */
  cdfzRecsFilter,         /* xFilter - configure scan constraints */
  cdfVTabNext,            /* xNext - advance a cursor */
  cdfzRecsEof,            /* xEof - check for end of scan */
  cdfzRecsColumn,         /* xColumn - read data */
  cdfVTabRowid,           /* xRowid - read data */
  cdfzRecsUpdate,         /* xUpdate - insert, update, delete CDF records */
  0,                      /* xBegin */
  0,                      /* xSync */
  0,                      /* xCommit */
  0,                      /* xRollback */
  0,                      /* xFindMethod */
  0,                      /* xRename */
};

/* Module CdfAttr */

typedef struct CdfVTab CdfAttrTable;
/* struct CdfAttrTable { */
/*   sqlite3_vtab sqlite_vtab;       /* Base class.  Must be first */
/*   CDFid        id;                /* CDF file identifier. */
/*   char         mode;              /* 'r' read only, 'w' add zVars, 'c' create file */
/* }; */
typedef struct CdfVTabCursor CdfAttrCursor;

static int cdfAttrsConnect(
        sqlite3 *db,
        void *pAux,
        int argc, const char *const*argv,
        sqlite3_vtab **ppVtab,
        char **pzErr)
{
    CDFid          id;
    char           mode;
    sqlite3_str   *zsql = sqlite3_str_new(db);
    int            rc;

    rc = cdf_prep_idmode(argc, argv, pzErr, &id, &mode);
    if( rc!=SQLITE_OK ) return rc;

    sqlite3_str_appendf(zsql, "CREATE TABLE cdf_gattrs_ignored (\n");
    sqlite3_str_appendf(zsql, "    Id INTEGER PRIMARY KEY NOT NULL,\n");
    sqlite3_str_appendf(zsql, "    Name TEXT NOT NULL,\n");
    sqlite3_str_appendf(zsql, "    Scope INTEGER NOT NULL\n");
    sqlite3_str_appendf(zsql, ");\n");

    return cdf_createvtab(db, zsql, id, mode, argv[2], pzErr, ppVtab);
}

static int cdfAttrsCreate(
        sqlite3 *db,
        void *pAux,
        int argc, const char *const*argv,
        sqlite3_vtab **ppVtab,
        char **pzErr
        ){
    return cdfAttrsConnect(db, pAux, argc, argv, ppVtab, pzErr);
}
/*
** Only a forward full table scan is supported.
*/
static int cdfAttrsBestIndex(
        sqlite3_vtab *vtabp,
        sqlite3_index_info *idxinfop
){
    CdfAttrTable *vp = (CdfAttrTable*) vtabp;
    CDFstatus status;
    long nattr;

    idxinfop->idxNum = 1;
    idxinfop->idxStr = "";
    status = CDFgetNumAttributes(vp->id, &nattr);
    idxinfop->estimatedCost = nattr;

    return SQLITE_OK;
}

static int cdfAttrsEof(sqlite3_vtab_cursor *curp){
    CdfAttrCursor *cp = (CdfAttrCursor*) curp;
    long nattrs=0;
    CDFstatus status = CDFgetNumAttributes(cp->id, &nattrs);
    return cp->rowid > nattrs;
}

static CDFstatus result_attrname(sqlite3_context *ctx, CDFid id, long rowid) {
    char attrName[CDF_ATTR_NAME_LEN256];
    CDFstatus status = CDFgetAttrName(id, rowid-1, attrName);
    if( status>=CDF_OK )
        sqlite3_result_text(ctx, attrName, -1, SQLITE_TRANSIENT);
    return status;
}
static CDFstatus result_scope(sqlite3_context *ctx, CDFid id, long rowid) {
    long scope=0;
    CDFstatus status = CDFgetAttrScope(id, rowid-1, &scope);
    if( status>=CDF_OK )
        sqlite3_result_int64(ctx, scope);
    return status;
}

static int cdfAttrsColumn(
        sqlite3_vtab_cursor *cp,    /* The cursor */
        sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
        int iCol                    /* Which column to return */
){
    CdfAttrCursor *curp = (CdfAttrCursor*) cp;
    CDFstatus status;

    static CDFstatus (*res[3])(sqlite3_context*, CDFid, long) = {
        result_zvarid, result_attrname, result_scope};

    status = (*res[iCol])(ctx, curp->id, curp->rowid);
    if( status!=CDF_OK )
        return SQLITE_ERROR;
    else
        return SQLITE_OK;
}

static int cdfAttrsUpdate(
        sqlite3_vtab *vtabp,
        int argc,
        sqlite3_value **argv,
        sqlite_int64 *pRowid
) {
    CdfAttrTable *vp = (CdfAttrTable*) vtabp;
    CDFstatus status;
    const char *attrName;
    long attrnum,scope;
    char cscope='x';

    if( vp->mode=='r' ) {
        vtabp->zErrMsg = sqlite3_mprintf("Read only, Attributes are not modified");
        return SQLITE_ERROR;
    }

    if( argc==1 ) {
        if( sqlite3_value_type(argv[0])!=SQLITE_NULL ) {
            attrnum = sqlite3_value_int64(argv[0])-1;
            status = CDFdeleteAttr(vp->id, attrnum);
            if( status!=CDF_OK ) {
                vtabp->zErrMsg = sqlite3_mprintf("When deleting attribute %d, libcdf returned '%d'!",
                                                 attrnum, status);
                return SQLITE_ERROR;
            }
        }
    } else {
        if( sqlite3_value_type(argv[0])==SQLITE_NULL ) {
            if( sqlite3_value_type(argv[1])!=SQLITE_NULL ) {
                vtabp->zErrMsg = sqlite3_mprintf("argv[1] is supposed to be NULL");
                return SQLITE_ERROR;
            }
            attrName = sqlite3_value_text(argv[3]);

            if( sqlite3_value_type(argv[4])==SQLITE_INTEGER ) {
                scope = sqlite3_value_int64(argv[4]);
                cscope = scope ? 'v' : 'g';
            }
            else if( sqlite3_value_type(argv[4])==SQLITE_TEXT )
                cscope = sqlite3_value_text(argv[4])[0];
            
            if( cscope!='g' && cscope!='v' )
                vtabp->zErrMsg = sqlite3_mprintf("Invalid scope for attribute '%s'", attrName);

            status = CDFcreateAttr(vp->id, sqlite3_value_text(argv[3]), (cscope=='v')+1, &attrnum);
            if( status!=CDF_OK ) {
                char statustext[CDF_STATUSTEXT_LEN+1];
                CDFgetStatusText(status, statustext);
                vtabp->zErrMsg = sqlite3_mprintf("Cannot createAttr %s, returned %d\n%s\n",
                        sqlite3_value_text(argv[3]), status), statustext;
                return SQLITE_ERROR;
            }
        } else {
            vtabp->zErrMsg = sqlite3_mprintf("UPDATE of attributes is not supported, 1st delete then insert");
            return SQLITE_ERROR;
        }
    }
}

static sqlite3_module CdfAttrsModule = {
  0,                       /* iVersion */
  cdfAttrsCreate,          /* xCreate */
  cdfAttrsConnect,         /* xConnect */
  cdfAttrsBestIndex,       /* xBestIndex */
  cdfVTabDisconnect,       /* xDisconnect */
  cdfVTabDisconnect,       /* xDestroy */
  cdfVTabOpen,             /* xOpen - open a cursor */
  cdfVTabClose,            /* xClose - close a cursor */
  cdfVTabFilter,           /* xFilter - configure scan constraints */
  cdfVTabNext,             /* xNext - advance a cursor */
  cdfAttrsEof,             /* xEof - check for end of scan */
  cdfAttrsColumn,          /* xColumn - read data */
  cdfVTabRowid,            /* xRowid - read data */
  cdfAttrsUpdate,          /* xUpdate */
  0,                       /* xBegin */
  0,                       /* xSync */
  0,                       /* xCommit */
  0,                       /* xRollback */
  0,                       /* xFindMethod */
  0,                       /* xRename */
};
/* End of Module CdfAttr */

/* Module CdfAttrgEntries */

typedef struct CdfAttrEntries CdfAttrEntries;
struct CdfAttrEntries {
    CdfVTab      cdfvtp;            /* Parent class.  Must be first */
    long         nattrs;           /* Nr of global attributes */
};

/* The cursor for the CDF gAttributeentries needs to iterate over attribute and entry nrs: */

typedef struct CdfAttrEntriesCursor CdfAttrEntriesCursor;
struct CdfAttrEntriesCursor {
    sqlite3_vtab_cursor basecur;     /* Base class.  Must be first */
    CDFid               id;          /* CDF file identifier. */
    sqlite_int64        rowid;       /* Effective rowid */
    sqlite_int64        attrid;      /* Attribute nr */
    sqlite_int64        entryid;     /* Entry nr */
};

static int cdfAttrgEntriesConnect(
        sqlite3 *db,
        void *pAux,
        int argc, const char *const*argv,
        sqlite3_vtab **ppVtab,
        char **pzErr)
{
    CDFid            id;
    char             mode,*z;
    CDFstatus        status;
    sqlite3_str     *zsql = sqlite3_str_new(db);
    CdfAttrEntries *vtabp = 0;
    int              rc;

    rc = cdf_prep_idmode(argc, argv, pzErr, &id, &mode);
    if( rc!=SQLITE_OK ) return rc;

    sqlite3_str_appendf(zsql, "CREATE TABLE cdf_attr_gentries_ignored (\n");
    sqlite3_str_appendf(zsql, "    Attrid INTEGER NOT NULL,\n");
    sqlite3_str_appendf(zsql, "    Name TEXT NOT NULL,\n");
    sqlite3_str_appendf(zsql, "    Entryid INTEGER NOT NULL,\n");
    sqlite3_str_appendf(zsql, "    Dataspec NOT NULL,\n");         /* CDF data type, id or name, of the entry */
    sqlite3_str_appendf(zsql, "    Nelems INTEGER,\n");
    sqlite3_str_appendf(zsql, "    Value,\n");
    sqlite3_str_appendf(zsql, "    PRIMARY KEY(Attrid,Entryid)\n");
    sqlite3_str_appendf(zsql, ");\n");

    rc = sqlite3_declare_vtab(db, sqlite3_str_value(zsql));
    if( rc!=SQLITE_OK ) {
        *pzErr = sqlite3_mprintf("Invalid schema \n%s\nerror code: %d\n", sqlite3_str_value(zsql), rc);
        goto cleanup;
    }

    vtabp = sqlite3_malloc( sizeof(*vtabp) );
    if( vtabp==0 ) { 
        rc = SQLITE_NOMEM;
        goto cleanup;
    }
    *ppVtab = (sqlite3_vtab*) vtabp;
    memset(vtabp, 0, sizeof(*vtabp));

    vtabp->cdfvtp.id   = id;
    vtabp->cdfvtp.mode = mode;
    vtabp->cdfvtp.db   = db;
    vtabp->cdfvtp.name = sqlite3_malloc( strlen(argv[2])+1 );
    stpcpy(vtabp->cdfvtp.name, argv[2]);

    status = CDFgetNumgAttributes(id, &(vtabp->nattrs));

cleanup:
    sqlite3_free(sqlite3_str_finish(zsql));
    return rc;
}

static int cdfAttrgEntriesCreate(
        sqlite3 *db,
        void *pAux,
        int argc, const char *const*argv,
        sqlite3_vtab **ppVtab,
        char **pzErr
        ){
    return cdfAttrgEntriesConnect(db, pAux, argc, argv, ppVtab, pzErr);
}
/*
** Only a forward full table scan is supported.
*/
static int cdfAttrgEntriesBestIndex(
        sqlite3_vtab *vtabp,
        sqlite3_index_info *idxinfop
){
    CdfAttrEntries *vp = (CdfAttrEntries*) vtabp;
    CDFid id = vp->cdfvtp.id;
    CDFstatus status;
    long nattrs,nentries,scope,cost=0,k;

    idxinfop->idxNum = 1;
    idxinfop->idxStr = "";
    status = CDFgetNumgAttributes(id, &nattrs);
    for( k=0; k<nattrs; k++ ) {
        CDFstatus status = CDFgetAttrScope(id, k, &scope);
        if( scope==GLOBAL_SCOPE ) {
            status = CDFgetNumAttrgEntries(id, k, &nentries);
            cost += nentries;
        }
    }

    idxinfop->estimatedCost = cost;

    return SQLITE_OK;
}

static int cdfAttrEntriesOpen(
        sqlite3_vtab* vtabp,
        sqlite3_vtab_cursor** ppcur)
{
    CdfAttrEntriesCursor *curp = sqlite3_malloc64(sizeof(CdfAttrEntriesCursor));
    if( curp==0 ) return SQLITE_NOMEM;

    curp->id    = ((CdfVTab*) vtabp)->id;
    curp->rowid = 1;
    curp->attrid = 1;
    curp->entryid = 1;

    *ppcur = (sqlite3_vtab_cursor*) curp;
    return SQLITE_OK;
}
static int cdfAttrEntriesClose(sqlite3_vtab_cursor *cp){
    sqlite3_free((CdfAttrEntriesCursor*) cp);

    return SQLITE_OK;
}

static int cdfAttrgEntriesFilter(
        sqlite3_vtab_cursor *curp, 
        int idxNum, const char *idxStr,
        int argc, sqlite3_value **argv
){
    CdfAttrEntriesCursor *cp = (CdfAttrEntriesCursor*) curp;
    char **pzErr = &cp->basecur.pVtab->zErrMsg;
    long      nattrs=0,maxentry=-1,nentries=0,scope=0,nelems=0;
    CDFstatus status;
    
    /* Make sure to start at the beginning: */

    cp->rowid   = 1;
    cp->attrid  = 1;
    cp->entryid = 1;

    status = CDFgetNumAttributes(cp->id, &nattrs);
    /* Loop over attributes until global scope and at least one entry: */
    while( cp->attrid<=nattrs ) {
        status = CDFgetAttrScope(cp->id, cp->attrid-1, &scope); 
        status = CDFgetNumAttrgEntries(cp->id, cp->attrid-1, &nentries);
        status = CDFgetAttrMaxgEntry(cp->id, cp->attrid-1, &maxentry);
        if( scope==GLOBAL_SCOPE && nentries>0 ) {
            while( cp->entryid-1<=maxentry ) { /* Omit empty entries, at least one non-empty should exist */
                status = CDFgetAttrgEntryNumElements(cp->id, cp->attrid-1, cp->entryid-1, &nelems);
                if( nelems>0 )
                    break;
                cp->entryid++;
            }  
            if( cp->entryid-1>maxentry ) {
                *pzErr = sqlite3_mprintf("AttrgEntriedFilter: No entry with nelems>0 found in %d entries??", nentries);
                return SQLITE_ERROR;
            }
            break;
        }
        cp->attrid++;
        cp->entryid = 1;
    }

    return SQLITE_OK;
}

static int cdfAttrgEntriesNext(sqlite3_vtab_cursor *curp) {
    CdfAttrEntriesCursor *cp = (CdfAttrEntriesCursor*) curp;
    char **pzErr = &cp->basecur.pVtab->zErrMsg;
    CDFstatus status;
    long      nattrs=0,scope=0,nentries=0,maxentry=-1,nelems=0;

    cp->rowid++;
    cp->entryid++;
    status = CDFgetNumAttrgEntries(cp->id, cp->attrid-1, &nentries);
    status = CDFgetAttrMaxgEntry(cp->id, cp->attrid-1, &maxentry);
    while( cp->entryid<=nentries ) { /* Omit empty entries */
        status = CDFgetAttrgEntryNumElements(cp->id, cp->attrid-1, cp->entryid-1, &nelems);
        if( nelems>0 )
            break;
        cp->entryid++;
    }
    /* Move to the next attribute if there are no more entries */ 
    if( cp->entryid-1>maxentry ) {
        cp->entryid = 1;
        status = CDFgetNumAttributes(cp->id, &nattrs);
        cp->attrid++;
        while( cp->attrid<=nattrs ) { /* Increase the attr. num. until variable scope and non-empty entry */
            status = CDFgetAttrScope(cp->id, cp->attrid-1, &scope);
            if( scope==GLOBAL_SCOPE ) {
                status = CDFgetNumAttrgEntries(cp->id, cp->attrid-1, &nentries);
                if( nentries>0 )
                    break;
            }
            cp->attrid++;
        }
        if( cp->attrid>nattrs ) { /* no more attibutes */
            *pzErr = sqlite3_mprintf("no more attributes??\n");
            return SQLITE_OK;
        }
        char attrname[CDF_ATTR_NAME_LEN256];
        memset(attrname, '\0', CDF_ATTR_NAME_LEN256);
        status = CDFgetAttrName(cp->id, cp->attrid-1, attrname);
        status = CDFgetAttrMaxgEntry(cp->id, cp->attrid-1, &maxentry);
        while( cp->entryid-1<=maxentry ) { /* Omit empty entries, at least one non-empty should exist */
            status = CDFgetAttrgEntryNumElements(cp->id, cp->attrid-1, cp->entryid-1, &nelems);
            if( nelems>0 )
                break;
            cp->entryid++;
        }  
        if( cp->entryid-1>maxentry ) {
            *pzErr = sqlite3_mprintf("No entry with nelems>0 found in %d entries??", nentries);
            return SQLITE_ERROR;
        }
    }
    
    return SQLITE_OK;
}

static int cdfAttrEntriesEof(sqlite3_vtab_cursor *curp){
    CdfAttrEntriesCursor *cp = (CdfAttrEntriesCursor*) curp;
    CDFstatus status;
    long nattrs;
    
    status = CDFgetNumAttributes(cp->id, &nattrs);
    return cp->attrid>nattrs;
}

/* Retrieve an INTEGER global attribute entry with NumElem=1: */
static CDFstatus result_gattrint(sqlite3_context *ctx, CDFid id, long attrid, long entryid, long) {
    sqlite_int64 ibuf;
    CDFstatus status = CDFgetAttrgEntry(id, attrid-1, entryid-1, &ibuf);
    sqlite3_result_int64(ctx, ibuf);
    
    return status;
}
/* Retrieve an REAL global attribute entry with NumElem=1: */
static CDFstatus result_gattrdouble(sqlite3_context *ctx, CDFid id, long attrid, long entryid, long) {
    double dbuf;
    CDFstatus status = CDFgetAttrgEntry(id, attrid-1, entryid-1, &dbuf);
    sqlite3_result_double(ctx, dbuf);
    
    return status;
}
/* Retrieve an TEXT global attribute entry: */
static CDFstatus result_gattrtext(sqlite3_context *ctx, CDFid id, long attrid, long entryid, long) {
    char *buf;
    long  n=0;
    CDFstatus status = CDFgetAttrgEntryNumElements(id, attrid-1, entryid-1, &n);
    buf    = sqlite3_malloc64(n+1);
    buf[n] = '\0';
    status = CDFgetAttrgEntry(id, attrid-1, entryid-1, buf);
    sqlite3_result_text64(ctx, buf, n+1, sqlite3_free, SQLITE_UTF8);
    
    return status;
}
/* Retrieve a global attribute entry that needs to be put into a BLOB: */
static CDFstatus result_gattrblob(sqlite3_context *ctx, CDFid id, long attrid, long entryid, long nbytes) {
    char buf8[8] = "";
    char *buf;
    CDFstatus status;

    if( nbytes<=8 ) {
        status = CDFgetAttrgEntry(id, attrid-1, entryid-1, buf8);
        sqlite3_result_blob64(ctx, buf8, nbytes, SQLITE_TRANSIENT);
    } else {
        buf    = sqlite3_malloc64(nbytes);
        status = CDFgetAttrgEntry(id, attrid-1, entryid-1, buf);
        sqlite3_result_blob64(ctx, buf, nbytes, sqlite3_free);
    }

    return status;
}
static CDFstatus result_gattrnull(sqlite3_context *ctx, CDFid, long, long, long) {
    sqlite3_result_null(ctx);
    return CDF_OK;
}

static int cdfAttrgEntriesColumn(
        sqlite3_vtab_cursor *curp,  /* The cursor */
        sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
        int iCol)
{
    CdfAttrEntriesCursor *cp = (CdfAttrEntriesCursor*) curp;
    CDFstatus status;
    static CDFstatus (*res[4])(sqlite3_context*, CDFid, long, long, long) = {
        result_gattrint, result_gattrdouble, result_gattrtext, result_gattrblob};
    char **pzErr = &cp->basecur.pVtab->zErrMsg;
    char attrname[CDF_ATTR_NAME_LEN256];
    long cdftype=0,numelems=0;

    switch( iCol ) {
        case 0:  /* CDF attr id */
            sqlite3_result_int64(ctx, cp->attrid);
            break;
        case 1:  /* CDF attr name */
            memset(attrname, '\0', CDF_ATTR_NAME_LEN256);
            status = CDFgetAttrName(cp->id, cp->attrid-1, attrname);
            sqlite3_result_text(ctx, attrname, strnlen(attrname, CDF_ATTR_NAME_LEN256), SQLITE_TRANSIENT);
            break;
        case 2:     /* Entry id */
            sqlite3_result_int64(ctx, cp->entryid);
            break;
        case 3:  /* CDF type, display as text */
            status = CDFgetAttrgEntryDataType(cp->id, cp->attrid-1, cp->entryid-1, &cdftype);
            sqlite3_result_text(ctx, cdf_typestr(cdftype), -1, SQLITE_STATIC);
            break;
        case 4:  /* CDF num of elements */
            status = CDFgetAttrgEntryNumElements(cp->id, cp->attrid-1, cp->entryid-1, &numelems);
            sqlite3_result_int64(ctx, numelems);
            break;
        case 5:  /* CDF value */
            status = CDFgetAttrgEntryNumElements(cp->id, cp->attrid-1, cp->entryid-1, &numelems);
            if( numelems<=0 )
                sqlite3_result_null(ctx);
            else {
                status = CDFgetAttrgEntryDataType(cp->id, cp->attrid-1, cp->entryid-1, &cdftype);
                if( status<CDF_OK ) {
                    char statustext[CDF_STATUSTEXT_LEN+1];
                    CDFgetStatusText(status, statustext);
                    *pzErr = sqlite3_mprintf("When getting attr. %d global entry %d: %s", cp->attrid, cp->entryid, statustext);
                    return SQLITE_ERROR;
                }
                int sqlitetype = cdf_sqlitetype(cdftype);

                if( sqlitetype==SQLITE_TEXT || ((sqlitetype==SQLITE_FLOAT || sqlitetype==SQLITE_INTEGER) && numelems==1) ) {
                    status = (*res[sqlitetype-1])(ctx, cp->id, cp->attrid, cp->entryid, 1);
                } else {
                    status = result_gattrblob(ctx, cp->id, cp->attrid-1, cp->entryid-1, cdf_elsize(cdftype)*numelems);
                }
                if( status<CDF_OK ) {
                    char statustext[CDF_STATUSTEXT_LEN+1];
                    CDFgetStatusText(status, statustext);
                    *pzErr = sqlite3_mprintf("When putting attr. global entry %d: %s", cp->entryid, statustext);
                    return SQLITE_ERROR;
                }
            }
            break;
        default:
            return SQLITE_ERROR;
    }

    return SQLITE_OK;
}

static int cdfAttrEntriesRowid(sqlite3_vtab_cursor *curp, sqlite_int64 *rowidp) {
    CdfAttrEntriesCursor *cp = (CdfAttrEntriesCursor*) curp;
    *rowidp = cp->rowid;
    return SQLITE_OK;
}

static int attrgentry_next(CDFid id, long *attrid, long *entryid) {
    long nentries;
    CDFstatus status = CDFgetNumAttrgEntries(id, *attrid, &nentries);

    *entryid++;
    if( *entryid>nentries ) {
        *attrid++;
        *entryid = 1;
    }

    return status;
}

static int attrzentry_next(CDFid id, long *attrid, long *entryid) {
    long nentries;
    CDFstatus status = CDFgetNumAttrzEntries(id, *attrid, &nentries);

    *entryid++;
    if( *entryid>nentries ) {
        *attrid++;
        *entryid = 1;
    }

    return status;
}

static CDFstatus attrent_int64_2cdf(sqlite3_value *val, CDFid id, long attrid, long entryid, long, char**)
{
    long icolval = sqlite3_value_int64(val);
    return CDFputAttrgEntry(id, attrid-1, entryid-1, CDF_INT8, 1, &icolval);
}

static CDFstatus attrent_double_2cdf(sqlite3_value *val, CDFid id, long attrid, long entryid, long, char**)
{
    long dcolval = sqlite3_value_double(val);
    return CDFputAttrgEntry(id, attrid-1, entryid-1, CDF_DOUBLE, 1, &dcolval);
}

static CDFstatus attrent_text_2cdf(sqlite3_value *val, CDFid id, long attrid, long entryid, long numelems, char**)
{
    return CDFputAttrgEntry(id, attrid-1, entryid-1, CDF_CHAR, numelems, (char*) sqlite3_value_text(val));
}

static CDFstatus attrent_float_2cdf(sqlite3_value *val, CDFid id, long attrid, long entryid, long, char **pzErr)
{
    int sqltype = sqlite3_value_type(val);
    if( sqltype==SQLITE_BLOB ) {
        if( sqlite3_value_bytes(val)!=4 ) {
            *pzErr = sqlite3_mprintf("insert of binary FLOAT needs a 4 octets long BLOB");
            return SQLITE_ERROR;
        }
        return CDFputAttrgEntry(id, attrid-1, entryid-1, CDF_FLOAT, 1, (void*) sqlite3_value_blob(val));
    } else {
        float fcolval = (float) sqlite3_value_double(val);
        return CDFputAttrgEntry(id, attrid-1, entryid-1, CDF_FLOAT, 1, &fcolval);
    }
}

static CDFstatus attrent_epoch_2cdf(sqlite3_value *val, CDFid id, long attrid, long entryid, long, char **pzErr)
{
    if( sqlite3_value_type(val)!=SQLITE_BLOB || sqlite3_value_bytes(val)!=16 ) {
            *pzErr = sqlite3_mprintf("insert of CDF_EPOCH16 needs a 16 octets long BLOB");
            return SQLITE_ERROR;
    }

    return CDFputAttrgEntry(id, attrid-1, entryid-1, CDF_EPOCH16, 1, (void*) sqlite3_value_blob(val));
}

static CDFstatus (*attrentfunc[5])(sqlite3_value*, CDFid, long, long, long, char**) = {
    attrent_int64_2cdf, attrent_double_2cdf, attrent_text_2cdf, attrent_float_2cdf, attrent_epoch_2cdf
};

/* determine which attrid is requested when inserting/updating an attribute entry */
static int attrent_upd_id(CDFid id, sqlite3_value **argv, long* attridp, char **pzErr) {
    CDFstatus status;
    long k,nattrs;
 
    /*
    if( sqlite3_value_type(argv[0])!=SQLITE_NULL /* UPDATE */ /*
            && (sqlite3_value_nochange(argv[0]) || !sqlite3_value_nochange(argv[3])
                || !sqlite3_value_nochange(argv[4])) ) { 
        *pzErr= sqlite3_mprintf("Attribute id, name, or entry/zvar id cannot be updated");
        return SQLITE_ERROR;
    }
    if( sqlite3_value_type(argv[0])==SQLITE_NULL )
        printf("attrent_upd_id: INSERT\n");
    else
        printf("attrent_upd_id: UPDATE\n");
    */

    *attridp = sqlite3_value_int64(argv[2]); /* the attrid is the 3rd argument*/
    status = CDFgetNumAttributes(id, &nattrs);
    if( *attridp>nattrs ) {
        *pzErr= sqlite3_mprintf("Attribute nr %d larger than the nr of attributes %d", *attridp, nattrs);
        return SQLITE_ERROR;
    }

    /* If NULL or negative then determine the attrid via the attribute name in the 4th argument: */
    if( *attridp<=0 && sqlite3_value_type(argv[3])==SQLITE_TEXT ) {
        *attridp = CDFgetAttrNum(id, (char *) sqlite3_value_text(argv[3]))+1;
        if( *attridp<1 ) {
            *pzErr= sqlite3_mprintf("'%s' not found in existing attributes", sqlite3_value_text(argv[3]));
            return SQLITE_ERROR;
        }
    } else if( sqlite3_value_type(argv[3])!=SQLITE_NULL ) { /* The attribute name cannot be changed, must match existing */
        char attrnm[CDF_ATTR_NAME_LEN256];
        memset(attrnm, 0, CDF_ATTR_NAME_LEN256);
        status = CDFgetAttrName(id, *attridp-1, attrnm);
        if( strcmp(attrnm, sqlite3_value_text(argv[3]))!=0 ) {
            *pzErr= sqlite3_mprintf("\"%s\" for id %d does not match \"%s\"", attrnm, *attridp, sqlite3_value_text(argv[3]));
            return SQLITE_ERROR;
        }
    }

    return SQLITE_OK;
}

/* Parse the arguments of xUpdate for updating attribute entries.
 * the parsed arguments are
 *     argv[2]:   CDF attrid (global or z)
 *     argv[3]:   CDF attribute name
 *     argv[4]:   CDF entryid (global) or zvarid (z)
 *     argv[5]:   CDF numerical typeid or text type
 *     argv[6]:   CDF number elements  (nr of bytes for string, else nr of elements)
 *     argv[7]:   SQLite value
 */
static int attrent_upd_pars(sqlite3_value **argv, long *cdftypep, long *numelemp, char **pzErr) {
    CDFstatus status;
    long numelem = sqlite3_value_int64(argv[6]);
    long cdftype;
    int k,ktstr;

    if( sqlite3_value_type(argv[5])==SQLITE_NULL ) { /* no CDF type specified, infer from value */
        cdftype = cdf_typesql(sqlite3_value_type(argv[7]));
        if( cdftype==0 ) {
            *pzErr= sqlite3_mprintf("No usable CDF type\n");
            return SQLITE_ERROR;
        }
    } else if( sqlite3_value_type(argv[5])==SQLITE_TEXT ) {
        if( strnlen(sqlite3_value_text(argv[5]), 12)>11 ) {
            *pzErr= sqlite3_mprintf("CDF type string too long");
            return SQLITE_ERROR;
        }
        cdftype = cdf_typeid(sqlite3_value_text(argv[5]));
        if( cdftype==0 ) {
            *pzErr= sqlite3_mprintf("Unidentified CDF type string %s\n", sqlite3_value_text(argv[5]));
            return SQLITE_ERROR;
        }
    } else {
        cdftype = sqlite3_value_int64(argv[5]);
        if( cdftype<=0 || cdftype>CDF_UCHAR) {
            *pzErr= sqlite3_mprintf("CDF typeid %d out of range\n", cdftype);
            return SQLITE_ERROR;
        }
    }
    *cdftypep = cdftype;

    if( sqlite3_value_type(argv[7])==SQLITE_TEXT ) {
        if( sqlite3_value_type(argv[0])==SQLITE_NULL && numelem>0 && numelem!=strnlen(sqlite3_value_text(argv[7]), 1025) ) {
            *pzErr= sqlite3_mprintf("Num of elems must the string length or NULL\n");
            return SQLITE_ERROR;
        } else
            numelem = strnlen(sqlite3_value_text(argv[7]), 1025);
    } else if( sqlite3_value_type(argv[7])==SQLITE_NULL ) {
        *pzErr= sqlite3_mprintf("CDF attribute entry cannot be a NULL\n");
        return SQLITE_ERROR;
    } else { /* set nr of elements to 1: */
        if( numelem<=0 )
            numelem = 1;
        if( numelem>1024 ) {
            *pzErr= sqlite3_mprintf("Nr of elements %d is excessively large", numelem);
            return SQLITE_ERROR;
        }
    }
    *numelemp = numelem;

    return SQLITE_OK;
}

static int cdfAttrgEntriesUpdate(
        sqlite3_vtab *vtabp,
        int argc,
        sqlite3_value **argv,
        sqlite_int64 *pRowid
) {
    CdfAttrEntries *vp = (CdfAttrEntries*) vtabp;
    CDFid id = vp->cdfvtp.id;
    CDFstatus status;
    char **pzErr = &vp->cdfvtp.base.zErrMsg;
    long k,ktstr,rowid,attrid,entryid,nattrs,scope,maxentry;
    long cdftype,sqltype,numelems,nbytes;
    int rc;

    if( strchr("rs", vp->cdfvtp.mode)!=NULL ) {
        *pzErr = sqlite3_mprintf("Read only, attribute entries are not added/updated/deleted!");
        return SQLITE_READONLY;
    }

    switch (argc) {
        case 1:  /* delete a record */
            printf("AttrgEntriesUpdate: delete\n");
            attrid  = 1;
            entryid = 1;
            rowid = sqlite3_value_int64(argv[0]);
            for( k=1; k<rowid; k++ )
                attrgentry_next(id, &attrid, &entryid);
            status = CDFdeleteAttrgEntry(id, attrid-1, entryid-1);
            break;
        default:  /* insert or replace or update */
            /* 
             * if( sqlite3_value_type(argv[1])==SQLITE_NULL ) { /* shouldn't be, this is WITH ROWID *//*
                *pzErr= sqlite3_mprintf("WITH ROWID, argv[1] is supposed to be not NULL");
                return SQLITE_ERROR;
            } */

            if( (rc = attrent_upd_id(id, argv, &attrid, pzErr))!=SQLITE_OK )
                return rc;

            status = CDFgetAttrScope(id, attrid-1, &scope);
            if( scope!=GLOBAL_SCOPE ) {
                *pzErr= sqlite3_mprintf("Attribute nr %d does not have global scope", attrid);
                return SQLITE_ERROR;
            }

            status  = CDFgetAttrMaxgEntry(id, attrid-1, &maxentry);
            entryid = sqlite3_value_int64(argv[4]);
            if( entryid<=0 )   /* NULL or negative, set entryid to the next available one: */
                entryid = maxentry+2;
            else if( entryid>maxentry+64 ) {
                    *pzErr= sqlite3_mprintf("Entry nr %d unreasonably large, max entry %d", entryid, maxentry);
                    return SQLITE_ERROR;
            }
            
            /* get numerical CDF typeid and numelem: */
            if( (rc = attrent_upd_pars(argv, &cdftype, &numelems, pzErr))!=SQLITE_OK )
                return rc;

            sqltype = cdf_sqlitetype(cdftype);
            /*
            if( sqltype==0 )  { /* Invalid CDF datatype */ /*
                *pzErr= sqlite3_mprintf("Invalid CDF datatype %d", cdftype);
                return SQLITE_ERROR;
            }
            */

            if( sqltype==SQLITE_TEXT
                    || ((sqltype==SQLITE_FLOAT || sqltype==SQLITE_INTEGER || sqltype==SQLITE_BLOB) && numelems==1) )
                status = attrentfunc[cdf_valfuncid(cdftype)](argv[7], id, attrid, entryid, numelems, pzErr);
            else { /* Vector of DOUBLE or FLOAT or INTEGER; check for correct numelems and insert as BLOB */
                if( cdf_elsize(cdftype)*numelems!=sqlite3_value_bytes(argv[7]) ) {
                    *pzErr= sqlite3_mprintf("sizeof(datatype)*(numelems) does not match blob size, %d * %d != %d",
                            cdf_elsize(cdftype), numelems, sqlite3_value_bytes(argv[7]));
                    return SQLITE_ERROR;
                }
                status = CDFputAttrgEntry(id, attrid-1, entryid-1, cdftype, numelems, (void*) sqlite3_value_blob(argv[7]));
            }
            if( status!=CDF_OK ) {
                char statustext[CDF_STATUSTEXT_LEN+1];
                CDFgetStatusText(status, statustext);
                *pzErr = sqlite3_mprintf("When putting attr %d entry %d: %s", attrid-1, entryid-1, statustext);
                return SQLITE_ERROR;
            } else
                return SQLITE_OK;
    }
}

static sqlite3_module CdfAttrgEntriesModule = {
  0,                           /* iVersion */
  cdfAttrgEntriesCreate,       /* xCreate */
  cdfAttrgEntriesConnect,      /* xConnect */
  cdfAttrgEntriesBestIndex,    /* xBestIndex */
  cdfVTabDisconnect,           /* xDisconnect */
  cdfVTabDisconnect,           /* xDestroy */
  cdfAttrEntriesOpen,          /* xOpen - open a cursor */
  cdfAttrEntriesClose,         /* xClose - close a cursor */
  cdfAttrgEntriesFilter,       /* xFilter - configure scan constraints */
  cdfAttrgEntriesNext,         /* xNext - advance a cursor */
  cdfAttrEntriesEof,           /* xEof - check for end of scan */
  cdfAttrgEntriesColumn,       /* xColumn - read data */
  cdfAttrEntriesRowid,         /* xRowid - read data */
  cdfAttrgEntriesUpdate,       /* xUpdate */
  0,                           /* xBegin */
  0,                           /* xSync */
  0,                           /* xCommit */
  0,                           /* xRollback */
  0,                           /* xFindMethod */
  0,                           /* xRename */
}; /**/

static int cdfAttrzEntriesConnect(
        sqlite3 *db,
        void *pAux,
        int argc, const char *const*argv,
        sqlite3_vtab **ppVtab,
        char **pzErr)
{
    CDFid            id;
    char             mode,*z;
    CDFstatus        status;
    sqlite3_str     *zsql = sqlite3_str_new(db);
    CdfAttrEntries *vtabp = 0;
    int              rc;

    rc = cdf_prep_idmode(argc, argv, pzErr, &id, &mode);
    if( rc!=SQLITE_OK ) return rc;

    sqlite3_str_appendf(zsql, "CREATE TABLE cdf_attr_zentries_ignored (\n");
    sqlite3_str_appendf(zsql, "    Attrid INTEGER NOT NULL,\n");
    sqlite3_str_appendf(zsql, "    Name TEXT NOT NULL,\n");
    sqlite3_str_appendf(zsql, "    zVar NOT NULL,\n");   /* zVar id or name to which the entry refers */
    sqlite3_str_appendf(zsql, "    Dataspec NOT NULL,\n");   /* CDF data type, id or name, of the entry */
    sqlite3_str_appendf(zsql, "    Nelems INTEGER,\n");
    sqlite3_str_appendf(zsql, "    Value,\n");
    sqlite3_str_appendf(zsql, "    PRIMARY KEY(attrid,zvar)\n");
    sqlite3_str_appendf(zsql, ");\n");

    rc = sqlite3_declare_vtab(db, sqlite3_str_value(zsql));
    if( rc!=SQLITE_OK ) {
        *pzErr = sqlite3_mprintf("Bad schema \n%s\nerror code: %d\n", z, rc);
        return SQLITE_ERROR;
    }
    z = sqlite3_str_finish(zsql);
    sqlite3_free(z);

    vtabp = sqlite3_malloc( sizeof(*vtabp) );
    if( vtabp==0 ) return SQLITE_NOMEM;
    *ppVtab = (sqlite3_vtab*) vtabp;
    memset(vtabp, 0, sizeof(*vtabp));

    vtabp->cdfvtp.id = id;
    vtabp->cdfvtp.mode = mode;
    status = CDFgetNumvAttributes(id, &(vtabp->nattrs));

    return rc;
}

static int cdfAttrzEntriesCreate(
        sqlite3 *db,
        void *pAux,
        int argc, const char *const*argv,
        sqlite3_vtab **ppVtab,
        char **pzErr
        ){
    return cdfAttrzEntriesConnect(db, pAux, argc, argv, ppVtab, pzErr);
}

static int cdfAttrzEntriesBestIndex(
        sqlite3_vtab *vtabp,
        sqlite3_index_info *idxinfop
){
    CdfAttrEntries *vp = (CdfAttrEntries*) vtabp;
    CDFid id = vp->cdfvtp.id;
    CDFstatus status;
    long nattrs,nentries,scope,cost=0,k;

    idxinfop->idxNum = 1;
    idxinfop->idxStr = "";
    status = CDFgetNumAttributes(id, &nattrs);
    for( k=0; k<nattrs; k++ ) {
        status = CDFgetAttrScope(id, k, &scope);
        if( scope==VARIABLE_SCOPE ) {
            status = CDFgetNumAttrzEntries(id, k, &nentries);
            cost += nentries;
        }
    }
    idxinfop->estimatedCost = cost;

    return SQLITE_OK;
}

static int cdfAttrzEntriesFilter(
        sqlite3_vtab_cursor *curp, 
        int idxNum, const char *idxStr,
        int argc, sqlite3_value **argv
){
    CdfAttrEntriesCursor *cp = (CdfAttrEntriesCursor*) curp;
    char **pzErr = &cp->basecur.pVtab->zErrMsg;
    CDFstatus status;
    long      nattrs,maxentry=-1,nentries=0,scope=0,nelems=0;
    
    /* Make sure to start at the beginning: */

    cp->rowid   = 1;
    cp->attrid  = 1;
    cp->entryid = 1;
    
    status = CDFgetNumAttributes(cp->id, &nattrs);
    /* Loop over attributes until variable scope and at least one non-empty entry: */
    while( cp->attrid<=nattrs ) {
        status = CDFgetAttrScope(cp->id, cp->attrid-1, &scope); 
        status = CDFgetNumAttrzEntries(cp->id, cp->attrid-1, &nentries);
        status = CDFgetAttrMaxzEntry(cp->id, cp->attrid-1, &maxentry);
        if( scope==VARIABLE_SCOPE && nentries>0) {
            while( cp->entryid-1<=maxentry ) { /* Omit empty entries, at least one non-empty should exist */
                status = CDFgetAttrzEntryNumElements(cp->id, cp->attrid-1, cp->entryid-1, &nelems);
                if( nelems>0 )
                    break;
                cp->entryid++;
            }  
            if( cp->entryid-1>maxentry) {
                *pzErr = sqlite3_mprintf("AttrzEntriedFilter: No entry with nelems>0 found in %d entries??", nentries);
                return SQLITE_ERROR;
            }
            break;
        }
        cp->attrid++;
        cp->entryid = 1;
    }

    return SQLITE_OK;
}

static int cdfAttrzEntriesNext(sqlite3_vtab_cursor *curp) {
    CdfAttrEntriesCursor *cp = (CdfAttrEntriesCursor*) curp;
    char **pzErr = &cp->basecur.pVtab->zErrMsg;
    CDFstatus status;
    long      nattrs=0,scope=0,nzentries=0,maxentry=-1,nelems=0;

    cp->rowid++;
    cp->entryid++;
    status = CDFgetNumAttrzEntries(cp->id, cp->attrid-1, &nzentries);
    status = CDFgetAttrMaxzEntry(cp->id, cp->attrid-1, &maxentry);
    while( cp->entryid<=nzentries ) { /* Omit empty entries */
        status = CDFgetAttrzEntryNumElements(cp->id, cp->attrid-1, cp->entryid-1, &nelems);
        if( nelems>0 )
            break;
        cp->entryid++;
    }  
    /* Move to the next attribute if there are no more entries */ 
    if( cp->entryid-1>maxentry ) {
        cp->entryid = 1;
        status = CDFgetNumAttributes(cp->id, &nattrs);
        cp->attrid++;
        while( cp->attrid<=nattrs ) { /* Increase the attr. num. until variable scope and non-empty entry */
            status = CDFgetAttrScope(cp->id, cp->attrid-1, &scope);
            if( scope==VARIABLE_SCOPE ) {
                status = CDFgetNumAttrzEntries(cp->id, cp->attrid-1, &nzentries);
                if( nzentries>0 )
                    break;
            }
            cp->attrid++;
        }
        if( cp->attrid>nattrs ) { /* no more attibutes */
            *pzErr = sqlite3_mprintf("no more attributes??\n");
            return SQLITE_OK;
        }
        status = CDFgetAttrMaxzEntry(cp->id, cp->attrid-1, &maxentry);
        cp->entryid = 1;
        while( cp->entryid-1<=maxentry ) { /* Omit empty entries, at least one non-empty should exist */
            status = CDFgetAttrzEntryNumElements(cp->id, cp->attrid-1, cp->entryid-1, &nelems);
            if( nelems>0 )
                break;
            cp->entryid++;
        }  
        if( cp->entryid-1>maxentry ) {
            *pzErr = sqlite3_mprintf("No entry with nelems>0 found in %d entries??", nzentries);
            return SQLITE_ERROR;
        }
    }
    
    return SQLITE_OK;
}

/* Retrieve an INTEGER attribute zentry with NumElem=1: */
static CDFstatus result_zattrint(sqlite3_context *ctx, CDFid id, long attrid, long entryid, long) {
    sqlite_int64 ibuf;
    CDFstatus status = CDFgetAttrzEntry(id, attrid-1, entryid-1, &ibuf);
    sqlite3_result_int64(ctx, ibuf);
    
    return status;
}
/* Retrieve an REAL attribute zentry with NumElem=1: */
static CDFstatus result_zattrdouble(sqlite3_context *ctx, CDFid id, long attrid, long entryid, long) {
    double dbuf;
    CDFstatus status = CDFgetAttrzEntry(id, attrid-1, entryid-1, &dbuf);
    sqlite3_result_double(ctx, dbuf);
    
    return status;
}
/* Retrieve an TEXT attribute zentry: */
static CDFstatus result_zattrtext(sqlite3_context *ctx, CDFid id, long attrid, long entryid, long) {
    char *buf;
    long  n=0;
    CDFstatus status = CDFgetAttrzEntryNumElements(id, attrid-1, entryid-1, &n);
    buf    = sqlite3_malloc64(n+1);
    buf[n] = '\0';
    status = CDFgetAttrzEntry(id, attrid-1, entryid-1, buf);
    sqlite3_result_text64(ctx, buf, n+1, sqlite3_free, SQLITE_UTF8);
    
    return status;
}
/* Retrieve an attribute zentry that needs to be put into a BLOB: */
static CDFstatus result_zattrblob(sqlite3_context *ctx, CDFid id, long attrid, long entryid, long nbytes) {
    char buf8[8] = "";
    char *buf;
    CDFstatus status;

    if( nbytes<=8 ) {
        status = CDFgetAttrzEntry(id, attrid-1, entryid-1, buf8);
        sqlite3_result_blob64(ctx, buf8, nbytes, SQLITE_TRANSIENT);
    } else {
        buf    = sqlite3_malloc64(nbytes);
        status = CDFgetAttrzEntry(id, attrid-1, entryid-1, buf);
        sqlite3_result_blob64(ctx, buf, nbytes, sqlite3_free);
    }

    return status;
}

static int cdfAttrzEntriesColumn(
        sqlite3_vtab_cursor *curp,  /* The cursor */
        sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
        int iCol)
{
    CdfAttrEntriesCursor *cp = (CdfAttrEntriesCursor*) curp;
    CDFstatus status;
    static CDFstatus (*res[4])(sqlite3_context*, CDFid, long, long, long) = {
        result_zattrint, result_zattrdouble, result_zattrtext, result_zattrblob};
    char **pzErr = &cp->basecur.pVtab->zErrMsg;
    char attrname[CDF_ATTR_NAME_LEN256];
    char varname[CDF_VAR_NAME_LEN256];
    long cdftype,numelems;

    switch( iCol ) {
        case 0:  /* CDF attr id */
            sqlite3_result_int64(ctx, cp->attrid);
            break;
        case 1:  /* CDF attr name */
            memset(attrname, '\0', CDF_ATTR_NAME_LEN256);
            status = CDFgetAttrName(cp->id, cp->attrid-1, attrname);
            sqlite3_result_text(ctx, attrname, strnlen(attrname, CDF_ATTR_NAME_LEN256), SQLITE_TRANSIENT);
            break;
        case 2:  /* CDF zvar name */
            memset(varname, '\0', CDF_VAR_NAME_LEN256);
            status = CDFgetzVarName(cp->id, cp->entryid-1, varname);
            sqlite3_result_text(ctx, varname, strnlen(varname, CDF_VAR_NAME_LEN256), SQLITE_TRANSIENT);
            break;
        case 3:  /* CDF type, display as text */
            status = CDFgetAttrzEntryDataType(cp->id, cp->attrid-1, cp->entryid-1, &cdftype);
            sqlite3_result_text(ctx, cdf_typestr(cdftype), -1, SQLITE_STATIC);
            break;
        case 4:  /* CDF num of elements */
            status = CDFgetAttrzEntryNumElements(cp->id, cp->attrid-1, cp->entryid-1, &numelems);
            sqlite3_result_int64(ctx, numelems);
            break;
        case 5:  /* CDF value */
            status = CDFgetAttrgEntryNumElements(cp->id, cp->attrid-1, cp->entryid-1, &numelems);
            if( numelems<=0 )
                sqlite3_result_null(ctx);
            else {
                status = CDFgetAttrzEntryDataType(cp->id, cp->attrid-1, cp->entryid-1, &cdftype);
                if( status<CDF_OK ) {
                    char statustext[CDF_STATUSTEXT_LEN+1];
                    CDFgetStatusText(status, statustext);
                    *pzErr = sqlite3_mprintf("When getting attr zvar entry %d: %s", cp->entryid, statustext);
                    return SQLITE_ERROR;
                }
                int sqlitetype = cdf_sqlitetype(cdftype);

                if( sqlitetype==SQLITE_TEXT || ((sqlitetype==SQLITE_FLOAT || sqlitetype==SQLITE_INTEGER) && numelems==1) ) {
                    status = (*res[sqlitetype-1])(ctx, cp->id, cp->attrid, cp->entryid, 1);
                } else {
                    status = result_zattrblob(ctx, cp->id, cp->attrid-1, cp->entryid-1, cdf_elsize(cdftype)*numelems);
                }
                if( status<CDF_OK ) {
                    char statustext[CDF_STATUSTEXT_LEN+1];
                    CDFgetStatusText(status, statustext);
                    *pzErr = sqlite3_mprintf("When getting attr zvar entry %d: %s", cp->entryid, statustext);
                    return SQLITE_ERROR;
                }
            }
            break;
        default:
            return SQLITE_ERROR;
    }

    return SQLITE_OK;
}

static CDFstatus attrzent_int64_2cdf(sqlite3_value *val, CDFid id, long attrid, long entryid, long, char**)
{
    long icolval = sqlite3_value_int64(val);
    return CDFputAttrzEntry(id, attrid-1, entryid-1, CDF_INT8, 1, &icolval);
}

static CDFstatus attrzent_double_2cdf(sqlite3_value *val, CDFid id, long attrid, long entryid, long, char**)
{
    long dcolval = sqlite3_value_double(val);
    return CDFputAttrzEntry(id, attrid-1, entryid-1, CDF_DOUBLE, 1, &dcolval);
}

static CDFstatus attrzent_text_2cdf(sqlite3_value *val, CDFid id, long attrid, long entryid, long numelems, char**)
{
    return CDFputAttrzEntry(id, attrid-1, entryid-1, CDF_CHAR, numelems, (char*) sqlite3_value_text(val));
}

static CDFstatus attrzent_float_2cdf(sqlite3_value *val, CDFid id, long attrid, long entryid, long, char **pzErr)
{
    int sqltype = sqlite3_value_type(val);
    if( sqltype==SQLITE_BLOB ) {
        if( sqlite3_value_bytes(val)!=4 ) {
            *pzErr = sqlite3_mprintf("insert of binary FLOAT needs a 4 octets long BLOB");
            return SQLITE_ERROR;
        }
        return CDFputAttrzEntry(id, attrid-1, entryid-1, CDF_FLOAT, 1, (void*) sqlite3_value_blob(val));
    } else {
        float fcolval = (float) sqlite3_value_double(val);
        return CDFputAttrzEntry(id, attrid-1, entryid-1, CDF_FLOAT, 1, &fcolval);
    }
}

static CDFstatus attrzent_epoch_2cdf(sqlite3_value *val, CDFid id, long attrid, long entryid, long, char **pzErr)
{
    if( sqlite3_value_type(val)!=SQLITE_BLOB || sqlite3_value_bytes(val)!=16 ) {
            *pzErr = sqlite3_mprintf("insert of CDF_EPOCH16 needs a 16 octets long BLOB");
            return SQLITE_ERROR;
    }

    return CDFputAttrzEntry(id, attrid-1, entryid-1, CDF_EPOCH16, 1, (void*) sqlite3_value_blob(val));
}

static CDFstatus (*attrzentfunc[5])(sqlite3_value*, CDFid, long, long, long, char**) = {
    attrzent_int64_2cdf, attrzent_double_2cdf, attrzent_text_2cdf, attrzent_float_2cdf, attrzent_epoch_2cdf
};

static int cdfAttrzEntriesUpdate(
        sqlite3_vtab *vtabp,
        int argc,
        sqlite3_value **argv,
        sqlite_int64 *pRowid
) {
    CdfAttrEntries *vp = (CdfAttrEntries*) vtabp;
    CDFid id = vp->cdfvtp.id;
    CDFstatus status;
    char **pzErr = &vp->cdfvtp.base.zErrMsg;
    long k,ktstr,rowid,attrid=0,entryid=0,nattrs,scope,maxentry,nzvars;
    long cdftype,sqltype,numelems,nbytes;
    int update,rc;

    if( strchr("rs", vp->cdfvtp.mode)!=NULL ) {
        *pzErr = sqlite3_mprintf("Read only, attribute entries are not added/updated/deleted!");
        return SQLITE_READONLY;
    }

    switch (argc) {
        case 1:  /* delete a record */
            attrid  = 1;
            entryid = 1;
            rowid = sqlite3_value_int64(argv[0]);
            for( k=1; k<rowid; k++ )
                attrzentry_next(id, &attrid, &entryid);
            status = CDFdeleteAttrzEntry(id, attrid-1, entryid-1);
            break;
        default:  /* insert or replace or update */
            update = sqlite3_value_type(argv[0])!=SQLITE_NULL;
            int sqlite_code = attrent_upd_id(id, argv, &attrid, pzErr);
            if( sqlite_code!=SQLITE_OK )
                 return sqlite_code;

            status = CDFgetAttrScope(id, attrid-1, &scope);
            if( scope!=VARIABLE_SCOPE ) {
                *pzErr= sqlite3_mprintf("Attribute nr %d is not variable scope", attrid);
                return SQLITE_ERROR;
            }

            /* Entryid/zVarnr: */
            status  = CDFgetNumzVars(id, &nzvars);
            if( nzvars<=0 ) {
                *pzErr= sqlite3_mprintf("No zvars are yet created");
                return SQLITE_ERROR;
            }
            if( sqlite3_value_type(argv[4])==SQLITE_INTEGER || sqlite3_value_type(argv[4])==SQLITE_FLOAT ) {
                entryid = sqlite3_value_int64(argv[4]);
                if( entryid>nzvars ) {
                    *pzErr= sqlite3_mprintf("Entry id %d too large, only %d zvars have been created", entryid, nzvars);
                    return SQLITE_ERROR;
                }
            } else if( sqlite3_value_type(argv[4])==SQLITE_TEXT ) {
                char zvarnm[CDF_VAR_NAME_LEN256];
                const unsigned char* zvarName = sqlite3_value_text(argv[4]);
                for( k=0; k<nzvars; k++ ) {
                    memset(zvarnm, '\0', CDF_VAR_NAME_LEN256);
                    status = CDFgetzVarName(id, k, zvarnm);
                    if( strncmp(zvarName, zvarnm, CDF_VAR_NAME_LEN256)==0 )
                        break;
                }
                if( k>=nzvars ) {
                    *pzErr= sqlite3_mprintf("'%s' not found in existing zvars", zvarName);
                    return SQLITE_ERROR;
                }
                entryid = k+1;
            } else if( sqlite3_value_type(argv[4])==SQLITE_NULL ) {  /* or the next available zvar: */
                status  = CDFgetAttrMaxzEntry(id, attrid-1, &maxentry);
                if( maxentry<nzvars-1 )
                    entryid = maxentry+2;
                else {
                    *pzErr= sqlite3_mprintf("No zvar left");
                    return SQLITE_ERROR;
                }
            } else  {
                *pzErr= sqlite3_mprintf("No usable zvarid could be obtained");
                return SQLITE_ERROR;
            }

            if( (rc = attrent_upd_pars(argv, &cdftype, &numelems, pzErr))!=SQLITE_OK )
                return rc;

            sqltype = cdf_sqlitetype(cdftype);
            if( sqltype==0 )  { /* Invalid CDF datatype */
                *pzErr= sqlite3_mprintf("Invalid CDF datatype %d", cdftype);
                return SQLITE_ERROR;
            }

            if( sqltype==SQLITE_TEXT
                    || ((sqltype==SQLITE_FLOAT || sqltype==SQLITE_INTEGER || sqltype==SQLITE_BLOB) && numelems==1) )
                status = attrzentfunc[cdf_valfuncid(cdftype)](argv[7], id, attrid, entryid, numelems, pzErr);
            else { /* Vector of DOUBLE or FLOAT or INTEGER; check for correct numelems and insert as BLOB */
                if( cdf_elsize(cdftype)*numelems!=sqlite3_value_bytes(argv[7]) ) {
                    *pzErr= sqlite3_mprintf("sizeof(datatype)*(numelems) does not match blob size, %d * %d != %d",
                            cdf_elsize(cdftype), numelems, sqlite3_value_bytes(argv[7]));
                    return SQLITE_ERROR;
                }
                status = CDFputAttrzEntry(id, attrid-1, entryid-1, cdftype, numelems, (void*) sqlite3_value_blob(argv[7]));
            }
            if( status!=CDF_OK ) {
                char statustext[CDF_STATUSTEXT_LEN+1];
                CDFgetStatusText(status, statustext);
                *pzErr = sqlite3_mprintf("When putting attr %d entry %d: %s", attrid-1, entryid-1, statustext);
                return SQLITE_ERROR;
            } else
                return SQLITE_OK;
    }
}

static sqlite3_module CdfAttrzEntriesModule = {
  0,                           /* iVersion */
  cdfAttrzEntriesCreate,       /* xCreate */
  cdfAttrzEntriesConnect,      /* xConnect */
  cdfAttrzEntriesBestIndex,    /* xBestIndex */
  cdfVTabDisconnect,           /* xDisconnect */
  cdfVTabDisconnect,           /* xDestroy */
  cdfAttrEntriesOpen,          /* xOpen - open a cursor */
  cdfAttrEntriesClose,         /* xClose - close a cursor */
  cdfAttrzEntriesFilter,       /* xFilter - configure scan constraints */
  cdfAttrzEntriesNext,         /* xNext - advance a cursor */
  cdfAttrEntriesEof,           /* xEof - check for end of scan */
  cdfAttrzEntriesColumn,       /* xColumn - read data */
  cdfAttrEntriesRowid,         /* xRowid - read data */
  cdfAttrzEntriesUpdate,       /* xUpdate */
  0,                           /* xBegin */
  0,                           /* xSync */
  0,                           /* xCommit */
  0,                           /* xRollback */
  0,                           /* xFindMethod */
  0,                           /* xRename */
}; /**/

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_cdf_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  rc = sqlite3_create_module(db, "cdffile", &CdfFileModule, 0);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_create_module(db, "cdfzvars", &CdfzVarsModule, 0);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_create_module(db, "cdfzrecs", &CdfRecsModule, 0);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_create_module(db, "cdfattrs", &CdfAttrsModule, 0);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_create_module(db, "cdfattrgentries", &CdfAttrgEntriesModule, 0);
  if( rc!=SQLITE_OK ) return rc;
  
  rc = sqlite3_create_module(db, "cdfattrzentries", &CdfAttrzEntriesModule, 0);
  return rc;
}
