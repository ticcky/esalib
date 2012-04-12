#ifdef STANDARD
/* STANDARD is defined, don't use any mysql functions */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef __WIN__
typedef unsigned __int64 ulonglong;/* Microsofts 64 bit types */
typedef __int64 longlong;
#else
typedef unsigned long long ulonglong;
typedef long long longlong;
#endif /*__WIN__*/
#else
#include <my_global.h>
#include <my_sys.h>
#if defined(MYSQL_SERVER)
#include <m_string.h>/* To get strmov() */
#else
/* when compiled as standalone */
#include <string.h>
#define strmov(a,b) stpcpy(a,b)
#define bzero(a,b) memset(a,0,b)
#define memcpy_fixed(a,b,c) memcpy(a,b,c)
#endif
#endif
#include <mysql.h>
#include <ctype.h>

#ifdef HAVE_DLOPEN

#if !defined(HAVE_GETHOSTBYADDR_R) || !defined(HAVE_SOLARIS_STYLE_GETHOST)
static pthread_mutex_t LOCK_hostname;
#endif

// -----------------------------------------------------------------------------
// HERE STARTS THE REAL CODE
#include <math.h>

#include "hashtable.h"
#include "searcher.h"

#define MAX_PATH_LEN 256

char * result;

// headers
//my_bool esa_search_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
//void esa_search_udf_deinit(UDF_INIT *initid __attribute__((unused)));
//double esa_search_udf(UDF_INIT* initid, UDF_ARGS* args __attribute__((unused)),
//                char* is_null __attribute__((unused)), char* error __attribute__((unused)));

my_bool esa_search_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
        if(!(args->arg_count == 3)) {
                strcpy(message, "esa_search arguments: vector, index_path, max_results");
                return 1;
        }
        args->arg_type[0] = STRING_RESULT;
        args->arg_type[1] = STRING_RESULT;
        args->arg_type[2] = INT_RESULT;

	int max_results;
	sscanf(args->args[2], "%d", &max_results);

	result = malloc(max_results * 8 + 4);

        return 0;
}

void esa_search_deinit(UDF_INIT *initid __attribute__((unused))) {
	if(result != NULL) {
		free(result);
	}
}

char* esa_search(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length, 
			char *is_null, char *error) {
	char* v = (char *)(args->args[0]); 
	char * vector;
	int v_len;

	char fn_2index[MAX_PATH_LEN];
	char fn_2index_cmap[MAX_PATH_LEN];
	char fn_2index_ndx[MAX_PATH_LEN];
	char fn_vectors[MAX_PATH_LEN];
	char fn_vectors_ndx[MAX_PATH_LEN];

	char* path = (char *)(args->args[1]);

	sprintf(fn_2index, "%s/2index", path);
	sprintf(fn_2index_cmap, "%s/2index.cmap", path);
	sprintf(fn_2index_ndx, "%s/2index.ndx", path);
	sprintf(fn_vectors, "%s/vectors", path);
	sprintf(fn_vectors_ndx, "%s/vectors.ndx", path);
	
	long long max_results = *(long long*) (args->args[2]);

	int len;
	result = vector = 
		esa_vector_search(v, &v_len,
				  fn_2index,
				  fn_2index_cmap,
				  fn_2index_ndx,
				  fn_vectors,
				  fn_vectors_ndx,
				  (int) max_results);
	*length = v_len;

	fprintf(stderr, "total length %d\n", v_len);

	return vector;
}

#endif /* HAVE_DLOPEN */
