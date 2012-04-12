#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>
#include <string.h>
#include <math.h>

#include "searcher.h"
#include "bits.h"
#include "hashtable.h"

struct st_dimension { 
    int concept_id;
    float score;
};

float _read_float(char* where) {
        int i_val = _read_int(where);
        float* val = (float*)(&i_val);
	return *val;

}

int min(int a, int b) {
	if(a > b )
		return b;
	else
		return a;
}

inline float cmp_float(float a, float b) {
	if(a < b )
		return 1;
	else if(a > b)
		return -1;
	else
		return 0;
}

int cmp_dimension(const void *a, const void *b) { 
	return cmp_float(_read_float(((char*)a)+sizeof(int)), _read_float(((char*)b)+sizeof(int)));
	//return cmp_float(*(float*)(a+sizeof(int)), *(float*)(b+sizeof(int)));
	struct st_dimension *ia = (struct st_dimension *)a;
        struct st_dimension *ib = (struct st_dimension *)b;
	return (int)(1000000.f*ia->score - 1000000.f*ib->score);
    	/* float comparison: returns negative if b > a 
	 * and positive if a > b. We multiplied result by 100.0
	 * to preserve decimal fraction */ 
} 

// very quick and hacky implementation of sqrt function
float qsqrt(float number) {
    long i;
    float x, y;
    const float f = 1.5F;

    x = number * 0.5F;
    y  = number;
    i  = * ( long * ) &y;
    i  = 0x5f3759df - ( i >> 1 );
    y  = * ( float * ) &i;
    y  = y * ( f - ( x * y * y ) );
    y  = y * ( f - ( x * y * y ) );
    return number * y;
}

void quickSort( int a[], int l, int r)
{
   int j;

   if( l < r ) {
   	// divide and conquer
        j = partition( a, l, r);
       quickSort( a, l, j-1);
       quickSort( a, j+1, r);
   }
}

// quick sort partition function
int partition( int a[], int l, int r) {
   int pivot, i, j, t;
   pivot = a[l];
   i = l; j = r+1;
		
   while( 1)
   {
   	do ++i; while( a[i] <= pivot && i <= r );
   	do --j; while( a[j] > pivot );
   	if( i >= j ) break;
   	t = a[i]; a[i] = a[j]; a[j] = t;
   }
   t = a[l]; a[l] = a[j]; a[j] = t;
   return j;
}

// binary search of an array
long scan(int start, int end, int c, int* map) {
	long middle;
	long b = start;
	long e = end;
	while(e - b > 1) {
		middle = b + (e - b) / 2;
		//printf("s: %d e: %d m: %d mm: %d c: %d\n", b, e, middle, map[middle], c);
		if(map[middle] > c || map[middle] == 0) {
			e = middle;
		} else {
			b = middle;
		}
	}
	
	//printf("b: %d e: %d rb: %d re: %d c: %d\n", b, e, map[b], map[e], c);
	if(map[b] == c)
		return b;
	else
		return -1;
}


int _read_int(char* where) {
        unsigned int res = 
		((((unsigned char)where[0]) << 24) | 
		(((unsigned char)where[1]) << 16) | 
		(((unsigned char)where[2]) << 8) | 
		((unsigned char)where[3]));
		return res;

}

// read an integer that was written by java
int read_int(char* where, int* cursor) {
        where += *cursor;

	unsigned int res = _read_int(where);

	//move the cursor forward
        *cursor += 4;

        return res;
}


// read a float written by java
float read_float(char* where, int* cursor) {
	float res = _read_float(where + *cursor);
	*cursor += 4;
        //int i_val = read_int(where, cursor);
        //float* val = (float*)(&i_val);
        //return *val;
	return res;
}

/** 
 * Compute the cosine similarity of the two vectors
 *  - in this program we always  assume the vectosr are in the
 *  following format: <int size>[<int concept_id><float score>]{size}
 */
double cosine_simil(char* v1, char*v2) {
        hash_table_t* table = hash_table_new(MODE_COPY);

        int cursor = 0;
        int i;
        int concept_id;
        float concept_value;
        float v1_norm = 0.0;
	int v1_len = read_int(v1, &cursor);
        for(i = 0; i < v1_len; i++) {
                concept_id = read_int(v1, &cursor);
                concept_value = read_float(v1, &cursor);
                HT_ADD(table, &concept_id, &concept_value);
                v1_norm += concept_value * concept_value;
		//printf("v1: %d %.4f\n", concept_id, concept_value);
        }

        float result = 0.0;
        cursor = 0;
        float v2_norm = 0.0;
	int v2_len = read_int(v2, &cursor);
        for(i = 0; i < v2_len; i++) {
                concept_id = read_int(v2, &cursor);
                concept_value = read_float(v2, &cursor);
                v2_norm += concept_value * concept_value;
		//printf("v2: %d %.4f\n", concept_id, concept_value);
                float* v = ((float*)HT_LOOKUP(table, &concept_id));
                if(v != NULL) {
                        result += concept_value * (*v);
                }
        }
        hash_table_delete(table);
        
	//printf("r: %.8lf %.8lf %.8lf\n", result, sqrt(v1_norm), sqrt(v2_norm));
        if(result > 0.000001) {
                result = result / (sqrt(v1_norm) * sqrt(v2_norm));
		if(result < 0.0)
			result *= -1;
	}


        return result;
}

int getHexVal(char c) {
	int curr = toupper(c);
	if(curr >= '0' && curr <= '9') {
		curr -= '0';
	} else if(curr >= 'A' && curr <= 'F') {
		curr -= 'A';
		curr += 10;
	}
	return curr;

}

int readHex(char* s, int* pos) {
	if(*pos < strlen(s)) {
		int curr = getHexVal(*(s + (*pos)));
		curr = curr << 4;
		curr += getHexVal(*(s + 1 + (*pos)));
		*pos += 2;
		return curr;
	} else {
		return -1;
	}


}

char* grow(char* mem, int* size, int itemSize) {
	(*size) *= 2;
	return realloc(mem, (*size));
}

void result_write_int(int val, char* result, int* cursor, int itemSize) {
	int i = 0;
	for(i = 0; i < sizeof(int); i++) {
		*((unsigned char*)(result + (*cursor) + i)) = 
			(val >> (sizeof(int)*8 - (i + 1) * 8)) & 0xff;
	}
	*cursor += sizeof(int);
}

void result_write_float(float val, char* result, int* cursor, int itemSize) {
	int* c = (int*) &val;
	result_write_int(*c, result, cursor, itemSize);

	
	//*((float*)(result + (*cursor))) = val;
	//*cursor += sizeof(float);
}

char* mapFile(char* fname, int* resFd, long* size, int itemSize) {
	struct stat statInfo;

	int fd = open(fname, O_RDONLY);
	printf("errno %d\n", errno);
	if(fd == -1) {
		perror(fname);
		return NULL;
	} else
		printf("open success: %s\n", fname);

	fstat(fd, &statInfo);
	long fsize =  (size_t) statInfo.st_size;
	printf("filesize: %ld\n", (size_t) fsize);
	*size = fsize / itemSize;
	*resFd = fd;
	char* res = mmap(0,  fsize, PROT_READ, MAP_PRIVATE, fd, 0);
	return res;
}


char* esa_vector_search(char* searchVector, int * len,
			char* fn_2index,
			char* fn_2index_cmap,
			char* fn_2index_ndx,
			char* fn_vectors,
			char* fn_vectors_ndx,
			int max_results) {
	int i;
	int cursor = 0;
	int numberOfConcepts = read_int(searchVector, &cursor);

	// gather the list of concepts, to limit the search
	int conceptList[numberOfConcepts];
	for(i = 0; i < numberOfConcepts; i++) {
		conceptList[i] = read_int(searchVector, &cursor);
		//printf("c_%d\n", conceptList[i]);
		read_float(searchVector, &cursor);
		//printf("--\n");
	}

	// sort them to make the search efficient
	quickSort(conceptList, 0, numberOfConcepts - 2);
	
	int fdC, fdA, fdAA, fdNdx, fdMem;

	long cSize;
	int* cmap = (int*) mapFile(fn_2index_cmap, 
					&fdC, &cSize, sizeof(int));
	
	long aSize;
	int* amap = (int*) mapFile(fn_vectors_ndx, 
					&fdA, &aSize, sizeof(int));

	long aaSize;
	char* a = mapFile(fn_vectors, 
					&fdAA, &aaSize, 1);
	
	long ndxSize;
	int* memNdx = (int*) mapFile(fn_2index_ndx, 
					&fdNdx, &ndxSize, sizeof(int));
	
	long memSize;
	int* mem = (int*) mapFile(fn_2index, 
					&fdMem, &memSize, sizeof(int));

	printf("%ld %ld %ld %ld %ld\n", aSize, cSize, aaSize, ndxSize, memSize);
	printf("%p %p %p %p %p\n", cmap, amap, a, memNdx, mem);
	//printf("c%d a%d n%d m%d\n", cSize, aSize, ndxSize, memSize); 

	//for(i = 0; i < 4000000; i++) printf("a:%d\n", amap[i]);
	//
	
	// create a bit-array of what we've searched, so that we don't search two times
	// for the same thing
	BitArrayDynamic(history, cSize); //4000000);

	// initialize result
	int resultSize = 1000;
	int resultCursor = sizeof(int);
	const int resultItemSize = VECTOR_ITEM_SIZE;
	char* result = calloc(1, resultSize);

	// filter the articles according to the interesting concepts
	const int vectorSize = aaSize / aSize; //804;
	printf("vector size: %d\n", vectorSize);
	long cNdx = 0;
	int resCntr = 0;
	for(i = 0; i < numberOfConcepts; i++) {
		// get the index of the concept[i]
		cNdx = scan(0, cSize, conceptList[i], cmap);
		if(cNdx < 0)
			continue;
		
		// iterate through the list of articles of this concept
		int ndx = memNdx[cNdx];
		long ascan = 0;
		while(mem[ndx] >= 0) {
			// find the article vector for this article
			ascan = scan(0, aSize, mem[ndx], amap);
			long aid = ascan;
			
			//printf("aid: %ld %ld\n",ascan, aSize);
			if(GET_BIT_DO(history, ascan) == 0) {
				//printf("%d\n", resultCursor);
				if(resultCursor >= resultSize - resultItemSize)
					result = grow(result, &resultSize, resultItemSize);
				
				float score = cosine_simil(a + vectorSize * aid, searchVector);
				//printf("score %f\n", score);

				result_write_int(mem[ndx], result, &resultCursor, resultItemSize);
				result_write_float(score, result, &resultCursor, resultItemSize);
				
				SET_BIT_DO(history, ascan, 1);
				resCntr++;
			} else {
				//printf("double l%d\n", aid);
			}
			ndx++;
		}
	}

	// tidy up
	munmap(cmap, cSize * sizeof(int));
	munmap(amap, aSize * sizeof(int));
	munmap(a, aaSize);
	munmap(memNdx, ndxSize * sizeof(int));
	munmap(mem, memSize * sizeof(int));
	
	close(fdC);
	close(fdA);
	close(fdNdx);
	close(fdAA);
	close(fdMem);

	free(history);
	
	// write the count of the items
	resultCursor = 0;
	result_write_int(min(max_results, resCntr), result, &resultCursor, resultItemSize);
	//result_write_int(resCntr, result, &resultCursor, resultItemSize);
	
	// align the allocation
	result = realloc(result, resCntr * resultItemSize + sizeof(int));

	qsort(result + sizeof(int), resCntr, sizeof(struct st_dimension), cmp_dimension);

	/*char* x = result + sizeof(int);
	int cursx = 0;
	for(i = 0; i < min(1000, resCntr); i++) {
		int concept_id = read_int(x, &cursx);
		float score = read_float(x, &cursx);
		//fprintf(stderr, "%d - %f\n", concept_id, score);
	}*/
	
	// return result
	*len = min(max_results, resCntr) * resultItemSize + sizeof(int); // resCntr * resultItemSize + sizeof(int);
	return result;
}

char* get_hex_result(char* vector, int v_size) {
	char* res = malloc(v_size * 2 + 1);
	printf("vsize %d\n", v_size);
	int i, n;
	for(i = 0; i < v_size; i++) {
		n = *((unsigned char*) (vector + i));
		res[i*2] = "0123456789ABCDEF"[((n >> 4) & 0xF)];
		res[i*2 + 1] = "0123456789ABCDEF"[((n) & 0xF)];
	}
	res[v_size] = '\0';
	
	return res;
	
}

char* vector_to_hex(char* vector) {
	int cursor = 0;
	int v_size = read_int(vector, &cursor);
	v_size = ((v_size)*(sizeof(int)+sizeof(float)) + sizeof(int));

	return get_hex_result(vector, v_size);

}

#define MAX_PATH_LEN 256
int main(int argc, char* argv[]) {
	if(argc != 3) {
		printf("Wrong # of arguments! Ending.\n");
		return 1;
	}

	// we expect the first argument to be the search vector
	int pos = 0;
	int cntr = 0;
	int c;
	char* searchVector = malloc(strlen(argv[1]) / 2);
	while((c = readHex(argv[1], &pos)) >= 0) {
		searchVector[cntr] = c;
		//printf("c %c%c %d\n", argv[1][pos - 2], argv[1][pos-1], c);
		cntr++;
	}

	char fn_2index[MAX_PATH_LEN];
	char fn_2index_cmap[MAX_PATH_LEN];
	char fn_2index_ndx[MAX_PATH_LEN];
	char fn_vectors[MAX_PATH_LEN];
	char fn_vectors_ndx[MAX_PATH_LEN];

	char* path = argv[2];

	sprintf(fn_2index, "%s/2index", path);
	sprintf(fn_2index_cmap, "%s/2index.cmap", path);
	sprintf(fn_2index_ndx, "%s/2index.ndx", path);
	sprintf(fn_vectors, "%s/vectors", path);
	sprintf(fn_vectors_ndx, "%s/vectors.ndx", path);
	
	int len;
	unsigned char * vector = 
		esa_vector_search(searchVector, &len,
				  fn_2index,
				  fn_2index_cmap,
				  fn_2index_ndx,
				  fn_vectors,
				  fn_vectors_ndx, 1000);

	char* hexvector = vector_to_hex(vector);
	printf("%s", hexvector);
	printf("\n");
	free(vector);
	free(hexvector);
	free(searchVector);
}
