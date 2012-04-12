#define VECTOR_ITEM_SIZE sizeof(int) + sizeof(float)

char* esa_vector_search(char* searchVector, int * len,
			char* fn_2index,
			char* fn_2index_cmap,
			char* fn_2index_ndx,
			char* fn_vectors,
			char* fn_vectors_ndx,
			int max_results);
char* vector_to_hex(char* vector);
