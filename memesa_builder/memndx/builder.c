#include <stdio.h>
#include <stdlib.h>


int main(int argc, char* argv[]) {
	if(argc != 7) {
		printf("Wrong number of args. Exiting!\n");
		exit(1);
	}
		
	// open indexindex file
	FILE* f = fopen(argv[1], "r");
	FILE* fw = fopen(argv[2], "w");
	FILE* fndx = fopen(argv[3], "w");
	FILE* fcmap = fopen(argv[4], "w");
	
	int concept_count = -1;
	if(sscanf(argv[5], "%d", &concept_count) != 1) {
		printf("Wrong number of concepts, exitting.\n");
	}

	int vector_size = -1;
	if(sscanf(argv[6], "%d", &vector_size) != 1) {
		printf("Wrong vector size, exitting.\n");
	}
	
	// allocate memory
	size_t mem_size = concept_count * vector_size * sizeof(int) +
		concept_count * sizeof(int);
	int* mem = malloc(mem_size);

	size_t memndx_size = sizeof(int) * concept_count;
	int* memNdx = malloc(memndx_size);

	size_t concept_size = sizeof(int) * concept_count;
	int* conceptMap = malloc(concept_size);

	// process
	int memptr = 0;
	int c_id, a_id;
	int curr_c = -1;
	int c_cntr = 0;
	while(fscanf(f, "%d %d", &c_id, &a_id) > 0) {
		if(curr_c != c_id) {
			curr_c = c_id;
			
			// if we are not writing about the first concept = initialization,
			// write the barrier
			if(c_cntr > 0) {
				*(mem + memptr) = -1;
				memptr += 1;
			}
			
			*(memNdx + c_cntr) = memptr;
			*(conceptMap + c_cntr) = curr_c;

			c_cntr++;

		}
		*(mem + memptr) = a_id;
		memptr += 1;
	}

	size_t cmap_out = fwrite(conceptMap, sizeof(int), concept_size, fcmap);
	fclose(fcmap);

	size_t memndx_out = fwrite(memNdx, sizeof(int), memndx_size, fndx);
	fclose(fndx);

	size_t mem_out = fwrite(mem, sizeof(int), mem_size, fw);
	fclose(fw);

	printf("Sizes: 2index = %ld, 2index.ndx = %ld, 2index.cmap = %ld\n", 
	       (long) mem_out, (long) memndx_out, (long) cmap_out);

	fclose(f);
}
