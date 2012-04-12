#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "esabox.h"

#define TERM_MAX_LENGTH 200
#define TERM_MAX_DOCS 100000



int main(int argc, char* argv[]) {
    if(argc != 2) {
	printf("Wrong number of arguments!\n");
    }
    // load the text file
    // go trhough the file and build vectors for each term
    // maybe try to do some prunning
    FILE* f = fopen(argv[1], "rb");

    int term_id, doc_id;
    float score;
    char term[TERM_MAX_LENGTH];
    char curr_term[TERM_MAX_LENGTH] = "";

    vector_t* curr_vector = grow_vector(NULL);
    int doc_cntr = 0;

    int cntr = 0;

    while(fscanf(f, "%d %s %d %f", &term_id, term, &doc_id, &score) > 0 || cntr > 1000) {
	printf("%d %d\n", curr_vector->length, doc_cntr);
	// if we are nearning the boundary, so grow the vector
	if(curr_vector->length + 1 == doc_cntr) 
	    curr_vector = grow_vector(curr_vector);
	
	// save the values to the current vector
	printf("%p %p\n", curr_vector->items, &(curr_vector->items[doc_cntr]));
	curr_vector->items[doc_cntr].id = doc_id;
	curr_vector->items[doc_cntr].score = score;
	
	// if the term has changed, save the current term vector and start a new one
	if(strcmp(term, curr_term) != 0) {
	    strncpy(curr_term, term, TERM_MAX_LENGTH);
	    doc_cntr = 0;	    
	}
	cntr++;
	doc_cntr++;
    }
    
    // for each line:
    //   read line - scanf(id, term, doc_id, score)
    //   if term has changed:
    //     save current vector & start a new one
    // 
}
